"""Host RPC for MaiBot v1.4.3. Only the matching plugin can invoke it."""
from __future__ import annotations

import asyncio
import hashlib

from sqlalchemy.orm import aliased
from sqlmodel import Session, select

from src.A_memorix.core.runtime.services.chat_filter_service import MemoryChatFilterService
from src.chat.utils.utils import is_bot_self
from src.common.database.database import engine
from src.common.database.database_model import Messages
from src.config.config import global_config
from src.person_info.person_info import get_person_id
from src.services.memory_service import memory_service

BATCH_SIZE = 40


def _rows(stream, after, through=None, *, tail=False):
    query = select(Messages).where(
        Messages.session_id == stream, Messages.platform == "qq",
        Messages.group_id.is_not(None), Messages.group_id != "", Messages.id > after,
    )
    if not tail:
        earlier = aliased(Messages)
        duplicate = select(earlier.id).where(
            earlier.session_id == Messages.session_id,
            earlier.platform == Messages.platform,
            earlier.message_id == Messages.message_id,
            earlier.received_at.is_not(None),
            earlier.id < Messages.id,
        ).exists()
        query = query.where(Messages.received_at.is_not(None), ~duplicate)
    if through is not None:
        query = query.where(Messages.id <= through)
    with Session(engine) as session:
        if tail:
            row = session.exec(query.order_by(Messages.id.desc()).limit(1)).first()
            return row.id if row else 0
        return list(session.exec(query.order_by(Messages.id).limit(BATCH_SIZE)))


def _eligible(rows):
    unique = {}
    for row in rows:
        if (row.message_id and row.user_id and (row.processed_plain_text or "").strip()
                and not (row.is_emoji or row.is_picture or row.is_notify or row.is_command)
                and not is_bot_self("qq", row.user_id)):
            unique.setdefault(row.message_id, row)
    return unique


def _require_write(result):
    if not result.success or result.detail == "chat_filtered":
        raise RuntimeError(result.detail or "A_Memorix 写入失败")


async def handle(plugin_id, capability, args):
    if plugin_id != "endedge.passive-memory":
        raise PermissionError("此能力仅供静默群聊记忆插件使用")
    stream = args["stream"]
    if not isinstance(stream, str) or not 0 < len(stream) <= 255:
        raise ValueError("聊天 ID 无效")
    action = args["action"]
    if action == "tail":
        return {"cursor": await asyncio.to_thread(_rows, stream, 0, tail=True)}
    if action not in {"read", "write"}:
        raise ValueError("未知操作")
    after = args["after"]
    through = args["through"] if action == "write" else None
    if type(after) is not int or after < 0:
        raise ValueError("消息进度无效")
    if action == "write" and (type(through) is not int or through <= after):
        raise ValueError("批次结束位置无效")

    rows = await asyncio.to_thread(_rows, stream, after, through)
    if not rows and action == "read":
        return {"cursor": after, "messages": []}
    if not rows:
        raise ValueError("原始消息已不可用")
    cursor = rows[-1].id
    group = str(rows[-1].group_id)
    config = global_config.a_memorix
    allowed = MemoryChatFilterService._chat_filter_config_allows(
        config.filter.model_dump(), stream_id=stream, group_id=group,
    )
    if not config.plugin.enabled or not allowed:
        if action == "write":
            raise RuntimeError("当前群未启用长期记忆写入")
        return {"cursor": after, "messages": [], "filtered": True}

    messages = _eligible(rows)
    if action == "read":
        return {"cursor": cursor, "messages": [
            {"id": key, "user": row.user_id, "name": row.user_cardname or row.user_nickname,
             "text": row.processed_plain_text.strip()[:1000]}
            for key, row in messages.items()
        ]}
    if cursor != through:
        raise ValueError("原始消息窗口已变化")
    summary = args["summary"]
    facts = args["facts"]
    if not isinstance(summary, str) or len(summary) > 1800 or not isinstance(facts, list) or len(facts) > 12:
        raise ValueError("摘要或人物事实超出限制")
    validated = []
    for fact in facts:
        content, evidence = fact["text"], fact["evidence"]
        if not isinstance(content, str) or not content.strip() or len(content) > 240:
            raise ValueError("人物事实正文无效")
        if not isinstance(evidence, list) or not 1 <= len(evidence) <= 3:
            raise ValueError("缺少人物事实证据")
        sources = [messages[key] for key in evidence]
        source = sources[0]
        if any(row.user_id != source.user_id for row in sources):
            raise ValueError("人物事实必须来自同一位用户自己的发言")
        validated.append((content.strip(), evidence, source))

    if summary.strip():
        if not messages:
            raise ValueError("摘要没有来源消息")
        timestamps = [row.timestamp.timestamp() for row in messages.values()]
        _require_write(await memory_service.ingest_summary(
            external_id=f"passive_summary:{stream}:{after}:{cursor}",
            chat_id=stream, group_id=group, text=summary.strip(),
            time_start=min(timestamps), time_end=max(timestamps),
            metadata={"writeback_source": "passive_memory", "evidence_message_ids": list(messages)},
        ))

    for content, evidence, source in validated:
        person_id = get_person_id("qq", source.user_id)
        name = source.user_cardname or source.user_nickname
        fingerprint = hashlib.md5(f"{person_id}|{stream}|{content}".encode()).hexdigest()
        _require_write(await memory_service.ingest_text(
            external_id=f"person_fact:{person_id}:{fingerprint}",
            source_type="person_fact", text=content, chat_id=stream, group_id=group,
            person_ids=[person_id], participants=[name] if name else [],
            timestamp=source.timestamp.timestamp(), tags=["person_fact"],
            metadata={
                "platform": "qq", "person_id": person_id, "person_name": name,
                "writeback_source": "memory_flow_service", "evidence_source": "user_supported",
                "evidence_message_ids": evidence,
                "fact_claim": {"authority": "direct_user", "stability": "stable", "profile_section": "stable_facts"},
            },
        ))
    return {"cursor": cursor}
