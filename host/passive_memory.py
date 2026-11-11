"""Host RPC for MaiBot v1.4.3. Only the matching plugin can invoke it."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime

from sqlalchemy import or_
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
SELF_REPORT = re.compile(
    r"^(?:我|本人)\s*((?:(?:平时|一直|经常|最|很|特别|比较|不)\s*)?"
    r"(?:喜欢|爱好|擅长|习惯|常玩|常用|爱吃|爱喝|爱玩|爱看|爱听|爱读|爱收集)"
    r"\s*[^，,。！？!?；;：:“”\"\n\r]{1,100})[。！!]?$"
)


def _cursor(row):
    return [row.received_at.isoformat(timespec="microseconds"), row.id]


def _position(value):
    if not isinstance(value, list) or len(value) != 2 or type(value[1]) is not int or value[1] < 1:
        raise ValueError("消息进度无效")
    received_at = datetime.fromisoformat(value[0])
    if received_at.tzinfo is not None:
        raise ValueError("消息进度必须使用宿主的接收时间")
    return received_at, value[1]


def _scope(stream):
    return select(Messages).where(
        Messages.session_id == stream, Messages.platform == "qq",
        Messages.group_id.is_not(None), Messages.group_id != "",
        Messages.received_at.is_not(None),
    )


def _tails(streams):
    result = {}
    with Session(engine) as session:
        for stream in streams:
            row = session.exec(_scope(stream).order_by(Messages.id.desc()).limit(1)).first()
            result[stream] = _cursor(row) if row else None
    return result


def _legacy_cursor(stream, row_id):
    with Session(engine) as session:
        row = session.exec(_scope(stream).where(Messages.id == row_id)).first()
        return _cursor(row) if row else None


def _rows(stream, after, through):
    query = _scope(stream)
    earlier = aliased(Messages)
    duplicate = select(earlier.id).where(
        earlier.session_id == Messages.session_id,
        earlier.platform == Messages.platform,
        earlier.message_id == Messages.message_id,
        earlier.received_at.is_not(None),
        earlier.id < Messages.id,
    ).exists()
    query = query.where(~duplicate)
    with Session(engine) as session:
        if after is not None:
            received_at, row_id = _position(after)
            anchor = session.exec(_scope(stream).where(
                Messages.id == row_id, Messages.received_at == received_at,
            )).first()
            # Ordinary scans use insertion order. Fall back to receipt time only if
            # the last row disappeared or its SQLite ID now belongs to another row.
            newer = Messages.id > row_id
            if anchor is None:
                newer = or_(newer, Messages.received_at > received_at)
            query = query.where(newer)
        if through is not None:
            _, row_id = _position(through)
            query = query.where(Messages.id <= row_id)
        return list(session.exec(query.order_by(Messages.id).limit(BATCH_SIZE + 1)))


def _eligible(rows):
    return {
        row.message_id: row for row in rows
        if row.message_id and row.user_id and (row.processed_plain_text or "").strip()
        and not (row.is_emoji or row.is_picture or row.is_notify or row.is_command)
        and not is_bot_self("qq", row.user_id)
    }


def _self_report(text):
    match = SELF_REPORT.fullmatch(text.strip())
    if not match or re.search(r"据说|听说|说过|开玩笑|假如|如果|但是|不过|并且|而且|然后|他说|她说|他们说|吗$|么$|吧$", match[1]):
        return None
    return match[1].strip()


def _snapshot(messages):
    source = [(_cursor(row), key, row.user_id, row.processed_plain_text) for key, row in messages.items()]
    return hashlib.sha256(json.dumps(source, ensure_ascii=False).encode()).hexdigest()


def _require_write(result):
    if not result.success or result.detail == "chat_filtered":
        raise RuntimeError(result.detail or "A_Memorix 写入失败")


def _valid_stream(value):
    return isinstance(value, str) and 0 < len(value) <= 255


async def handle(plugin_id, capability, args):
    if plugin_id != "endedge.passive-memory":
        raise PermissionError("此能力仅供静默群聊记忆插件使用")
    action = args["action"]
    if action == "tails":
        streams = args["streams"]
        if not isinstance(streams, list) or len(streams) > 1000 or not all(map(_valid_stream, streams)):
            raise ValueError("聊天 ID 无效")
        return await asyncio.to_thread(_tails, streams)

    stream = args["stream"]
    if not _valid_stream(stream):
        raise ValueError("聊天 ID 无效")
    if action == "migrate":
        old_cursor = args["after"]
        if type(old_cursor) is not int or old_cursor < 0:
            raise ValueError("旧消息进度无效")
        return {"cursor": await asyncio.to_thread(_legacy_cursor, stream, old_cursor)}
    if action not in {"read", "write"}:
        raise ValueError("未知操作")

    after = args["after"]
    through = args["through"] if action == "write" else None
    if action == "write" and (through is None or through == after):
        raise ValueError("批次结束位置无效")

    rows = await asyncio.to_thread(_rows, stream, after, through)
    if not rows:
        return {"stale": True} if action == "write" else {"cursor": after, "messages": [], "more": False}
    window = rows[:BATCH_SIZE]
    cursor = _cursor(window[-1])
    group = str(window[-1].group_id)
    config = global_config.a_memorix
    allowed = MemoryChatFilterService._chat_filter_config_allows(
        config.filter.model_dump(), stream_id=stream, group_id=group,
    )
    if not config.plugin.enabled or not allowed:
        if action == "write":
            raise RuntimeError("当前群未启用长期记忆写入")
        return {"cursor": after, "messages": [], "more": False, "filtered": True}

    messages = _eligible(window)
    snapshot = _snapshot(messages)
    if action == "read":
        return {"cursor": cursor, "snapshot": snapshot, "more": len(rows) > BATCH_SIZE, "messages": [
            {"id": key, "user": row.user_id, "name": row.user_cardname or row.user_nickname,
             "text": row.processed_plain_text.strip()[:1000],
             "self_report": bool(_self_report(row.processed_plain_text))}
            for key, row in messages.items()
        ]}
    if cursor != through or snapshot != args["snapshot"]:
        return {"stale": True}

    summary, facts = args["summary"], args["facts"]
    if not isinstance(summary, str) or len(summary) > 1800 or not isinstance(facts, list) or len(facts) > 12:
        raise ValueError("摘要或人物事实超出限制")

    if summary.strip():
        if not messages:
            raise ValueError("摘要没有来源消息")
        timestamps = [row.timestamp.timestamp() for row in messages.values()]
        _require_write(await memory_service.ingest_summary(
            external_id=f"passive_summary:{stream}:{snapshot}",
            chat_id=stream, group_id=group, text=summary.strip(),
            time_start=min(timestamps), time_end=max(timestamps),
            metadata={"writeback_source": "passive_memory", "evidence_message_ids": list(messages)},
        ))

    for key in dict.fromkeys(key for key in facts if isinstance(key, str)):
        source = messages.get(key)
        content = _self_report(source.processed_plain_text) if source else None
        if not content:
            continue
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
                "evidence_message_ids": [key],
                "fact_claim": {"authority": "direct_user", "stability": "stable", "profile_section": "stable_facts"},
            },
        ))
    return {"cursor": cursor}
