"""Regressions for silent processing, retries, and chat selection."""
import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, PropertyMock, patch

from plugin.plugin import MAX_BATCHES_PER_CYCLE, PassiveMemory, _parse

BEFORE = ["2026-01-01T12:00:00.000000", 7]
AFTER = ["2026-01-01T12:00:02.000000", 8]
MESSAGES = [{"id": "m8", "user": "1", "text": "我喜欢集邮", "self_report": True}]
BATCH = {"cursor": AFTER, "messages": MESSAGES, "snapshot": "batch-key", "more": False}


class BatchTests(unittest.IsolatedAsyncioTestCase):
    def make_plugin(self):
        plugin = PassiveMemory()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        plugin._path = Path(folder.name) / "state.json"
        plugin._state = {"group": {"cursor": BEFORE}}
        plugin._selection_lock = asyncio.Lock()
        plugin._errors = set()
        return plugin

    async def test_idle_scan_does_not_invoke_generation_or_advance(self):
        plugin = self.make_plugin()
        plugin._host = AsyncMock(return_value={"cursor": BEFORE, "messages": [], "more": False})
        await plugin._process("group")
        await plugin._process("group")
        self.assertEqual(plugin._state, {"group": {"cursor": BEFORE}})
        self.assertEqual([call.args[0] for call in plugin._host.await_args_list], ["read", "read"])

    async def test_failed_write_retries_saved_output_without_generating_again(self):
        plugin = self.make_plugin()
        plugin._host = AsyncMock(side_effect=[
            BATCH, RuntimeError("write failed"), {"cursor": AFTER},
            {"cursor": AFTER, "messages": [], "more": False},
        ])
        generate = AsyncMock(return_value={"success": True, "response": '{"summary":"摘要","facts":["m8"]}'})
        with (
            patch.object(PassiveMemory, "ctx", new_callable=PropertyMock, return_value=SimpleNamespace(call_capability=generate)),
            patch.object(PassiveMemory, "config", new_callable=PropertyMock, return_value=SimpleNamespace(model_name="")),
        ):
            with self.assertRaisesRegex(RuntimeError, "write failed"):
                await plugin._process("group")
            self.assertEqual(plugin._state["group"]["cursor"], BEFORE)
            self.assertTrue(plugin._path.exists())
            await plugin._process("group")
            await plugin._process("group")
        generate.assert_awaited_once()
        self.assertEqual(plugin._host.await_args_list[1], plugin._host.await_args_list[2])
        self.assertEqual(plugin._state, {"group": {"cursor": AFTER}})

    async def test_deleted_evidence_discards_pending_and_reads_from_last_commit(self):
        plugin = self.make_plugin()
        plugin._state["group"]["pending"] = {
            "through": AFTER, "snapshot": "old", "more": False, "summary": "旧摘要", "facts": ["m8"],
        }
        plugin._host = AsyncMock(side_effect=[
            {"stale": True}, {"cursor": BEFORE, "messages": [], "more": False},
        ])
        self.assertTrue(await plugin._process("group"))
        self.assertFalse(await plugin._process("group"))
        self.assertEqual(plugin._host.await_args.kwargs["after"], BEFORE)
        self.assertEqual(plugin._state, {"group": {"cursor": BEFORE}})

    async def test_deselection_finishes_while_host_write_is_pending(self):
        plugin = self.make_plugin()
        plugin._state["group"]["pending"] = {
            "through": AFTER, "snapshot": "pending", "more": True, "summary": "摘要", "facts": [],
        }
        started = asyncio.Event()
        finish = asyncio.Event()

        async def slow_write(*args, **kwargs):
            started.set()
            await finish.wait()
            return {"cursor": AFTER}

        plugin._host = AsyncMock(side_effect=slow_write)
        plugin._groups = AsyncMock(return_value=[{"session_id": "group", "group_id": "1"}])
        processing = asyncio.create_task(plugin._process("group"))
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
            self.assertEqual(await asyncio.wait_for(plugin.save_groups([]), timeout=2), {"count": 0})
        finally:
            finish.set()
            await processing
        self.assertFalse(processing.result())
        self.assertEqual(plugin._state, {})
        plugin._host.assert_awaited_once()

    async def test_cycle_drains_backlog_but_yields_between_groups(self):
        plugin = self.make_plugin()
        plugin._state["quiet"] = {"cursor": BEFORE}

        async def process(stream):
            return stream == "group"

        plugin._process = AsyncMock(side_effect=process)
        await plugin._cycle()
        self.assertEqual(
            [call.args[0] for call in plugin._process.await_args_list],
            ["group", "quiet"] + ["group"] * (MAX_BATCHES_PER_CYCLE - 1),
        )

    def test_model_can_only_choose_valid_self_reports(self):
        result = _parse(
            '{"summary":"","facts":["a","b","b","unknown",{"text":"fabricated"}]}',
            [{"id": "a", "self_report": False}, {"id": "b", "self_report": True}],
        )
        self.assertEqual(result["facts"], ["b"])


if __name__ == "__main__":
    unittest.main()
