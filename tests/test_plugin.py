"""Focused regressions for quiet and interrupted batches."""
import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, PropertyMock, patch

from plugin.plugin import PassiveMemory, _parse


class BatchTests(unittest.IsolatedAsyncioTestCase):
    def make_plugin(self):
        plugin = PassiveMemory()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        plugin._path = Path(folder.name) / "state.json"
        plugin._state = {"group": {"cursor": 7}}
        plugin._lock = asyncio.Lock()
        return plugin

    async def test_idle_scan_does_not_invoke_generation_or_advance(self):
        plugin = self.make_plugin()
        plugin._host = AsyncMock(return_value={"cursor": 7, "messages": []})
        await plugin._process("group")
        await plugin._process("group")
        self.assertEqual(plugin._state, {"group": {"cursor": 7}})
        self.assertEqual([call.args[0] for call in plugin._host.await_args_list], ["read", "read"])

    async def test_failed_write_retries_saved_output_without_generating_again(self):
        plugin = self.make_plugin()
        plugin._host = AsyncMock(side_effect=[
            {"cursor": 8, "messages": [{"id": "m8", "user": "1", "text": "I collect stamps"}]},
            RuntimeError("write failed"),
            {"cursor": 8},
            {"cursor": 8, "messages": []},
        ])
        generate = AsyncMock(return_value={"success": True, "response": '{"summary":"摘要","facts":[]}'})
        with (
            patch.object(PassiveMemory, "ctx", new_callable=PropertyMock, return_value=SimpleNamespace(call_capability=generate)),
            patch.object(PassiveMemory, "config", new_callable=PropertyMock, return_value=SimpleNamespace(model_name="")),
        ):
            with self.assertRaisesRegex(RuntimeError, "write failed"):
                await plugin._process("group")
            self.assertEqual(plugin._state["group"]["cursor"], 7)
            self.assertTrue(plugin._path.exists())
            await plugin._process("group")
            await plugin._process("group")
        generate.assert_awaited_once()
        self.assertEqual(plugin._host.await_args_list[1], plugin._host.await_args_list[2])
        self.assertEqual(plugin._state, {"group": {"cursor": 8}})

    def test_fact_requires_a_single_evidence_author(self):
        result = _parse(
            '{"summary":"","facts":[{"text":"likes tea","evidence":["a","b"]},'
            '{"text":"likes bread","evidence":["b"]}]}',
            [{"id": "a", "user": "1"}, {"id": "b", "user": "2"}],
        )
        self.assertEqual(result["facts"], [{"text": "likes bread", "evidence": ["b"]}])


if __name__ == "__main__":
    unittest.main()
