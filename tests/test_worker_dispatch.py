import asyncio
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from worker_dispatch import ManagementDispatcher


class ManagementDispatcherTest(unittest.IsolatedAsyncioTestCase):
    async def test_status_responds_while_sync_waits_and_sync_is_not_replayed(self):
        started = asyncio.Event()
        release = asyncio.Event()
        status_ready = asyncio.Event()
        results = []
        calls = []

        async def sync(params):
            calls.append(params)
            started.set()
            await release.wait()
            return {"success": True}

        async def status(params):
            return {"total_tokens": 1}

        def send(message):
            results.append(message)
            if message["id"] == "status":
                status_ready.set()

        dispatcher = ManagementDispatcher(SimpleNamespace(
            handle_extension_sync=sync, handle_status=status, send_line=send,
        ))
        dispatcher.submit("sync", "extension_sync", {"synthetic": True})
        await asyncio.wait_for(started.wait(), 1)
        dispatcher.submit("status", "status", {})
        await asyncio.wait_for(status_ready.wait(), 1)
        self.assertEqual(results, [{"id": "status", "done": True, "result": {"total_tokens": 1}}])
        self.assertEqual(len(calls), 1)
        release.set()
        await asyncio.gather(*list(dispatcher.tasks))
        self.assertEqual(results[-1]["id"], "sync")
        await dispatcher.shutdown()

    async def test_real_control_dispatch_streams_delta_done_and_one_receipt_while_mutation_waits(self):
        # Exercise the command selected by worker.main, not just the handler.
        saved_stdout = sys.stdout
        try:
            from worker import GFlowService
        finally:
            sys.stdout = saved_stdout
        service = GFlowService.__new__(GFlowService)
        result = {
            "id": "control-fixture", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "capabilities"},
                         "finish_reason": "stop"}],
        }
        service.handle_project_control = AsyncMock(return_value=result)
        service._handle_chat_completion = AsyncMock()
        events = []
        service.send_line = events.append
        dispatcher = ManagementDispatcher(service)
        params = {"model": "gflow-project-control", "stream": True}
        async with dispatcher.mutations:
            dispatcher.submit("probe", "project_control", params)
            await asyncio.wait_for(asyncio.gather(*list(dispatcher.tasks)), 1)
        self.assertEqual(len(events), 3)
        chunk = json.loads(events[0]["chunk"].removeprefix("data: ").strip())
        self.assertEqual(chunk["object"], "chat.completion.chunk")
        self.assertEqual(chunk["choices"][0]["delta"], {"role": "assistant", "content": "capabilities"})
        self.assertEqual(events[1], {"id": "probe", "stream": True, "chunk": "data: [DONE]\n\n"})
        self.assertEqual(events[2], {"id": "probe", "done": True, "result": result})
        self.assertEqual(sum(event.get("done") is True for event in events), 1)
        self.assertTrue(all(event["id"] == "probe" for event in events))
        service.handle_project_control.assert_awaited_once_with({**params, "stream": False})
        service._handle_chat_completion.assert_not_called()
        await dispatcher.shutdown()

    async def test_stream_control_runs_real_protocol_parser_without_mutating_input(self):
        saved_stdout = sys.stdout
        try:
            from worker import GFlowService
        finally:
            sys.stdout = saved_stdout
        from project_control import parse_control_request
        service = GFlowService.__new__(GFlowService)
        service.initialize = AsyncMock()
        payload = {"version": 1, "op": "capabilities",
                   "workspaceId": "00000000-0000-4000-8000-000000000001"}
        service.project_control = SimpleNamespace(
            instance_id="fixture-instance", handle=AsyncMock(return_value={"accepted": True}))
        service._handle_chat_completion = AsyncMock()
        events = []
        service.send_line = events.append
        dispatcher = ManagementDispatcher(service)
        params = {"model": "gflow-project-control", "stream": True,
                  "messages": [{"role": "user", "content": json.dumps(payload)}]}
        dispatcher.submit("probe", "project_control", params)
        await asyncio.wait_for(asyncio.gather(*list(dispatcher.tasks)), 1)
        chunk = json.loads(events[0]["chunk"].removeprefix("data: ").strip())
        self.assertEqual(json.loads(chunk["choices"][0]["delta"]["content"]), {"accepted": True})
        service.project_control.handle.assert_awaited_once_with(
            parse_control_request({**params, "stream": False}))
        self.assertIs(params["stream"], True)
        self.assertEqual(sum(event.get("done") is True for event in events), 1)
        service._handle_chat_completion.assert_not_called()
        await dispatcher.shutdown()

    async def test_nonstream_control_dispatch_keeps_single_completion_receipt(self):
        result = {"choices": [{"message": {"content": "capabilities"}}]}
        service = SimpleNamespace(
            handle_project_control=AsyncMock(return_value=result),
            handle_chat_completion=AsyncMock(), send_line=lambda message: events.append(message),
        )
        events = []
        dispatcher = ManagementDispatcher(service)
        params = {"model": "gflow-project-control", "stream": False}
        dispatcher.submit("probe", "project_control", params)
        await asyncio.wait_for(asyncio.gather(*list(dispatcher.tasks)), 1)
        self.assertEqual(events, [{"id": "probe", "done": True, "result": result}])
        service.handle_project_control.assert_awaited_once_with(params)
        service.handle_chat_completion.assert_not_called()
        await dispatcher.shutdown()

    async def test_mutating_requests_stay_serialized(self):
        started = asyncio.Event()
        release = asyncio.Event()
        order = []

        async def update(params):
            order.append(params["index"])
            started.set()
            await release.wait()
            return {"success": True}

        dispatcher = ManagementDispatcher(SimpleNamespace(handle_update_config=update, send_line=lambda message: None))
        dispatcher.submit("first", "update_config", {"index": 1})
        await asyncio.wait_for(started.wait(), 1)
        dispatcher.submit("second", "update_config", {"index": 2})
        await asyncio.sleep(0)
        self.assertEqual(order, [1])
        release.set()
        await asyncio.gather(*list(dispatcher.tasks))
        self.assertEqual(order, [1, 2])
        await dispatcher.shutdown()

    async def test_shutdown_reports_unknown_result_without_restarting_mutation(self):
        started = asyncio.Event()
        results = []

        async def sync(params):
            started.set()
            await asyncio.Event().wait()

        dispatcher = ManagementDispatcher(SimpleNamespace(handle_extension_sync=sync, send_line=results.append))
        dispatcher.submit("sync", "extension_sync", {})
        await asyncio.wait_for(started.wait(), 1)
        await dispatcher.shutdown()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["error"]["code"], "worker_stopped")
        self.assertFalse(dispatcher.tasks)


if __name__ == "__main__":
    unittest.main()
