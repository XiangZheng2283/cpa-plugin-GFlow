"""Synthetic stage-progress boundary tests; never call Google media APIs."""
import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "python"), str(ROOT / "python/flow2api")]
from generation_progress import (
    CURRENT_PROGRESS, GenerationProgressMixin, attach_progress,
    progress_scope, record_stage,
)


class UpstreamFixture:
    def __init__(self):
        self.calls = []

    async def _update_request_log_progress(self, *args, **kwargs):
        self.calls.append(("update", args, kwargs))
        return "upstream-update"

    async def _log_request(self, *args, **kwargs):
        self.calls.append(("log", args, kwargs))
        return "upstream-log"

    def _create_stream_chunk(self, content, role=None, finish_reason=None):
        delta = {"content" if finish_reason else "reasoning_content": content}
        if role:
            delta["role"] = role
        return "data: " + json.dumps({"choices": [
            {"index": 0, "delta": delta, "finish_reason": finish_reason}
        ]}) + "\n\n"


class Handler(GenerationProgressMixin, UpstreamFixture):
    pass


def decode(chunk):
    return json.loads(chunk[6:].strip())


class ProgressTest(unittest.IsolatedAsyncioTestCase):
    async def test_control_probe_streams_standard_completion_without_media(self):
        saved = sys.stdout
        try:
            from worker import GFlowService
        finally:
            sys.stdout = saved
        worker = GFlowService.__new__(GFlowService)
        worker.handle_project_control = AsyncMock(return_value={
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "capabilities"}, "finish_reason": "stop"}]})
        worker._handle_chat_completion = AsyncMock()
        events = []
        worker.send_line = events.append
        await worker.handle_chat_completion("probe", {"model": "gflow-project-control", "stream": True})
        self.assertEqual(decode(events[0]["chunk"])["choices"][0]["delta"]["content"], "capabilities")
        self.assertEqual(events[1]["chunk"], "data: [DONE]\n\n")
        self.assertTrue(events[-1]["done"])
        worker._handle_chat_completion.assert_not_called()
        worker.handle_project_control.assert_awaited_once_with(
            {"model": "gflow-project-control", "stream": False})

    async def test_callbacks_emit_stage_metadata_without_credentials_or_media_text(self):
        handler = Handler()
        events = []
        state = {"id": 12}
        with progress_scope(events.append):
            result = await handler._update_request_log_progress(
                state, token_id=42, status_text="submitting_image", progress=48,
                response_extra={"credential": "synthetic-secret"},
            )
            self.assertEqual(result, "upstream-update")
            self.assertEqual(len(handler.calls), 1)
            self.assertEqual(handler.calls[0][2]["token_id"], 42)
            event = decode(events[0])
            expected = {"stage": "submitting_image", "percent": 48, "basis": "stage"}
            self.assertEqual(event["gflow_progress"], expected)
            self.assertEqual(event["choices"][0]["delta"], {"content": "", "gflow_progress": expected})
            self.assertNotIn("synthetic-secret", events[0])
            self.assertEqual(event["choices"][0]["delta"]["content"], "")
            self.assertIsNone(event["choices"][0]["finish_reason"])
        self.assertIsNone(CURRENT_PROGRESS.get())

    async def test_existing_media_and_reasoning_chunks_keep_their_content(self):
        handler = Handler()
        with progress_scope():
            await handler._update_request_log_progress({}, status_text="caching_image", progress=90)
            for content, finish in [("uploading reference", None),
                                    ("![Generated Image](data:image/png;base64,c3ludGhldGlj)", "stop")]:
                event = decode(handler._create_stream_chunk(content, finish_reason=finish))
                key = "content" if finish else "reasoning_content"
                self.assertEqual(event["choices"][0]["delta"][key], content)
                self.assertEqual(event["choices"][0]["finish_reason"], finish)
                self.assertEqual(event["gflow_progress"]["percent"], 90)
                self.assertEqual(event["choices"][0]["delta"]["gflow_progress"], event["gflow_progress"])
            self.assertEqual(attach_progress("data: [DONE]\n\n"), "data: [DONE]\n\n")

    async def test_upstream_handler_wrapper_uses_actual_callbacks_without_modifying_request_log(self):
        from src.services.generation_handler import GenerationHandler
        class WrappedHandler(GenerationProgressMixin, GenerationHandler):
            pass
        handler = WrappedHandler.__new__(WrappedHandler)
        handler.db = type("DatabaseFixture", (), {"update_request_log": AsyncMock()})()
        events = []
        state = {"id": 5}
        with progress_scope(events.append):
            await handler._update_request_log_progress(
                state, token_id=8, status_text="submitting_image", progress=48,
            )
            self.assertEqual(handler.db.update_request_log.await_args.kwargs["progress"], 48)
            self.assertEqual(decode(events[-1])["gflow_progress"]["percent"], 48)
            media = decode(handler._create_stream_chunk("![Generated Image](https://example.invalid/test.png)", finish_reason="stop"))
            self.assertEqual(media["choices"][0]["delta"]["content"], "![Generated Image](https://example.invalid/test.png)")
            self.assertEqual(media["gflow_progress"]["stage"], "submitting_image")

    async def test_parallel_request_scopes_and_cancelled_callbacks_do_not_cross(self):
        handler = Handler()
        events = {"first": [], "second": []}
        async def run(name, stage, percent):
            with progress_scope(events[name].append):
                await asyncio.sleep(0)
                await handler._update_request_log_progress({}, status_text=stage, progress=percent)
                await asyncio.sleep(0)
                self.assertEqual(decode(handler._create_stream_chunk("same"))["gflow_progress"]["stage"], stage)
        await asyncio.gather(run("first", "project_ready", 22), run("second", "image_generated", 72))
        self.assertEqual(decode(events["first"][0])["gflow_progress"]["percent"], 22)
        self.assertEqual(decode(events["second"][0])["gflow_progress"]["percent"], 72)
        self.assertIsNone(CURRENT_PROGRESS.get())
        release = asyncio.Event()
        late = []
        async def child():
            await release.wait()
            record_stage("completed", 100)
        with progress_scope(late.append):
            task = asyncio.create_task(child())
        release.set()
        await task
        self.assertEqual(late, [])

    async def test_polling_attempts_do_not_fake_percent_or_repeat_unchanged_updates(self):
        events = []
        handler = Handler()
        with progress_scope(events.append):
            for percent in (45, 65, 95):
                await handler._update_request_log_progress({}, status_text="video_polling", progress=percent)
                await asyncio.sleep(0)
            self.assertEqual(len(events), 1)
            self.assertEqual(decode(events[0])["gflow_progress"]["percent"], 45)
            await handler._update_request_log_progress({}, status_text="untrusted-secret-stage", progress=96)
            self.assertEqual(len(events), 1)
            await handler._log_request(status_text="failed", progress=100)
            self.assertEqual(len(events), 1)
            await handler._log_request(status_text="completed", progress=100)
            self.assertEqual(decode(events[-1])["gflow_progress"]["percent"], 100)
            self.assertEqual(len(handler.calls), 6)

    async def test_worker_scopes_bind_stream_events_to_their_request_and_keep_nonstream_compatible(self):
        saved = sys.stdout
        try:
            from worker import GFlowService
        finally:
            sys.stdout = saved
        worker = GFlowService.__new__(GFlowService)
        worker.initialize = AsyncMock()
        events = []
        worker.send_line = events.append
        handler = Handler()
        async def pipeline(req_id, params):
            await handler._update_request_log_progress({}, status_text="project_ready", progress=22)
            await asyncio.sleep(0)
            await handler._update_request_log_progress({}, status_text="image_generated", progress=72)
            worker.send_line({"id": req_id, "done": True, "result": {"unchanged": True}})
        worker._handle_chat_completion = pipeline
        await asyncio.gather(
            worker.handle_chat_completion("one", {"model": "gflow-image", "stream": True}),
            worker.handle_chat_completion("two", {"model": "gflow-image", "stream": True}),
            worker.handle_chat_completion("three", {"model": "gflow-image", "stream": False}),
        )
        for req_id in ("one", "two"):
            own = [e for e in events if e["id"] == req_id]
            self.assertEqual([decode(e["chunk"])["gflow_progress"]["percent"] for e in own if e.get("stream")], [22, 72])
            self.assertEqual(own[-1]["result"], {"unchanged": True})
        self.assertEqual([e for e in events if e["id"] == "three"],
                         [{"id": "three", "done": True, "result": {"unchanged": True}}])
        self.assertIsNone(CURRENT_PROGRESS.get())


if __name__ == "__main__":
    unittest.main()
