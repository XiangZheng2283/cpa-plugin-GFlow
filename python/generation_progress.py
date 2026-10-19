"""Request-local, non-media stage progress for the plugin streaming boundary."""
from contextlib import contextmanager
from contextvars import ContextVar
import json
import time

# These are process milestones, not a timed estimate of generated pixels.
# The upstream video_polling percentage is based on polling attempts; keep
# that milestone fixed rather than presenting elapsed time as real progress.
STAGE_PERCENT = {
    "started": 0, "token_selected": 8, "token_ready": 15,
    "project_ready": 22, "preparing_video": 24, "uploading_images": 28,
    "submitting_image": 28, "solving_image_captcha": 38,
    "video_submitted": 45, "video_polling": 45, "image_generated": 72,
    "upsampling_2k": 82, "upsampling_4k": 82, "caching_image": 90,
    "caching_video": 92, "completed": 100,
}
CURRENT_PROGRESS = ContextVar("gflow_generation_progress", default=None)


@contextmanager
def progress_scope(emit=None):
    state = {"latest": None, "emit": emit}
    token = CURRENT_PROGRESS.set(state)
    try:
        yield state
    finally:
        # Child tasks may retain their copied ContextVar; late callbacks must
        # not emit after this request has returned or been cancelled.
        state["emit"] = None
        CURRENT_PROGRESS.reset(token)


def record_stage(stage, progress=None):
    state = CURRENT_PROGRESS.get()
    if state is None or stage not in STAGE_PERCENT:
        return
    percent = STAGE_PERCENT[stage]
    if stage != "video_polling" and type(progress) is int:
        percent = max(0, min(100, progress))
    previous = state["latest"]
    if previous:
        percent = max(percent, previous["percent"])
    value = {"stage": stage, "percent": percent, "basis": "stage"}
    if previous == value:
        return
    state["latest"] = value
    emit = state["emit"]
    if emit:
        emit(progress_chunk(value))


def progress_chunk(value):
    # Keep an ordinary choice/delta for hosts that require a valid OpenAI
    # chunk. The extension is also in delta for same-format translators.
    payload = {
        "id": "chatcmpl-gflow-progress",
        "object": "chat.completion.chunk",
        "created": int(time.time()), "model": "flow2api",
        "gflow_progress": value,
        "choices": [{"index": 0, "delta": {"content": "", "gflow_progress": value},
                     "finish_reason": None}],
    }
    return "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"


def attach_progress(chunk):
    state = CURRENT_PROGRESS.get()
    if not state or not state["latest"] or not chunk.startswith("data: "):
        return chunk
    try:
        payload = json.loads(chunk[6:].strip())
    except (ValueError, TypeError):
        return chunk
    if not isinstance(payload, dict):
        return chunk
    value = dict(state["latest"])
    payload["gflow_progress"] = value
    choices = payload.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        delta = choices[0].get("delta")
        if isinstance(delta, dict):
            delta["gflow_progress"] = value
    return "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"


class GenerationProgressMixin:
    """Wrap the upstream handler without changing its source or retries."""

    async def _update_request_log_progress(self, *args, **kwargs):
        result = await super()._update_request_log_progress(*args, **kwargs)
        record_stage(kwargs.get("status_text"), kwargs.get("progress"))
        return result

    async def _log_request(self, *args, **kwargs):
        result = await super()._log_request(*args, **kwargs)
        # Upstream completes the request through _log_request, rather than
        # _update_request_log_progress. Failures retain the last known stage.
        record_stage(kwargs.get("status_text"), kwargs.get("progress"))
        return result

    def _create_stream_chunk(self, *args, **kwargs):
        return attach_progress(super()._create_stream_chunk(*args, **kwargs))
