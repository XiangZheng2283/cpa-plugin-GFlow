"""Request-local diagnostics for ordinary browser operations, without secrets.

This adapter only observes existing calls. It does not execute verification,
change retries, collect cookies, or expose exception text or page addresses.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

CURRENT_BROWSER_DIAGNOSTIC = ContextVar("gflow_browser_diagnostic", default=None)
STAGE_TEXT = {
    "browser_startup": "浏览器启动",
    "page_load": "浏览器页面加载",
    "browser_session": "浏览器连接",
}
REASON_TEXT = {
    "timeout": "超时",
    "not_found": "所需程序或文件不存在",
    "access_denied": "访问被拒绝",
    "disconnected": "连接已断开",
    "failed": "未完成",
}


@contextmanager
def browser_diagnostic_scope():
    state = {"failure": None, "sequence": 0}
    context = CURRENT_BROWSER_DIAGNOSTIC.set(state)
    try:
        yield state
    finally:
        # Child tasks can retain the ContextVar object after request completion.
        state.clear()
        CURRENT_BROWSER_DIAGNOSTIC.reset(context)


def record_browser_failure(stage, error):
    state = CURRENT_BROWSER_DIAGNOSTIC.get()
    if not state or stage not in STAGE_TEXT:
        return
    if isinstance(error, TimeoutError):
        reason = "timeout"
    elif isinstance(error, (FileNotFoundError, ImportError)):
        reason = "not_found"
    elif isinstance(error, PermissionError):
        reason = "access_denied"
    elif isinstance(error, (ConnectionError, BrokenPipeError, EOFError)):
        reason = "disconnected"
    else:
        reason = "failed"
    # Never keep exception strings, arguments, labels, URLs or traceback frames.
    state["failure"] = {"stage": stage, "reason": reason}
    state["sequence"] += 1


def _observe_method(cls, name, stage):
    original = getattr(cls, name, None)
    if not callable(original) or getattr(original, "_gflow_browser_diagnostic", False):
        return

    @wraps(original)
    async def observed(self, *args, **kwargs):
        state = CURRENT_BROWSER_DIAGNOSTIC.get()
        sequence = state.get("sequence") if state else None
        try:
            result = await original(self, *args, **kwargs)
        except Exception as error:
            # Keep a more specific nested navigation failure instead of
            # relabeling it as a general initialize failure on unwind.
            if not state or state.get("sequence") == sequence:
                record_browser_failure(stage, error)
            raise
        state = CURRENT_BROWSER_DIAGNOSTIC.get()
        if state and (state.get("failure") or {}).get("stage") == stage:
            state["failure"] = None
        return result

    observed._gflow_browser_diagnostic = True
    setattr(cls, name, observed)


def install_browser_observers(browser_class):
    for name, stage in (
        ("initialize", "browser_startup"),
        ("_browser_get", "page_load"),
        ("_tab_get", "page_load"),
        ("_browser_send_command", "browser_session"),
    ):
        _observe_method(browser_class, name, stage)


class BrowserDiagnosticFlowMixin:
    async def _get_recaptcha_token(self, *args, **kwargs):
        from src.core.config import config
        if CURRENT_BROWSER_DIAGNOSTIC.get() is not None and (
            kwargs.get("method_override") or config.captcha_method
        ) == "personal":
            try:
                from src.services.browser_captcha_personal import BrowserCaptchaService
                install_browser_observers(BrowserCaptchaService)
            except Exception as error:
                # The original implementation remains responsible for import and
                # availability handling. Diagnostics do not replace its behavior.
                record_browser_failure("browser_startup", error)
        result = await super()._get_recaptcha_token(*args, **kwargs)
        if result and result[0]:
            state = CURRENT_BROWSER_DIAGNOSTIC.get()
            if state:
                state["failure"] = None
        return result


def diagnose_browser_error(error):
    if not isinstance(error, dict) or "Failed to obtain reCAPTCHA token" not in str(error.get("message", "")):
        return error
    state = CURRENT_BROWSER_DIAGNOSTIC.get()
    failure = state.get("failure") if state else None
    stage = failure["stage"] if failure else "browser_session"
    reason = failure["reason"] if failure else "failed"
    detail = (f"{STAGE_TEXT[stage]}阶段失败（{REASON_TEXT[reason]}）"
              if failure else "浏览器验证未完成，当前没有更具体的安全诊断")
    status = error.get("status_code")
    if type(status) is not int or not 400 <= status <= 599:
        status = 503
    return {
        "status_code": status,
        "type": "browser_runtime_error",
        "message": f"{detail}。请检查插件专用浏览器运行状态；程序未重新提交生成。",
        "code": "gflow_" + stage + "_failed",
        "browser_stage": stage,
        "browser_reason": reason,
    }
