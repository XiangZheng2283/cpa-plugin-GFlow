"""Local synthetic browser failures only; no browser or Google requests."""
import asyncio
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from browser_diagnostics import (
    CURRENT_BROWSER_DIAGNOSTIC, browser_diagnostic_scope, diagnose_browser_error,
    install_browser_observers, record_browser_failure,
)

GENERIC_ERROR = {"message": "Failed to obtain reCAPTCHA token",
                 "code": "generation_failed", "status_code": 500}
SECRET_ERROR = "https://example.invalid/?access_token=PRIVATE cookie=PRIVATE sitekey=PRIVATE"


class BrowserDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_pool_swallow_preserves_stage_without_exception_text(self):
        class Browser:
            async def initialize(self):
                raise TimeoutError(SECRET_ERROR)
        install_browser_observers(Browser)
        with browser_diagnostic_scope():
            # Same behavior as the upstream browser pool: keep trying, then None.
            for _ in range(2):
                try:
                    await Browser().initialize()
                except TimeoutError:
                    pass
            result = diagnose_browser_error({**GENERIC_ERROR, "traceback": SECRET_ERROR, "url": SECRET_ERROR})
            self.assertEqual(result["browser_stage"], "browser_startup")
            self.assertEqual(result["browser_reason"], "timeout")
            self.assertIn("浏览器启动", result["message"])
            self.assertNotIn("PRIVATE", json.dumps(result))
            self.assertNotIn("example.invalid", json.dumps(result))
            self.assertEqual(result["status_code"], 500)

    async def test_observation_does_not_change_retries_return_or_exception_identity(self):
        failure = PermissionError(SECRET_ERROR)
        class Browser:
            calls = 0
            async def _browser_get(self, *_args, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise failure
                return "same-tab-object"
        install_browser_observers(Browser)
        install_browser_observers(Browser)
        browser = Browser()
        with browser_diagnostic_scope() as state:
            try:
                await browser._browser_get("https://example.invalid/private")
            except PermissionError as actual:
                self.assertIs(actual, failure)
            self.assertEqual(browser.calls, 1)
            self.assertEqual(state["failure"], {"stage": "page_load", "reason": "access_denied"})
            self.assertEqual(await browser._browser_get("about:blank"), "same-tab-object")
            self.assertEqual(browser.calls, 2)
            self.assertIsNone(state["failure"])

    async def test_request_concurrency_is_isolated_and_late_tasks_are_inactive(self):
        captured = []
        async def request(stage, reason):
            with browser_diagnostic_scope() as state:
                record_browser_failure(stage, reason)
                await asyncio.sleep(0)
                captured.append(state)
                return diagnose_browser_error(GENERIC_ERROR)
        first, second = await asyncio.gather(
            request("browser_startup", FileNotFoundError(SECRET_ERROR)),
            request("page_load", TimeoutError(SECRET_ERROR)),
        )
        self.assertEqual(first["browser_stage"], "browser_startup")
        self.assertEqual(first["browser_reason"], "not_found")
        self.assertEqual(second["browser_stage"], "page_load")
        self.assertTrue(all(not state for state in captured))
        self.assertIsNone(CURRENT_BROWSER_DIAGNOSTIC.get())
        for state in captured:
            handle = CURRENT_BROWSER_DIAGNOSTIC.set(state)
            try:
                record_browser_failure("page_load", TimeoutError("late"))
                self.assertFalse(state)
            finally:
                CURRENT_BROWSER_DIAGNOSTIC.reset(handle)

    async def test_cancellation_is_not_recorded_or_changed(self):
        class Browser:
            async def _tab_get(self):
                raise asyncio.CancelledError()
        install_browser_observers(Browser)
        with browser_diagnostic_scope() as state:
            with self.assertRaises(asyncio.CancelledError):
                await Browser()._tab_get()
            self.assertIsNone(state["failure"])

    async def test_disconnection_and_unknown_failures_expose_only_enums(self):
        for error, reason in [(ConnectionResetError(SECRET_ERROR), "disconnected"),
                              (RuntimeError(SECRET_ERROR), "failed")]:
            with self.subTest(reason=reason), browser_diagnostic_scope():
                record_browser_failure("browser_session", error)
                result = diagnose_browser_error(GENERIC_ERROR)
                self.assertEqual(result["browser_reason"], reason)
                self.assertNotIn("PRIVATE", json.dumps(result))
        with browser_diagnostic_scope():
            result = diagnose_browser_error(GENERIC_ERROR)
            self.assertIn("没有更具体", result["message"])

    async def test_unrelated_generation_errors_are_unchanged(self):
        error = {"message": "insufficient quota", "code": "insufficient_quota", "status_code": 402}
        with browser_diagnostic_scope():
            record_browser_failure("browser_startup", TimeoutError(SECRET_ERROR))
            self.assertIs(diagnose_browser_error(error), error)

    async def test_nested_navigation_failure_keeps_specific_page_stage(self):
        class Browser:
            async def _tab_get(self):
                raise TimeoutError(SECRET_ERROR)
            async def initialize(self):
                return await self._tab_get()
        install_browser_observers(Browser)
        with browser_diagnostic_scope():
            with self.assertRaises(TimeoutError):
                await Browser().initialize()
            result = diagnose_browser_error(GENERIC_ERROR)
            self.assertEqual(result["browser_stage"], "page_load")
            self.assertEqual(result["browser_reason"], "timeout")

    async def test_success_without_prior_failure_does_not_raise(self):
        class Browser:
            async def initialize(self):
                return "ready"
        install_browser_observers(Browser)
        with browser_diagnostic_scope() as state:
            self.assertEqual(await Browser().initialize(), "ready")
            self.assertIsNone(state["failure"])


if __name__ == "__main__":
    unittest.main()
