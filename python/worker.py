# -*- coding: utf-8 -*-
"""
GFlow worker process:
Communicates with the GFlow native plugin via standard input/output (line-delimited JSON).
Executes Flow2API tasks: token management, quota inquiry, configuration, and media generation.
"""

import asyncio
import base64
import json
import os
import sys
import traceback

# 专用浏览器在后台运行，不打开空白的未登录窗口。
os.environ.setdefault("PERSONAL_BROWSER_HEADLESS", "1")
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
from pathlib import Path
from typing import Any, Dict, List, Optional
from project_control import ProjectControlError

# Reconfigure standard streams to UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stdin, "reconfigure"):
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")

# Save dedicated RPC stdout before redirecting sys.stdout to sys.stderr
_rpc_stdout = sys.stdout
sys.stdout = sys.stderr

# Ensure flow2api package is in sys.path
base_dir = Path(__file__).resolve().parent
flow_pkg = base_dir / "flow2api"
if str(flow_pkg) not in sys.path:
    sys.path.insert(0, str(flow_pkg))

# Windows event loop setup
if os.name == "nt":
    try:
        policy = getattr(asyncio, "WindowsProactorEventLoopPolicy", None)
        if policy and not isinstance(asyncio.get_event_loop_policy(), policy):
            asyncio.set_event_loop_policy(policy())
    except Exception:
        pass


def get_gflow_model_catalog() -> List[Dict[str, Any]]:
    """返回供 EasyCPA 与客户端模型选择器展示的核心模型列表（严格限制为 7 个）。

    包括 3 个核心图片模型与 4 个核心视频模型。
    客户端选择这 7 个模型时，后台会根据比例/画质/时长自动调配底层笛卡尔积变体。
    """
    base_models = [
        ("gflow-nano-banana-pro", "nano-banana-pro", "image", "Nano Banana Pro", "Google Flow 核心图片模型 - 支持 1:1, 16:9, 9:16, 4:3, 3:4 与 1K/2K/4K 自动调配"),
        ("gflow-nano-banana-2-lite", "nano-banana-2-lite", "image", "Nano Banana 2 Lite", "Google Flow 核心图片模型 - 支持 1:1, 16:9, 9:16, 4:3, 3:4 与 1K/2K/4K 自动调配"),
        ("gflow-nano-banana-2.1", "nano-banana-2.1", "image", "Nano Banana 2.1", "Google Flow 核心图片模型 - 支持 1:1, 16:9, 9:16, 4:3, 3:4 与 1K/2K/4K 自动调配"),
        ("gflow-omni-1.1-flash", "omni-1.1-flash", "video", "Omni 1.1 Flash", "Google Flow 核心视频模型 - 支持 4s/6s/8s/10s 时长与横/竖屏自动调配"),
        ("gflow-veo-3.1-lite", "veo-3.1-lite", "video", "Veo 3.1 - Lite", "Google Flow 核心视频模型 - 支持 4s/6s/8s 时长与横/竖屏自动调配"),
        ("gflow-veo-3.1-fast", "veo-3.1-fast", "video", "Veo 3.1 - Fast", "Google Flow 核心视频模型 - 支持 4s/6s/8s 时长与横/竖屏自动调配"),
        ("gflow-veo-3.1-quality", "veo-3.1-quality", "video", "Veo 3.1 - Quality", "Google Flow 核心视频模型 - 支持 4s/6s/8s 时长与横/竖屏自动调配"),
    ]
    models = []
    for mid, orig_id, mtype, dname, desc in base_models:
        models.append({
            "id": mid,
            "original_id": orig_id,
            "type": mtype,
            "display_name": dname,
            "description": desc,
        })
    return models


class GFlowService:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir = self.data_dir / "tmp"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self.data_dir / "flow.db")

        self.db = None
        self.proxy_manager = None
        self.flow_client = None
        self.token_manager = None
        self.concurrency_manager = None
        self.load_balancer = None
        self.generation_handler = None
        self.project_control = None
        self.initialized = False
        self.initialization_lock = asyncio.Lock()
        self.account_validation_failures = {}

    async def initialize(self):
        async with self.initialization_lock:
            await self._initialize()

    async def _initialize(self):
        if self.initialized:
            return

        from src.core.config import config
        from src.core.database import Database
        from src.services.proxy_manager import ProxyManager
        from src.services.flow_client import FlowClient
        from src.services.token_manager import TokenManager
        from src.services.concurrency_manager import ConcurrencyManager
        from src.services.load_balancer import LoadBalancer
        from src.services.generation_handler import GenerationHandler
        from generation_progress import GenerationProgressMixin
        from src.api import routes

        self.db = Database(self.db_path)
        is_first = not self.db.db_exists()
        await self.db.init_db()

        raw_cfg = config.get_raw_config()
        if is_first:
            await self.db.init_config_from_toml(raw_cfg, is_first_startup=True)
        else:
            await self.db.check_and_migrate_db(raw_cfg)

        await self.db.reload_config_to_memory()
        # 一次用户生成只尝试一次；防止失败重启浏览器并重复提交。
        config.set_flow_max_retries(1)

        self.proxy_manager = ProxyManager(self.db)
        self.flow_client = FlowClient(self.proxy_manager, self.db)
        # 保留 flow2api 自身的分步骤超时；整项任务由 GenerationManager 截止和取消。
        from project_control import ProjectControl, PoolDatabase, BoundTokenManager, BoundLoadBalancer
        self.project_control = ProjectControl(self)
        await self.project_control.initialize()
        self.token_manager = TokenManager(PoolDatabase(self.db), self.flow_client)
        self.concurrency_manager = ConcurrencyManager()
        self.load_balancer = LoadBalancer(self.token_manager, self.concurrency_manager)
        class PluginGenerationHandler(GenerationProgressMixin, GenerationHandler):
            pass

        self.generation_handler = PluginGenerationHandler(
            self.flow_client,
            BoundTokenManager(self.token_manager),
            BoundLoadBalancer(self.load_balancer, self.token_manager),
            self.db,
            self.concurrency_manager,
            self.proxy_manager,
        )

        # Set cache dir to our local tmp_dir
        self.generation_handler.file_cache.cache_dir = self.tmp_dir
        self.generation_handler.file_cache.default_timeout = config.cache_timeout

        tokens = await self.token_manager.get_all_tokens()
        await self.concurrency_manager.initialize(tokens)

        # Register handler into routes module so normalization works
        routes.set_generation_handler(self.generation_handler)

        self.initialized = True
        sys.stderr.write(f"[GFlowService] Initialized. DB={self.db_path}, Accounts={len(tokens)}\n")
        sys.stderr.flush()

    def send_line(self, data: Dict[str, Any]):
        out = json.dumps(data, ensure_ascii=False)
        _rpc_stdout.write(out + "\n")
        _rpc_stdout.flush()

    async def handle_ping(self, _params: Dict[str, Any]) -> Dict[str, Any]:
        return {"pong": True}

    def _account_health(self, token):
        from account_health import account_health
        cache = getattr(self.token_manager, "_at_validation_cache", {})
        failures = getattr(self, "account_validation_failures", {})
        return account_health(
            token, recent_validation_expires=cache.get(token.id, 0),
            validation_failure=failures.get(token.id),
        )

    async def handle_status(self, _params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        tokens = await self.token_manager.get_all_tokens()
        from account_health import summarize_account_health
        health = {t.id: self._account_health(t) for t in tokens}
        summary = summarize_account_health(tokens, health)
        return {
            "initialized": self.initialized,
            "python": sys.executable,
            "version": sys.version.split()[0],
            "database_path": self.db_path,
            "cache_dir": str(self.tmp_dir),
            **summary,
            "extension_sync": getattr(self, "last_extension_sync", None),
            "sync_revision": "20261017.3",
        }

    async def handle_list_tokens(self, _params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        tokens = await self.token_manager.get_all_tokens()
        items = []
        for t in tokens:
            email = t.email or ""
            if "@" in email:
                user, dom = email.split("@", 1)
                masked = (user[0] + "***" if len(user) > 0 else "***") + "@" + dom
            else:
                masked = email[:2] + "***" if len(email) > 2 else "***"

            items.append({
                "id": t.id,
                "email": masked,
                "name": t.name or "",
                "remark": t.remark or "",
                "is_active": t.is_active,
                "health": self._account_health(t),
                "credits": t.credits,
                "user_paygate_tier": t.user_paygate_tier or "FREE",
                "current_project_id": t.current_project_id or "",
                "current_project_name": t.current_project_name or "",
                "image_enabled": t.image_enabled,
                "video_enabled": t.video_enabled,
                "ban_reason": t.ban_reason,
                "banned_at": t.banned_at.isoformat() if t.banned_at else None,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "last_used_at": t.last_used_at.isoformat() if t.last_used_at else None,
                "use_count": t.use_count,
            })
        return {"tokens": items, "total": len(items)}

    async def handle_add_token(self, params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        from credentials import normalize_account_input
        st, google_cookies = normalize_account_input(params.get("st"), params.get("google_cookies"))
        project_id = params.get("project_id", "").strip() or None
        project_name = params.get("project_name", "").strip() or None
        remark = params.get("remark", "").strip() or None
        captcha_proxy = params.get("captcha_proxy_url", "").strip() or None

        token_st = st
        if not token_st and not google_cookies:
            raise ValueError("Cookie 或 st 不能为空")

        new_token = await self.token_manager.add_token(
            st=token_st,
            project_id=project_id,
            project_name=project_name,
            remark=remark,
            captcha_proxy_url=captcha_proxy,
            google_cookies=google_cookies or None,
        )
        if google_cookies:
            await self.token_manager.update_token(
                new_token.id,
                google_cookies=google_cookies,
            )

        try:
            await self.token_manager.refresh_credits(new_token.id)
        except Exception as e:
            sys.stderr.write(f"[GFlowService] Warning: credit refresh failed on add: {e}\n")

        return {"success": True, "token_id": new_token.id}

    async def refresh_extension_login(self, cookies, email):
        from src.services.protocol_login import protocol_loginer
        proxy = await self.proxy_manager.get_request_proxy_url()
        return await protocol_loginer.login(
            cookies, proxy=proxy, email=email,
        )

    async def handle_extension_sync(self, params: Dict[str, Any]) -> Dict[str, Any]:
        from extension_sync import ExtensionSyncError, sync_extension_account
        try:
            result = await sync_extension_account(self, params)
        except ExtensionSyncError as error:
            result = error.result()
        # 留下可公开的最近诊断，便于定位用户重试；不保留请求或 Cookie。
        self.last_extension_sync = {
            "success": result.get("success", False),
            "error": result.get("error"),
        }
        return result

    async def handle_delete_token(self, params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        tid = params.get("token_id")
        if not tid:
            raise ValueError("token_id 必须指定")
        await self.token_manager.delete_token(int(tid))
        return {"success": True}

    async def handle_toggle_token(self, params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        tid = int(params.get("token_id", 0))
        enabled = bool(params.get("enabled", True))
        if enabled:
            await self.token_manager.enable_token(tid)
        else:
            await self.token_manager.disable_token(tid)
        return {"success": True, "token_id": tid, "is_active": enabled}

    async def handle_refresh_credits(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """手动查额仅调用原版额度接口；失败不伪装成零余额，也不启动登录浏览器。"""
        await self.initialize()
        tid = int(params.get("token_id", 0))
        token = await self.db.get_token(tid)
        if token is None:
            return {"success": False, "status_code": 404,
                    "error": {"code": "gflow_account_not_found", "message": "GFlow 账号不存在"}}
        try:
            result = await self.flow_client.get_credits(
                token.at, google_cookies=token.google_cookies, token_id=tid,
            )
        except Exception as error:
            from extension_sync import classify_sync_failure
            failure = classify_sync_failure(error)
            self.account_validation_failures[tid] = failure.code
            self.token_manager._clear_at_validation_cache(tid)
            if failure.code == "gflow_google_auth_rejected":
                return {"success": False, "status_code": 401,
                        "error": {"code": "gflow_session_expired",
                                  "message": "Google 未接受 GFlow 保存的登录态，请在已登录的 Flow 网页通过登录助手更新；历史余额保持不变。"}}
            return failure.result()
        credits = int(result.get("credits", 0))
        await self.db.update_token(
            tid, credits=credits, user_paygate_tier=result.get("userPaygateTier"),
        )
        self.token_manager._mark_at_valid(tid)
        self.account_validation_failures.pop(tid, None)
        return {"success": True, "credits": credits}

    async def handle_get_config(self, _params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        cap_cfg = await self.db.get_captcha_config()
        proxy_cfg = await self.db.get_proxy_config()
        call_cfg = await self.db.get_call_logic_config()
        cache_cfg = await self.db.get_cache_config()

        return {
            "captcha": {
                "method": cap_cfg.captcha_method,
                "yescaptcha_api_key": cap_cfg.yescaptcha_api_key or "",
                "yescaptcha_base_url": cap_cfg.yescaptcha_base_url or "",
                "twocaptcha_api_key": cap_cfg.twocaptcha_api_key or "",
                "capsolver_api_key": cap_cfg.capsolver_api_key or "",
            },
            "proxy": {
                "enabled": proxy_cfg.enabled if proxy_cfg else False,
                "url": getattr(proxy_cfg, "proxy_url", "") if proxy_cfg else "",
            },
            "call_logic": {
                "mode": getattr(call_cfg, "call_mode", "polling") if call_cfg else "polling",
            },
            "cache": {
                "timeout": cache_cfg.cache_timeout if cache_cfg else 7200,
            }
        }

    async def handle_update_config(self, params: Dict[str, Any]) -> Dict[str, Any]:
        await self.initialize()
        if "captcha" in params:
            c = params["captcha"]
            await self.db.update_captcha_config(
                captcha_method=c.get("method"),
                yescaptcha_api_key=c.get("yescaptcha_api_key"),
                yescaptcha_base_url=c.get("yescaptcha_base_url"),
                twocaptcha_api_key=c.get("twocaptcha_api_key"),
                capsolver_api_key=c.get("capsolver_api_key"),
            )
        if "proxy" in params:
            p = params["proxy"]
            await self.proxy_manager.update_proxy_config(
                enabled=bool(p.get("enabled")),
                proxy_url=p.get("url"),
            )
        return {"success": True}

    async def handle_list_models(self, _params: Dict[str, Any]) -> Dict[str, Any]:
        return {"models": get_gflow_model_catalog()}

    async def handle_project_control(self, params: Dict[str, Any]):
        from project_control import parse_control_request, completion, error_completion
        request = None
        try:
            request = parse_control_request(params)
            await self.initialize()
            return completion(await self.project_control.handle(request))
        except ProjectControlError as error:
            return error_completion(error, instance_id=getattr(getattr(self, "project_control", None), "instance_id", ""),
                                    workspace_id=request.get("workspaceId") if request else None)
        except Exception:
            # 不把凭据、数据库路径或上游原始异常带入公开控制响应。
            return error_completion(ProjectControlError("project_control_unavailable", "项目服务暂不可用，请检查插件日志", 503),
                                    instance_id=getattr(getattr(self, "project_control", None), "instance_id", ""),
                                    workspace_id=request.get("workspaceId") if request else None)

    async def handle_chat_completion(self, req_id: str, params: Dict[str, Any]):
        from project_control import CONTROL_MODEL, CURRENT_BINDING, ProjectControlError
        context = None
        try:
            if params.get("model") == CONTROL_MODEL:
                # The versioned control protocol is a nonstream operation;
                # SSE is only its transport wrapper at the host boundary.
                control_params = dict(params)
                control_params["stream"] = False
                result = await self.handle_project_control(control_params)
                if params.get("stream"):
                    # Control probes use the same native stream boundary without
                    # starting a Google media request.
                    streamed = dict(result)
                    streamed["object"] = "chat.completion.chunk"
                    streamed["choices"] = [{"index": choice.get("index", 0),
                        "delta": choice.get("message", {}),
                        "finish_reason": choice.get("finish_reason", "stop")}
                        for choice in result.get("choices", [])]
                    self.send_line({"id": req_id, "stream": True,
                        "chunk": "data: " + json.dumps(streamed, ensure_ascii=False) + "\n\n"})
                    self.send_line({"id": req_id, "stream": True, "chunk": "data: [DONE]\n\n"})
                self.send_line({"id": req_id, "done": True, "result": result})
                return
            await self.initialize()
            binding = None
            if any(field in params for field in ("gflow_workspace_id", "gflow_project_id", "gflow_instance_id")):
                binding = await self.project_control.binding_from_request(params)
            context = CURRENT_BINDING.set(binding)
            from generation_progress import progress_scope
            emit = (lambda chunk: self.send_line({"id": req_id, "stream": True, "chunk": chunk})) if params.get("stream") else None
            with progress_scope(emit):
                await self._handle_chat_completion(req_id, params)
        except ProjectControlError as error:
            self.send_line({"id": req_id, "done": True, "error": error.payload()})
        finally:
            if context is not None:
                CURRENT_BINDING.reset(context)

    async def _handle_chat_completion(self, req_id: str, params: Dict[str, Any]):
        await self.initialize()
        from model_adapter import normalize_model_alias, resolve_model_name
        from src.services.generation_handler import MODEL_CONFIG
        from src.api.routes import (
            ChatCompletionRequest,
            _normalize_openai_request,
            _collect_non_stream_result,
            _parse_handler_result,
            _iterate_openai_stream,
        )

        model_raw = params.get("model", "")
        if model_raw.startswith("gflow-"):
            actual_model = model_raw[len("gflow-"):]
        else:
            actual_model = model_raw

        params_clean = dict(params)
        params_clean["model"] = normalize_model_alias(actual_model)

        active_tokens = await self.token_manager.get_active_tokens()
        if not active_tokens:
            from generation_errors import account_pool_error
            self.send_line({
                "id": req_id, "done": True,
                "error": account_pool_error(await self.token_manager.get_all_tokens()),
            })
            return

        is_stream = bool(params.get("stream", False))
        base_url_override = "/v0/management/plugins/gflow/api"

        try:
            req_obj = ChatCompletionRequest(**params_clean)
            normalized = await _normalize_openai_request(req_obj)
            normalized.model = resolve_model_name(
                actual_model, request=req_obj, model_config=MODEL_CONFIG,
                images=normalized.images,
            )

            if is_stream:
                async for chunk in _iterate_openai_stream(normalized, base_url_override):
                    from generation_errors import generation_error
                    error = generation_error(chunk)
                    if error:
                        # Send failure through the native executor error channel.
                        # An error inside a normal SSE chunk is counted as success by the host.
                        self.send_line({"id": req_id, "done": True, "error": error})
                        return
                    from media_response import inline_cached_images
                    chunk = inline_cached_images(chunk, self.tmp_dir)
                    self.send_line({
                        "id": req_id,
                        "stream": True,
                        "chunk": chunk,
                    })
                self.send_line({
                    "id": req_id,
                    "stream": True,
                    "done": True,
                })
            else:
                raw_chunk = await _collect_non_stream_result(
                    normalized.model,
                    normalized.prompt,
                    normalized.images,
                    base_url_override=base_url_override,
                    video_media_id=normalized.video_media_id,
                )
                payload = _parse_handler_result(raw_chunk)
                from generation_errors import generation_error
                error = generation_error(payload)
                if error:
                    self.send_line({"id": req_id, "done": True, "error": error})
                    return
                from media_response import inline_cached_images
                payload = inline_cached_images(payload, self.tmp_dir)
                self.send_line({
                    "id": req_id,
                    "done": True,
                    "result": payload,
                })
        except ProjectControlError:
            raise
        except Exception as e:
            tb = traceback.format_exc()
            sys.stderr.write(f"[GFlowService] Generation error: {tb}\n")
            self.send_line({
                "id": req_id,
                "done": True,
                "error": {
                    "message": str(e),
                    "type": "generation_error",
                    "traceback": tb,
                    "status_code": 500,
                }
            })


async def main():
    if len(sys.argv) > 1:
        data_dir = Path(sys.argv[1])
    else:
        data_dir = Path(__file__).resolve().parent.parent / "data"

    from generation_lifecycle import GenerationLifecycle
    from worker_dispatch import ManagementDispatcher
    service = GFlowService(data_dir)
    lifecycle = GenerationLifecycle(service)
    dispatcher = ManagementDispatcher(service)
    await lifecycle.start()
    service.send_line({"ready": True, "pid": os.getpid(), "data_dir": str(data_dir)})

    loop = asyncio.get_running_loop()

    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        try:
            msg = json.loads(line)
        except Exception as e:
            sys.stderr.write(f"[GFlowService] Invalid JSON: {e}\n")
            continue

        req_id = msg.get("id", "")
        cmd = msg.get("cmd", "")
        params = msg.get("params", {})

        if cmd == "shutdown":
            service.send_line({"id": req_id, "done": True, "result": {"shutdown": True}})
            break

        if cmd == "chat_completion":
            from project_control import CONTROL_MODEL
            if params.get("model") == CONTROL_MODEL:
                dispatcher.submit(req_id, "project_control", params)
            else:
                lifecycle.submit(req_id, params)
            continue

        dispatcher.submit(req_id, cmd, params)

    await dispatcher.shutdown()
    await lifecycle.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
