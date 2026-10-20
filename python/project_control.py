"""CutFlow 项目控制适配：不调用生成模型；固定项目及所属账号。"""
import asyncio
import contextvars
import hashlib
import json
import time
import uuid

CONTROL_MODEL = "gflow-project-control"
CURRENT_BINDING = contextvars.ContextVar("gflow_project_binding", default=None)
BASE_MODELS = [
    ("gflow-nano-banana-pro", "image"), ("gflow-nano-banana-2-lite", "image"),
    ("gflow-nano-banana-2.1", "image"), ("gflow-omni-1.1-flash", "video"),
    ("gflow-veo-3.1-lite", "video"), ("gflow-veo-3.1-fast", "video"),
    ("gflow-veo-3.1-quality", "video"),
]


class ProjectControlError(ValueError):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code, self.status = code, status

    def payload(self):
        return {"code": self.code, "message": str(self), "status_code": self.status}


def _uuid(value, field):
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        raise ProjectControlError("invalid_request", field + " 必须为 UUID") from None


def _text(value, field, limit=128):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or any(ord(c) < 32 for c in value):
        raise ProjectControlError("invalid_request", field + " 无效")
    return value.strip()


def parse_control_request(params):
    if params.get("stream") is not False:
        raise ProjectControlError("invalid_request", "项目控制请求必须使用 stream=false")
    messages = params.get("messages")
    if not isinstance(messages, list) or len(messages) != 1 or not isinstance(messages[0], dict) or messages[0].get("role") != "user":
        raise ProjectControlError("invalid_request", "项目控制请求必须包含一条 user JSON 消息")
    content = messages[0].get("content")
    if not isinstance(content, str) or len(content) > 8192:
        raise ProjectControlError("invalid_request", "项目控制 JSON 消息无效")
    try:
        value = json.loads(content)
    except (ValueError, TypeError):
        raise ProjectControlError("invalid_request", "项目控制消息不是 JSON") from None
    allowed = {"version", "op", "workspaceId", "operationId", "projectId", "name", "preferredModel", "flowProjectId"}
    if not isinstance(value, dict) or set(value) - allowed or type(value.get("version")) is not int or value["version"] != 1:
        raise ProjectControlError("unsupported_protocol", "不支持的项目控制协议")
    if not isinstance(value.get("op"), str) or value.get("op") not in {"capabilities", "projects.list", "projects.create", "projects.get"}:
        raise ProjectControlError("invalid_request", "不支持的项目控制操作")
    if "flowProjectId" in value:
        if value["op"] != "projects.get":
            raise ProjectControlError("invalid_request", "flowProjectId 仅用于项目查询诊断")
        if value["flowProjectId"] is not None:
            value["flowProjectId"] = _uuid(value["flowProjectId"], "flowProjectId")
    value["workspaceId"] = _uuid(value.get("workspaceId"), "workspaceId")
    if value["op"] in {"projects.create", "projects.get"}:
        value["projectId"] = _uuid(value.get("projectId"), "projectId")
    if value["op"] == "projects.create":
        value["operationId"] = _text(value.get("operationId"), "operationId")
        value["name"] = _text(value.get("name"), "name")
        value["preferredModel"] = _text(value.get("preferredModel") or "gflow-nano-banana-2-lite", "preferredModel")
        if value["preferredModel"] not in dict(BASE_MODELS):
            raise ProjectControlError("invalid_model", "项目首选模型必须来自 GFlow 基础模型目录")
    return value


def completion(payload):
    return {
        "id": "gflow-control-" + str(uuid.uuid4()), "object": "chat.completion",
        "created": int(time.time()), "model": CONTROL_MODEL,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": json.dumps(payload, ensure_ascii=False)}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def error_completion(error, *, instance_id="", workspace_id=None):
    """控制错误留在标准 completion 中，避免旧宿主 ABI 丢失业务错误码。"""
    payload = {"version": 1, "projectBinding": True, "error": error.payload()}
    if instance_id:
        payload["instanceId"] = instance_id
    if workspace_id:
        payload["workspaceId"] = workspace_id
    return completion(payload)


def account_fingerprint(token):
    identity = str(getattr(token, "email", "") or getattr(token, "login_account", "") or "").strip().casefold()
    if not identity:
        raise ProjectControlError("account_identity_unknown", "账号缺少已验证身份，无法固定项目归属", 409)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def account_label(token):
    email = str(getattr(token, "email", "") or getattr(token, "login_account", "") or "")
    if "@" in email:
        user, domain = email.split("@", 1)
        return (user[:1] or "*") + "***@" + domain
    return "Google 账号 " + str(token.id)


def model_capabilities(tokens=None):
    from src.core.account_tiers import get_paygate_tier_rank
    rank = max((get_paygate_tier_rank(getattr(t, "user_paygate_tier", None)) for t in (tokens or [])), default=0)
    sizes = ["1K", "2K", "4K"][:rank + 1]
    result = []
    for model, kind in BASE_MODELS:
        metadata = {
            "id": model, "kind": kind, "maxReferences": 3,
            "aspects": ["1:1", "16:9", "9:16", "4:3", "3:4"] if kind == "image" else ["16:9", "9:16"],
            "imageSizes": sizes if kind == "image" else [],
            "durations": [],
        }
        if kind == "video":
            durations = [4, 6, 8, 10] if "omni" in model else [4, 6, 8]
            # Match model_adapter's reference branch and the current MODEL_CONFIG:
            # Veo 4/6s uses I2V (2); Lite/Fast 8s uses R2V (3).
            # Quality 8s is currently T2V and discards references, so forbid them.
            if "omni" in model:
                limits = {str(seconds): 3 for seconds in durations}
            else:
                limits = {"4": 2, "6": 2, "8": 0 if "quality" in model else 3}
            metadata.update(durations=durations, maxReferencesByDuration=limits,
                            maxReferences=min(limits.values()))
        result.append(metadata)
    return result


class PoolDatabase:
    """保持上游数据库原样；TokenManager 的项目枚举只看轮询池。"""
    def __init__(self, database):
        self.database = database

    def __getattr__(self, name):
        return getattr(self.database, name)

    async def get_projects_by_token(self, token_id):
        from src.core.models import Project
        async with self.database._connect() as db:
            db.row_factory = __import__("aiosqlite").Row
            cursor = await db.execute("SELECT * FROM projects WHERE token_id=? AND purpose='pool' ORDER BY created_at DESC", (token_id,))
            return [Project(**dict(row)) for row in await cursor.fetchall()]


class BoundTokenManager:
    def __init__(self, manager):
        self.manager = manager

    def __getattr__(self, name):
        return getattr(self.manager, name)

    async def resolve_project_id(self, token_id):
        """Resolve the request project without falling back from a CutFlow binding."""
        binding = CURRENT_BINDING.get()
        if binding is None:
            return await self.manager.ensure_project_exists(token_id)
        if binding["token_id"] != token_id:
            raise ProjectControlError("project_account_conflict", "bound account does not own this project", 409)
        project_id = str(binding.get("flow_project_id") or "").strip()
        if not project_id:
            raise ProjectControlError("project_not_ready", "bound Flow project is not ready", 409)
        return project_id

    async def ensure_project_exists(self, token_id):
        binding = CURRENT_BINDING.get()
        if binding is None:
            return await self.manager.ensure_project_exists(token_id)
        if binding["token_id"] != token_id:
            raise ProjectControlError("project_account_conflict", "生成账号与项目归属不一致", 409)
        return await self.resolve_project_id(token_id)

    async def ensure_valid_token(self, token):
        result = await self.manager.ensure_valid_token(token)
        binding = CURRENT_BINDING.get()
        if binding is not None:
            image, video, model = binding.get("_generation_constraints", (False, False, None))
            BoundLoadBalancer._validate_token(result, binding, image, video, model)
        return result


class BoundLoadBalancer:
    """绑定请求禁止切换账号；未绑定请求继续使用原轮询行为。"""
    def __init__(self, balancer, token_manager):
        self.balancer, self.token_manager = balancer, token_manager

    def __getattr__(self, name):
        return getattr(self.balancer, name)

    @staticmethod
    def _validate_token(token, binding, image, video, model):
        from src.core.account_tiers import supports_model_for_tier
        if not token or not token.is_active or account_fingerprint(token) != binding["account_fingerprint"]:
            raise ProjectControlError("project_account_unavailable", "项目所属账号已移除、停用或身份变更", 409)
        if token.id != binding["token_id"]:
            raise ProjectControlError("project_account_conflict", "项目所属账号已变更", 409)
        if (image and not token.image_enabled) or (video and not token.video_enabled):
            raise ProjectControlError("project_account_unavailable", "项目账号未启用此类生成", 409)
        if not supports_model_for_tier(model, token.user_paygate_tier):
            raise ProjectControlError("project_model_unavailable", "当前项目账号不支持所选画质或模型；请降低画质或新建项目", 403)

    async def select_token(self, **kwargs):
        binding = CURRENT_BINDING.get()
        if binding is None:
            return await self.balancer.select_token(**kwargs)
        image, video = kwargs.get("for_image_generation", False), kwargs.get("for_video_generation", False)
        # 上游 handler 还会再次刷新；约束只留在该请求的 ContextVar 字典内，不落库。
        binding["_generation_constraints"] = (image, video, kwargs.get("model"))
        token = await self.token_manager.get_token(binding["token_id"])
        self._validate_token(token, binding, image, video, kwargs.get("model"))
        token = await self.token_manager.ensure_valid_token(token)
        # 刷新可返回新对象，必须重新校验权限和档位，不能沿用旧账号快照。
        self._validate_token(token, binding, image, video, kwargs.get("model"))
        route_ok, _ = await self.balancer._check_extension_route(token)
        if not route_ok:
            raise ProjectControlError("project_account_unavailable", "项目所属账号的浏览器验证路由未就绪", 409)
        # 固定项目始终做非阻塞并发预检；这里不占用槽位，不影响上游视频槽位获取。
        # 上游传 enforce_concurrency_filter=False 不能绕过固定账号的忙碌检查。
        manager = self.balancer.concurrency_manager
        if manager:
            available = await manager.can_use_image(token.id) if image else await manager.can_use_video(token.id) if video else True
            if not available:
                raise ProjectControlError("project_account_busy", "项目所属账号并发已满，请稍后再试", 409)
        if kwargs.get("reserve") and not await self.balancer._reserve_slot(token.id, image, video):
            raise ProjectControlError("project_account_busy", "项目所属账号当前正忙，请稍后再试", 409)
        if kwargs.get("track_pending"):
            await self.balancer._add_pending(token.id, image, video)
        return token


class ProjectControl:
    def __init__(self, service):
        self.service = service
        self.instance_id = ""
        self.creation_lock = asyncio.Lock()

    async def initialize(self):
        async with self.service.db._connect(write=True) as db:
            cursor = await db.execute("PRAGMA table_info(projects)")
            if "purpose" not in {row[1] for row in await cursor.fetchall()}:
                await db.execute("ALTER TABLE projects ADD COLUMN purpose TEXT NOT NULL DEFAULT 'pool'")
            await db.execute("CREATE TABLE IF NOT EXISTS gflow_control_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            await db.execute("INSERT OR IGNORE INTO gflow_control_meta VALUES ('instance_id', ?)", (str(uuid.uuid4()),))
            cursor = await db.execute("SELECT value FROM gflow_control_meta WHERE key='instance_id'")
            self.instance_id = (await cursor.fetchone())[0]
            await db.execute("""CREATE TABLE IF NOT EXISTS cutflow_projects (
                workspace_id TEXT NOT NULL, project_id TEXT NOT NULL, operation_id TEXT NOT NULL,
                request_hash TEXT NOT NULL, name TEXT NOT NULL, preferred_model TEXT NOT NULL,
                flow_project_id TEXT NOT NULL DEFAULT '', token_id INTEGER, account_fingerprint TEXT NOT NULL DEFAULT '',
                account_label TEXT NOT NULL DEFAULT '', state TEXT NOT NULL, error_code TEXT, error_message TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                PRIMARY KEY (workspace_id, project_id), UNIQUE (workspace_id, operation_id))""")
            # 重启中断的创建不能自动重发。
            await db.execute("UPDATE cutflow_projects SET state='creation_unknown', error_code='creation_unknown', error_message='服务在项目创建期间中断，请核查 Google Flow；不会自动重建' WHERE state='creating'")
            await db.commit()

    async def _rows(self, where, args):
        async with self.service.db._connect() as db:
            db.row_factory = __import__("aiosqlite").Row
            cursor = await db.execute("SELECT * FROM cutflow_projects WHERE " + where, args)
            return [dict(row) for row in await cursor.fetchall()]

    def _public(self, row):
        result = {"id": row["project_id"], "name": row["name"], "flowProjectId": row["flow_project_id"],
                  "state": row["state"], "accountLabel": row["account_label"], "instanceId": self.instance_id}
        if row.get("error_code"):
            result["error"] = {"code": row["error_code"], "message": row["error_message"]}
        return result

    async def handle(self, request):
        payload = {"version": 1, "instanceId": self.instance_id, "workspaceId": request["workspaceId"],
                   "projectBinding": True, "capabilities": {"projectBinding": True}}
        op, workspace = request["op"], request["workspaceId"]
        if op == "capabilities":
            payload["models"] = model_capabilities(await self.service.token_manager.get_active_tokens())
        elif op == "projects.list":
            payload["projects"] = [self._public(row) for row in await self._rows("workspace_id=? ORDER BY created_at DESC", (workspace,))]
        else:
            if op == "projects.get":
                rows = await self._rows("workspace_id=? AND project_id=?", (workspace, request["projectId"]))
                if not rows:
                    raise ProjectControlError("project_not_found", "此工作区没有该项目", 404)
                row = rows[0]
                if request.get("flowProjectId") and request["flowProjectId"] != row["flow_project_id"]:
                    raise ProjectControlError("project_binding_conflict", "项目的 Google Flow 标识与保存的绑定不一致", 409)
            else:
                row = await self.create(request)
            payload["project"] = self._public(row)
            token = await self.service.db.get_token(row["token_id"]) if row["token_id"] else None
            payload["models"] = model_capabilities([token] if token else [])
        return payload

    async def _fail(self, request, state, code, message):
        async with self.service.db._connect(write=True) as db:
            await db.execute("UPDATE cutflow_projects SET state=?, error_code=?, error_message=?, updated_at=? WHERE workspace_id=? AND project_id=?",
                             (state, code, message, time.time(), request["workspaceId"], request["projectId"]))
            await db.commit()

    async def create(self, request):
        key = {name: request[name] for name in ("workspaceId", "projectId", "name", "preferredModel")}
        digest = hashlib.sha256(json.dumps(key, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        async with self.creation_lock:
            previous = await self._rows("workspace_id=? AND (operation_id=? OR project_id=?)",
                                        (request["workspaceId"], request["operationId"], request["projectId"]))
            if previous:
                row = previous[0]
                if row["operation_id"] != request["operationId"] or row["request_hash"] != digest:
                    raise ProjectControlError("operation_conflict", "项目或 operationId 已用于其他请求", 409)
                return row
            now = time.time()
            async with self.service.db._connect(write=True) as db:
                await db.execute("""INSERT INTO cutflow_projects
                    (workspace_id,project_id,operation_id,request_hash,name,preferred_model,state,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,'creating',?,?)""",
                    (request["workspaceId"],request["projectId"],request["operationId"],digest,request["name"],request["preferredModel"],now,now))
                await db.commit()
            submitted = False
            try:
                kind = dict(BASE_MODELS)[request["preferredModel"]]
                token = await self.service.load_balancer.select_token(for_image_generation=kind=="image", for_video_generation=kind=="video",
                                                                      model=request["preferredModel"], reserve=False, track_pending=False)
                if token is None:
                    raise ProjectControlError("project_account_unavailable", "没有可用的 Google Flow 账号，请同步登录后新建项目", 503)
                fingerprint = account_fingerprint(token)
                label = account_label(token)
                async with self.service.db._connect(write=True) as db:
                    await db.execute("UPDATE cutflow_projects SET token_id=?,account_fingerprint=?,account_label=? WHERE workspace_id=? AND project_id=?",
                                     (token.id,fingerprint,label,request["workspaceId"],request["projectId"]))
                    await db.commit()
                submitted = True
                remote_id = await self.service.flow_client.create_project(token.st, request["name"], google_cookies=token.google_cookies)
                remote_id = _uuid(remote_id, "Google Flow projectId")
                async with self.service.db._connect(write=True) as db:
                    await db.execute("INSERT INTO projects(project_id,token_id,project_name,tool_name,is_active,purpose) VALUES (?,?,?,'PINHOLE',1,'cutflow')",
                                     (remote_id,token.id,request["name"]))
                    await db.execute("UPDATE cutflow_projects SET flow_project_id=?,state='ready',error_code=NULL,error_message=NULL,updated_at=? WHERE workspace_id=? AND project_id=?",
                                     (remote_id,time.time(),request["workspaceId"],request["projectId"]))
                    await db.commit()
            except BaseException as error:
                if submitted:
                    state, code, message = "creation_unknown", "creation_unknown", "Google Flow 项目创建回执未确认，请核查云端；不会自动重建"
                elif isinstance(error, ProjectControlError):
                    state, code, message = "failed", error.code, str(error)
                else:
                    state, code, message = "failed", "project_creation_failed", "项目账号准备失败，请检查登录和网络"
                await asyncio.shield(self._fail(request, state, code, message))
                if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    raise
            return (await self._rows("workspace_id=? AND project_id=?", (request["workspaceId"],request["projectId"])))[0]

    async def binding_from_request(self, params):
        fields = ("gflow_workspace_id", "gflow_project_id", "gflow_instance_id")
        if not any(field in params for field in fields):
            return None
        if not all(isinstance(params.get(field), str) and params[field] for field in fields):
            raise ProjectControlError("project_binding_required", "项目生成必须同时提供工作区、项目和服务实例标识")
        if params["gflow_instance_id"] != self.instance_id:
            raise ProjectControlError("project_service_changed", "生成服务已变更，请重新连接项目", 409)
        workspace = _uuid(params["gflow_workspace_id"], "gflow_workspace_id")
        project = _uuid(params["gflow_project_id"], "gflow_project_id")
        rows = await self._rows("workspace_id=? AND flow_project_id=?", (workspace, project))
        if not rows:
            raise ProjectControlError("project_not_found", "此工作区没有该项目", 404)
        row = rows[0]
        if row["state"] != "ready":
            raise ProjectControlError("project_not_ready", "项目尚未完成 Google Flow 绑定，禁止生成", 409)
        token = await self.service.db.get_token(row["token_id"])
        if not token or not token.is_active or account_fingerprint(token) != row["account_fingerprint"]:
            raise ProjectControlError("project_account_unavailable", "项目所属账号已移除、停用或身份变更", 409)
        return row
