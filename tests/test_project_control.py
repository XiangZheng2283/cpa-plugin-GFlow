"""虚构账号与云端响应的项目绑定回归；不会访问 Google 或提交生成。"""
import asyncio
import contextlib
import io
import json
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "python"), str(ROOT / "python/flow2api")]
from project_control import (
    BASE_MODELS, CONTROL_MODEL, CURRENT_BINDING, BoundLoadBalancer, BoundTokenManager,
    PoolDatabase, ProjectControl, ProjectControlError, account_fingerprint,
    completion, model_capabilities, parse_control_request,
)
from src.core.database import Database
from src.core.models import Project, Token


def request(op="projects.create", **changes):
    value = {"version": 1, "op": op, "workspaceId": str(uuid.uuid4())}
    if op in {"projects.create", "projects.get"}:
        value["projectId"] = str(uuid.uuid4())
    if op == "projects.create":
        value.update(operationId=str(uuid.uuid4()), name="测试项目", preferredModel="gflow-nano-banana-2-lite")
    value.update(changes)
    return value


def envelope(value, **changes):
    result = {"model": CONTROL_MODEL, "stream": False, "messages": [{"role": "user", "content": json.dumps(value)}]}
    result.update(changes)
    return result


class ControlProtocolTest(unittest.TestCase):
    def test_parser_accepts_standard_message_and_defaults_preferred_model(self):
        value = request()
        del value["preferredModel"]
        self.assertEqual(parse_control_request(envelope(value))["preferredModel"], "gflow-nano-banana-2-lite")

    def test_parser_rejects_streaming_and_malformed_shapes(self):
        values = [
            envelope(request(), stream=True), envelope(request(), messages=[]),
            envelope(request(), messages=[1]), envelope(request(), messages=[{"role":"assistant","content":"{}"}]),
            envelope(request(), messages=[{"role":"user","content":"not JSON"}]),
            envelope(request(), messages=[{"role":"user","content":"[]"}]),
        ]
        for value in values:
            with self.subTest(value=value), self.assertRaises(ProjectControlError):
                parse_control_request(value)

    def test_parser_rejects_unknown_version_operation_and_fields(self):
        for change in ({"version":2}, {"version":True}, {"op":"projects.delete"}, {"managementKey":"synthetic"}, {"workspaceId":"../flow.db"}):
            with self.subTest(change=change), self.assertRaises(ProjectControlError):
                parse_control_request(envelope(request(**change)))

    def test_parser_rejects_unbounded_names_and_non_catalog_models(self):
        for change in ({"name":"x"*129}, {"operationId":""}, {"name":"bad\nname"}, {"preferredModel":"gflow-other"}):
            with self.subTest(change=change), self.assertRaises(ProjectControlError):
                parse_control_request(envelope(request(**change)))

    def test_standard_completion_and_tier_capabilities(self):
        payload = {"version":1, "instanceId":"synthetic"}
        self.assertEqual(json.loads(completion(payload)["choices"][0]["message"]["content"]), payload)
        models = model_capabilities([SimpleNamespace(user_paygate_tier="PAYGATE_TIER_TWO")])
        self.assertEqual(len(models), 7)
        self.assertEqual(models[0]["imageSizes"], ["1K", "2K", "4K"])
        self.assertEqual(models[3]["durations"], [4, 6, 8, 10])
        pro_models = model_capabilities([SimpleNamespace(user_paygate_tier="PAYGATE_TIER_ONE")])
        self.assertEqual(pro_models[0]["imageSizes"], ["1K", "2K"])
        self.assertEqual(model_capabilities()[0]["imageSizes"], ["1K"])

    def test_reference_limits_follow_each_resolved_video_route_and_duration(self):
        from model_adapter import resolve_model_name
        from src.services.generation_handler import MODEL_CONFIG
        models = {model["id"]: model for model in model_capabilities()}
        expected = {
            "gflow-omni-1.1-flash": {"4": 3, "6": 3, "8": 3, "10": 3},
            "gflow-veo-3.1-lite": {"4": 2, "6": 2, "8": 3},
            "gflow-veo-3.1-fast": {"4": 2, "6": 2, "8": 3},
            "gflow-veo-3.1-quality": {"4": 2, "6": 2, "8": 0},
        }
        for model_id, limits in expected.items():
            metadata = models[model_id]
            self.assertEqual(metadata["maxReferencesByDuration"], limits)
            # Older clients without the duration map must receive a safe limit.
            self.assertEqual(metadata["maxReferences"], min(limits.values()))
            for seconds, limit in limits.items():
                for aspect in ("16:9", "9:16"):
                    with self.subTest(model=model_id, seconds=seconds, aspect=aspect):
                        resolved = resolve_model_name(model_id,
                            request={"duration": int(seconds), "aspect_ratio": aspect},
                            model_config=MODEL_CONFIG, images=[b"synthetic"])
                        config = MODEL_CONFIG[resolved]
                        actual = config.get("max_images", 0) if config.get("supports_images") else 0
                        self.assertEqual(limit, actual)
                        self.assertEqual(metadata["durations"], [int(value) for value in limits])
        for model in models.values():
            if model["kind"] == "image":
                self.assertNotIn("maxReferencesByDuration", model)
                self.assertEqual(model["maxReferences"], 3)


class ProjectPersistenceTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder = ROOT / ".build" / "project-control-tests" / str(uuid.uuid4())
        self.folder.mkdir(parents=True)
        self.db = Database(str(self.folder / "flow.db"))
        await self.db.init_db()
        self.token = Token(st="synthetic", at="synthetic", email="owner@example.invalid",
                           google_cookies="SID=synthetic", credits=1000,
                           user_paygate_tier="PAYGATE_TIER_TWO", is_active=True)
        self.token.id = await self.db.add_token(self.token)
        self.manager = SimpleNamespace(
            get_active_tokens=AsyncMock(return_value=[self.token]),
            get_token=AsyncMock(side_effect=self.db.get_token),
            ensure_valid_token=AsyncMock(side_effect=lambda token: token),
        )
        self.balancer = SimpleNamespace(
            select_token=AsyncMock(return_value=self.token),
            _check_extension_route=AsyncMock(return_value=(True, "")),
            _reserve_slot=AsyncMock(return_value=True), _add_pending=AsyncMock(), concurrency_manager=None,
        )
        self.remote_id = str(uuid.uuid4())
        self.service = SimpleNamespace(db=self.db, token_manager=self.manager, load_balancer=self.balancer,
                                       flow_client=SimpleNamespace(create_project=AsyncMock(return_value=self.remote_id)))
        self.control = ProjectControl(self.service)
        await self.control.initialize()

    async def create(self, **changes):
        value = request(**changes)
        return value, (await self.control.handle(value))["project"]

    async def test_empty_capabilities_does_not_generate_or_require_account(self):
        self.manager.get_active_tokens.return_value = []
        result = await self.control.handle(request("capabilities"))
        self.assertTrue(result["projectBinding"])
        self.assertEqual(result["capabilities"], {"projectBinding":True})
        self.assertEqual(len(result["models"]), 7)
        self.balancer.select_token.assert_not_awaited()
        self.service.flow_client.create_project.assert_not_awaited()

    async def test_instance_survives_controller_restart(self):
        other = ProjectControl(self.service)
        await other.initialize()
        self.assertEqual(other.instance_id, self.control.instance_id)
        uuid.UUID(other.instance_id)

    async def test_migration_preserves_existing_pool_and_excludes_dedicated_project(self):
        pool_id = str(uuid.uuid4())
        await self.db.add_project(Project(project_id=pool_id, token_id=self.token.id, project_name="旧池"))
        value, project = await self.create()
        self.assertEqual(project["state"], "ready")
        pools = await PoolDatabase(self.db).get_projects_by_token(self.token.id)
        self.assertEqual([p.project_id for p in pools], [pool_id])
        self.assertEqual(len(await self.db.get_projects_by_token(self.token.id)), 2)
        async with self.db._connect() as db:
            cursor = await db.execute("SELECT purpose FROM projects WHERE project_id=?", (self.remote_id,))
            self.assertEqual((await cursor.fetchone())[0], "cutflow")

    async def test_create_is_persisted_and_public_metadata_is_redacted(self):
        value, project = await self.create()
        self.assertEqual(project["id"], value["projectId"])
        self.assertEqual(project["flowProjectId"], self.remote_id)
        self.assertEqual(project["accountLabel"], "o***@example.invalid")
        public = json.dumps(await self.control.handle(request("projects.get", workspaceId=value["workspaceId"],projectId=value["projectId"])))
        self.assertNotIn("owner@example.invalid", public)
        self.assertNotIn("SID=", public)
        self.assertNotIn("token_id", public)
        self.assertNotIn("account_fingerprint", public)
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_get_optional_remote_diagnostic_never_changes_saved_binding(self):
        value, project = await self.create()
        for remote in (None,self.remote_id):
            checked = parse_control_request(envelope(request("projects.get",workspaceId=value["workspaceId"],
                projectId=value["projectId"],flowProjectId=remote)))
            self.assertEqual((await self.control.handle(checked))["project"],project)
        mismatch = request("projects.get",workspaceId=value["workspaceId"],projectId=value["projectId"],flowProjectId=str(uuid.uuid4()))
        with self.assertRaisesRegex(ProjectControlError,"不一致"):
            await self.control.handle(parse_control_request(envelope(mismatch)))
        with self.assertRaises(ProjectControlError):
            parse_control_request(envelope(request(flowProjectId=self.remote_id)))

    async def test_same_operation_is_not_replayed_across_restart(self):
        value, project = await self.create()
        other = ProjectControl(self.service)
        await other.initialize()
        again = await other.handle(value)
        self.assertEqual(again["project"], project)
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_changed_operation_or_payload_is_conflict(self):
        value, _ = await self.create()
        for change in ({"name":"改名"}, {"projectId":str(uuid.uuid4())}, {"operationId":str(uuid.uuid4())}):
            with self.subTest(change=change), self.assertRaisesRegex(ProjectControlError, "operationId"):
                await self.control.handle(dict(value, **change))
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_duplicate_concurrent_create_calls_remote_once(self):
        value = request()
        results = await asyncio.gather(self.control.handle(value), self.control.handle(value))
        self.assertEqual(results[0]["project"], results[1]["project"])
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_workspace_listing_does_not_leak_other_workspace(self):
        value, _ = await self.create()
        listed = await self.control.handle(request("projects.list", workspaceId=value["workspaceId"]))
        self.assertEqual(len(listed["projects"]), 1)
        self.assertEqual((await self.control.handle(request("projects.list")))["projects"], [])
        with self.assertRaises(ProjectControlError):
            await self.control.handle(request("projects.get", projectId=value["projectId"]))

    async def test_no_account_is_known_failure_without_remote_submission(self):
        self.balancer.select_token.return_value = None
        value, project = await self.create()
        self.assertEqual(project["state"], "failed")
        self.assertEqual(project["error"]["code"], "project_account_unavailable")
        self.service.flow_client.create_project.assert_not_awaited()
        await self.control.handle(value)
        self.balancer.select_token.assert_awaited_once()

    async def test_timeout_is_unknown_and_cannot_automatically_create_again(self):
        self.service.flow_client.create_project.side_effect = TimeoutError("Cookie=DO_NOT_EXPOSE")
        value, project = await self.create()
        self.assertEqual(project["state"], "creation_unknown")
        self.assertNotIn("DO_NOT_EXPOSE", json.dumps(project))
        await self.control.handle(value)
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_cancellation_persists_unknown_before_propagating(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def remote(*args, **kwargs):
            started.set()
            await release.wait()
        self.service.flow_client.create_project.side_effect = remote
        value = request()
        task = asyncio.create_task(self.control.handle(value))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        result = await self.control.handle(value)
        self.assertEqual(result["project"]["state"], "creation_unknown")
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_restart_marks_inflight_creation_unknown(self):
        value, _ = await self.create()
        await self.control._fail(value, "creating", None, None)
        other = ProjectControl(self.service)
        await other.initialize()
        result = await other.handle(value)
        self.assertEqual(result["project"]["state"], "creation_unknown")
        self.service.flow_client.create_project.assert_awaited_once()

    async def test_binding_requires_full_tuple_and_same_service(self):
        value, _ = await self.create()
        params = {"gflow_workspace_id":value["workspaceId"], "gflow_project_id":self.remote_id, "gflow_instance_id":self.control.instance_id}
        self.assertEqual((await self.control.binding_from_request(params))["flow_project_id"], self.remote_id)
        self.assertIsNone(await self.control.binding_from_request({}))
        for change in ({"gflow_instance_id":"other"}, {"gflow_project_id":value["projectId"]}, {"gflow_workspace_id":str(uuid.uuid4())}, {"gflow_instance_id":""}):
            with self.subTest(change=change), self.assertRaises(ProjectControlError):
                await self.control.binding_from_request(dict(params, **change))

    async def test_removed_disabled_or_changed_owner_cannot_use_binding(self):
        value, _ = await self.create()
        params = {"gflow_workspace_id":value["workspaceId"], "gflow_project_id":self.remote_id, "gflow_instance_id":self.control.instance_id}
        await self.db.update_token(self.token.id, is_active=False)
        with self.assertRaises(ProjectControlError):
            await self.control.binding_from_request(params)
        await self.db.update_token(self.token.id, is_active=True, email="different@example.invalid")
        with self.assertRaises(ProjectControlError):
            await self.control.binding_from_request(params)

    async def test_bound_proxies_do_not_select_or_create_from_pool(self):
        value, _ = await self.create()
        row = await self.control.binding_from_request({"gflow_workspace_id":value["workspaceId"],"gflow_project_id":self.remote_id,"gflow_instance_id":self.control.instance_id})
        original_manager = SimpleNamespace(ensure_project_exists=AsyncMock(return_value="pool"), ensure_valid_token=AsyncMock(side_effect=lambda t:t))
        self.balancer.select_token.reset_mock()
        context = CURRENT_BINDING.set(row)
        try:
            selected = await BoundLoadBalancer(self.balancer,self.manager).select_token(for_image_generation=True,model="gemini-3.1-flash-image-square-4k",track_pending=True)
            remote = await BoundTokenManager(original_manager).ensure_project_exists(selected.id)
            self.assertEqual(remote,self.remote_id)
            self.balancer.select_token.assert_not_awaited()
            self.balancer._add_pending.assert_awaited_once()
            original_manager.ensure_project_exists.assert_not_awaited()
            with self.assertRaises(ProjectControlError):
                await BoundTokenManager(original_manager).ensure_project_exists(selected.id+1)
        finally:
            CURRENT_BINDING.reset(context)
        self.assertEqual(await BoundTokenManager(original_manager).ensure_project_exists(self.token.id), "pool")

    async def test_bound_project_resolution_never_reads_token_current_project(self):
        value, _ = await self.create()
        row = await self.control.binding_from_request({
            "gflow_workspace_id": value["workspaceId"],
            "gflow_project_id": self.remote_id,
            "gflow_instance_id": self.control.instance_id,
        })
        # The token may still point to an older shared-pool project. A bound
        # generation must keep using the persisted dedicated Flow project.
        self.token.current_project_id = str(uuid.uuid4())
        manager = SimpleNamespace(
            ensure_project_exists=AsyncMock(return_value=self.token.current_project_id),
        )
        context = CURRENT_BINDING.set(row)
        try:
            bound = BoundTokenManager(manager)
            self.assertEqual(await bound.resolve_project_id(self.token.id), self.remote_id)
            self.assertEqual(await bound.ensure_project_exists(self.token.id), self.remote_id)
            manager.ensure_project_exists.assert_not_awaited()
        finally:
            CURRENT_BINDING.reset(context)

    async def test_bound_proxy_rejects_tier_and_route_without_fallback(self):
        value, _ = await self.create()
        row = (await self.control._rows("workspace_id=?", (value["workspaceId"],)))[0]
        context = CURRENT_BINDING.set(row)
        try:
            low = self.token.model_copy(update={"user_paygate_tier":"PAYGATE_TIER_NOT_PAID"})
            self.manager.get_token = AsyncMock(return_value=low)
            self.balancer.select_token.reset_mock()
            with self.assertRaisesRegex(ProjectControlError,"画质"):
                await BoundLoadBalancer(self.balancer,self.manager).select_token(for_image_generation=True,model="image-4k")
            self.manager.get_token = AsyncMock(return_value=self.token)
            self.balancer._check_extension_route.return_value = (False,"private failure")
            with self.assertRaisesRegex(ProjectControlError,"路由"):
                await BoundLoadBalancer(self.balancer,self.manager).select_token(for_image_generation=True,model="image")
            self.balancer.select_token.assert_not_awaited()
        finally:
            CURRENT_BINDING.reset(context)

    async def test_context_isolated_between_concurrent_bound_and_legacy_calls(self):
        value, _ = await self.create()
        row = (await self.control._rows("workspace_id=?", (value["workspaceId"],)))[0]
        manager = BoundTokenManager(SimpleNamespace(ensure_project_exists=AsyncMock(return_value="pool")))
        async def bound():
            context = CURRENT_BINDING.set(row)
            try:
                await asyncio.sleep(0)
                return await manager.ensure_project_exists(self.token.id)
            finally:
                CURRENT_BINDING.reset(context)
        bound_result, legacy = await asyncio.gather(bound(),manager.ensure_project_exists(self.token.id))
        self.assertEqual((bound_result,legacy),(self.remote_id,"pool"))
        self.assertIsNone(CURRENT_BINDING.get())



    async def test_bound_proxy_checks_concurrency_and_identity_after_refresh(self):
        value, _ = await self.create()
        row = (await self.control._rows("workspace_id=?", (value["workspaceId"],)))[0]
        context = CURRENT_BINDING.set(row)
        try:
            self.balancer.concurrency_manager = SimpleNamespace(can_use_image=AsyncMock(return_value=False))
            with self.assertRaisesRegex(ProjectControlError, "并发"):
                await BoundLoadBalancer(self.balancer,self.manager).select_token(for_image_generation=True,model="image")
            changed = self.token.model_copy(update={"email":"changed@example.invalid"})
            self.manager.ensure_valid_token = AsyncMock(return_value=changed)
            with self.assertRaisesRegex(ProjectControlError, "身份变更"):
                await BoundLoadBalancer(self.balancer,self.manager).select_token(for_image_generation=True,model="image")
        finally:
            CURRENT_BINDING.reset(context)

    async def test_bound_proxy_rechecks_refreshed_enable_flags_tier_and_identity(self):
        value, _ = await self.create()
        row = (await self.control._rows("workspace_id=?", (value["workspaceId"],)))[0]
        context = CURRENT_BINDING.set(row)
        try:
            changes = [
                ({"image_enabled":False}, {"for_image_generation":True,"model":"image"}, "未启用"),
                ({"video_enabled":False}, {"for_video_generation":True,"model":"video"}, "未启用"),
                ({"user_paygate_tier":"PAYGATE_TIER_NOT_PAID"}, {"for_image_generation":True,"model":"image-4k"}, "画质"),
                ({"is_active":False}, {"for_image_generation":True,"model":"image"}, "停用"),
                ({"id":self.token.id+1}, {"for_image_generation":True,"model":"image"}, "已变更"),
            ]
            for change, options, expected in changes:
                with self.subTest(change=change):
                    self.manager.ensure_valid_token = AsyncMock(return_value=self.token.model_copy(update=change))
                    self.balancer._add_pending.reset_mock()
                    self.balancer.select_token.reset_mock()
                    with self.assertRaisesRegex(ProjectControlError, expected):
                        await BoundLoadBalancer(self.balancer,self.manager).select_token(**options,track_pending=True)
                    self.balancer._add_pending.assert_not_awaited()
                    self.balancer.select_token.assert_not_awaited()
        finally:
            CURRENT_BINDING.reset(context)

    async def test_actual_handler_flags_cannot_skip_busy_check_or_double_reserve(self):
        value, _ = await self.create()
        row = (await self.control._rows("workspace_id=?", (value["workspaceId"],)))[0]
        context = CURRENT_BINDING.set(row)
        try:
            self.balancer.concurrency_manager = SimpleNamespace(
                can_use_image=AsyncMock(return_value=False),can_use_video=AsyncMock(return_value=False))
            for options, check in [
                ({"for_image_generation":True,"model":"image"}, self.balancer.concurrency_manager.can_use_image),
                ({"for_video_generation":True,"model":"video"}, self.balancer.concurrency_manager.can_use_video),
            ]:
                with self.subTest(options=options):
                    with self.assertRaisesRegex(ProjectControlError,"并发"):
                        await BoundLoadBalancer(self.balancer,self.manager).select_token(**options,
                            enforce_concurrency_filter=False,reserve=False,track_pending=True)
                    check.assert_awaited_once_with(self.token.id)
            self.balancer._reserve_slot.assert_not_awaited()
            self.balancer._add_pending.assert_not_awaited()
            self.balancer.concurrency_manager.can_use_image.return_value = True
            selected = await BoundLoadBalancer(self.balancer,self.manager).select_token(for_image_generation=True,model="image",
                enforce_concurrency_filter=False,reserve=False,track_pending=True)
            self.assertEqual(selected.id,self.token.id)
            self.balancer._reserve_slot.assert_not_awaited()
            self.balancer._add_pending.assert_awaited_once_with(self.token.id,True,False)
        finally:
            CURRENT_BINDING.reset(context)

    async def test_every_token_manager_refresh_rechecks_bound_request_constraints(self):
        value, _ = await self.create()
        row = (await self.control._rows("workspace_id=?", (value["workspaceId"],)))[0]
        row["_generation_constraints"] = (True,False,"image-4k")
        context = CURRENT_BINDING.set(row)
        try:
            manager = SimpleNamespace(ensure_valid_token=AsyncMock())
            for change in ({"image_enabled":False}, {"user_paygate_tier":"PAYGATE_TIER_NOT_PAID"},
                           {"email":"other@example.invalid"}, {"id":self.token.id+1}, {"is_active":False}):
                with self.subTest(change=change):
                    manager.ensure_valid_token.return_value = self.token.model_copy(update=change)
                    with self.assertRaises(ProjectControlError):
                        await BoundTokenManager(manager).ensure_valid_token(self.token)
            manager.ensure_valid_token.return_value = None
            with self.assertRaises(ProjectControlError):
                await BoundTokenManager(manager).ensure_valid_token(self.token)
        finally:
            CURRENT_BINDING.reset(context)

    async def test_actual_worker_stream_and_nonstream_keep_project_and_explicit_references(self):
        saved = sys.stdout
        try:
            from worker import GFlowService
        finally:
            sys.stdout = saved
        from src.api import routes
        from src.services.generation_handler import GenerationHandler
        value, _ = await self.create()
        records = []
        self.manager.ensure_project_exists = AsyncMock(side_effect=AssertionError("pool must not be used"))
        self.manager.record_usage = AsyncMock()
        self.manager.record_success = AsyncMock()
        self.manager.record_error = AsyncMock()
        self.balancer.release_pending = AsyncMock()
        handler = GenerationHandler.__new__(GenerationHandler)
        handler.flow_client = SimpleNamespace(prefill_remote_browser_pool=AsyncMock())
        handler.token_manager = BoundTokenManager(self.manager)
        handler.load_balancer = BoundLoadBalancer(self.balancer,self.manager)
        handler._log_request = AsyncMock()
        handler._update_request_log_progress = AsyncMock()
        async def pipeline(token, project_id, model, prompt, images, stream, **kwargs):
            records.append((token.id,project_id,prompt,images,stream))
            kwargs["generation_result"]["success"] = True
            if stream:
                yield handler._create_stream_chunk("![Generated Image](https://example.invalid/synthetic.png)")
            else:
                yield handler._create_completion_response("https://example.invalid/synthetic.png")
        handler._handle_image_generation = pipeline
        handler._handle_video_generation = pipeline
        worker = GFlowService.__new__(GFlowService)
        worker.initialize = AsyncMock()
        worker.project_control = self.control
        worker.token_manager = self.manager
        worker.tmp_dir = self.folder
        messages = []
        worker.send_line = messages.append
        self.balancer.select_token.reset_mock()
        image_bytes = b"synthetic-reference-image"
        import base64
        image_uri = "data:image/png;base64," + base64.b64encode(image_bytes).decode()
        for stream in (False,True):
            for model in ("gflow-nano-banana-2-lite","gflow-veo-3.1-fast"):
                with patch.object(routes,"generation_handler",handler):
                    await worker.handle_chat_completion("integration",{
                        "model":model,"stream":stream,"aspect_ratio":"16:9",
                        "messages":[{"role":"user","content":[{"type":"text","text":"only this session"},
                            {"type":"image_url","image_url":{"url":image_uri}}]}],
                        "gflow_workspace_id":value["workspaceId"],"gflow_project_id":self.remote_id,
                        "gflow_instance_id":self.control.instance_id,
                    })
                self.assertIsNone(CURRENT_BINDING.get())
        self.assertEqual(len(records),4)
        for token_id, project_id, prompt, images, stream in records:
            self.assertEqual((token_id,project_id,prompt,images),(self.token.id,self.remote_id,"only this session",[image_bytes]))
        self.assertFalse(any("error" in msg for msg in messages), messages)
        self.balancer.select_token.assert_not_awaited()
        self.manager.ensure_project_exists.assert_not_awaited()
        self.assertEqual(self.balancer.release_pending.await_count,4)
        # 原生成器在 balancer 后会第二次刷新，此时开关变化也必须阻止进入生成管线。
        self.manager.ensure_valid_token.side_effect = [
            self.token, self.token.model_copy(update={"image_enabled":False})]
        messages.clear()
        with patch.object(routes,"generation_handler",handler):
            await worker.handle_chat_completion("second-refresh-disabled",{
                "model":"gflow-nano-banana-2-lite","stream":False,
                "messages":[{"role":"user","content":"must not reach media pipeline"}],
                "gflow_workspace_id":value["workspaceId"],"gflow_project_id":self.remote_id,
                "gflow_instance_id":self.control.instance_id,
            })
        self.assertEqual(len(records),4)
        self.assertIn("未启用",messages[-1]["error"]["message"])
        self.assertIsNone(CURRENT_BINDING.get())

class WorkerProjectContextTest(unittest.IsolatedAsyncioTestCase):
    def worker(self):
        saved = sys.stdout
        try:
            from worker import GFlowService
        finally:
            sys.stdout = saved
        service = GFlowService.__new__(GFlowService)
        service.initialize = AsyncMock()
        service.send_line = Mock()
        service.project_control = SimpleNamespace(binding_from_request=AsyncMock(return_value={"token_id":7}))
        return service

    async def test_control_bypasses_generation_handler_and_account_gate(self):
        service = self.worker()
        service.handle_project_control = AsyncMock(return_value=completion({"version":1}))
        service._handle_chat_completion = AsyncMock()
        await service.handle_chat_completion("control",envelope(request("capabilities")))
        service._handle_chat_completion.assert_not_awaited()
        self.assertIn("result",service.send_line.call_args.args[0])

    async def test_bound_context_reaches_stream_and_nonstream_and_resets(self):
        for stream in (False,True):
            service = self.worker()
            seen = []
            async def generate(*args):
                seen.append(CURRENT_BINDING.get())
                await asyncio.sleep(0)
            service._handle_chat_completion = AsyncMock(side_effect=generate)
            await service.handle_chat_completion("test",{"model":"gflow-image","stream":stream,"gflow_project_id":"synthetic"})
            self.assertEqual(seen,[{"token_id":7}])
            self.assertIsNone(CURRENT_BINDING.get())

    async def test_cancel_and_error_reset_context_without_leaking_to_legacy(self):
        for error in (asyncio.CancelledError(), RuntimeError("synthetic")):
            service = self.worker()
            service._handle_chat_completion = AsyncMock(side_effect=error)
            with self.assertRaises(type(error)):
                await service.handle_chat_completion("test",{"model":"gflow-image","gflow_project_id":"synthetic"})
            self.assertIsNone(CURRENT_BINDING.get())
            seen = []
            async def legacy(*args):
                seen.append(CURRENT_BINDING.get())
            service._handle_chat_completion = AsyncMock(side_effect=legacy)
            await service.handle_chat_completion("legacy",{"model":"gflow-image"})
            self.assertEqual(seen,[None])



    async def test_control_error_code_survives_native_result_envelope(self):
        service = self.worker()
        from project_control import ProjectControlError
        service.project_control.handle = AsyncMock(side_effect=ProjectControlError(
            "project_binding_conflict","项目绑定冲突",409))
        service.project_control.instance_id = "synthetic-instance"
        service.handle_project_control = __import__("worker").GFlowService.handle_project_control.__get__(service)
        await service.handle_chat_completion("conflict",envelope(request("projects.get")))
        receipt = service.send_line.call_args.args[0]
        self.assertNotIn("error",receipt)
        control = json.loads(receipt["result"]["choices"][0]["message"]["content"])
        self.assertEqual(control["error"],{"code":"project_binding_conflict","message":"项目绑定冲突","status_code":409})

    async def test_control_validation_and_internal_failures_remain_safe_json(self):
        service = self.worker()
        method = __import__("worker").GFlowService.handle_project_control
        invalid = await method(service,envelope(request(),stream=True))
        value = json.loads(invalid["choices"][0]["message"]["content"])
        self.assertEqual(value["error"]["code"],"invalid_request")
        service.initialize = AsyncMock(side_effect=RuntimeError("Cookie=NEVER_EXPOSE private-database-path"))
        safe = await method(service,envelope(request("capabilities")))
        value = json.loads(safe["choices"][0]["message"]["content"])
        self.assertEqual(value["error"]["code"],"project_control_unavailable")
        self.assertNotIn("NEVER_EXPOSE",json.dumps(value))
        self.assertNotIn("private-database-path",json.dumps(value))

    async def test_cancelled_control_dispatcher_uses_completion_error_contract(self):
        from worker_dispatch import ManagementDispatcher
        started = asyncio.Event()
        async def control(params):
            started.set()
            await asyncio.Event().wait()
        messages = []
        dispatcher = ManagementDispatcher(SimpleNamespace(handle_project_control=control,send_line=messages.append))
        dispatcher.submit("cancel-control","project_control",{})
        await started.wait()
        await dispatcher.shutdown()
        self.assertEqual(len(messages),1)
        self.assertNotIn("error",messages[0])
        value = json.loads(messages[0]["result"]["choices"][0]["message"]["content"])
        self.assertEqual(value["error"]["code"],"worker_stopped")

if __name__ == "__main__":
    unittest.main()
