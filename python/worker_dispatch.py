import asyncio
import sys
import traceback
from project_control import ProjectControlError, error_completion


class ManagementDispatcher:
    def __init__(self, service):
        self.service = service
        self.tasks = set()
        self.mutations = asyncio.Lock()

    def submit(self, request_id, command, params):
        task = asyncio.create_task(self.run(request_id, command, params))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def run(self, request_id, command, params):
        try:
            if command == "project_control" and params.get("stream"):
                # Control requests share the real chat stream path and its sole
                # completion receipt; they remain independent of mutation locks.
                await self.service.handle_chat_completion(request_id, params)
                return
            handler = getattr(self.service, "handle_" + command, None)
            if handler is None:
                raise ValueError("Unknown command: " + command)
            if command in {"ping", "status", "list_tokens", "list_models", "get_config", "project_control"}:
                result = await handler(params)
            else:
                async with self.mutations:
                    result = await handler(params)
            self.service.send_line({"id": request_id, "done": True, "result": result})
        except asyncio.CancelledError:
            error = {"code": "worker_stopped", "message": "GFlow backend stopped; operation result may be unknown", "status_code": 503}
            if command == "project_control":
                self.service.send_line({"id": request_id, "done": True, "result": error_completion(ProjectControlError(
                    "worker_stopped", "项目操作被中断，结果需要核查；不会自动重建项目", 503))})
            else:
                self.service.send_line({"id": request_id, "done": True, "error": error})
        except ProjectControlError as error:
            field, value = ("result", error_completion(error)) if command == "project_control" else ("error", error.payload())
            self.service.send_line({"id": request_id, "done": True, field: value})
        except Exception as error:
            if command == "project_control":
                self.service.send_line({"id": request_id, "done": True, "result": error_completion(ProjectControlError(
                    "project_control_unavailable", "项目服务暂不可用，请检查插件日志", 503))})
            else:
                sys.stderr.write(f"[GFlowService] Error in {command}: {traceback.format_exc()}\n")
                self.service.send_line({"id": request_id, "done": True, "error": {"message": str(error)}})

    async def shutdown(self):
        pending = list(self.tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
