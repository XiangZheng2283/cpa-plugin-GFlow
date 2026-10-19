"""使用空账号、随机密钥和隔离目录验证真实 CPA 的免费项目控制路由。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
import uuid
import zipfile


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def http_json(base, path, key=None, payload=None, timeout=70):
    headers = {"Content-Type":"application/json"}
    if key:
        headers["Authorization"] = "Bearer " + key
    request = urllib.request.Request(base + path, data=json.dumps(payload).encode() if payload is not None else None,
                                     headers=headers, method="POST" if payload is not None else "GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            result = json.loads(error.read())
        except ValueError:
            result = {}
        return error.code, result


def run(host, archive, stage, report_path):
    import psutil
    root = Path(__file__).resolve().parents[2]
    stage = stage.resolve()
    if not stage.is_relative_to(root) or stage.exists():
        raise ValueError("验收目录必须是当前插件项目内的全新目录")
    stage.mkdir(parents=True)
    host_copy = stage / "cli-proxy-api.exe"
    shutil.copy2(host, host_copy)
    plugin_dir = stage / "plugins/windows/amd64"
    plugin_dir.mkdir(parents=True)
    with zipfile.ZipFile(archive) as bundle:
        if bundle.namelist() != ["gflow.dll"]:
            raise ValueError("插件包必须只包含 gflow.dll")
        (plugin_dir / "gflow.dll").write_bytes(bundle.read("gflow.dll"))
    auths = stage / "auths"
    auths.mkdir()
    # 仅供宿主选择插件执行器的虚构凭据，不含 Google 或平台登录态。
    (auths / "gflow-synthetic.json").write_text(json.dumps({
        "type":"gflow", "provider":"gflow", "email":"synthetic@example.invalid",
        "disabled":False, "access_token":"synthetic-routing-only",
    }),encoding="utf-8")
    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1",0))
        port = port_socket.getsockname()[1]
    api_key = "smoke-" + secrets.token_urlsafe(32)
    configuration = f'''server:
  host: "127.0.0.1"
  port: {port}
  discovery:
    enabled: false
management:
  allow-remote: false
  secret-key: ""
  disable-control-panel: true
  disable-auto-update-panel: true
access:
  api-keys:
    - {json.dumps(api_key)}
oauth:
  auth-dir: {json.dumps(str(auths))}
plugins:
  enabled: true
  dir: {json.dumps(str(stage / "plugins"))}
  configs:
    gflow:
      enabled: true
      data_dir: {json.dumps(str(stage / "gflow-data"))}
observability:
  logs:
    debug: false
    logging-to-file: false
    request-log: false
  usage:
    usage-statistics-enabled: false
'''
    config = stage / "config.yaml"
    config.write_text(configuration,encoding="utf-8")
    temporary = stage / "tmp"
    temporary.mkdir()
    profile = stage / "profile"
    profile.mkdir()
    environment = os.environ.copy()
    environment.update(TMP=str(temporary),TEMP=str(temporary),TMPDIR=str(temporary),
                       APPDATA=str(profile/"Roaming"),LOCALAPPDATA=str(profile/"Local"),
                       PYTHONDONTWRITEBYTECODE="1")
    environment.pop("GFLOW_WORKER_BINARY",None)
    environment.pop("PYTHONPATH",None)
    protected = {}
    for existing in psutil.process_iter(["name", "create_time"]):
        if (existing.info["name"] or "").lower() in ("cutflow.exe", "easycliproxyapi.exe", "cli-proxy-api.exe", "gflow-worker.exe"):
            protected[existing.pid] = existing.info["create_time"]
    summary = {
        "hostExecutable":str(host), "hostSha256":checksum(host),
        "archive":archive.name,"archiveSha256":checksum(archive),
        "dllSha256":checksum(plugin_dir/"gflow.dll"),"runtimeRoot":str(stage),
        "checks":{},"cloudProjectCreation":"not called","mediaGeneration":"not called",
    }
    children = []
    process = None
    workspace = str(uuid.uuid4())
    try:
        with (stage/"host.log").open("wb") as log:
            process = subprocess.Popen([str(host_copy),"-config",str(config),"-local-model"],cwd=stage,env=environment,
                                       stdout=log,stderr=subprocess.STDOUT,
                                       creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0))
            summary["isolatedHostPid"] = process.pid
            base = "http://127.0.0.1:" + str(port)
            deadline = time.monotonic()+100
            catalog = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("隔离 CPA 提前退出；见本项目 host.log")
                try:
                    status,catalog = http_json(base,"/v1/models",api_key,timeout=3)
                    if status == 200 and any(item.get("id") == "gflow-project-control" for item in catalog.get("data",[])):
                        break
                except (OSError,ValueError):
                    pass
                time.sleep(.3)
            if not catalog:
                raise RuntimeError("隔离 CPA 未就绪")
            models = [item["id"] for item in catalog.get("data",[])]
            summary["models"] = [model for model in models if model.startswith("gflow-")]
            assert "gflow-project-control" in models, "宿主未注册项目控制模型"
            summary["checks"]["control_model_registered"] = "passed"
            status,_ = http_json(base,"/v1/models","invalid-smoke-key")
            assert status == 401, ("unauthorized models status",status)
            summary["checks"]["ordinary_api_key_required"] = "passed"
            first_instance = None
            for op in ("capabilities","projects.list"):
                value = {"version":1,"op":op,"workspaceId":workspace}
                body = {"model":"gflow-project-control","stream":False,
                        "messages":[{"role":"user","content":json.dumps(value)}]}
                status,receipt = http_json(base,"/v1/chat/completions",api_key,body)
                assert status == 200, ("control HTTP status",status,receipt)
                control = json.loads(receipt["choices"][0]["message"]["content"])
                assert "error" not in control, control
                assert control["workspaceId"] == workspace and control["projectBinding"] is True
                first_instance = first_instance or control["instanceId"]
                assert first_instance == control["instanceId"]
                if op == "capabilities":
                    assert len(control["models"]) == 7
                else:
                    assert control["projects"] == []
                summary["checks"][op] = "passed"
            invalid = {"model":"gflow-project-control","stream":False,"messages":[{
                "role":"user","content":json.dumps({"version":999,"op":"capabilities","workspaceId":workspace})}]}
            status,receipt = http_json(base,"/v1/chat/completions",api_key,invalid)
            assert status == 200, status
            control = json.loads(receipt["choices"][0]["message"]["content"])
            assert control["error"]["code"] == "unsupported_protocol"
            summary["checks"]["structured_control_error_survives_host"] = "passed"
            # A free control request exercises the real native host-stream
            # boundary. No Google account or generation endpoint is available.
            control_request = {"version":1,"op":"capabilities","workspaceId":workspace}
            body = {"model":"gflow-project-control","stream":True,
                    "messages":[{"role":"user","content":json.dumps(control_request)}]}
            request = urllib.request.Request(base + "/v1/chat/completions",
                data=json.dumps(body).encode(),
                headers={"Content-Type":"application/json", "Authorization":"Bearer " + api_key}, method="POST")
            began = time.monotonic()
            with urllib.request.urlopen(request, timeout=30) as response:
                assert response.status == 200
                text = response.read().decode("utf-8")
            summary["streamBodyBytes"] = len(text.encode("utf-8"))
            assert text.count("data: [DONE]") == 1, "宿主应只输出一个流结束标记"
            assert "data: data:" not in text, "SSE 被重复封装"
            content = []
            finished = False
            for line in text.splitlines():
                if line.startswith("data:") and line[5:].strip() != "[DONE]":
                    chunk = json.loads(line[5:].strip())
                    choices = chunk.get("choices", [{}])
                    content.append(choices[0].get("delta", {}).get("content", ""))
                    finished = finished or any(choice.get("finish_reason") for choice in choices)
            control = json.loads("".join(content))
            assert "error" not in control and finished, control
            assert control["workspaceId"] == workspace and control["instanceId"] == first_instance
            summary["streamControlSeconds"] = round(time.monotonic() - began, 3)
            summary["checks"]["real_host_stream_control"] = "passed"
            with sqlite3.connect(stage/"gflow-data/flow.db") as db:
                assert db.execute("SELECT COUNT(*) FROM tokens").fetchone()[0] == 0
                assert db.execute("SELECT COUNT(*) FROM cutflow_projects").fetchone()[0] == 0
                summary["emptyGoogleAccountCount"] = 0
                summary["cloudProjectRecordCount"] = 0
            summary["instanceId"] = first_instance
            summary["checks"]["empty_google_database"] = "passed"
    finally:
        if process:
            try:
                children = psutil.Process(process.pid).children(recursive=True)
            except psutil.NoSuchProcess:
                children = []
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            for child in children:
                try:
                    if Path(child.exe()).resolve().is_relative_to(stage):
                        child.terminate()
                except (psutil.NoSuchProcess,psutil.AccessDenied):
                    pass
            _,alive = psutil.wait_procs(children,timeout=5)
            for child in alive:
                try:
                    if Path(child.exe()).resolve().is_relative_to(stage):
                        child.kill()
                except (psutil.NoSuchProcess,psutil.AccessDenied):
                    pass
            summary["hostStopped"] = process.poll() is not None
        summary["existingServicesUnchanged"] = all(psutil.pid_exists(pid) and psutil.Process(pid).create_time()==started for pid,started in protected.items())
        # 所有密钥均留在隔离临时目录；报告不含配置或密钥。
        summary["protectedProcessIds"] = list(protected)
        summary["passed"] = len(summary["checks"])==7 and summary.get("hostStopped",False) and summary["existingServicesUnchanged"]
        report_path.parent.mkdir(parents=True,exist_ok=True)
        report_path.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    assert summary["passed"], summary
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",type=Path,required=True)
    parser.add_argument("--archive",type=Path,required=True)
    parser.add_argument("--stage",type=Path,required=True)
    parser.add_argument("--report",type=Path,required=True)
    options = parser.parse_args()
    print(json.dumps(run(options.host.resolve(),options.archive.resolve(),options.stage,options.report.resolve()),ensure_ascii=False))
