"""Native IPC regression fixture, never imports the real service."""
import json
import sys
import threading
release = threading.Event()
output_lock = threading.Lock()


def send(message):
    with output_lock:
        print(json.dumps(message), flush=True)


def finish_stream(request):
    release.wait(timeout=4)
    send({"id": request["id"], "done": True, "result": {}})


print(json.dumps({"ready": True}), flush=True)
for line in sys.stdin:
    request = json.loads(line)
    if request.get("cmd") == "shutdown":
        break
    if request.get("cmd") == "status":
        send({"id": request["id"], "done": True, "result": {"total_tokens": 1}})
        continue
    if request.get("cmd") == "release":
        release.set()
        send({"id": request["id"], "done": True, "result": {}})
        continue
    if request.get("cmd") == "stall":
        continue
    if request["params"].get("abi_stream_fixture"):
        for index in range(48):
            chunk = "data: " + json.dumps({"choices": [{"index": 0,
                "delta": {"reasoning_content": "synthetic stage " + str(index)}, "finish_reason": None}]}) + "\n\n"
            send({"id": request["id"], "stream": True, "chunk": chunk})
        media = "![Generated Image](data:image/jpeg;base64," + "A" * (5 * 1024 * 1024) + ")"
        chunk = "data: " + json.dumps({"choices": [{"index": 0,
            "delta": {"content": media}, "finish_reason": "stop"}]}) + "\n\n"
        send({"id": request["id"], "stream": True, "chunk": chunk})
        send({"id": request["id"], "stream": True, "done": True})
        continue
    if request["params"].get("progress_fixture"):
        progress = {"stage": "image_generated", "percent": 72, "basis": "stage"}
        chunk = "data: " + json.dumps({"gflow_progress": progress,
            "choices": [{"index": 0, "delta": {"gflow_progress": progress}, "finish_reason": None}]}) + "\n\n"
        send({"id": request["id"], "stream": True, "chunk": chunk})
        send({"id": request["id"], "stream": True, "done": True})
        continue
    if request["params"].get("hold_for_release"):
        send({"id": request["id"], "chunk": "started"})
        threading.Thread(target=finish_stream, args=(request,), daemon=True).start()
        continue
    if request["params"].get("stream"):
        if request["params"].get("force_eof"):
            break
        print(json.dumps({"id": request["id"], "done": True, "error": {
            "message": "synthetic upstream rejection", "status_code": 503}}), flush=True)
    else:
        # A transport loss must not cause native code to submit generation twice.
        break
