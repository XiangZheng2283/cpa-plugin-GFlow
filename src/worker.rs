use std::io::{BufRead, BufReader, Write};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

type WorkerMessage = Result<serde_json::Value, String>;
type PendingRequests = Arc<Mutex<HashMap<String, mpsc::Sender<WorkerMessage>>>>;
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct WorkerRequest {
    id: String,
    receiver: mpsc::Receiver<WorkerMessage>,
    pending: PendingRequests,
}

impl WorkerRequest {
    fn receive(&self, deadline: Instant) -> WorkerMessage {
        self.receiver.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => "GFlow backend request timed out; the operation may still be running; no automatic retry".to_string(),
                mpsc::RecvTimeoutError::Disconnected => "GFlow worker connection ended without a completion receipt; generation result is unknown".to_string(),
            })?
    }
}

impl Drop for WorkerRequest {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.id);
        }
    }
}

fn fail_pending(pending: &PendingRequests, message: &str) {
    if let Ok(mut requests) = pending.lock() {
        for (_, sender) in requests.drain() {
            let _ = sender.send(Err(message.to_string()));
        }
    }
}

#[derive(Clone)]
pub struct WorkerManager {
    inner: Arc<Mutex<WorkerInner>>,
}

struct WorkerInner {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    pending: PendingRequests,
    data_dir: PathBuf,
    #[cfg(test)]
    fixture_command: Option<(PathBuf, PathBuf)>,
}

impl WorkerManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WorkerInner {
                child: None,
                stdin: None,
                pending: Arc::new(Mutex::new(HashMap::new())),
                data_dir,
                #[cfg(test)]
                fixture_command: None,
            })),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_fixture(data_dir: PathBuf, python: PathBuf, script: PathBuf) -> Self {
        let worker = Self::new(data_dir);
        worker.inner.lock().unwrap().fixture_command = Some((python, script));
        worker
    }

    #[allow(dead_code)]
    pub fn set_data_dir(&self, dir: PathBuf) {
        if let Ok(mut lock) = self.inner.lock() {
            if lock.data_dir != dir {
                lock.data_dir = dir;
                lock.stop();
            }
        }
    }

    pub fn data_dir(&self) -> Result<PathBuf, String> {
        self.inner.lock().map(|value| value.data_dir.clone()).map_err(|error| error.to_string())
    }

    pub fn execute(&self, cmd: &str, params: serde_json::Value) -> Result<serde_json::Value, String> {
        let timeout = match cmd {
            "chat_completion" => Duration::from_secs(1800),
            "ping" | "status" | "list_tokens" | "list_models" | "get_config" => Duration::from_secs(20),
            _ => Duration::from_secs(120),
        };
        let request = self.send_request(cmd, params)?;
        let deadline = Instant::now() + timeout;
        loop {
            let response = request.receive(deadline)?;
            if let Some(error) = response.get("error") {
                return Err(error.get("message").and_then(|value| value.as_str()).unwrap_or("Worker error").to_string());
            }
            if response.get("done").and_then(|value| value.as_bool()) == Some(true) {
                return Ok(response.get("result").cloned().unwrap_or(serde_json::Value::Null));
            }
        }
    }

    pub fn execute_stream<F>(&self, params: serde_json::Value, mut on_chunk: F) -> Result<(), String>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        let request = self.send_request("chat_completion", params)?;
        let deadline = Instant::now() + Duration::from_secs(1800);
        loop {
            let response = request.receive(deadline)?;
            if let Some(error) = response.get("error") {
                return Err(error.get("message").and_then(|value| value.as_str()).unwrap_or("Stream generation error").to_string());
            }
            if response.get("done").and_then(|value| value.as_bool()) == Some(true) {
                return Ok(());
            }
            if let Some(chunk) = response.get("chunk").and_then(|value| value.as_str()) {
                on_chunk(chunk)?;
            }
        }
    }

    fn send_request(&self, command: &str, params: serde_json::Value) -> Result<WorkerRequest, String> {
        let mut inner = self.inner.lock().map_err(|error| error.to_string())?;
        inner.ensure_started()?;
        let id = format!("req_{}_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(), REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = mpsc::channel();
        let request = WorkerRequest { id: id.clone(), receiver, pending: inner.pending.clone() };
        inner.pending.lock().map_err(|error| error.to_string())?.insert(id.clone(), sender);
        let mut payload = serde_json::to_vec(&serde_json::json!({"id": id, "cmd": command, "params": params}))
            .map_err(|error| error.to_string())?;
        payload.push(b'\n');
        let stdin = inner.stdin.as_mut().ok_or_else(|| "Worker stdin unavailable".to_string())?;
        let result = stdin.write_all(&payload).and_then(|_| stdin.flush());
        if let Err(error) = result {
            inner.stop();
            return Err(format!("GFlow worker write failed; request was not retried: {error}"));
        }
        Ok(request)
    }

    pub fn shutdown(&self) {
        if let Ok(mut lock) = self.inner.lock() {
            lock.stop();
        }
    }
}

impl WorkerInner {
    fn ensure_started(&mut self) -> Result<(), String> {
        if let Some(child) = self.child.as_mut() {
            if let Ok(Some(_)) = child.try_wait() {
                self.stop();
            } else {
                return Ok(());
            }
        }

        #[cfg(test)]
        let fixture = self.fixture_command.clone();
        #[cfg(not(test))]
        let fixture: Option<(PathBuf, PathBuf)> = None;
        let temporary = self.data_dir.join("tmp");
        let logs = self.data_dir.join("logs");
        std::fs::create_dir_all(&temporary).map_err(|error| error.to_string())?;
        std::fs::create_dir_all(&logs).map_err(|error| error.to_string())?;
        let diagnostics = std::fs::OpenOptions::new().create(true).append(true)
            .open(logs.join("worker.log")).map_err(|error| error.to_string())?;
        let mut cmd = if let Some((python, script)) = fixture {
            let mut command = Command::new(python);
            command.arg(script);
            command
        } else {
            Command::new(crate::runtime::prepare_worker(&self.data_dir)?)
        };
        cmd.arg(&self.data_dir);
        cmd.current_dir(&self.data_dir);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::from(diagnostics));
        cmd.env("PYTHONDONTWRITEBYTECODE", "1");
        cmd.env("TEMP", &temporary).env("TMP", &temporary).env("TMPDIR", &temporary);
        cmd.env("PERSONAL_BROWSER_USER_DATA_DIR", self.data_dir.join("browser-profile"));
        cmd.env_remove("PYTHONHOME").env_remove("PYTHONPATH");

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x08000000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        let mut child = cmd.spawn().map_err(|e| {
            format!("Failed to start bundled GFlow worker: {}", e)
        })?;

        let stdin = child.stdin.take().ok_or_else(|| "Failed to capture stdin".to_string())?;
        let stdout = child.stdout.take().ok_or_else(|| "Failed to capture stdout".to_string())?;
        self.pending = Arc::new(Mutex::new(HashMap::new()));
        let pending = self.pending.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut ready_line = String::new();
            let result = reader.read_line(&mut ready_line).map(|_| ready_line);
            if sender.send(result).is_err() { return; }
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
                if line.trim().is_empty() { continue; }
                let Ok(response) = serde_json::from_str::<serde_json::Value>(line.trim()) else { break; };
                let Some(id) = response.get("id").and_then(|value| value.as_str()) else { continue; };
                let finished = response.get("done").and_then(|value| value.as_bool()) == Some(true) || response.get("error").is_some();
                let recipient = pending.lock().ok().and_then(|mut requests| {
                    if finished { requests.remove(id) } else { requests.get(id).cloned() }
                });
                if let Some(recipient) = recipient { let _ = recipient.send(Ok(response)); }
            }
            fail_pending(&pending, "GFlow worker connection ended without a completion receipt; generation result is unknown; no automatic retry");
        });
        let ready_line = match receiver.recv_timeout(std::time::Duration::from_secs(90)) {
            Ok(Ok(result)) => result,
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("Bundled worker did not become ready within 90 seconds; see gflow-data/logs/worker.log".to_string());
            }
        };
        let ready: serde_json::Value = serde_json::from_str(ready_line.trim()).unwrap_or_default();
        if ready.get("ready").and_then(|value| value.as_bool()) != Some(true) {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Bundled worker startup failed; see gflow-data/logs/worker.log".to_string());
        }

        self.child = Some(child);
        self.stdin = Some(stdin);

        Ok(())
    }

    fn stop(&mut self) {
        fail_pending(&self.pending, "GFlow worker stopped before a completion receipt; generation result is unknown; no automatic retry");
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.write_all(b"{\"cmd\":\"shutdown\"}\n");
            let _ = stdin.flush();
        }
        if let Some(mut child) = self.child.take() {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
            while matches!(child.try_wait(), Ok(None)) && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            if matches!(child.try_wait(), Ok(None)) {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }

}

impl Drop for WorkerInner {
    fn drop(&mut self) {
        self.stop();
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> WorkerManager {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let worker = WorkerManager::new(root.join(".build/native-tests").join(nonce.to_string()));
        worker.inner.lock().unwrap().fixture_command = Some((
            PathBuf::from(std::env::var("GFLOW_TEST_PYTHON").unwrap_or_else(|_| "python".to_string())),
            root.join("tests/worker_failure_fixture.py"),
        ));
        worker
    }

    #[test]
    fn upstream_failure_is_an_executor_error() {
        let worker = fixture();
        let result = worker.execute_stream(serde_json::json!({"stream": true}), |_| Ok(()));
        assert_eq!(result.unwrap_err(), "synthetic upstream rejection");
        worker.shutdown();
    }

    #[test]
    fn unexpected_stream_eof_is_not_a_success() {
        let worker = fixture();
        let result = worker.execute_stream(serde_json::json!({"stream": true, "force_eof": true}), |_| Ok(()));
        assert!(result.unwrap_err().contains("without a completion receipt"));
        worker.shutdown();
    }

    #[test]
    fn progress_metadata_survives_native_stream_transport() {
        let worker = fixture();
        let mut chunks = Vec::new();
        worker.execute_stream(serde_json::json!({"stream": true, "progress_fixture": true}), |chunk| {
            chunks.push(chunk.to_string());
            Ok(())
        }).unwrap();
        assert_eq!(chunks.len(), 1);
        let value: serde_json::Value = serde_json::from_str(chunks[0].strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(value["gflow_progress"], serde_json::json!({"stage":"image_generated", "percent":72, "basis":"stage"}));
        assert_eq!(value["choices"][0]["delta"]["gflow_progress"], value["gflow_progress"]);
        assert!(value["choices"][0]["delta"].get("content").is_none());
        worker.shutdown();
    }

    #[test]
    fn management_status_is_not_blocked_by_a_stream() {
        let worker = fixture();
        let streaming = worker.clone();
        let (started, ready) = mpsc::channel();
        let generation = std::thread::spawn(move || streaming.execute_stream(
            serde_json::json!({"stream": true, "hold_for_release": true}),
            |_| { let _ = started.send(()); Ok(()) },
        ));
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let management = worker.clone();
        let (sender, receiver) = mpsc::channel();
        let lookup = std::thread::spawn(move || {
            let _ = sender.send(management.execute("status", serde_json::json!({})));
        });
        let response = receiver.recv_timeout(Duration::from_secs(2)).expect("status was blocked by generation").unwrap();
        assert_eq!(response["total_tokens"], 1);
        worker.execute("release", serde_json::json!({})).unwrap();
        lookup.join().unwrap();
        generation.join().unwrap().unwrap();
        worker.shutdown();
    }

    #[test]
    fn timed_out_reader_does_not_stop_other_requests() {
        let worker = fixture();
        let pending = worker.send_request("stall", serde_json::json!({})).unwrap();
        assert!(pending.receive(Instant::now() + Duration::from_millis(20)).unwrap_err().contains("timed out"));
        drop(pending);
        assert_eq!(worker.execute("status", serde_json::json!({})).unwrap()["total_tokens"], 1);
        worker.shutdown();
    }
}
