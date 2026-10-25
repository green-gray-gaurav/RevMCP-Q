import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

from rmcpq.models import JobInput


def cli(server, *arguments, input=None):
    return subprocess.run(
        [sys.executable, "-m", "rmcpq", "--url", server.url, *arguments],
        input=input,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )


def test_enqueue_stdin_and_files_json_and_human_output(server, tmp_path):
    context = tmp_path / "context.json"
    context.write_text('{"name":"Alex"}', encoding="utf-8")
    queued = cli(
        server,
        "enqueue",
        "--queue",
        "a",
        "--context-file",
        str(context),
        "--json",
        input="Summarize café",
    )
    assert queued.returncode == 0, queued.stderr
    job = json.loads(queued.stdout)
    assert job["instruction"] == "Summarize café" and job["context"] == {"name": "Alex"}
    instruction = tmp_path / "instruction.txt"
    instruction.write_text("Extract the contact", encoding="utf-8")
    second = cli(
        server,
        "--json",
        "enqueue",
        "--queue",
        "b",
        "--instruction-file",
        str(instruction),
        "--result-format",
        "json",
    )
    assert second.returncode == 0
    assert json.loads(second.stdout)["instruction"] == "Extract the contact"
    status = cli(server, "status", job["id"])
    assert "queued" in status.stdout and job["id"] in status.stdout
    assert cli(server, "queues", "--json").returncode == 0
    assert json.loads(cli(server, "workers", "--json").stdout) == {"workers": []}


def test_cli_results_wait_failures_cancel_and_retry(server):
    store = server.app.state.broker.store
    job, _ = store.enqueue(JobInput(queue="a", instruction="Answer"))
    registration = store.start(["a"], "worker")
    claimed = store.claim("worker", registration["generation"])["job"]
    pending = cli(server, "result", job["id"], "--json")
    assert pending.returncode == 1 and json.loads(pending.stdout)["status"] == "processing"
    store.complete(job["id"], claimed["claim_token"], "A useful answer")
    assert cli(server, "result", job["id"]).stdout.strip() == "A useful answer"
    completed = cli(server, "wait", job["id"], "--json")
    assert completed.returncode == 0 and json.loads(completed.stdout)["result"] == "A useful answer"
    next_job, _ = store.enqueue(JobInput(queue="a", instruction="Another"))
    claimed = store.claim("worker", registration["generation"])["job"]
    store.fail(next_job["id"], claimed["claim_token"], "Cannot read source")
    failed = cli(server, "wait", next_job["id"], "--json")
    assert failed.returncode == 1 and json.loads(failed.stdout)["status"] == "failed"
    retried = cli(server, "retry", next_job["id"], "--json")
    assert retried.returncode == 0 and json.loads(retried.stdout)["status"] == "queued"
    assert cli(server, "cancel", next_job["id"]).returncode == 0
    cancelled = cli(server, "wait", next_job["id"], "--json")
    assert cancelled.returncode == 1 and json.loads(cancelled.stdout)["status"] == "cancelled"


def test_cli_wait_timeout_and_invalid_input(server, tmp_path):
    queued = json.loads(
        cli(server, "enqueue", "--queue", "a", "--instruction", "Answer", "--json").stdout
    )
    timed = cli(
        server, "wait", queued["id"], "--timeout", "0.05", "--poll-interval", "0.01", "--json"
    )
    assert timed.returncode == 124
    assert json.loads(timed.stdout)["error"]["code"] == "wait_timeout"
    invalid = cli(
        server, "enqueue", "--queue", "bad queue", "--instruction", "private contents", "--json"
    )
    assert invalid.returncode == 2 and "private contents" not in invalid.stdout
    assert cli(server, "status", "missing", "--json").returncode == 2
    assert cli(server, "wait", queued["id"], "--timeout", "nan").returncode == 2
    assert (
        cli(
            server, "enqueue", "--queue", "a", "--instruction-file", str(tmp_path / "absent")
        ).returncode
        == 2
    )


def test_cli_wait_deadline_includes_slow_http_requests(server):
    job, _ = server.app.state.broker.store.enqueue(JobInput(queue="a", instruction="Answer"))
    original = server.app.state.broker.call

    async def slow_read(method, *args, **kwargs):
        if method == "get_job":
            await asyncio.sleep(2)
        return await original(method, *args, **kwargs)

    server.app.state.broker.call = slow_read
    started = time.monotonic()
    expired = cli(server, "wait", job["id"], "--timeout", "0.05", "--json")
    assert expired.returncode == 124, expired.stderr
    assert json.loads(expired.stdout)["error"]["code"] == "wait_timeout"
    assert time.monotonic() - started < 1.8


def test_cli_native_json_result(server):
    store = server.app.state.broker.store
    job, _ = store.enqueue(JobInput(queue="json", instruction="Extract", result_format="json"))
    worker = store.start(["json"], "json-worker")
    claimed = store.claim("json-worker", worker["generation"])["job"]
    store.complete(job["id"], claimed["claim_token"], {"items": ["one", 2], "empty": None})
    result = cli(server, "result", job["id"], "--json")
    assert result.returncode == 0
    assert json.loads(result.stdout)["result"] == {"items": ["one", 2], "empty": None}


def test_cli_invalid_url_returns_json_error(server):
    invalid = cli(server, "--url", "http://[", "status", "missing", "--json")
    assert invalid.returncode == 2
    assert json.loads(invalid.stdout)["error"]["code"] == "client_error"
    assert "Traceback" not in invalid.stderr


def test_packaged_cli_serve_runs_real_broker(tmp_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    executable = str(Path(sys.executable).parent / "rmcpq.exe")
    if os.name != "nt":
        executable = str(Path(sys.executable).parent / "rmcpq")
    process = subprocess.Popen(
        [executable, "serve", "--port", str(port), "--db", str(tmp_path / "cli.sqlite")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 10
        with httpx.Client(trust_env=False) as client:
            while True:
                assert process.poll() is None, "CLI server exited during startup"
                try:
                    response = client.get(f"http://127.0.0.1:{port}/v1/health", timeout=0.5)
                    if response.status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                assert time.monotonic() < deadline, "CLI server did not become ready"
                time.sleep(0.05)
            assert response.json()["ok"]
    finally:
        process.terminate()
        process.communicate(timeout=5)
