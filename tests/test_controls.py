import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.testclient import TestClient

from rmcpq.app import create_app
from rmcpq.controls import Launcher, LaunchInput
from rmcpq.models import BrokerError, JobInput


def test_launcher_uses_fixed_argv_without_shell_and_reports_failure(tmp_path, monkeypatch):
    import rmcpq.controls as module

    monkeypatch.setattr(module.importlib.util, "find_spec", lambda name: object())
    calls = []
    process = SimpleNamespace(pid=123, returncode=None, poll=lambda: None)

    def spawn(argv, **kwargs):
        calls.append((argv, kwargs))
        kwargs["stdout"].write(b'{"event":"ready","model":"resolved-model"}\n')
        kwargs["stdout"].flush()
        return process

    monkeypatch.setattr(module.subprocess, "Popen", spawn)
    launcher = Launcher(tmp_path / "logs", cwd=tmp_path)
    result = launcher.launch(
        LaunchInput(queues=["images", "images"], worker_id="image-worker", model="model-one"),
        "http://127.0.0.1:8791",
    )
    assert result["state"] == "running" and result["model"] == "resolved-model"
    argv, kwargs = calls[0]
    assert kwargs["shell"] is False
    assert argv[1:4] == ["-m", "rmcpq", "worker"]
    assert argv.count("--queue") == 1
    assert argv[argv.index("--model") + 1] == "model-one"
    assert launcher.list()["cwd"] == str(tmp_path)
    with pytest.raises(BrokerError) as exc:
        launcher.launch(
            LaunchInput(queues=["images"], worker_id="image-worker"), "http://127.0.0.1:8791"
        )
    assert exc.value.code == "worker_running"
    process.poll = lambda: 2
    logfile = next((tmp_path / "logs").glob("*.log"))
    logfile.write_text(
        json.dumps({"error": {"code": "subscription_required", "message": "Sign in first"}}),
        encoding="utf-8",
    )
    assert launcher.list()["runners"][0]["error"]["code"] == "subscription_required"
    assert launcher.list()["runners"][0]["model"] == "resolved-model"


def test_launcher_reports_missing_optional_sdk(tmp_path, monkeypatch):
    import rmcpq.controls as module

    monkeypatch.setattr(module.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(BrokerError) as exc:
        Launcher(tmp_path).launch(LaunchInput(queues=["a"]), "http://127.0.0.1:8791")
    assert exc.value.code == "missing_codex"


def test_dashboard_control_api_validation_origin_and_launch_dispatch(tmp_path, monkeypatch):
    app = create_app(tmp_path / "controls.sqlite")
    calls = []

    def launch(request, url):
        calls.append((request, url))
        return {"worker_id": "demo", "state": "starting"}

    monkeypatch.setattr(app.state.launcher, "launch", launch)
    with TestClient(app, base_url="http://127.0.0.1:8791") as client:
        assert client.get("/v1/runners").json()["runners"] == []
        assert (
            client.post(
                "/v1/runners", json={"queues": ["images"], "model": "model-one"}
            ).status_code
            == 202
        )
        assert calls[0][1] == "http://127.0.0.1:8791"
        assert calls[0][0].model == "model-one"
        for value in (
            {"queues": []},
            {"queues": ["bad queue"]},
            {"queues": ["a"], "sandbox": "full-access"},
            {"queues": ["a"], "command": "anything"},
        ):
            assert client.post("/v1/runners", json=value).status_code == 422
        assert client.post("/v1/runners", content="{").status_code == 415
        assert (
            client.post(
                "/v1/runners", content="{", headers={"Content-Type": "application/json"}
            ).status_code
            == 400
        )
        for path in ("/v1/runners", "/v1/workers/demo/stop"):
            assert (
                client.post(
                    path, json={"queues": ["a"]}, headers={"Origin": "https://evil.example"}
                ).status_code
                == 403
            )


@pytest.mark.anyio
async def test_http_worker_stop_releases_claim_and_fences_submission(server):
    store = server.app.state.broker.store
    registration = store.start(["a"], "worker")
    job = store.enqueue(JobInput(queue="a", instruction="Work"))[0]
    active = store.claim("worker", registration["generation"])["job"]
    async with httpx.AsyncClient(base_url=server.url, trust_env=False) as client:
        stopped = await client.post("/v1/workers/worker/stop")
        assert stopped.status_code == 200 and stopped.json()["released_jobs"] == 1
        assert (await client.get("/v1/jobs/" + job["id"])).json()["status"] == "queued"
    with pytest.raises(BrokerError):
        store.complete(job["id"], active["claim_token"], "late")


def test_search_covers_all_jobs_and_treats_sql_characters_literally(store):
    first = store.enqueue(JobInput(queue="a", instruction="Find the needle"))[0]
    for _ in range(35):
        store.enqueue(JobInput(queue="b", instruction="Other work"))
    assert store.list_jobs(q="NEEDLE")["jobs"][0]["id"] == first["id"]
    assert store.list_jobs(q=first["id"][:10])["total"] == 1
    assert store.list_jobs(q="b")["total"] >= 35
    assert store.list_jobs(q="' OR 1=1 --")["total"] == 0
    assert store.list_jobs(q="%_")["total"] == 0
