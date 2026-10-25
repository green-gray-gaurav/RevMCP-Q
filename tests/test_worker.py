"""Runner tests use real HTTP/MCP transports and a deterministic host."""

import asyncio
import json
from contextlib import suppress

import httpx
import pytest

from rmcpq.hosts.codex import validate_subscription
from rmcpq.models import JobInput
from rmcpq.worker import QueueClient, Runner, WorkerError, WorkerOptions, parse_result
from rmcpq.worker_state import Journal, WorkerLock, identity_key

pytestmark = pytest.mark.anyio


class Host:
    def __init__(self, *, response="answer", delay=0, reject=False):
        self.response, self.delay, self.reject = response, delay, reject
        self.executions = 0
        self.preflights = 0
        self.active = False
        self.interrupted = asyncio.Event()
        self.started = asyncio.Event()

    async def preflight(self):
        self.preflights += 1
        if self.reject:
            raise WorkerError("subscription_required", "Subscription required", fatal=True)

    async def execute(self, job, saved, record):
        self.executions += 1
        self.active = True
        await record({"phase": "running", "thread_id": "thread-1", "turn_id": "turn-1"})
        self.started.set()
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            raise
        self.active = False
        return self.response

    async def interrupt(self):
        if self.active:
            self.active = False
            self.interrupted.set()


def runner(server, tmp_path, host, **overrides):
    options = WorkerOptions(
        url=server.url, queues=["a"], cwd=tmp_path, wait_seconds=0.15, max_jobs=1, **overrides
    )
    return Runner(options, host, Journal(tmp_path / "journal.sqlite"), poll_seconds=0.04)


def enqueue(server, **payload):
    return server.app.state.broker.store.enqueue(
        JobInput(queue="a", instruction="private task", **payload)
    )[0]


async def until(check, timeout=5):
    async with asyncio.timeout(timeout):
        while not check():
            await asyncio.sleep(0.02)


async def test_idle_waits_do_not_invoke_host_and_shutdown_cleans_registration(server, tmp_path):
    host = Host()
    worker = runner(server, tmp_path, host)
    task = asyncio.create_task(worker.run())
    await until(lambda: host.preflights >= 4)
    assert host.executions == 0
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    assert server.app.state.broker.store.worker("codex-worker")["running"] is False
    await until(lambda: not server.app.state.broker.pending)


async def test_wrong_auth_rejected_before_registration_or_claim(server, tmp_path):
    job = enqueue(server)
    with pytest.raises(WorkerError, match="Subscription required"):
        await runner(server, tmp_path, Host(reject=True)).run()
    assert server.app.state.broker.store.list_workers()["workers"] == []
    assert server.app.state.broker.store.get_job(job["id"])["attempts"] == 0


async def test_worker_queue_conflict_keeps_domain_error_and_original_registration(server):
    queue = QueueClient(server.url)
    await queue.tool("start", queues=["summaries"], worker_id="existing")
    with pytest.raises(WorkerError) as exc:
        await queue.tool("start", queues=["images"], worker_id="existing")
    assert exc.value.code == "worker_scope_conflict"
    assert "--worker-id" in exc.value.message
    saved = server.app.state.broker.store.worker("existing")
    assert saved["running"] and saved["queues"] == ["summaries"]


async def test_json_result_and_journal_omit_claim_token(server, tmp_path):
    job = enqueue(server, result_format="json")
    host = Host(response='{"name":"Alex"}')
    worker = runner(server, tmp_path, host)
    await worker.run()
    saved = server.app.state.broker.store.get_job(job["id"])
    assert saved["result"] == {"name": "Alex"} and saved["status"] == "completed"
    state = worker.journal.read(job["id"], 1)
    assert state["phase"] == "completed"
    assert state["thread_id"] == "thread-1" and state["turn_id"] == "turn-1"
    assert "claim_token" not in json.dumps(state)
    assert host.executions == 1


async def test_effective_host_model_registered_and_saved_per_job(server, tmp_path):
    job = enqueue(server)
    host = Host()
    host.model = "resolved-model"
    worker = runner(server, tmp_path, host)
    events = []
    worker.emit = events.append
    await worker.run()
    store = server.app.state.broker.store
    assert store.worker("codex-worker")["model"] == "resolved-model"
    assert store.get_job(job["id"])["model"] == "resolved-model"
    assert store.get_job(job["id"])["history"][0]["model"] == "resolved-model"
    assert next(e for e in events if e["event"] == "ready")["model"] == "resolved-model"


async def test_runner_renews_lease_while_model_is_busy(server_factory, tmp_path):
    server = server_factory(lease_seconds=2, sweep_seconds=0.04)
    job = enqueue(server)
    await runner(server, tmp_path, Host(delay=3)).run()
    saved = server.app.state.broker.store.get_job(job["id"])
    assert saved["status"] == "completed" and saved["attempts"] == 1


@pytest.mark.parametrize("action", ["cancel", "stop"])
async def test_cancellation_or_stop_interrupts_host_and_rejects_result(server, tmp_path, action):
    job = enqueue(server)
    host = Host(delay=20)
    worker = runner(server, tmp_path, host)
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(host.started.wait(), 3)
    if action == "cancel":
        async with httpx.AsyncClient(base_url=server.url, trust_env=False) as client:
            response = await client.post(f"/v1/jobs/{job['id']}/cancel")
            assert response.status_code == 200
    else:
        await QueueClient(server.url).tool("stop", worker_id="codex-worker")
    await asyncio.wait_for(task, 5)
    assert host.interrupted.is_set()
    saved = server.app.state.broker.store.get_job(job["id"])
    assert saved["result"] is None
    assert saved["status"] == ("cancelled" if action == "cancel" else "queued")


async def test_job_timeout_interrupts_and_records_terminal_failure(server, tmp_path):
    job = enqueue(server)
    host = Host(delay=20)
    await runner(server, tmp_path, host, job_timeout=0.2).run()
    saved = server.app.state.broker.store.get_job(job["id"])
    assert host.interrupted.is_set() and saved["status"] == "failed"
    assert saved["error"].startswith("job_timeout:")


async def test_completed_journal_recovers_claim_after_broker_restart_without_model(
    server_factory, tmp_path
):
    path = tmp_path / "broker.sqlite"
    first = server_factory(path)
    job = enqueue(first)
    store = first.app.state.broker.store
    registration = store.start(["a"], "codex-worker")
    store.claim("codex-worker", registration["generation"])
    worker = runner(first, tmp_path, Host())
    worker.journal.write(
        job["id"],
        1,
        {
            "phase": "completed",
            "result": "recovered",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "settings": {"cwd": str(tmp_path), "model": None, "sandbox": "workspace-write"},
        },
    )
    first.close()
    second = server_factory(path)
    # Journal normally keys by URL; keep a stable broker URL when restarting in
    # production. This fixture deliberately supplies the same journal to a new port.
    worker.options.url = second.url
    worker.queue = QueueClient(second.url)
    await worker.run()
    saved = second.app.state.broker.store.get_job(job["id"])
    assert saved["result"] == "recovered" and saved["attempts"] == 1
    assert worker.host.executions == 0


async def test_lost_submission_response_retries_identically(server, tmp_path, monkeypatch):
    import rmcpq.worker as module

    job = enqueue(server)
    worker = runner(server, tmp_path, Host())
    original = module.Client
    submissions = []

    class LoseOneResponse:
        def __init__(self, *args, **kwargs):
            self.client = original(*args, **kwargs)

        async def __aenter__(self):
            await self.client.__aenter__()
            return self

        async def __aexit__(self, *args):
            return await self.client.__aexit__(*args)

        async def call_tool(self, name, arguments):
            result = await self.client.call_tool(name, arguments)
            if name == "submit_result":
                submissions.append(arguments)
                if len(submissions) == 1:
                    raise OSError("Simulated lost submission response")
            return result

    monkeypatch.setattr(module, "Client", LoseOneResponse)
    await worker.run()
    assert len(submissions) == 2 and submissions[0] == submissions[1]
    assert worker.host.executions == 1
    assert server.app.state.broker.store.get_job(job["id"])["attempts"] == 1


async def test_invalid_json_fails_without_overwriting_result(server, tmp_path):
    job = enqueue(server, result_format="json")
    await runner(server, tmp_path, Host(response="```json\n{}\n```")).run()
    saved = server.app.state.broker.store.get_job(job["id"])
    assert saved["status"] == "failed" and saved["result"] is None


def test_subscription_validation_fails_closed():
    config = {"forced_login_method": "chatgpt", "model_provider": "openai"}
    validate_subscription({"account": {"type": "chatgpt"}}, config)
    for account in (None, {"account": None}, {"account": {"type": "apiKey"}}):
        with pytest.raises(WorkerError):
            validate_subscription(account, config)
    with pytest.raises(WorkerError, match="override"):
        validate_subscription(
            {"account": {"type": "chatgpt"}},
            {**config, "model_providers": {"openai": {"env_key": "KEY"}}},
        )


def test_process_lock_and_validation(tmp_path):
    assert identity_key("http://localhost:8790", "w") == identity_key("http://127.0.0.1:8790", "w")
    assert identity_key("http://localhost:8790", "w") != identity_key("http://localhost:8791", "w")
    with WorkerLock(tmp_path, "worker"):
        with pytest.raises(ValueError, match="already running"):
            with WorkerLock(tmp_path, "worker"):
                pass
    with WorkerLock(tmp_path, "worker"):
        pass
    for url in ("http://example.org", "http://user@localhost:8790", "http://localhost:8790/path"):
        with pytest.raises(ValueError):
            WorkerOptions(url=url, queues=["a"], cwd=tmp_path).validate()
    with pytest.raises(WorkerError):
        parse_result("NaN", "json")


async def test_two_autonomous_workers_only_process_subscribed_queues(server, tmp_path):
    store = server.app.state.broker.store
    a = enqueue(server)
    b = store.enqueue(JobInput(queue="b", instruction="Extract", result_format="json"))[0]
    first = runner(server, tmp_path, Host(response="summary"))
    options = WorkerOptions(
        url=server.url,
        queues=["b"],
        worker_id="extractor",
        cwd=tmp_path,
        max_jobs=1,
        wait_seconds=0.2,
    )
    second = Runner(options, Host(response='{"ok":true}'), Journal(tmp_path / "other.sqlite"))
    await asyncio.gather(first.run(), second.run())
    assert store.get_job(a["id"])["result"] == "summary"
    assert store.get_job(b["id"])["result"] == {"ok": True}
    assert store.get_job(a["id"])["worker_id"] == "codex-worker"
    assert store.get_job(b["id"])["worker_id"] == "extractor"
