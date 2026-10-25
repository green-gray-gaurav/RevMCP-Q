import asyncio
import json
import time
from contextlib import suppress

import httpx
import pytest
from mcp import Client
from starlette.testclient import TestClient

from rmcpq.app import create_app

pytestmark = pytest.mark.anyio


async def tool(client, name, **arguments):
    result = await client.call_tool(name, arguments)
    assert not result.is_error, result.content
    return result.structured_content


async def eventually(check, timeout=3):
    deadline = time.monotonic() + timeout
    while not await check():
        if time.monotonic() >= deadline:
            raise AssertionError("Condition did not become true")
        await asyncio.sleep(0.02)


async def test_real_mcp_wait_wakes_and_producer_reads_result(server):
    async with Client(server.url + "/mcp", read_timeout_seconds=5) as worker:
        ready = await tool(
            worker, "start", queues=["summaries"], worker_id="worker", model="test-model"
        )
        assert ready["model"] == "test-model"
        assert ready["next_action"]["tool"] == "wait_for_job"
        pending = asyncio.create_task(
            tool(worker, "wait_for_job", worker_id="worker", wait_seconds=2)
        )
        async with httpx.AsyncClient(base_url=server.url, trust_env=False) as producer:

            async def is_waiting():
                return (await producer.get("/v1/workers")).json()["workers"][0][
                    "state"
                ] == "waiting"

            await eventually(is_waiting)
            response = await producer.post(
                "/v1/jobs",
                json={
                    "queue": "summaries",
                    "instruction": "Summarize",
                    "context": {"text": "A short article"},
                },
            )
            assert response.status_code == 201
            job = response.json()
            answer = await pending
            assert answer["job"]["id"] == job["id"]
            token = answer["job"]["claim_token"]
            renewed = await tool(worker, "renew_job", job_id=job["id"], claim_token=token)
            assert renewed["lease_until"]
            submitted = await tool(
                worker,
                "submit_result",
                job_id=job["id"],
                claim_token=token,
                result="A useful summary",
            )
            assert submitted["next_action"]["tool"] == "wait_for_job"
            assert (
                await tool(
                    worker,
                    "submit_result",
                    job_id=job["id"],
                    claim_token=token,
                    result="A useful summary",
                )
                == submitted
            )
            saved = (await producer.get("/v1/jobs/" + job["id"])).json()
            assert saved["model"] == "test-model"
            assert saved["history"][0]["model"] == "test-model"
            assert saved["result"] == "A useful summary"
            assert saved["history"][0]["status"] == "completed"
            assert token not in json.dumps(saved)
            conflicting = await worker.call_tool(
                "submit_result", {"job_id": job["id"], "claim_token": token, "result": "Changed"}
            )
            assert conflicting.is_error
            assert conflicting.structured_content["error"]["code"] == "stale_claim"
            idle = await tool(worker, "wait_for_job", worker_id="worker", wait_seconds=0)
            assert idle["status"] == "idle" and idle["next_action"]
            await tool(worker, "stop", worker_id="worker")


async def test_two_producers_two_workers_and_broker_restart(server_factory, tmp_path):
    db_path = tmp_path / "restart.sqlite"
    first = server_factory(db_path)
    async with (
        httpx.AsyncClient(base_url=first.url, trust_env=False) as p1,
        httpx.AsyncClient(base_url=first.url, trust_env=False) as p2,
    ):
        text, structured = await asyncio.gather(
            p1.post("/v1/jobs", json={"queue": "summaries", "instruction": "Summarize"}),
            p2.post(
                "/v1/jobs",
                json={"queue": "extraction", "instruction": "Extract", "result_format": "json"},
            ),
        )
        text_id, json_id = text.json()["id"], structured.json()["id"]
    # Register durable worker identities, then end their protocol sessions before restarting.
    async with Client(first.url + "/mcp") as w1, Client(first.url + "/mcp") as w2:
        await tool(w1, "start", queues=["summaries"], worker_id="summarizer")
        await tool(w2, "start", queues=["extraction"], worker_id="extractor")
    first.close()
    second = server_factory(db_path)
    async with Client(second.url + "/mcp") as w1, Client(second.url + "/mcp") as w2:
        a, b = await asyncio.gather(
            tool(w1, "wait_for_job", worker_id="summarizer", wait_seconds=0),
            tool(w2, "wait_for_job", worker_id="extractor", wait_seconds=0),
        )
        assert a["job"]["id"] == text_id and b["job"]["id"] == json_id
        await asyncio.gather(
            tool(
                w1,
                "submit_result",
                job_id=text_id,
                claim_token=a["job"]["claim_token"],
                result="Summary",
            ),
            tool(
                w2,
                "submit_result",
                job_id=json_id,
                claim_token=b["job"]["claim_token"],
                result={"name": "Alex", "email": "alex@example.org"},
            ),
        )
    async with httpx.AsyncClient(base_url=second.url, trust_env=False) as producer:
        assert (await producer.get(f"/v1/jobs/{text_id}")).json()["result"] == "Summary"
        assert (await producer.get(f"/v1/jobs/{json_id}")).json()["result"]["name"] == "Alex"


async def test_explicit_mcp_cancellation_cleans_wait_and_same_worker_can_resume(server):
    async with Client(server.url + "/mcp", read_timeout_seconds=5) as worker:
        await tool(worker, "start", queues=["a"], worker_id="cancellable")
        async with httpx.AsyncClient(base_url=server.url, trust_env=False) as producer:

            async def is_waiting():
                return (await producer.get("/v1/workers")).json()["workers"][0][
                    "state"
                ] == "waiting"

            pending = asyncio.create_task(
                worker.call_tool("wait_for_job", {"worker_id": "cancellable", "wait_seconds": 4})
            )
            await eventually(is_waiting)
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending

            async def cleared():
                return not await is_waiting()

            await eventually(cleared)
            result = await tool(worker, "wait_for_job", worker_id="cancellable", wait_seconds=0)
            assert result["status"] == "idle"


async def test_stop_wakes_wait_and_fences_active_claim(server):
    async with (
        Client(server.url + "/mcp", read_timeout_seconds=5) as worker,
        Client(server.url + "/mcp", read_timeout_seconds=5) as controller,
    ):
        await tool(worker, "start", queues=["a"], worker_id="stoppable")
        pending = asyncio.create_task(
            tool(worker, "wait_for_job", worker_id="stoppable", wait_seconds=4)
        )
        await asyncio.sleep(0.1)
        duplicate = await controller.call_tool(
            "wait_for_job", {"worker_id": "stoppable", "wait_seconds": 0}
        )
        assert duplicate.is_error
        await tool(controller, "stop", worker_id="stoppable")
        assert (await pending)["status"] == "stopped"
        await tool(worker, "start", queues=["a"], worker_id="stoppable")
        async with httpx.AsyncClient(base_url=server.url, trust_env=False) as producer:
            job = (
                await producer.post("/v1/jobs", json={"queue": "a", "instruction": "Answer"})
            ).json()
            got = (await tool(worker, "wait_for_job", worker_id="stoppable", wait_seconds=0))["job"]
            await tool(controller, "stop", worker_id="stoppable")
            stale = await worker.call_tool(
                "submit_result",
                {"job_id": job["id"], "claim_token": got["claim_token"], "result": "Late"},
            )
            assert stale.is_error
            assert (await producer.get("/v1/jobs/" + job["id"])).json()["status"] == "queued"


async def test_cancel_retry_failure_and_result_types_over_real_transports(server):
    async with (
        Client(server.url + "/mcp") as worker,
        httpx.AsyncClient(base_url=server.url, trust_env=False) as producer,
    ):
        await tool(worker, "start", queues=["a"], worker_id="worker")
        job = (await producer.post("/v1/jobs", json={"queue": "a", "instruction": "Answer"})).json()
        got = (await tool(worker, "wait_for_job", worker_id="worker", wait_seconds=0))["job"]
        invalid = await worker.call_tool(
            "submit_result",
            {"job_id": job["id"], "claim_token": got["claim_token"], "result": {"wrong": "type"}},
        )
        assert invalid.is_error
        assert (await producer.post(f"/v1/jobs/{job['id']}/cancel")).json()["status"] == "cancelled"
        stale = await worker.call_tool(
            "renew_job", {"job_id": job["id"], "claim_token": got["claim_token"]}
        )
        assert stale.is_error
        assert (await producer.post(f"/v1/jobs/{job['id']}/retry")).json()["run"] == 2
        got = (await tool(worker, "wait_for_job", worker_id="worker", wait_seconds=0))["job"]
        failed = await tool(
            worker,
            "fail_job",
            job_id=job["id"],
            claim_token=got["claim_token"],
            error="Missing source",
        )
        assert failed["job"]["status"] == "failed" and failed["next_action"]
        invalid_wait = await worker.call_tool(
            "wait_for_job", {"worker_id": "worker", "wait_seconds": 86401}
        )
        assert invalid_wait.is_error


async def test_24_hour_wait_survives_old_25_second_cutoff_and_repeats(server):
    async with (
        Client(server.url + "/mcp", read_timeout_seconds=40) as worker,
        httpx.AsyncClient(base_url=server.url, trust_env=False) as producer,
    ):
        ready = await tool(
            worker, "start", queues=["long-wait"], worker_id="day-worker", wait_seconds=86400
        )
        assert ready["wait_seconds"] == 86400
        assert ready["next_action"]["arguments"]["wait_seconds"] == 86400
        # Omit the override: the saved worker preference must control the actual request.
        pending = asyncio.create_task(tool(worker, "wait_for_job", worker_id="day-worker"))
        await asyncio.sleep(26)
        assert not pending.done(), "A 24-hour wait ended at the former 25-second default"
        response = await producer.post(
            "/v1/jobs", json={"queue": "long-wait", "instruction": "Answer"}
        )
        assert response.status_code == 201
        claimed = (await asyncio.wait_for(pending, 3))["job"]
        assert claimed["id"] == response.json()["id"]
        submitted = await tool(
            worker,
            "submit_result",
            job_id=claimed["id"],
            claim_token=claimed["claim_token"],
            result="Ready",
        )
        assert submitted["next_action"]["arguments"]["wait_seconds"] == 86400
        # Nonblocking probes must not replace the configured duration with zero.
        idle = await tool(worker, "wait_for_job", worker_id="day-worker", wait_seconds=0)
        assert idle["next_action"]["arguments"]["wait_seconds"] == 86400


async def test_custom_wait_preserved_after_failures_idle_and_resume(server):
    async with (
        Client(server.url + "/mcp") as worker,
        httpx.AsyncClient(base_url=server.url, trust_env=False) as producer,
    ):
        ready = await tool(worker, "start", queues=["custom"], worker_id="custom", wait_seconds=600)
        assert ready["next_action"]["arguments"]["wait_seconds"] == 600
        await producer.post("/v1/jobs", json={"queue": "custom", "instruction": "Answer"})
        claimed = (await tool(worker, "wait_for_job", worker_id="custom", wait_seconds=0))["job"]
        failed = await tool(
            worker,
            "fail_job",
            job_id=claimed["id"],
            claim_token=claimed["claim_token"],
            error="Missing input",
        )
        assert failed["next_action"]["arguments"]["wait_seconds"] == 600
        idle = await tool(worker, "wait_for_job", worker_id="custom", wait_seconds=0.01)
        assert idle["next_action"]["arguments"]["wait_seconds"] == 600
        resumed = await tool(worker, "start", queues=["custom"], worker_id="custom")
        assert resumed["wait_seconds"] == 600
        for duration in (0, -1, 86401):
            invalid = await worker.call_tool(
                "start", {"queues": ["custom"], "wait_seconds": duration}
            )
            assert invalid.is_error


async def test_periodic_recovery_runs_without_queue_access(server_factory):
    server = server_factory(lease_seconds=0.05, sweep_seconds=0.02)
    async with (
        Client(server.url + "/mcp") as worker,
        httpx.AsyncClient(base_url=server.url, trust_env=False) as producer,
    ):
        await tool(worker, "start", queues=["a"], worker_id="worker")
        await producer.post("/v1/jobs", json={"queue": "a", "instruction": "Answer"})
        await tool(worker, "wait_for_job", worker_id="worker", wait_seconds=0)
        await asyncio.sleep(0.2)
        # Direct SQL bypasses read-side recovery, proving the background sweep ran.
        import sqlite3

        with sqlite3.connect(server.app.state.broker.store.path) as db:
            assert db.execute("SELECT status FROM jobs").fetchone()[0] == "queued"


async def test_sweeper_recovers_after_transient_storage_failure(tmp_path):
    import sqlite3

    app = create_app(tmp_path / "transient.sqlite", sweep_seconds=0.01)
    broker = app.state.broker
    original = broker.call
    attempts = 0
    successful = asyncio.Event()

    async def intermittent(method, *args, **kwargs):
        nonlocal attempts
        if method == "recover_expired":
            attempts += 1
            if attempts == 1:
                raise sqlite3.OperationalError("temporary storage failure")
            successful.set()
        return await original(method, *args, **kwargs)

    broker.call = intermittent
    async with app.router.lifespan_context(app):
        await asyncio.wait_for(successful.wait(), 1)
    assert attempts >= 2


def sse_result(text):
    if text.startswith("{"):
        return json.loads(text)
    return next(
        json.loads(line[6:])
        for line in text.splitlines()
        if line.startswith("data: ") and '"result"' in line
    )


async def test_legacy_http_disconnect_has_bounded_wait_cleanup(server):
    headers = {"Accept": "application/json, text/event-stream"}
    async with httpx.AsyncClient(base_url=server.url, headers=headers, trust_env=False) as http:
        init = await http.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "disconnect-test", "version": "1"},
                },
            },
        )
        assert init.status_code == 200
        http.headers["Mcp-Session-Id"] = init.headers["mcp-session-id"]
        http.headers["Mcp-Protocol-Version"] = "2025-11-25"
        await http.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"})
        started = await http.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "start",
                    "arguments": {"queues": ["a"], "worker_id": "disconnected"},
                },
            },
        )
        assert not sse_result(started.text)["result"].get("isError", False)
        async with http.stream(
            "POST",
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "wait_for_job",
                    "arguments": {"worker_id": "disconnected", "wait_seconds": 0.3},
                },
            },
        ) as response:
            assert response.status_code == 200
            # Close the response stream before the wait completes.

        async def cleared():
            workers = (await http.get("/v1/workers")).json()["workers"]
            return workers[0]["state"] == "unknown"

        await eventually(cleared)
        await http.delete("/mcp")


async def test_shutdown_wakes_pending_wait_without_persistent_locks(tmp_path):
    app = create_app(tmp_path / "shutdown.sqlite")
    broker = app.state.broker
    async with app.router.lifespan_context(app):
        await broker.mutate("start", ["a"], "worker")
        pending = asyncio.create_task(broker.wait("worker", 300))

        async def waiting():
            return (await broker.workers())["workers"][0]["state"] == "waiting"

        await eventually(waiting)
    assert (await asyncio.wait_for(pending, 1))["status"] == "stopped"
    assert (await broker.workers())["workers"][0]["state"] == "unknown"
    reopened = create_app(tmp_path / "shutdown.sqlite")
    async with reopened.router.lifespan_context(reopened):
        assert (await reopened.state.broker.wait("worker", 0))["status"] == "idle"


def test_http_validation_idempotency_and_local_security(tmp_path):
    with TestClient(
        create_app(tmp_path / "security.sqlite"), base_url="http://127.0.0.1:8790"
    ) as http:
        assert http.get("/v1/health").json()["ok"]
        payload = {"queue": "a", "instruction": "private task text", "idempotency_key": "key"}
        first = http.post("/v1/jobs", json=payload)
        assert first.status_code == 201
        assert http.post("/v1/jobs", json=payload).status_code == 200
        assert http.post("/v1/jobs", json={**payload, "instruction": "changed"}).status_code == 409
        assert http.get("/v1/jobs?queue=a&status=queued").json()["total"] == 1
        assert http.get("/v1/jobs?limit=0").status_code == 422
        assert http.get("/v1/jobs?status=bogus").status_code == 422
        assert http.get("/v1/jobs/no-such-job").status_code == 404
        assert (
            http.post(
                "/v1/jobs", content="{", headers={"Content-Type": "application/json"}
            ).status_code
            == 400
        )
        assert http.post("/v1/jobs", content="hello").status_code == 415
        invalid = http.post(
            "/v1/jobs", json={"queue": "bad queue", "instruction": "private task text"}
        )
        assert invalid.status_code == 422 and "private task text" not in invalid.text
        for path in ("/v1/jobs", "/mcp"):
            assert http.post(path, json={}, headers={"Host": "evil.example"}).status_code == 403
            assert (
                http.post(path, json={}, headers={"Origin": "https://evil.example"}).status_code
                == 403
            )
            assert http.post(path, json={}, headers={"Origin": "null"}).status_code == 403
            assert http.post(path, content=b"x" * 1_048_577).status_code == 413
        assert (
            http.get("/v1/health", headers={"Origin": "http://127.0.0.1:8790"}).status_code == 200
        )
        assert (
            http.post("/v1/jobs", content=iter([b"x" * 600_000, b"x" * 600_000])).status_code == 413
        )
