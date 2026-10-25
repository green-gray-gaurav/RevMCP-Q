"""Opt-in verification against a real signed-in Codex subscription, never run by default."""

import asyncio
import os

import httpx
import pytest

from rmcpq.hosts.codex import CodexHost
from rmcpq.models import JobInput
from rmcpq.worker import Runner, WorkerOptions
from rmcpq.worker_state import Journal

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        os.environ.get("RMCPQ_LIVE_CODEX") != "1",
        reason="Requires explicit live subscription verification",
    ),
]


async def test_dashboard_launch_and_stop_subscription_worker(server):
    worker_id = "dashboard-smoke-worker"
    async with httpx.AsyncClient(base_url=server.url, trust_env=False, timeout=10) as http:
        launched = await http.post(
            "/v1/runners",
            json={
                "worker_id": worker_id,
                "queues": ["dashboard-smoke-empty"],
                "sandbox": "read-only",
            },
        )
        assert launched.status_code == 202
        try:
            async with asyncio.timeout(40):
                while True:
                    workers = (await http.get("/v1/workers")).json()["workers"]
                    worker = next((w for w in workers if w["worker_id"] == worker_id), None)
                    if worker and worker["state"] == "waiting":
                        assert worker["model"]
                        break
                    runs = (await http.get("/v1/runners")).json()["runners"]
                    assert not any(r.get("exit_code") is not None for r in runs), runs
                    await asyncio.sleep(0.2)
            stopped = await http.post(f"/v1/workers/{worker_id}/stop")
            assert stopped.status_code == 200
            async with asyncio.timeout(20):
                while True:
                    runs = (await http.get("/v1/runners")).json()["runners"]
                    if runs[0]["state"] == "exited":
                        assert runs[0]["exit_code"] == 0
                        break
                    await asyncio.sleep(0.2)
        finally:
            await http.post(f"/v1/workers/{worker_id}/stop")
            process = server.app.state.launcher.runs[worker_id]["process"]
            if process.poll() is None:
                await asyncio.sleep(1)
            if process.poll() is None:
                process.terminate()
                await asyncio.to_thread(process.wait, 10)


async def test_real_subscription_text_json_and_file_tool(server, tmp_path):
    (tmp_path / "source.txt").write_text("ORBIT-4729", encoding="utf-8")
    store = server.app.state.broker.store
    first = store.enqueue(
        JobInput(
            queue="live",
            instruction="Read source.txt in the working directory using a file or shell tool. "
            "Return only the exact content as plain text.",
        )
    )[0]
    second = store.enqueue(
        JobInput(
            queue="live",
            instruction='Extract name and count from context. Return {"name":string,"count":integer}.',
            context={"text": "Alex has 7 apples."},
            result_format="json",
        )
    )[0]
    options = WorkerOptions(
        url=server.url,
        queues=["live"],
        worker_id="live-codex",
        cwd=tmp_path,
        model=os.environ.get("RMCPQ_CODEX_MODEL"),
        sandbox="read-only",
        wait_seconds=1,
        max_jobs=2,
        job_timeout=180,
    )
    async with CodexHost(options) as host:
        runner = Runner(options, host, Journal(tmp_path / "state" / "journal.sqlite"))
        await asyncio.wait_for(runner.run(), 400)
    text = store.get_job(first["id"])
    structured = store.get_job(second["id"])
    assert text["status"] == "completed", text.get("error")
    assert text["result"].strip() == "ORBIT-4729"
    assert structured["status"] == "completed", structured.get("error")
    assert structured["result"] == {"name": "Alex", "count": 7}
    for job in (text, structured):
        state = runner.journal.read(job["id"], 1)
        assert state["thread_id"] and state["turn_id"]
    # A fresh app-server process must recover the original persisted turn.
    # Pretend the result was not yet journaled when the previous runner exited.
    saved = runner.journal.read(first["id"], 1)
    async with CodexHost(options) as restarted:
        await restarted.preflight()
        _, recovered = await restarted.recover({**saved, "phase": "running"})
        assert (await restarted.finish(recovered, "text")).strip() == "ORBIT-4729"


async def test_real_subscription_reports_execution_failure(server, tmp_path):
    store = server.app.state.broker.store
    job = store.enqueue(
        JobInput(
            queue="live-failure",
            instruction="Read missing-input-7931.txt and summarize its contents. If it does not exist, "
            "report execution failure. Do not create or invent the missing input.",
            result_format="json",
        )
    )[0]
    options = WorkerOptions(
        url=server.url,
        queues=["live-failure"],
        worker_id="live-failure",
        cwd=tmp_path,
        model=os.environ.get("RMCPQ_CODEX_MODEL"),
        sandbox="read-only",
        max_jobs=1,
        wait_seconds=1,
        job_timeout=180,
    )
    async with CodexHost(options) as host:
        worker = Runner(options, host, Journal(tmp_path / "state" / "failure.sqlite"))
        await asyncio.wait_for(worker.run(), 200)
    saved = store.get_job(job["id"])
    assert saved["status"] == "failed" and saved["result"] is None
    assert saved["error"].startswith("execution_failed:")


@pytest.mark.skipif(
    os.environ.get("RMCPQ_LIVE_IMAGE") != "1", reason="Explicit image generation opt-in"
)
async def test_real_subscription_image_saved_in_workspace(server, tmp_path):
    store = server.app.state.broker.store
    job = store.enqueue(
        JobInput(
            queue="live-image",
            instruction="Generate an image of a blue sky with a few soft white clouds. "
            "Use the built-in image generation tool. Save the actual image to the destination. "
            "Return JSON containing its absolute filepath.",
            context={"filepath": str(tmp_path / "output")},
            result_format="json",
        )
    )[0]
    options = WorkerOptions(
        url=server.url,
        queues=["live-image"],
        worker_id="live-image",
        cwd=tmp_path,
        model=os.environ.get("RMCPQ_CODEX_MODEL"),
        sandbox="workspace-write",
        max_jobs=1,
        wait_seconds=1,
        job_timeout=420,
    )
    async with CodexHost(options) as host:
        worker = Runner(options, host, Journal(tmp_path / "state" / "image.sqlite"))
        await asyncio.wait_for(worker.run(), 450)
    saved = store.get_job(job["id"])
    assert saved["status"] == "completed", saved.get("error")
    from pathlib import Path

    output = Path(saved["result"]["filepath"])
    assert output.is_absolute() and output.resolve().is_relative_to(tmp_path.resolve())
    assert output.is_file() and output.stat().st_size > 1000


async def test_real_subscription_cancellation_interrupts_turn(server, tmp_path):
    store = server.app.state.broker.store
    job = store.enqueue(
        JobInput(
            queue="live-cancel",
            instruction="Use a shell command to sleep for 30 seconds, then reply done. This is a cancellation check.",
        )
    )[0]
    options = WorkerOptions(
        url=server.url,
        queues=["live-cancel"],
        worker_id="live-cancel",
        cwd=tmp_path,
        model=os.environ.get("RMCPQ_CODEX_MODEL"),
        sandbox="read-only",
        max_jobs=1,
        wait_seconds=1,
    )
    journal = Journal(tmp_path / "state" / "cancel.sqlite")
    async with CodexHost(options) as host:
        runner = Runner(options, host, journal, poll_seconds=0.1)
        task = asyncio.create_task(runner.run())
        try:
            async with asyncio.timeout(30):
                while host.active is None:
                    if task.done():
                        await task
                        pytest.fail("Worker finished before a live turn started")
                    await asyncio.sleep(0.05)
            store.cancel(job["id"])
            await asyncio.wait_for(task, 15)
            assert store.get_job(job["id"])["status"] == "cancelled"
            assert store.get_job(job["id"])["result"] is None
            state = journal.read(job["id"], 1)
            thread = await host.codex.thread_resume(state["thread_id"], **host.thread_options())
            read = (await thread.read(include_turns=True)).model_dump(mode="json", by_alias=True)
            turn = next(t for t in read["thread"]["turns"] if t["id"] == state["turn_id"])
            assert turn["status"] == "interrupted"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
