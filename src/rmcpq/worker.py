"""Persistent queue runner. Idle waits never invoke a model."""

import asyncio
import json
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from mcp import Client

from .models import MAX_PAYLOAD_BYTES, WorkerInput, canonical
from .worker_state import Journal, WorkerLock, identity_key


class WorkerError(Exception):
    """A fixed, safe diagnostic suitable for terminal output and job errors."""

    def __init__(self, code, message, *, fatal=False):
        super().__init__(message)
        self.code, self.message, self.fatal = code, message, fatal


@dataclass
class WorkerOptions:
    url: str = "http://127.0.0.1:8790"
    queues: list[str] = field(default_factory=list)
    worker_id: str = "codex-worker"
    cwd: Path = field(default_factory=Path.cwd)
    state_dir: Path = field(default_factory=lambda: Path.home() / ".rmcpq" / "workers")
    model: str | None = None
    sandbox: str = "workspace-write"
    wait_seconds: float = 25
    job_timeout: float = 3600
    max_jobs: int | None = None
    check: bool = False

    def validate(self):
        self.url = self.url.rstrip("/")
        parsed = urlsplit(self.url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path
        ):
            raise ValueError("Worker broker URL must be a loopback HTTP origin")
        parsed.port
        self.cwd = self.cwd.resolve(strict=True)
        if not self.cwd.is_dir():
            raise ValueError("Worker cwd must be a directory")
        WorkerInput(
            queues=self.queues or (["check"] if self.check else []),
            worker_id=self.worker_id,
            wait_seconds=self.wait_seconds,
            model=self.model,
        )
        if not 0 < self.wait_seconds <= 300:
            raise ValueError("Runner waits must be greater than zero and at most 300 seconds")
        if not 0 < self.job_timeout < float("inf"):
            raise ValueError("Job timeout must be finite and positive")
        if self.max_jobs is not None and self.max_jobs < 1:
            raise ValueError("max-jobs must be positive")
        if self.sandbox not in {"read-only", "workspace-write"}:
            raise ValueError("Unsupported worker sandbox")


class QueueClient:
    def __init__(self, url):
        self.url = url

    async def tool(self, name, **arguments):
        # Each operation owns its own protocol session and cancellation scope. Durable
        # worker identity belongs to the broker, independently of those sessions.
        for retry in range(3):
            try:
                timeout = arguments.get("wait_seconds", 0) + 15
                async with Client(self.url + "/mcp", read_timeout_seconds=timeout) as client:
                    answer = await client.call_tool(name, arguments)
                # Raise domain errors after leaving the MCP/AnyIO context. Raising
                # inside it wraps them in ExceptionGroup and hides the actual cause
                # behind the transient-transport retry handler.
                if answer.is_error:
                    error = (answer.structured_content or {}).get("error", {})
                    code = error.get("code", "broker_error")
                    message = (
                        "Worker ID already subscribes to different queues; use a different "
                        "--worker-id or stop the existing worker first"
                        if code == "worker_scope_conflict"
                        else "Broker rejected the worker operation"
                    )
                    raise WorkerError(code, message)
                if not isinstance(answer.structured_content, dict):
                    raise WorkerError("protocol_error", "Broker response is invalid", fatal=True)
                return answer.structured_content
            except WorkerError:
                raise
            except Exception:
                if retry == 2:
                    raise WorkerError(
                        "broker_unavailable", "Broker unavailable after three attempts", fatal=True
                    ) from None
                await asyncio.sleep(0.25 * 2**retry)

    async def job(self, job_id):
        try:
            async with httpx.AsyncClient(base_url=self.url, timeout=5, trust_env=False) as client:
                response = await client.get("/v1/jobs/" + job_id)
                response.raise_for_status()
                return response.json()
        except (httpx.HTTPError, ValueError):
            raise WorkerError(
                "broker_unavailable", "Cannot inspect the active claim", fatal=True
            ) from None


def lease_remaining(lease_until):
    return (datetime.fromisoformat(lease_until) - datetime.now(timezone.utc)).total_seconds()


def parse_result(text, result_format):
    if not isinstance(text, str):
        raise WorkerError("missing_result", "Codex completed without a final answer")
    try:
        if len(text.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValueError("Result exceeds 1 MiB")
        value = json.loads(text) if result_format == "json" else text
        canonical(value)
    except (ValueError, RecursionError, UnicodeError):
        raise WorkerError("invalid_result", "Codex did not return valid bounded JSON") from None
    return value


class Runner:
    def __init__(self, options, host, journal, *, queue=None, emit=None, poll_seconds=1):
        self.options, self.host, self.journal = options, host, journal
        self.queue = queue or QueueClient(options.url)
        self.emit = emit or (lambda event: None)
        self.poll_seconds = poll_seconds

    def event(self, status, **fields):
        self.emit({"event": status, "worker_id": self.options.worker_id, **fields})

    async def monitor(self, job):
        deadline = time.monotonic() + self.options.job_timeout
        until = job["lease_until"]
        renew_at = time.monotonic() + max(0.01, lease_remaining(until) / 3)
        while True:
            await asyncio.sleep(min(self.poll_seconds, max(0.01, renew_at - time.monotonic())))
            if time.monotonic() >= deadline:
                raise WorkerError("job_timeout", "Managed Codex job exceeded its time limit")
            current = await self.queue.job(job["id"])
            if (
                current["status"] != "processing"
                or current["attempts"] != job["attempts"]
                or current["worker_id"] != self.options.worker_id
            ):
                raise WorkerError("stale_claim", "Job was cancelled, stopped, or reassigned")
            if time.monotonic() >= renew_at:
                renewed = await self.queue.tool(
                    "renew_job", job_id=job["id"], claim_token=job["claim_token"]
                )
                until = renewed["lease_until"]
                renew_at = time.monotonic() + max(0.01, lease_remaining(until) / 3)

    async def execute(self, job):
        saved = await asyncio.to_thread(self.journal.read, job["id"], job["attempts"])
        settings = {
            "cwd": str(self.options.cwd),
            "model": getattr(self.host, "model", None) or self.options.model,
            "sandbox": self.options.sandbox,
        }
        if getattr(self.host, "outcome_contract", None) is not None:
            settings["outcome_contract"] = self.host.outcome_contract
        state = dict(saved or {})

        async def record(update):
            state.update(update)
            await asyncio.to_thread(
                self.journal.write, job["id"], job["attempts"], {**state, "settings": settings}
            )

        if saved and saved.get("settings") != settings:
            if (saved.get("settings") or {}).get("outcome_contract") != settings.get(
                "outcome_contract"
            ):
                raise WorkerError(
                    "recovery_contract_changed",
                    "Saved execution uses an older outcome contract; restart the worker for a fresh attempt",
                    fatal=True,
                )
            raise WorkerError(
                "recovery_config_changed",
                "Resume with the original cwd, model and sandbox",
                fatal=True,
            )
        if saved and saved["phase"] == "completed":
            return saved["result"]
        text = await self.host.execute(job, saved, record)
        result = parse_result(text, job["result_format"])
        # Persist before submitting, so a lost response or a process restart cannot
        # cause a second model invocation for this still-valid attempt.
        await record({"phase": "completed", "result": result})
        return result

    async def process(self, job):
        self.event("claimed", job_id=job["id"], attempt=job["attempts"])
        work = asyncio.create_task(self.execute(job))
        monitor = asyncio.create_task(self.monitor(job))
        try:
            done, _ = await asyncio.wait({work, monitor}, return_when=asyncio.FIRST_COMPLETED)
            # A cancellation/renewal failure wins a race with model completion.
            if monitor in done:
                await monitor
            result = await work
            await self.queue.tool("renew_job", job_id=job["id"], claim_token=job["claim_token"])
            await self.queue.tool(
                "submit_result", job_id=job["id"], claim_token=job["claim_token"], result=result
            )
            self.event("completed", job_id=job["id"])
        except WorkerError as exc:
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            await self.host.interrupt()
            if exc.code == "stale_claim":
                self.event("claim_lost", job_id=job["id"])
            elif exc.fatal:
                raise
            else:
                await self.queue.tool(
                    "fail_job",
                    job_id=job["id"],
                    claim_token=job["claim_token"],
                    error=f"{exc.code}: {exc.message}",
                )
                self.event("failed", job_id=job["id"], code=exc.code)
        finally:
            for task in (work, monitor):
                task.cancel()
            await asyncio.gather(work, monitor, return_exceptions=True)
            # Interrupt must happen before the worker releases an unfinished claim.
            await self.host.interrupt()

    async def run(self):
        # Reject a wrong account before registering or claiming work.
        await self.host.preflight()
        registered = False
        count = 0
        try:
            await self.queue.tool(
                "start",
                queues=self.options.queues,
                worker_id=self.options.worker_id,
                wait_seconds=self.options.wait_seconds,
                model=getattr(self.host, "model", None) or self.options.model,
            )
            registered = True
            self.event(
                "ready",
                queues=self.options.queues,
                cwd=str(self.options.cwd),
                model=getattr(self.host, "model", None) or self.options.model,
            )
            while self.options.max_jobs is None or count < self.options.max_jobs:
                # A transport context can suppress CancelledError during teardown.
                # Honor the outstanding cancellation before starting another wait.
                if asyncio.current_task().cancelling():
                    raise asyncio.CancelledError
                await self.host.preflight()
                answer = await self.queue.tool(
                    "wait_for_job",
                    worker_id=self.options.worker_id,
                    wait_seconds=self.options.wait_seconds,
                )
                if answer["status"] == "stopped":
                    return
                if answer["status"] == "idle":
                    continue
                await self.host.preflight()
                await self.process(answer["job"])
                count += 1
        finally:
            await self.host.interrupt()
            if registered:
                with suppress(WorkerError):
                    await self.queue.tool("stop", worker_id=self.options.worker_id)
            self.event("stopped")


async def run_worker(options, emit):
    options.validate()
    # Import only for this optional command; HTTP/MCP users need no Codex runtime.
    from .hosts.codex import CodexHost

    key = identity_key(options.url, options.worker_id)
    # Global identity lock also prevents accidental duplicate runners launched from
    # different working directories or with different journal directories.
    lock_dir = Path.home() / ".rmcpq" / "locks"
    with WorkerLock(lock_dir, key):
        async with CodexHost(options) as host:
            await host.preflight()
            if options.check:
                emit(
                    {
                        "event": "check_passed",
                        "account_type": "chatgpt",
                        "cwd": str(options.cwd),
                        "sandbox": options.sandbox,
                        "model": host.model,
                    }
                )
                return
            journal = await asyncio.to_thread(Journal, options.state_dir / (key + ".sqlite"))
            await Runner(options, host, journal, emit=emit).run()
