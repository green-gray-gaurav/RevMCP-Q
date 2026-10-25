"""Local dashboard worker launcher. Spawn fixed CLI argv, never shell commands."""

import asyncio
import hashlib
import importlib.util
import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from .models import BrokerError, ModelId, Name, WorkerId


class LaunchInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    queues: list[Name] = Field(min_length=1, max_length=100)
    worker_id: WorkerId | None = None
    model: ModelId | None = None
    sandbox: str = Field(default="workspace-write", pattern=r"^(read-only|workspace-write)$")


async def codex_models(cwd):
    from .hosts.codex import CodexHost
    from .worker import WorkerError, WorkerOptions

    try:
        async with asyncio.timeout(30):
            async with CodexHost(WorkerOptions(cwd=Path(cwd), check=True)) as host:
                await host.preflight()
                page = await host.codex.models()
                models, cursors = {}, set()
                while True:
                    for item in page.data:
                        if not item.hidden:
                            slug = item.model or item.id
                            models[slug] = {"id": slug, "label": item.display_name or slug}
                    cursor = page.next_cursor
                    if not cursor:
                        break
                    if cursor in cursors or len(cursors) >= 100:
                        raise BrokerError(
                            "model_catalog_error", "Codex returned invalid model pagination", 503
                        )
                    cursors.add(cursor)
                    page = await host.codex._client.request(
                        "model/list",
                        {"cursor": cursor, "includeHidden": False},
                        response_model=type(page),
                    )
                return {"models": list(models.values()), "default_model": host.model}
    except WorkerError as exc:
        raise BrokerError(exc.code, exc.message, 503) from None
    except BrokerError:
        raise
    except Exception:
        raise BrokerError(
            "model_catalog_unavailable",
            "Cannot load Codex models; check sign-in and the optional codex installation",
            503,
        ) from None


class ModelCatalog:
    def __init__(self, cwd, ttl=60):
        self.cwd, self.ttl = cwd, ttl
        self.cached, self.expires = None, 0
        self.lock = asyncio.Lock()

    async def get(self, force=False):
        async with self.lock:
            if force or self.cached is None or time.monotonic() >= self.expires:
                self.cached = await codex_models(self.cwd)
                self.expires = time.monotonic() + self.ttl
            return self.cached


class Launcher:
    def __init__(self, directory, cwd=None):
        self.directory = Path(directory)
        self.cwd = Path(cwd or Path.cwd()).resolve()
        self.runs = {}
        self.lock = threading.Lock()

    def launch(self, request, url):
        if importlib.util.find_spec("openai_codex") is None:
            raise BrokerError(
                "missing_codex", "Install the worker extra: uv sync --extra codex", 503
            )
        worker_id = request.worker_id or "dashboard-" + str(uuid4())[:8]
        with self.lock:
            if worker_id in self.runs and self.runs[worker_id]["process"].poll() is None:
                raise BrokerError("worker_running", "This dashboard worker is already running")
            if sum(run["process"].poll() is None for run in self.runs.values()) >= 16:
                raise BrokerError("worker_limit", "Stop a dashboard worker before launching more")
            self.directory.mkdir(parents=True, exist_ok=True)
            key = hashlib.sha256(worker_id.encode()).hexdigest()[:24]
            logfile = self.directory / (key + ".log")
            argv = [
                sys.executable,
                "-m",
                "rmcpq",
                "worker",
                "--json",
                "--url",
                url,
                "--worker-id",
                worker_id,
                "--cwd",
                str(self.cwd),
                "--sandbox",
                request.sandbox,
            ]
            for queue in sorted(set(request.queues)):
                argv.extend(["--queue", queue])
            if request.model:
                argv.extend(["--model", request.model])
            try:
                with logfile.open("wb") as output:
                    process = subprocess.Popen(
                        argv,
                        cwd=self.cwd,
                        stdin=subprocess.DEVNULL,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        shell=False,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
            except OSError:
                raise BrokerError(
                    "launch_failed", "Cannot launch the local worker process", 503
                ) from None
            self.runs[worker_id] = {
                "process": process,
                "logfile": logfile,
                "worker_id": worker_id,
                "queues": sorted(set(request.queues)),
                "model": request.model,
                "sandbox": request.sandbox,
            }
            return self._public(self.runs[worker_id])

    def _public(self, run):
        code = run["process"].poll()
        value = {key: run[key] for key in ("worker_id", "queues", "model", "sandbox")}
        value.update(
            pid=run["process"].pid, state="starting" if code is None else "exited", exit_code=code
        )
        try:
            with run["logfile"].open("rb") as source:
                source.seek(0, 2)
                size = source.tell()
                source.seek(0)
                data = source.read(8192)
                if size > 8192:
                    source.seek(max(8192, size - 32768))
                    data += b"\n" + source.read()
                lines = data.decode("utf-8", errors="replace").splitlines()
            for line in lines:
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(event, dict):
                    continue
                if event.get("event") == "ready":
                    run["model"] = event.get("model")
                    value["model"] = run["model"]
                    if code is None:
                        value["state"] = "running"
                if isinstance(event.get("error"), dict):
                    value["error"] = {
                        "code": str(event["error"].get("code", "worker_error"))[:100],
                        "message": str(event["error"].get("message", "Worker exited"))[:500],
                    }
        except OSError:
            pass
        return value

    def list(self):
        with self.lock:
            return {
                "runners": [self._public(run) for run in self.runs.values()],
                "cwd": str(self.cwd),
            }
