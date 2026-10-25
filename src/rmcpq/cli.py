"""Producer CLI and loopback-only broker launcher."""

import argparse
import asyncio
import json
import math
import os
import sys
import time
from pathlib import Path

import httpx
from pydantic import ValidationError

from .models import JobInput


def nonnegative(value):
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("Must be a finite, nonnegative number")
    return parsed


def positive(value):
    parsed = nonnegative(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return parsed


def common(parser, *, root=False):
    default = None if root else argparse.SUPPRESS
    parser.add_argument("--url", default=default, help="Broker URL (or RMCPQ_URL)")
    parser.add_argument(
        "--json",
        action="store_true",
        default=False if root else argparse.SUPPRESS,
        help="Print machine-readable JSON",
    )


def parser():
    root = argparse.ArgumentParser(prog="rmcpq", description="Local reverse-MCP queue broker")
    common(root, root=True)
    commands = root.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the broker on loopback")
    common(serve)
    serve.add_argument("--host", choices=["127.0.0.1", "localhost", "::1"], default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8790)
    serve.add_argument("--db", type=Path, default=Path("data/rmcpq.sqlite"))
    serve.add_argument("--lease-seconds", type=positive, default=600)
    serve.add_argument("--max-attempts", type=int, default=3)
    enqueue = commands.add_parser("enqueue", help="Submit a self-contained job")
    common(enqueue)
    enqueue.add_argument("--queue", required=True)
    source = enqueue.add_mutually_exclusive_group()
    source.add_argument("--instruction")
    source.add_argument("--instruction-file", help="UTF-8 file, or - for stdin; defaults to stdin")
    enqueue.add_argument("--context-file", help="JSON file (or - for stdin)")
    enqueue.add_argument("--result-format", choices=["text", "json"], default="text")
    enqueue.add_argument("--idempotency-key")
    for name in ("status", "result", "wait", "cancel", "retry"):
        command = commands.add_parser(name)
        common(command)
        command.add_argument("job_id")
        if name == "wait":
            command.add_argument("--timeout", type=nonnegative, default=300)
            command.add_argument("--poll-interval", type=positive, default=1)
    for name in ("queues", "workers"):
        common(commands.add_parser(name))
    worker = commands.add_parser(
        "worker", help="Run an autonomous subscription-backed Codex worker"
    )
    common(worker)
    worker.add_argument("--host", choices=["codex"], default="codex")
    worker.add_argument(
        "--queue", action="append", default=[], help="Queue to subscribe to (repeatable)"
    )
    worker.add_argument("--worker-id", default="codex-worker")
    worker.add_argument("--cwd", type=Path, default=Path.cwd())
    worker.add_argument("--state-dir", type=Path, default=Path.home() / ".rmcpq" / "workers")
    worker.add_argument("--model", help="Optional Codex model; defaults to the host configuration")
    worker.add_argument(
        "--sandbox", choices=["read-only", "workspace-write"], default="workspace-write"
    )
    worker.add_argument("--wait-seconds", type=positive, default=25)
    worker.add_argument("--job-timeout", type=positive, default=3600)
    worker.add_argument("--max-jobs", type=int, help="Stop after this many claimed jobs")
    worker.add_argument(
        "--check", action="store_true", help="Verify subscription sign-in without claiming jobs"
    )
    return root


def read_text(path):
    return sys.stdin.read() if path in (None, "-") else Path(path).read_text(encoding="utf-8")


def display(value, as_json=False, *, result_only=False):
    if as_json:
        print(json.dumps(value, ensure_ascii=False, allow_nan=False))
    elif result_only and value.get("status") == "completed":
        result = value["result"]
        print(
            result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, indent=2)
        )
    elif "id" in value:
        print(
            f"{value['id']}  {value['status']}  queue={value['queue']}  attempts={value['attempts']}"
        )
        if value.get("error"):
            print(value["error"])
    else:
        print(json.dumps(value, ensure_ascii=False, indent=2))


class RemoteError(Exception):
    def __init__(self, payload):
        self.payload = payload


class WaitExpired(Exception):
    def __init__(self, job=None):
        self.payload = {"error": {"code": "wait_timeout", "message": "Job wait timed out"}}
        if job is not None:
            self.payload["job"] = job


def request(client, method, path, **kwargs):
    response = client.request(method, path, **kwargs)
    try:
        value = response.json()
    except ValueError:
        raise RemoteError(
            {"error": {"code": "invalid_response", "message": "Broker did not return JSON"}}
        ) from None
    if response.is_error:
        raise RemoteError(value)
    return value


def wait_job(client, job_id, timeout, poll_interval):
    deadline = time.monotonic() + timeout
    job = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 and job is not None:
            raise WaitExpired(job)
        try:
            job = request(
                client, "GET", f"/v1/jobs/{job_id}", timeout=min(10, max(0.001, remaining))
            )
        except httpx.TimeoutException:
            # A request clipped to the caller's remaining budget can time out just
            # before monotonic() reaches its deadline due to timer granularity.
            if remaining <= 10:
                raise WaitExpired(job) from None
            raise
        if job["status"] in ("completed", "failed", "cancelled"):
            return job
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise WaitExpired(job)
        time.sleep(min(poll_interval, remaining))


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "serve":
            if not 1 <= args.port <= 65535 or args.max_attempts < 1:
                raise ValueError("Port must be 1..65535 and max-attempts must be positive")
            import uvicorn

            from .app import create_app

            app = create_app(
                args.db, lease_seconds=args.lease_seconds, max_attempts=args.max_attempts
            )
            host = f"[{args.host}]" if ":" in args.host else args.host
            dashboard_url = f"http://{host}:{args.port}/"
            if args.json:
                print(json.dumps({"event": "starting", "dashboard": dashboard_url}), flush=True)
            else:
                print(f"Starting R-mcpq. Dashboard: {dashboard_url}", flush=True)
            uvicorn.run(
                app,
                host=args.host,
                port=args.port,
                access_log=False,
                log_level="warning",
                timeout_graceful_shutdown=5,
            )
            return 0
        url = (args.url or os.environ.get("RMCPQ_URL", "http://127.0.0.1:8790")).rstrip("/")
        if args.command == "worker":
            from .worker import WorkerError, WorkerOptions, run_worker

            options = WorkerOptions(
                url=url,
                queues=args.queue,
                worker_id=args.worker_id,
                cwd=args.cwd,
                state_dir=args.state_dir,
                model=args.model,
                sandbox=args.sandbox,
                wait_seconds=args.wait_seconds,
                job_timeout=args.job_timeout,
                max_jobs=args.max_jobs,
                check=args.check,
            )

            def emit(event):
                if args.json:
                    print(json.dumps(event, ensure_ascii=False), flush=True)
                else:
                    details = " ".join(f"{k}={v}" for k, v in event.items() if k != "event")
                    print(f"{event['event']} {details}", flush=True)

            try:
                asyncio.run(run_worker(options, emit))
                return 0
            except WorkerError as exc:
                if args.json:
                    display({"error": {"code": exc.code, "message": exc.message}}, True)
                else:
                    print(f"{exc.code}: {exc.message}", file=sys.stderr)
                return 2
        with httpx.Client(base_url=url, timeout=10, trust_env=False) as client:
            if args.command == "enqueue":
                uses_stdin = args.instruction is None and args.instruction_file in (None, "-")
                if uses_stdin and args.context_file == "-":
                    raise ValueError("Instruction and context cannot both read stdin")
                context = json.loads(read_text(args.context_file)) if args.context_file else None
                instruction = (
                    args.instruction
                    if args.instruction is not None
                    else read_text(args.instruction_file)
                )
                job = JobInput(
                    queue=args.queue,
                    instruction=instruction,
                    context=context,
                    result_format=args.result_format,
                    idempotency_key=args.idempotency_key,
                )
                value = request(client, "POST", "/v1/jobs", json=job.model_dump())
            elif args.command in ("queues", "workers"):
                value = request(client, "GET", f"/v1/{args.command}")
            elif args.command in ("cancel", "retry"):
                value = request(client, "POST", f"/v1/jobs/{args.job_id}/{args.command}")
            elif args.command == "wait":
                value = wait_job(client, args.job_id, args.timeout, args.poll_interval)
            else:
                value = request(client, "GET", f"/v1/jobs/{args.job_id}")
            display(value, args.json, result_only=args.command in ("result", "wait"))
            if args.command in ("result", "wait") and value["status"] != "completed":
                return 1
            return 0
    except KeyboardInterrupt:
        return 130
    except WaitExpired as exc:
        display(exc.payload, args.json)
        return 124
    except RemoteError as exc:
        if args.json:
            display(exc.payload, True)
        else:
            print(
                exc.payload.get("error", {}).get("message", "Broker request failed"),
                file=sys.stderr,
            )
        return 2
    except (ValueError, OSError, httpx.HTTPError, httpx.InvalidURL, ValidationError) as exc:
        # Avoid printing request bodies, credentials, or URLs from exception objects.
        message = "Invalid input or connection failure; check the broker and input files"
        if args.json:
            display({"error": {"code": "client_error", "message": message}}, True)
        else:
            print(f"{message} ({type(exc).__name__})", file=sys.stderr)
        return 2
