"""HTTP producer API and MCP worker endpoint in one local ASGI application."""

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import __version__
from .broker import Broker
from .mcp_server import create_mcp
from .models import MAX_PAYLOAD_BYTES, BrokerError, JobInput, ListQuery
from .store import Store

LOOPBACK = {"127.0.0.1", "localhost", "::1"}


class LocalOnly:
    """Apply Host/Origin validation and request size limits to HTTP and MCP alike."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = {}
        for key, value in scope["headers"]:
            if key in (b"host", b"origin", b"content-length") and key in headers:
                return await JSONResponse(
                    {"error": {"code": "invalid_headers", "message": "Duplicate header"}}, 400
                )(scope, receive, send)
            headers[key] = value.decode("latin-1")
        try:
            host = urlsplit("//" + headers.get(b"host", ""))
            valid = (
                host.hostname in LOOPBACK
                and not host.username
                and not host.password
                and not host.path
                and not host.query
                and not host.fragment
            )
            host.port  # Validate malformed ports too.
            origin = headers.get(b"origin")
            if origin is not None:
                parsed = urlsplit(origin)
                valid = valid and (
                    parsed.scheme == scope.get("scheme", "http")
                    and parsed.netloc.lower() == host.netloc.lower()
                    and not parsed.path
                    and not parsed.query
                    and not parsed.fragment
                )
        except ValueError:
            valid = False
        if not valid:
            return await JSONResponse(
                {
                    "error": {
                        "code": "forbidden_origin",
                        "message": "Only same-origin loopback access is allowed",
                    }
                },
                403,
            )(scope, receive, send)
        # Buffer a bounded body before downstream parsing, including chunked requests.
        chunks, size = [], 0
        try:
            length = int(headers.get(b"content-length", "0"))
        except ValueError:
            length = -1
        if length < 0:
            return await JSONResponse(
                {"error": {"code": "invalid_length", "message": "Invalid length"}}, 400
            )(scope, receive, send)
        if length > MAX_PAYLOAD_BYTES:
            return await JSONResponse(
                {"error": {"code": "payload_too_large", "message": "Limit is 1 MiB"}}, 413
            )(scope, receive, send)
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > MAX_PAYLOAD_BYTES:
                return await JSONResponse(
                    {"error": {"code": "payload_too_large", "message": "Limit is 1 MiB"}}, 413
                )(scope, receive, send)
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": b"".join(chunks), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def create_app(
    db_path: str | Path = "data/rmcpq.sqlite",
    *,
    lease_seconds: float = 600,
    max_attempts: int = 3,
    sweep_seconds: float = 1,
) -> Starlette:
    store = Store(db_path, lease_seconds=lease_seconds, max_attempts=max_attempts)
    broker = Broker(store, sweep_seconds=sweep_seconds)
    mcp = create_mcp(broker)
    # Stateful SSE enables explicit MCP cancellation; application workers remain durable.
    mcp_app = mcp.streamable_http_app(
        json_response=False, stateless_http=False, max_request_body_size=MAX_PAYLOAD_BYTES
    )

    @asynccontextmanager
    async def lifespan(app):
        await broker.call("initialize")
        async with mcp.session_manager.run():
            sweep = asyncio.create_task(broker.sweeper())
            try:
                yield
            finally:
                broker.shutdown()
                sweep.cancel()
                with suppress(asyncio.CancelledError):
                    await sweep

    async def health(request):
        # A real DB operation ensures the broker is ready to accept work.
        await broker.call("recover_expired")
        return JSONResponse({"ok": True, "version": __version__})

    async def jobs(request: Request):
        if request.method == "POST":
            if (
                request.headers.get("content-type", "").split(";")[0].strip().lower()
                != "application/json"
            ):
                raise BrokerError("unsupported_media_type", "Use application/json", 415)
            try:
                payload = await request.json()
            except (json.JSONDecodeError, UnicodeError, RecursionError):
                raise BrokerError("invalid_json", "Request must contain valid JSON", 400) from None
            job, created = await broker.mutate("enqueue", JobInput.model_validate(payload))
            return JSONResponse(
                job, 201 if created else 200, headers={"Location": f"/v1/jobs/{job['id']}"}
            )
        query = ListQuery.model_validate(dict(request.query_params))
        return JSONResponse(await broker.call("list_jobs", **query.model_dump()))

    async def job(request):
        return JSONResponse(await broker.call("get_job", request.path_params["job_id"]))

    async def action(request):
        return JSONResponse(
            await broker.mutate(request.path_params["action"], request.path_params["job_id"])
        )

    async def queues(request):
        return JSONResponse(await broker.call("list_queues"))

    async def workers(request):
        return JSONResponse(await broker.workers())

    async def broker_error(request, exc):
        return JSONResponse({"error": {"code": exc.code, "message": exc.message}}, exc.status)

    async def validation_error(request, exc):
        return JSONResponse(
            {
                "error": {
                    "code": "validation_error",
                    "message": "Invalid request",
                    "details": [
                        {"location": list(e["loc"]), "message": e["msg"]}
                        for e in exc.errors(include_input=False, include_context=False)
                    ],
                }
            },
            422,
        )

    async def storage_error(request, exc):
        return JSONResponse(
            {
                "error": {
                    "code": "storage_unavailable",
                    "message": "Storage unavailable; retry later",
                }
            },
            503,
        )

    async def http_error(request, exc):
        return JSONResponse(
            {"error": {"code": "http_error", "message": str(exc.detail)}},
            exc.status_code,
            headers=exc.headers,
        )

    async def unexpected_error(request, exc):
        # Do not expose task data, SQL bindings, paths, or exception text.
        logging.getLogger("rmcpq").error("Request failed (%s)", type(exc).__name__)
        return JSONResponse({"error": {"code": "internal_error", "message": "Internal error"}}, 500)

    routes = [
        Route("/v1/health", health),
        Route("/v1/jobs", jobs, methods=["GET", "POST"]),
        Route("/v1/jobs/{job_id}", job),
        Route("/v1/queues", queues),
        Route("/v1/workers", workers),
    ]
    # Fixed routes prevent path input from selecting an arbitrary Store method.
    for name in ("cancel", "retry"):

        async def dispatch(request, operation=name):
            request.path_params["action"] = operation
            return await action(request)

        routes.append(Route(f"/v1/jobs/{{job_id}}/{name}", dispatch, methods=["POST"]))
    routes.append(Mount("/", mcp_app))
    app = Starlette(
        routes=routes,
        lifespan=lifespan,
        exception_handlers={
            BrokerError: broker_error,
            ValidationError: validation_error,
            sqlite3.Error: storage_error,
            HTTPException: http_error,
            Exception: unexpected_error,
        },
    )
    app.add_middleware(LocalOnly)
    app.state.broker = broker
    app.state.mcp = mcp
    return app
