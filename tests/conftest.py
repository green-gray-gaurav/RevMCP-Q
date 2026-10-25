import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn

from rmcpq.app import create_app
from rmcpq.store import Store


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds=11):
        self.now += seconds


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def store(tmp_path, clock):
    result = Store(tmp_path / "queue.sqlite", lease_seconds=10, clock=clock)
    result.initialize()
    return result


class LiveServer:
    def __init__(self, db_path: Path, **options):
        self.app = create_app(db_path, **options)
        self.socket = socket.socket()
        self.socket.bind(("127.0.0.1", 0))
        self.url = f"http://127.0.0.1:{self.socket.getsockname()[1]}"
        self.server = uvicorn.Server(
            uvicorn.Config(
                self.app, log_level="critical", access_log=False, timeout_graceful_shutdown=2
            )
        )
        self.thread = threading.Thread(
            target=self.server.run, kwargs={"sockets": [self.socket]}, daemon=True
        )
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if not self.thread.is_alive() or time.monotonic() >= deadline:
                raise RuntimeError("Test broker failed to start")
            time.sleep(0.01)

    def close(self):
        self.server.should_exit = True
        self.thread.join(timeout=6)
        self.socket.close()
        assert not self.thread.is_alive(), "Test broker did not shut down"


@pytest.fixture
def server_factory(tmp_path):
    servers = []

    def factory(db_path=None, **options):
        server = LiveServer(db_path or tmp_path / f"server-{len(servers)}.sqlite", **options)
        servers.append(server)
        return server

    yield factory
    for server in servers:
        if server.thread.is_alive():
            server.close()


@pytest.fixture
def server(server_factory):
    return server_factory()
