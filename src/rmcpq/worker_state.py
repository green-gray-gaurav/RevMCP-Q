"""Local execution journal and process lock. No credentials or claim tokens are stored."""

import hashlib
import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import urlsplit


def identity_key(url, worker_id):
    parsed = urlsplit(url)
    # All supported loopback aliases identify the same local broker port. This
    # prevents localhost vs 127.0.0.1 from bypassing the per-worker process lock.
    origin = f"{parsed.scheme}://loopback:{parsed.port or 80}"
    return hashlib.sha256(f"{origin}\n{worker_id}".encode()).hexdigest()


class WorkerLock:
    """OS locks are released even when a runner is killed without cleanup."""

    def __init__(self, directory, key):
        self.path = Path(directory) / (key + ".lock")
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+b")
        self.file.seek(0, 2)
        if self.file.tell() == 0:
            self.file.write(b"0")
            self.file.flush()
        self.file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            self.file = None
            raise ValueError("This worker ID is already running on this computer") from None
        return self

    def __exit__(self, *args):
        if self.file is not None:
            if os.name == "nt":
                import msvcrt

                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file, fcntl.LOCK_UN)
            self.file.close()


class Journal:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS executions (
                job_id TEXT NOT NULL, attempt INTEGER NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY(job_id, attempt))""")

    def read(self, job_id, attempt):
        with sqlite3.connect(self.path) as db:
            row = db.execute(
                "SELECT payload FROM executions WHERE job_id=? AND attempt=?", (job_id, attempt)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def write(self, job_id, attempt, payload):
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        with sqlite3.connect(self.path) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                "INSERT INTO executions VALUES(?,?,?) ON CONFLICT(job_id,attempt) "
                "DO UPDATE SET payload=excluded.payload",
                (job_id, attempt, encoded),
            )
