"""Transactional queue core. Each operation owns its SQLite connection and transaction.

Call this synchronous class in a worker thread, never on an ASGI event loop.
"""

import json
import math
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from secrets import token_urlsafe
from uuid import uuid4

from .models import DEFAULT_WAIT, MAX_WAIT, BrokerError, JobInput, canonical


def timestamp(value: float | None) -> str | None:
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


class Store:
    def __init__(
        self,
        path: str | Path,
        *,
        lease_seconds: float = 600,
        max_attempts: int = 3,
        clock: Callable[[], float] = time.time,
    ):
        if lease_seconds <= 0 or max_attempts < 1:
            raise ValueError("lease_seconds and max_attempts must be positive")
        if str(path) == ":memory:":
            raise ValueError("Use a temporary database file; connections are operation-scoped")
        self.path = Path(path)
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.clock = clock

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=5000")
        return db

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = self._connect()
        try:
            db.execute("PRAGMA journal_mode=WAL")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2, 3):
                raise RuntimeError(f"Unsupported database schema version: {version}")
            db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS workers (
                    id TEXT PRIMARY KEY, queues TEXT NOT NULL, running INTEGER NOT NULL,
                    generation INTEGER NOT NULL, created_at REAL NOT NULL, seen_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
                    queue TEXT NOT NULL, instruction TEXT NOT NULL, context TEXT NOT NULL,
                    result_format TEXT NOT NULL CHECK(result_format IN ('text','json')),
                    idempotency_key TEXT, request TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN
                        ('queued','processing','completed','failed','cancelled')),
                    result TEXT, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    attempts_in_run INTEGER NOT NULL DEFAULT 0, run INTEGER NOT NULL DEFAULT 1,
                    max_attempts INTEGER NOT NULL, worker_id TEXT REFERENCES workers(id),
                    claim_token TEXT, lease_until REAL, created_at REAL NOT NULL,
                    updated_at REAL NOT NULL, finished_at REAL,
                    UNIQUE(queue, idempotency_key)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    job_id TEXT NOT NULL REFERENCES jobs(id), number INTEGER NOT NULL,
                    run INTEGER NOT NULL, worker_id TEXT NOT NULL REFERENCES workers(id),
                    claim_token TEXT NOT NULL, status TEXT NOT NULL,
                    started_at REAL NOT NULL, lease_until REAL NOT NULL,
                    ended_at REAL, error TEXT, PRIMARY KEY(job_id,number)
                );
                CREATE INDEX IF NOT EXISTS queue_claim ON jobs(status, queue, seq);
                CREATE UNIQUE INDEX IF NOT EXISTS one_claim_per_worker
                    ON jobs(worker_id) WHERE status='processing';
            """)
            if version < 2:
                db.execute(f"""ALTER TABLE workers ADD COLUMN wait_seconds REAL NOT NULL
                               DEFAULT {DEFAULT_WAIT} CHECK(wait_seconds>0 AND wait_seconds<={MAX_WAIT})""")
            if version < 3:
                db.execute("ALTER TABLE workers ADD COLUMN model TEXT")
                db.execute("ALTER TABLE jobs ADD COLUMN model TEXT")
                db.execute("ALTER TABLE attempts ADD COLUMN model TEXT")
            db.execute("PRAGMA user_version=3")
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()
        self.recover_expired()

    def _job(self, db, job_id):
        row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise BrokerError("not_found", "Job not found", 404)
        return row

    def _worker(self, db, worker_id):
        row = db.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        if row is None:
            raise BrokerError("not_found", "Worker not found; call start first", 404)
        return row

    def _public_job(self, row):
        keys = (
            "id",
            "queue",
            "instruction",
            "result_format",
            "status",
            "error",
            "attempts",
            "attempts_in_run",
            "run",
            "max_attempts",
            "worker_id",
            "model",
        )
        data = {key: row[key] for key in keys}
        data.update(
            context=json.loads(row["context"]),
            result=json.loads(row["result"]) if row["result"] is not None else None,
        )
        for key in ("created_at", "updated_at", "finished_at", "lease_until"):
            data[key] = timestamp(row[key])
        return data

    def _public_worker(self, row):
        return {
            "worker_id": row["id"],
            "model": row["model"],
            "queues": json.loads(row["queues"]),
            "running": bool(row["running"]),
            "generation": row["generation"],
            "created_at": timestamp(row["created_at"]),
            "last_seen": timestamp(row["seen_at"]),
            "wait_seconds": row["wait_seconds"],
        }

    def _end_attempt(self, db, row, outcome, reason, now):
        db.execute(
            "UPDATE attempts SET status=?,ended_at=?,error=? WHERE job_id=? AND number=?",
            (outcome, now, reason, row["id"], row["attempts"]),
        )

    def _release(self, db, row, outcome, reason, now):
        exhausted = row["attempts_in_run"] >= row["max_attempts"]
        self._end_attempt(db, row, outcome, reason, now)
        db.execute(
            """UPDATE jobs SET status=?,worker_id=NULL,claim_token=NULL,lease_until=NULL,
                      error=?,updated_at=?,finished_at=? WHERE id=?""",
            (
                "failed" if exhausted else "queued",
                "Attempt limit reached: " + reason if exhausted else None,
                now,
                now if exhausted else None,
                row["id"],
            ),
        )

    def _recover(self, db, now):
        rows = db.execute(
            "SELECT * FROM jobs WHERE status='processing' AND lease_until<=?", (now,)
        ).fetchall()
        for row in rows:
            self._release(db, row, "expired", "Claim lease expired", now)
        return len(rows)

    def recover_expired(self):
        with self._tx() as db:
            return self._recover(db, self.clock())

    def enqueue(self, job: JobInput):
        request = canonical(job.model_dump(exclude={"idempotency_key"}))
        with self._tx() as db:
            if job.idempotency_key is not None:
                old = db.execute(
                    "SELECT * FROM jobs WHERE queue=? AND idempotency_key=?",
                    (job.queue, job.idempotency_key),
                ).fetchone()
                if old:
                    if old["request"] != request:
                        raise BrokerError(
                            "idempotency_conflict", "Key already used for a different job"
                        )
                    return self._public_job(old), False
            now, job_id = self.clock(), str(uuid4())
            db.execute(
                """INSERT INTO jobs(id,queue,instruction,context,result_format,idempotency_key,
                          request,status,max_attempts,created_at,updated_at)
                          VALUES(?,?,?,?,?,?,?,'queued',?,?,?)""",
                (
                    job_id,
                    job.queue,
                    job.instruction,
                    canonical(job.context),
                    job.result_format,
                    job.idempotency_key,
                    request,
                    self.max_attempts,
                    now,
                    now,
                ),
            )
            return self._public_job(self._job(db, job_id)), True

    def get_job(self, job_id):
        with self._tx() as db:
            self._recover(db, self.clock())
            data = self._public_job(self._job(db, job_id))
            rows = db.execute(
                """SELECT number,run,worker_id,status,started_at,lease_until,ended_at,error,model
                                 FROM attempts WHERE job_id=? ORDER BY number""",
                (job_id,),
            ).fetchall()
            data["history"] = []
            for row in rows:
                attempt = dict(row)
                for key in ("started_at", "lease_until", "ended_at"):
                    attempt[key] = timestamp(attempt[key])
                data["history"].append(attempt)
            return data

    def list_jobs(self, *, queue=None, status=None, limit=100, offset=0, q=None):
        clauses, args = [], []
        for key, value in (("queue", queue), ("status", status)):
            if value is not None:
                clauses.append(f"{key}=?")
                args.append(value)
        if q and q.strip():
            clauses.append(
                "(instr(lower(id),lower(?))>0 OR instr(lower(instruction),lower(?))>0 OR instr(lower(queue),lower(?))>0)"
            )
            args.extend([q.strip()] * 3)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._tx() as db:
            self._recover(db, self.clock())
            rows = db.execute(
                "SELECT * FROM jobs" + where + " ORDER BY seq LIMIT ? OFFSET ?",
                (*args, limit, offset),
            ).fetchall()
            total = db.execute("SELECT count(*) FROM jobs" + where, args).fetchone()[0]
            return {
                "jobs": [self._public_job(row) for row in rows],
                "total": total,
                "limit": limit,
                "offset": offset,
            }

    def start(
        self,
        queues: list[str],
        worker_id: str | None = None,
        wait_seconds: float | None = None,
        model: str | None = None,
    ):
        if wait_seconds is not None and (
            not math.isfinite(wait_seconds) or not 0 < wait_seconds <= MAX_WAIT
        ):
            raise BrokerError(
                "invalid_wait",
                f"Worker wait_seconds must be greater than 0 and at most {MAX_WAIT}",
                422,
            )
        worker_id = worker_id or "worker-" + str(uuid4())
        queues_json = canonical(sorted(set(queues)))
        with self._tx() as db:
            now = self.clock()
            self._recover(db, now)
            old = db.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
            selected_model = (
                model if model is not None else (old["model"] if old and old["running"] else None)
            )
            selected_wait = (
                wait_seconds
                if wait_seconds is not None
                else (old["wait_seconds"] if old else DEFAULT_WAIT)
            )
            if old and old["running"]:
                if old["queues"] != queues_json:
                    raise BrokerError(
                        "worker_scope_conflict", "Stop the worker before changing queues"
                    )
                db.execute(
                    "UPDATE workers SET seen_at=?,wait_seconds=?,model=? WHERE id=?",
                    (now, selected_wait, selected_model, worker_id),
                )
            elif old:
                db.execute(
                    """UPDATE workers SET queues=?,running=1,generation=generation+1,seen_at=?,wait_seconds=?,model=?
                              WHERE id=?""",
                    (queues_json, now, selected_wait, selected_model, worker_id),
                )
            else:
                db.execute(
                    """INSERT INTO workers(id,queues,running,generation,created_at,seen_at,wait_seconds,model)
                       VALUES(?,?,1,1,?,?,?,?)""",
                    (worker_id, queues_json, now, now, selected_wait, selected_model),
                )
            return self._public_worker(self._worker(db, worker_id))

    def worker(self, worker_id):
        with self._tx() as db:
            return self._public_worker(self._worker(db, worker_id))

    def claim(self, worker_id, generation):
        with self._tx() as db:
            now = self.clock()
            self._recover(db, now)
            worker = self._worker(db, worker_id)
            if not worker["running"] or worker["generation"] != generation:
                return {"status": "stopped", "job": None}
            active = db.execute(
                "SELECT * FROM jobs WHERE worker_id=? AND status='processing'", (worker_id,)
            ).fetchone()
            # A lost wait response can be recovered by the same stable worker identity.
            if active is None:
                queues = json.loads(worker["queues"])
                placeholders = ",".join("?" for _ in queues)
                active = db.execute(
                    f"""SELECT * FROM jobs WHERE status='queued'
                                       AND queue IN ({placeholders}) ORDER BY seq LIMIT 1""",
                    queues,
                ).fetchone()
                if active:
                    token, until = token_urlsafe(32), now + self.lease_seconds
                    number = active["attempts"] + 1
                    db.execute(
                        """UPDATE jobs SET status='processing',worker_id=?,claim_token=?,
                                  lease_until=?,attempts=?,attempts_in_run=attempts_in_run+1,
                                  updated_at=?,model=? WHERE id=?""",
                        (worker_id, token, until, number, now, worker["model"], active["id"]),
                    )
                    db.execute(
                        """INSERT INTO attempts(job_id,number,run,worker_id,claim_token,status,
                           started_at,lease_until,model) VALUES(?,?,?,?,?,'processing',?,?,?)""",
                        (
                            active["id"],
                            number,
                            active["run"],
                            worker_id,
                            token,
                            now,
                            until,
                            worker["model"],
                        ),
                    )
                    active = self._job(db, active["id"])
            db.execute("UPDATE workers SET seen_at=? WHERE id=?", (now, worker_id))
            if active:
                return {
                    "status": "job",
                    "job": {**self._public_job(active), "claim_token": active["claim_token"]},
                }
            return {"status": "idle", "job": None}

    def _assert_claim(self, row, token, now):
        if (
            row["status"] != "processing"
            or row["claim_token"] != token
            or row["lease_until"] <= now
        ):
            raise BrokerError("stale_claim", "Claim is no longer valid; wait for a fresh claim")

    def complete(self, job_id, claim_token, result):
        try:
            encoded = canonical(result)
        except ValueError as exc:
            raise BrokerError("invalid_result", str(exc), 422) from exc
        with self._tx() as db:
            row, now = self._job(db, job_id), self.clock()
            if row["result_format"] == "text" and not isinstance(result, str):
                raise BrokerError("invalid_result", "Text jobs require a string result", 422)
            if (
                row["status"] == "completed"
                and row["claim_token"] == claim_token
                and row["result"] == encoded
            ):
                return self._public_job(row)
            self._assert_claim(row, claim_token, now)
            self._end_attempt(db, row, "completed", None, now)
            db.execute(
                """UPDATE jobs SET status='completed',result=?,error=NULL,lease_until=NULL,
                          updated_at=?,finished_at=? WHERE id=?""",
                (encoded, now, now, job_id),
            )
            db.execute("UPDATE workers SET seen_at=? WHERE id=?", (now, row["worker_id"]))
            return self._public_job(self._job(db, job_id))

    def renew(self, job_id, claim_token):
        with self._tx() as db:
            row, now = self._job(db, job_id), self.clock()
            self._assert_claim(row, claim_token, now)
            until = now + self.lease_seconds
            db.execute(
                "UPDATE jobs SET lease_until=?,updated_at=? WHERE id=?", (until, now, job_id)
            )
            db.execute(
                "UPDATE attempts SET lease_until=? WHERE job_id=? AND number=?",
                (until, job_id, row["attempts"]),
            )
            db.execute("UPDATE workers SET seen_at=? WHERE id=?", (now, row["worker_id"]))
            return {"job_id": job_id, "lease_until": timestamp(until)}

    def fail(self, job_id, claim_token, error):
        with self._tx() as db:
            row, now = self._job(db, job_id), self.clock()
            # A lost failure response is safe to repeat too.
            if (
                row["status"] == "failed"
                and row["claim_token"] == claim_token
                and row["error"] == error
            ):
                return self._public_job(row)
            self._assert_claim(row, claim_token, now)
            self._end_attempt(db, row, "failed", error, now)
            db.execute(
                """UPDATE jobs SET status='failed',error=?,lease_until=NULL,updated_at=?,
                          finished_at=? WHERE id=?""",
                (error, now, now, job_id),
            )
            db.execute("UPDATE workers SET seen_at=? WHERE id=?", (now, row["worker_id"]))
            return self._public_job(self._job(db, job_id))

    def cancel(self, job_id):
        with self._tx() as db:
            self._recover(db, self.clock())
            row, now = self._job(db, job_id), self.clock()
            if row["status"] == "cancelled":
                return self._public_job(row)
            if row["status"] not in ("queued", "processing"):
                raise BrokerError(
                    "invalid_state", "Only queued or processing jobs can be cancelled"
                )
            if row["status"] == "processing":
                self._end_attempt(db, row, "cancelled", "Cancelled by producer", now)
            db.execute(
                """UPDATE jobs SET status='cancelled',claim_token=NULL,lease_until=NULL,
                          updated_at=?,finished_at=? WHERE id=?""",
                (now, now, job_id),
            )
            return self._public_job(self._job(db, job_id))

    def retry(self, job_id):
        with self._tx() as db:
            self._recover(db, self.clock())
            row, now = self._job(db, job_id), self.clock()
            if row["status"] not in ("failed", "cancelled"):
                raise BrokerError("invalid_state", "Only failed or cancelled jobs can be retried")
            db.execute(
                """UPDATE jobs SET status='queued',worker_id=NULL,claim_token=NULL,
                          lease_until=NULL,result=NULL,error=NULL,attempts_in_run=0,run=run+1,
                          max_attempts=?,updated_at=?,finished_at=NULL WHERE id=?""",
                (self.max_attempts, now, job_id),
            )
            return self._public_job(self._job(db, job_id))

    def stop(self, worker_id):
        with self._tx() as db:
            self._worker(db, worker_id)
            now = self.clock()
            rows = db.execute(
                "SELECT * FROM jobs WHERE status='processing' AND worker_id=?", (worker_id,)
            ).fetchall()
            for row in rows:
                self._release(db, row, "released", "Worker stopped", now)
            db.execute(
                "UPDATE workers SET running=0,generation=generation+1,seen_at=? WHERE id=?",
                (now, worker_id),
            )
            return {"worker_id": worker_id, "released_jobs": len(rows)}

    def list_queues(self):
        with self._tx() as db:
            self._recover(db, self.clock())
            names = {r[0] for r in db.execute("SELECT DISTINCT queue FROM jobs")}
            for row in db.execute("SELECT queues FROM workers"):
                names.update(json.loads(row[0]))
            queues = []
            for name in sorted(names):
                counts = {
                    state: 0
                    for state in ("queued", "processing", "completed", "failed", "cancelled")
                }
                for row in db.execute(
                    "SELECT status,count(*) FROM jobs WHERE queue=? GROUP BY status", (name,)
                ):
                    counts[row[0]] = row[1]
                queues.append({"queue": name, "counts": counts})
            return {"queues": queues}

    def list_workers(self):
        with self._tx() as db:
            self._recover(db, self.clock())
            workers = []
            for row in db.execute("SELECT * FROM workers ORDER BY created_at,id"):
                data = self._public_worker(row)
                job = db.execute(
                    "SELECT id FROM jobs WHERE status='processing' AND worker_id=?", (row["id"],)
                ).fetchone()
                data.update(
                    active_job_id=job[0] if job else None,
                    state="stopped" if not row["running"] else "claimed" if job else "unknown",
                )
                workers.append(data)
            return {"workers": workers}
