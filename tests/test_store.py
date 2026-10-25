import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from pydantic import ValidationError

from rmcpq.models import BrokerError, JobInput, WorkerInput
from rmcpq.store import Store


def enqueue(store, queue="a", **kwargs):
    return store.enqueue(
        JobInput(queue=queue, instruction="Explain the supplied context", **kwargs)
    )[0]


def claim(store, worker="worker", queues=None):
    registration = store.start(queues or ["a"], worker)
    return store.claim(worker, registration["generation"])["job"]


def assert_error(code, action):
    with pytest.raises(BrokerError) as caught:
        action()
    assert caught.value.code == code


def test_exclusive_claims_and_public_redaction(store):
    job = enqueue(store)
    workers = [store.start(["a"], f"w{i}") for i in range(12)]
    with ThreadPoolExecutor(max_workers=12) as pool:
        answers = list(pool.map(lambda w: store.claim(w["worker_id"], w["generation"]), workers))
    claimed = [a["job"] for a in answers if a["job"]]
    assert len(claimed) == 1
    assert claimed[0]["id"] == job["id"]
    assert len(claimed[0]["claim_token"]) > 30
    for public in (store.get_job(job["id"]), store.list_jobs(), store.list_workers()):
        assert "claim_token" not in json.dumps(public)
        assert claimed[0]["claim_token"] not in json.dumps(public)


def test_fifo_routing_and_one_active_job_per_worker(store):
    ignored = enqueue(store, "other")
    first = enqueue(store, "b")
    second = enqueue(store, "a")
    got = claim(store, queues=["a", "b"])
    assert got["id"] == first["id"]
    recovered = claim(store, queues=["a", "b"])
    assert recovered["claim_token"] == got["claim_token"]
    assert recovered["attempts"] == 1
    store.complete(got["id"], got["claim_token"], "first")
    assert claim(store, queues=["a", "b"])["id"] == second["id"]
    assert store.get_job(ignored["id"])["status"] == "queued"


def test_lease_renewal_and_stale_token_fencing(store, clock):
    job = enqueue(store)
    old = claim(store)
    clock.advance(6)
    renewed = store.renew(job["id"], old["claim_token"])
    clock.advance(6)
    assert store.get_job(job["id"])["status"] == "processing"
    assert renewed["lease_until"] == store.get_job(job["id"])["lease_until"]
    clock.advance(5)
    fresh = claim(store, "replacement")
    assert fresh["claim_token"] != old["claim_token"]
    for operation in (
        lambda: store.complete(job["id"], old["claim_token"], "old"),
        lambda: store.renew(job["id"], old["claim_token"]),
        lambda: store.fail(job["id"], old["claim_token"], "late"),
    ):
        assert_error("stale_claim", operation)
    assert store.complete(job["id"], fresh["claim_token"], "new")["result"] == "new"
    assert [a["status"] for a in store.get_job(job["id"])["history"]] == ["expired", "completed"]


def test_attempt_limit_manual_retry_and_history(store, clock):
    job = enqueue(store)
    for number in range(1, 4):
        got = claim(store)
        assert got["attempts"] == number
        clock.advance()
        store.recover_expired()
    failed = store.get_job(job["id"])
    assert failed["status"] == "failed" and len(failed["history"]) == 3
    assert claim(store) is None
    retried = store.retry(job["id"])
    assert retried["attempts"] == 3 and retried["attempts_in_run"] == 0 and retried["run"] == 2
    got = claim(store)
    assert got["attempts"] == 4 and got["attempts_in_run"] == 1
    store.fail(job["id"], got["claim_token"], "Source incomplete")
    assert store.fail(job["id"], got["claim_token"], "Source incomplete")["status"] == "failed"
    assert len(store.get_job(job["id"])["history"]) == 4


@pytest.mark.parametrize(
    "result", ["", "explanation", {"a": [1, True, None]}, [1, 2], None, 5, False]
)
def test_result_resubmissions_are_duplicate_safe(store, result):
    job = enqueue(store, result_format="json")
    got = claim(store)
    saved = store.complete(job["id"], got["claim_token"], result)
    assert saved == store.complete(job["id"], got["claim_token"], result)
    assert saved["result"] == result
    assert_error(
        "stale_claim", lambda: store.complete(job["id"], got["claim_token"], {"changed": True})
    )
    assert len(store.get_job(job["id"])["history"]) == 1


def test_invalid_results_do_not_complete_job(store):
    job = enqueue(store)
    got = claim(store)
    assert_error("invalid_result", lambda: store.complete(job["id"], got["claim_token"], {}))
    assert_error(
        "invalid_result", lambda: store.complete(job["id"], got["claim_token"], float("nan"))
    )
    assert_error(
        "invalid_result", lambda: store.complete(job["id"], got["claim_token"], "x" * 1_048_576)
    )
    assert store.get_job(job["id"])["status"] == "processing"


def test_queue_scoped_idempotency_and_payload_conflicts(store):
    original = JobInput(
        queue="a", instruction="Do it", context={"a": 1, "b": 2}, idempotency_key="key"
    )
    first, created = store.enqueue(original)
    second, created_again = store.enqueue(original.model_copy(update={"context": {"b": 2, "a": 1}}))
    assert created and not created_again and first["id"] == second["id"]
    assert_error(
        "idempotency_conflict",
        lambda: store.enqueue(original.model_copy(update={"instruction": "Changed"})),
    )
    other, _ = store.enqueue(original.model_copy(update={"queue": "b"}))
    assert other["id"] != first["id"]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.enqueue(original), range(8)))
    assert {r[0]["id"] for r in results} == {first["id"]}


def test_cancel_invalidates_claim_retry_does_not_reuse_it(store):
    job = enqueue(store)
    old = claim(store)
    store.cancel(job["id"])
    assert store.cancel(job["id"])["status"] == "cancelled"
    assert_error("stale_claim", lambda: store.complete(job["id"], old["claim_token"], "late"))
    store.retry(job["id"])
    fresh = claim(store)
    assert fresh["claim_token"] != old["claim_token"]
    store.complete(job["id"], fresh["claim_token"], "fresh")
    assert_error("invalid_state", lambda: store.cancel(job["id"]))
    assert_error("invalid_state", lambda: store.retry(job["id"]))


def test_stop_releases_and_generation_fences_old_waits(store):
    job = enqueue(store)
    old = claim(store)
    old_generation = store.worker("worker")["generation"]
    assert_error("worker_scope_conflict", lambda: store.start(["b"], "worker"))
    assert store.stop("worker")["released_jobs"] == 1
    assert store.get_job(job["id"])["status"] == "queued"
    assert_error("stale_claim", lambda: store.complete(job["id"], old["claim_token"], "late"))
    restarted = store.start(["b"], "worker")
    assert restarted["generation"] > old_generation
    assert store.claim("worker", old_generation)["status"] == "stopped"
    assert store.claim("worker", restarted["generation"])["job"] is None


def test_restart_preserves_jobs_workers_and_claims_until_expiry(store, clock):
    job = enqueue(store)
    old = claim(store)
    reopened = Store(store.path, lease_seconds=10, clock=clock)
    reopened.initialize()
    assert reopened.get_job(job["id"])["status"] == "processing"
    assert claim(reopened)["claim_token"] == old["claim_token"]
    clock.advance()
    restarted = Store(store.path, lease_seconds=10, clock=clock)
    restarted.initialize()
    assert restarted.get_job(job["id"])["status"] == "queued"
    assert restarted.list_workers()["workers"][0]["state"] == "unknown"


def test_schema_version_rejects_future_database(store):
    with sqlite3.connect(store.path) as db:
        db.execute("PRAGMA user_version=999")
    with pytest.raises(RuntimeError, match="Unsupported database"):
        store.initialize()


@pytest.mark.parametrize(
    "fields",
    [
        {"queue": "../bad"},
        {"instruction": " "},
        {"context": {"bad": float("nan")}},
        {"unexpected": "x"},
        {"instruction": "x" * 200_001},
    ],
)
def test_input_validation(fields):
    payload = {"queue": "a", "instruction": "Do it", **fields}
    with pytest.raises(ValidationError):
        JobInput.model_validate(payload)


def test_worker_subscriptions_are_explicit_and_normalized():
    assert WorkerInput(queues=["b", "a", "b"]).queues == ["a", "b"]
    with pytest.raises(ValidationError):
        WorkerInput(queues=[])


def test_inspection_filters_and_queue_counts(store):
    a = enqueue(store)
    enqueue(store, "b")
    store.cancel(a["id"])
    assert store.list_jobs(queue="a", status="cancelled")["total"] == 1
    assert store.list_jobs(limit=1, offset=1)["jobs"][0]["queue"] == "b"
    counts = {q["queue"]: q["counts"] for q in store.list_queues()["queues"]}
    assert counts["a"]["cancelled"] == counts["b"]["queued"] == 1
