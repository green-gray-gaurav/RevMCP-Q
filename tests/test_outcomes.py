import json

import pytest

from rmcpq.hosts.codex import CodexHost, decode_outcome, validate_artifacts
from rmcpq.models import JobInput
from rmcpq.worker import Runner, WorkerError, WorkerOptions
from rmcpq.worker_state import Journal


def envelope(result=None, *, outcome="success", error=None, artifacts=None):
    return json.dumps(
        {"outcome": outcome, "result": result, "error": error, "artifacts": artifacts or []}
    )


def test_success_payload_keeps_application_error_keys_opaque():
    result = {"status": "error", "message": "A log entry being analyzed"}
    text, paths = decode_outcome(envelope(result), "json")
    assert json.loads(text) == result and paths == []
    assert decode_outcome(envelope("answer"), "text")[0] == "answer"
    assert decode_outcome(envelope(None), "json")[0] == "null"


@pytest.mark.parametrize(
    "value",
    [
        "plain text",
        '{"status":"error"}',
        envelope("error", outcome="failure", error="Failed"),
        envelope(None, outcome="failure", error=""),
        envelope("ok", error="Failed"),
        envelope({}, outcome="unknown"),
        envelope({}, artifacts=[12]),
    ],
)
def test_invalid_outcome_fails_closed(value):
    with pytest.raises(WorkerError) as exc:
        decode_outcome(value, "json")
    assert exc.value.code == "invalid_outcome"


def test_model_failure_becomes_execution_failed():
    with pytest.raises(WorkerError) as exc:
        decode_outcome(envelope(outcome="failure", error="Image tool refused the request"), "json")
    assert exc.value.code == "execution_failed"
    assert "refused" in exc.value.message
    with pytest.raises(WorkerError):
        decode_outcome(envelope({}), "text")


def test_artifacts_must_be_existing_nonempty_absolute_files(tmp_path):
    output = tmp_path / "output.png"
    for path in (str(output), str(tmp_path), "relative.png"):
        with pytest.raises(WorkerError) as exc:
            validate_artifacts([path])
        assert exc.value.code == "missing_artifact"
    output.touch()
    with pytest.raises(WorkerError):
        validate_artifacts([str(output)])
    output.write_bytes(b"fixture")
    validate_artifacts([str(output)])


class OutcomeHost:
    outcome_contract = CodexHost.outcome_contract

    def __init__(self, answer):
        self.answer = answer
        self.adapter = CodexHost(WorkerOptions())

    async def preflight(self):
        pass

    async def interrupt(self):
        pass

    async def execute(self, job, saved, record):
        await record({"phase": "running", "thread_id": "thread", "turn_id": "turn"})
        return await self.adapter.finish(self.answer, job["result_format"])


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["failure", "missing_file", "success"])
async def test_outcomes_use_correct_broker_transition(server, tmp_path, kind):
    path = tmp_path / "generated.png"
    if kind == "success":
        path.write_bytes(b"generated fixture")
    answer = (
        envelope(outcome="failure", error="Generation was refused")
        if kind == "failure"
        else envelope({"filepath": str(path)}, artifacts=[str(path)])
    )
    store = server.app.state.broker.store
    job = store.enqueue(JobInput(queue="images", instruction="Generate", result_format="json"))[0]
    options = WorkerOptions(url=server.url, queues=["images"], cwd=tmp_path, max_jobs=1)
    worker = Runner(options, OutcomeHost(answer), Journal(tmp_path / "journal.sqlite"))
    await worker.run()
    saved = store.get_job(job["id"])
    assert saved["status"] == ("completed" if kind == "success" else "failed")
    assert saved["history"][0]["status"] == saved["status"]
    if kind == "success":
        assert saved["result"] == {"filepath": str(path)}
    else:
        assert saved["result"] is None and saved["error"]


@pytest.mark.anyio
async def test_legacy_completed_journal_is_not_treated_as_verified_success(server, tmp_path):
    store = server.app.state.broker.store
    job = store.enqueue(JobInput(queue="images", instruction="Generate", result_format="json"))[0]
    registration = store.start(["images"], "codex-worker")
    store.claim("codex-worker", registration["generation"])
    journal = Journal(tmp_path / "legacy.sqlite")
    journal.write(
        job["id"],
        1,
        {
            "phase": "completed",
            "result": {"status": "error"},
            "settings": {"cwd": str(tmp_path), "model": None, "sandbox": "workspace-write"},
        },
    )
    options = WorkerOptions(url=server.url, queues=["images"], cwd=tmp_path, max_jobs=1)
    worker = Runner(options, OutcomeHost(envelope("unused")), journal)
    with pytest.raises(WorkerError) as exc:
        await worker.run()
    assert exc.value.code == "recovery_contract_changed"
    assert store.get_job(job["id"])["status"] == "queued"
    assert store.get_job(job["id"])["result"] is None
