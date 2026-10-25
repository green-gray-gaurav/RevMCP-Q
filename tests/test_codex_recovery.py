"""Recovery inspects existing turns instead of blindly creating new turns."""

from types import SimpleNamespace

import pytest

from rmcpq.hosts.codex import CodexHost
from rmcpq.worker import WorkerError, WorkerOptions

pytestmark = pytest.mark.anyio


class Thread:
    id = "thread-1"

    def __init__(self, turns):
        self.turns = turns

    async def read(self, **kwargs):
        return SimpleNamespace(model_dump=lambda **kwargs: {"thread": {"turns": self.turns}})


async def host_with_history(tmp_path, turns):
    host = CodexHost(WorkerOptions(cwd=tmp_path))
    thread = Thread(turns)

    async def resume(thread_id, **kwargs):
        assert thread_id == thread.id
        return thread

    host.codex = SimpleNamespace(thread_resume=resume)
    # Keep these recovery tests independent of the optional SDK installation.
    host.thread_options = lambda: {}
    return host, thread


async def test_recovered_completed_turn_uses_final_answer(tmp_path):
    host, _ = await host_with_history(
        tmp_path,
        [
            {
                "id": "turn-1",
                "status": "completed",
                "items": [
                    {"type": "agentMessage", "phase": "commentary", "text": "working"},
                    {"type": "agentMessage", "phase": "final_answer", "text": "saved answer"},
                ],
            }
        ],
    )
    _, result = await host.recover(
        {"phase": "running", "thread_id": "thread-1", "turn_id": "turn-1"}
    )
    assert result == "saved answer" and host.active is None


async def test_lost_start_response_recovers_single_existing_turn(tmp_path):
    host, _ = await host_with_history(
        tmp_path,
        [
            {
                "id": "turn-1",
                "status": "completed",
                "items": [
                    {"type": "agentMessage", "text": "answer"},
                ],
            }
        ],
    )
    _, result = await host.recover({"phase": "starting", "thread_id": "thread-1"})
    assert result == "answer"


async def test_prepared_thread_without_turn_can_start_safely(tmp_path):
    host, thread = await host_with_history(tmp_path, [])
    assert await host.recover({"phase": "prepared", "thread_id": "thread-1"}) == (thread, None)


@pytest.mark.parametrize("turns", [[], [{"id": "x"}, {"id": "y"}]])
async def test_ambiguous_start_is_not_rerun(tmp_path, turns):
    host, _ = await host_with_history(tmp_path, turns)
    with pytest.raises(WorkerError) as exc:
        await host.recover({"phase": "starting", "thread_id": "thread-1"})
    assert exc.value.code == "ambiguous_execution"


async def test_failed_turn_requires_manual_retry(tmp_path):
    host, _ = await host_with_history(tmp_path, [{"id": "turn-1", "status": "interrupted"}])
    with pytest.raises(WorkerError) as exc:
        await host.recover({"phase": "running", "thread_id": "thread-1", "turn_id": "turn-1"})
    assert exc.value.code == "codex_turn_failed"
