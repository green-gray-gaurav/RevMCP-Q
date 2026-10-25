import json
import subprocess
import sys


def invoke(*args):
    return subprocess.run(
        [sys.executable, "-m", "rmcpq", *args], capture_output=True, text=True, timeout=15
    )


def test_worker_cli_validates_before_host_start():
    for args in (
        ("--wait-seconds", "301", "--queue", "a"),
        ("--max-jobs", "0", "--queue", "a"),
        ("--url", "http://example.org", "--queue", "a"),
        (),
    ):
        result = invoke("worker", "--json", *args)
        assert result.returncode == 2
        assert json.loads(result.stdout)["error"]["code"] == "client_error"


def test_worker_cli_reports_missing_optional_sdk():
    # A subprocess import hook simulates a base-only installation without changing
    # the test environment or touching the user's SDK installation.
    script = """
import sys
class NoCodex:
    def find_spec(self, fullname, *args):
        if fullname == "openai_codex":
            raise ImportError("Optional SDK absent")
sys.meta_path.insert(0, NoCodex())
from rmcpq.cli import main
sys.exit(main(["worker", "--check", "--json"]))
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 2
    assert json.loads(result.stdout)["error"]["code"] == "missing_codex"
