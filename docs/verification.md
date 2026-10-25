# Verification record

Verified on Windows with Python 3.11 and locked MCP SDK 2.3.0:

- `pytest -q`: **41 passed**, including the two-producer/two-worker restart scenario.
- Ruff lint and formatting checks passed.
- `uv lock --check --offline` passed.
- `uv build --offline` produced a wheel and source distribution. Archive inspection confirmed
  that runtime databases, virtual environments, and caches are excluded.

The automated suite exercises the broker's behavior without a model API or live AI session.

- Queue-core tests use temporary SQLite files and controlled clocks for expiry boundaries,
  exclusive concurrent claims, duplicate submissions, FIFO routing, scope, retries, history,
  cancellation, stop/resume, restart recovery, input validation, and schema-version rejection.
- Transport tests run actual Uvicorn HTTP servers and official MCP Python SDK clients. They
  cover enqueue/wait wakeup/result retrieval, explicit MCP cancellation, bounded disconnect
  cleanup, stop, error results, periodic recovery, and local Host/Origin/body-size checks.
- The acceptance scenario uses two HTTP producers and two MCP workers on separate queues,
  restarts the broker with queued jobs and persisted registrations, then verifies text and
  native JSON results through the producer API.
- CLI tests launch subprocesses and the packaged command. They cover stdin and UTF-8 files,
  JSON context/results, human-readable output, wait timeout, terminal failures, cancellation,
  retry, inspection, invalid input, and a real CLI-launched server.

Live AI-host verification is separate: **not performed**. Automated MCP clients generate fixture
answers; they cannot establish that an AI host will keep following the listener loop, renew
leases during real reasoning, or respect its own usage/permission limits. The README startup
instructions support a manual smoke test without changing global host configuration.

The original Bench checkout and its database are not modified by this project or test suite.
