# Verification record

Publication check for `codex-api-broker`: **108 passed, 5 opt-in live tests skipped**.
Ruff lint/format and offline dependency lock checks passed. Result rendering was removed
at the user's request; the dashboard displays raw text/JSON results with copy/download controls.

Verified on 2026-10-25, Windows, Python 3.13.0, locked MCP SDK 2.3.0, and optional
official Codex Python SDK/runtime 0.163.0:

- `pytest -q`: **71 passed, 2 opt-in live tests skipped**, including the two-producer/two-worker
  restart scenario and autonomous runner tests. The two live tests passed in a separate run.
- Ruff lint and formatting checks passed.
- `uv lock --check --offline` passed.
- `uv build --offline` produced a wheel and source distribution. Archive inspection confirmed
  that runtime databases, virtual environments, and caches are excluded.

The automated suite exercises the broker's behavior without a model API or live AI session.

- Long-wait coverage registers a worker with `wait_seconds=86400`, leaves a real MCP request
  open for 26 seconds (past the former 25-second cutoff), then enqueues work and verifies it
  wakes immediately. Subsequent answer, failure, and idle actions retain the saved preference.
  The full 24-hour duration has not been soak-tested. Schema-v1 migration preserves existing
  jobs, claims, and history while adding the worker wait preference.

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

Autonomous runner tests use real HTTP/MCP transports with a deterministic host. They cover
idle waits without model execution, rejecting wrong authentication before registration/claim,
JSON result parsing, automatic renewal across a lease boundary, cancellation/stop/timeouts,
two runners on separate queues, and recovering a completed journal after broker restart
without another model call. A deliberately lost submission response triggers an identical
retry over a fresh MCP session. Recovery tests inspect final answers from existing turns and
reject ambiguous history. Worker CLI subprocess tests cover argument validation and an
installation without the optional SDK. OS locks prevent duplicate worker IDs, including
different loopback aliases and working directories.

Live AI-host verification was performed separately: **2 live tests passed**. Codex reported
account type `chatgpt`. R-mcpq supplied no model API key, read no authentication file, and
did not edit global Codex configuration.

- A managed worker used host tools to read a local file and returned its exact text. A
  second job extracted `{"name":"Alex","count":7}` as native JSON. Both results were saved
  through the broker. Each job recorded thread/turn IDs. After closing the original
  app-server, a fresh Codex runtime resumed the persisted thread and recovered the original
  completed answer without starting another turn.
- Cancelling a real active job interrupted the host turn, left the job cancelled, and saved
  no result. Reading persisted turn history confirmed status `interrupted`.

To run the opt-in checks explicitly (they consume subscription usage):

```powershell
uv run --extra codex rmcpq worker --check --json
$env:RMCPQ_LIVE_CODEX = "1"
uv run --extra codex pytest tests/test_codex_live.py -q --basetemp .cache/pytest-live
Remove-Item Env:RMCPQ_LIVE_CODEX
```

Live checks used isolated temporary databases and working directories. They do not establish
that every host tool is available, automatic reauthentication, uninterrupted operation through
subscription limits, or exactly-once external side effects. Abrupt process termination at
every possible start/submit boundary has not been exhaustively tested.

The original Bench checkout and its database are not modified by this project or test suite.

## Dashboard verification

Dashboard changes passed **20 targeted HTTP/MCP and CLI regression tests**. Browser checks
used a separate preview database on port 8792: submission, text/JSON result rendering,
queue/status filters, pagination, cancellation and retry. Task markup is displayed as inert
text. Desktop and 390-pixel mobile layouts were inspected; the browser console reported no
errors or warnings after the form-validation fix. Screenshots are in `output/playwright/`.
The wheel was checked for the packaged dashboard HTML, CSS, JavaScript and icon assets.

## Explicit execution outcomes

The outcome contract passed **34 targeted automated tests** covering the worker, outcome
validation and recovery. Reported failure and missing/empty/relative artifact paths produce
failed jobs with no result; successful text and native JSON values remain unchanged. JSON
application data with its own `status: error` is preserved when the execution envelope says
success. Legacy cached results cannot bypass the upgraded outcome contract.

Three live subscription scenarios passed in separate runs after the change: successful
text/JSON output plus persisted-turn recovery; an actually missing input file reported as
`execution_failed` in the broker; and interruption of a cancelled active turn. The live text
check initially exposed ambiguity between producer formatting and the internal envelope.
Explicit result-field instructions and examples resolved it, and the text/JSON check passed.
These checks do not verify image generation capability or the visual correctness of media.

## Built-in image output bridge

The image bridge, outcome/recovery and worker checks passed **43 targeted automated tests**
in two runs (29 artifact/outcome/recovery checks and 14 worker checks). Coverage includes
thread-scoped sources, path rewriting, destination directories and filenames, preserving
existing files, invalid/missing files, rejecting another thread's outputs, and enforcing cwd
and read-only restrictions.

A separate real Codex subscription image job **passed** with the configured default model.
The built-in generator created a sky image, the runner copied it into the requested workspace
output directory, and the broker returned `completed` with the readable PNG's absolute path.
The saved file was 1,669,452 bytes and visually inspected. No API key was used.

The first live attempt caught Windows sandbox ownership restrictions on a model-created
destination. Image copying now belongs to the runner; Codex reports its actual generated
source path. The second live attempt passed. The prior failed sky job's image was also found
in its exact thread output directory and recovered into `image-91bb1cd9.png` without changing
the historical failed job record.

Run the image check explicitly:

```powershell
$env:RMCPQ_LIVE_CODEX = "1"
$env:RMCPQ_LIVE_IMAGE = "1"
uv run --extra codex pytest tests/test_codex_live.py -q -k image_saved --basetemp .cache/pytest-live-image
Remove-Item Env:RMCPQ_LIVE_IMAGE
Remove-Item Env:RMCPQ_LIVE_CODEX
```

The per-thread Codex image directory is a local runtime convention. Its handling is isolated
in `hosts/artifacts.py`; changes in future Codex runtimes may require updating that adapter.

## Model visibility

The full automated suite passed: **99 passed, 4 opt-in live tests skipped**. Ruff lint,
formatting, dependency lock validation and wheel/source builds also passed.

Schema v3 adds optional model metadata to worker registrations, jobs and individual attempts.
Migration checks retain v1/v2 jobs, active claim tokens and attempt history; historical model
values remain unknown. New claims snapshot their worker's selected model, preserving that
value through later worker model changes and retries. Real HTTP/MCP checks cover transmission
of the model from worker registration to job history.

A live subscription `worker --check --json` reported `model: gpt-6-luna`. Browser inspection
confirmed the selected model appears on the worker card, job details, and attempt history,
with no console errors or warnings. The preview screenshot is
`output/playwright/dashboard-model.png`. No model inference was needed for this UI check.

## Dashboard controls

The complete automated suite passed **104 tests**, with **5 explicit live tests skipped**.
After a small launch-status metadata adjustment, the six dashboard/control tests passed again.
Ruff checks, JavaScript syntax validation and package builds passed.

An opt-in real subscription test launched a worker through HTTP, observed its model and
outstanding wait, stopped it through HTTP, and confirmed process exit code 0. The queue was
empty, so this tested authentication and lifecycle without model inference. The same launch
and stop flow also passed through the actual browser form.

Browser checks on a preview database covered global search, editable duplication, result
download, select-page cancellation and retry, pause/resume/manual refresh, worker launch/stop,
and the stopped-worker filter. A 390-pixel mobile viewport had no horizontal overflow.
Task data remained on the local broker. Browser console checks were clean during normal use.
An intentional mocked old-broker 404 verified restart guidance without incorrectly labeling
a healthy broker disconnected. Screenshots are `output/playwright/dashboard-controls.png`
and `output/playwright/dashboard-controls-mobile.png`.

Launcher tests verify fixed argv without a shell, missing-SDK errors, duplicate-launch handling,
resolved model retention after exit, request validation, Host/Origin protection, claim release
and stale-result fencing. Search treats SQL-looking strings and wildcard characters literally.

## Model selection dropdown

Ten targeted model-catalog/control/dashboard tests passed. Catalog checks cover cache refresh,
visible models across paginated responses, subscription errors, request routing and local
Host checks. A live catalog request returned seven visible models and configured default
`gpt-6-luna`, with no model inference. Browser verification selected that model, refreshed the
catalog while preserving selection, and confirmed it was sent in the worker launch request
(launch was intercepted for that form serialization check). Normal browser console checks
reported no errors or warnings. Screenshot: `output/playwright/model-dropdown.png`.
