"use strict";
const $ = (id) => document.getElementById(id);
const state = { selected: null, page: 0, busy: false, queued: false, total: 0, connectionError: false,
  jobsSignature: null, detailSignature: null, queuesSignature: null, viewVersion: 0,
  paused: false, checked: new Set(), jobs: [], workers: [], runners: [], workersSignature: null,
  queueCardsSignature: null, workerTimes: new Map(), bulkBusy: false, controlsAvailable: true,
  modelsLoaded: false, modelsLoading: null };
const pageSize = 30;
function node(tag, text, className) {
  const el = document.createElement(tag);
  if (text !== undefined) el.textContent = text;
  if (className) el.className = className;
  return el;
}
function badge(status) { return node("span", status, "badge " + status); }
function when(value) { return value ? new Date(value).toLocaleString() : "—"; }
function error(message) { $("error").textContent = message || ""; $("error").hidden = !message; }
function notice(message) { $("notice").textContent = message; $("notice").hidden = !message; }
function elapsed(start, end) {
  if (!start || !end) return "—";
  const seconds = Math.max(0, Math.round((new Date(end) - new Date(start)) / 1000));
  return seconds < 60 ? seconds + "s" : Math.floor(seconds / 60) + "m " + seconds % 60 + "s";
}
async function api(path, options = {}) {
  const response = await fetch(path, { cache: "no-store", ...options });
  const data = await response.json();
  if (!response.ok) {
    const exc = new Error(data.error?.message || "The broker request failed."); exc.status = response.status; throw exc;
  }
  return data;
}
function renderQueues(queues) {
  const select = $("queue-filter"), current = select.value;
  const signature = JSON.stringify(queues.map(queue => queue.queue));
  if (signature !== state.queuesSignature) {
    select.replaceChildren(node("option", "All queues"));
    select.firstChild.value = "";
    for (const queue of queues) {
      const option = node("option", queue.queue); option.value = queue.queue; select.append(option);
    }
    select.value = current; state.queuesSignature = signature;
  }
  const counts = { queued: 0, processing: 0, completed: 0, failed: 0, cancelled: 0 };
  for (const queue of queues) for (const key of Object.keys(counts)) counts[key] += queue.counts[key] || 0;
  for (const key of ["queued", "processing", "completed"]) $("count-" + key).textContent = counts[key];
  $("count-ended").textContent = counts.failed + counts.cancelled;
  const cardsSignature = JSON.stringify([queues, state.workers.map(w => [w.worker_id, w.queues, w.state])]);
  if (cardsSignature === state.queueCardsSignature) return;
  state.queueCardsSignature = cardsSignature;
  const cards = $("queue-overview"); cards.replaceChildren();
  if (!queues.length) cards.append(node("p", "No queues yet. Submit a job or launch a worker to get started.", "muted"));
  for (const queue of queues) {
    const workers = state.workers.filter(w => w.queues.includes(queue.queue));
    const waiting = workers.filter(w => w.state === "waiting").length;
    const busy = workers.filter(w => w.state === "claimed").length;
    const card = node("button", undefined, "queue-card"); card.type = "button";
    card.append(node("strong", queue.queue), node("span", queue.counts.queued + " queued · " + queue.counts.processing + " processing"),
      node("small", waiting + " waiting workers · " + busy + " busy"));
    const totals = Object.values(queue.counts).reduce((a, b) => a + b, 0);
    const track = node("div", undefined, "queue-track");
    for (const status of ["completed", "processing", "queued", "failed", "cancelled"]) {
      const bar = node("span", undefined, "track-" + status);
      // Native meter avoids inline styles under the dashboard CSP.
      const meter = node("meter"); meter.min = 0; meter.max = Math.max(1, totals); meter.value = queue.counts[status];
      meter.title = status + ": " + queue.counts[status]; bar.append(meter); track.append(bar);
    }
    card.append(track);
    card.addEventListener("click", () => { $("queue-filter").value = queue.queue; state.page = 0; resetSelection(); refresh(); });
    cards.append(card);
  }
}
function renderJobs(jobs) {
  state.jobs = jobs;
  const signature = JSON.stringify([jobs, state.selected, state.page, state.total, [...state.checked]]);
  if (signature === state.jobsSignature) return;
  state.jobsSignature = signature;
  const list = $("jobs"); list.replaceChildren();
  if (!jobs.length) list.append(node("p", "No jobs here yet. Submit a job or change the filters.", "empty"));
  for (const job of [...jobs].reverse()) {
    const row = node("div", undefined, "job-row");
    const select = node("input"); select.type = "checkbox"; select.checked = state.checked.has(job.id);
    select.setAttribute("aria-label", "Select job " + job.id.slice(0, 8));
    select.addEventListener("change", () => {
      if (select.checked) state.checked.add(job.id); else state.checked.delete(job.id);
      updateSelection();
    });
    row.append(select);
    const item = node("button", undefined, "job" + (state.selected === job.id ? " selected" : ""));
    item.type = "button"; item.setAttribute("aria-pressed", String(state.selected === job.id));
    const top = node("div", undefined, "job-top");
    top.append(node("span", job.queue, "job-queue"), badge(job.status));
    const meta = node("div", undefined, "job-meta");
    meta.append(node("span", job.id.slice(0, 8) + " · " + job.result_format + (job.model ? " · " + job.model : "")), node("span", when(job.created_at)));
    item.append(top, node("p", job.instruction, "job-title"), meta);
    item.addEventListener("click", () => { state.selected = job.id; renderJobs(jobs); loadDetail(); });
    row.append(item); list.append(row);
  }
  $("job-total").textContent = "(" + state.total + ")";
  $("page-label").textContent = state.total ? "Page " + (state.page + 1) + " of " + Math.ceil(state.total / pageSize) : "0 jobs";
  $("newer").disabled = state.page === 0;
  $("older").disabled = (state.page + 1) * pageSize >= state.total;
  updateSelection();
}
function updateSelection() {
  $("selected-count").textContent = state.checked.size + " selected";
  $("bulk-cancel").disabled = state.bulkBusy || !state.jobs.some(j => state.checked.has(j.id) && ["queued", "processing"].includes(j.status));
  $("bulk-retry").disabled = state.bulkBusy || !state.jobs.some(j => state.checked.has(j.id) && ["failed", "cancelled"].includes(j.status));
  $("select-page").checked = state.jobs.length > 0 && state.jobs.every(j => state.checked.has(j.id));
  $("select-page").indeterminate = state.checked.size > 0 && !$("select-page").checked;
}
function section(title, value, className = "") {
  const block = node("section", undefined, "detail-section");
  const heading = node("div", undefined, "section-title"); heading.append(node("h3", title));
  const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  const copy = node("button", "Copy"); copy.type = "button";
  copy.addEventListener("click", async () => {
    try { await navigator.clipboard.writeText(text); copy.textContent = "Copied"; }
    catch { error("Could not copy. Select the result text and copy it manually."); }
  });
  heading.append(copy); block.append(heading, node("pre", text, className)); return block;
}
async function loadDetail() {
  const id = state.selected; if (!id) return;
  try {
    const job = await api("/v1/jobs/" + encodeURIComponent(id));
    if (state.selected !== id) return;
    const signature = JSON.stringify(job);
    if (signature === state.detailSignature) {
      const duration = $("attempt-elapsed");
      if (duration) duration.textContent = elapsed(job.history.at(-1)?.started_at,
        job.history.at(-1)?.ended_at || (job.status === "processing" ? new Date().toISOString() : null));
      return;
    }
    state.detailSignature = signature;
    $("detail-status").replaceChildren(badge(job.status));
    const body = node("div", undefined, "detail-body"); body.append(node("code", job.id, "job-id"));
    const facts = node("div", undefined, "facts");
    for (const [title, value] of [["Queue", job.queue], ["Result format", job.result_format],
      ["Attempts", job.attempts + " · run " + job.run], ["Worker", job.worker_id || "—"],
      ["Model (last attempt)", job.model || "Not reported"],
      ["Elapsed (last attempt)", elapsed(job.history.at(-1)?.started_at, job.history.at(-1)?.ended_at || (job.status === "processing" ? new Date().toISOString() : null))],
      ["Lease deadline", when(job.lease_until)],
      ["Created", when(job.created_at)], ["Updated", when(job.updated_at)]]) {
      const fact = node("div"), content = node("div", value);
      if (title === "Elapsed (last attempt)") content.id = "attempt-elapsed";
      fact.append(node("span", title), content); facts.append(fact);
    }
    body.append(facts);
    if (job.status === "completed") body.append(section("Result",
      job.result_format === "json" ? JSON.stringify(job.result, null, 2) : job.result, "result-block"));
    else body.append(node("p", job.status === "processing" ? "A worker is processing this job. The result will appear automatically." :
      job.status === "queued" ? "Waiting for a worker subscribed to this queue." : "This job has no completed result.", "muted"));
    if (job.error) body.append(section("Error", job.error));
    body.append(section("Instruction", job.instruction));
    if (job.context !== null) body.append(section("Context", JSON.stringify(job.context, null, 2)));
    const historyBlock = node("section", undefined, "detail-section");
    const title = node("div", undefined, "section-title"); title.append(node("h3", "Attempt history")); historyBlock.append(title);
    const history = node("ol", undefined, "history");
    for (const attempt of job.history) history.append(node("li", "Attempt " + attempt.number + " · " + attempt.status +
      " · " + (attempt.model || "Model not reported") +
      " · " + attempt.worker_id + "\n" + when(attempt.started_at) + (attempt.error ? " · " + attempt.error : "")));
    historyBlock.append(history.children.length ? history : node("p", "No attempts yet.", "muted")); body.append(historyBlock);
    const actions = node("div", undefined, "job-actions");
    const duplicate = node("button", "Duplicate / edit"); duplicate.type = "button";
    duplicate.addEventListener("click", () => {
      const form = $("enqueue"); form.elements.queue.value = job.queue;
      form.elements.instruction.value = job.instruction; form.elements.result_format.value = job.result_format;
      form.elements.context.value = job.context === null ? "" : JSON.stringify(job.context, null, 2);
      $("compose").open = true; $("compose").scrollIntoView({block:"start"}); form.elements.instruction.focus();
      notice("Job copied into the form. Edit it and enqueue to create a new job.");
    }); actions.append(duplicate);
    const copyId = node("button", "Copy job ID"); copyId.type = "button";
    copyId.addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(job.id); notice("Job ID copied."); }
      catch { error("Could not copy the job ID."); }
    }); actions.append(copyId);
    if (job.status === "completed") {
      const download = node("button", "Download result"); download.type = "button";
      download.addEventListener("click", () => {
        const text = job.result_format === "json" ? JSON.stringify(job.result, null, 2) : job.result;
        const blob = new Blob([text], {type: job.result_format === "json" ? "application/json" : "text/plain"});
        const url = URL.createObjectURL(blob), link = node("a"); link.href = url;
        link.download = "rmcpq-" + job.id + (job.result_format === "json" ? ".json" : ".txt");
        document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
      }); actions.append(download);
    }
    const action = ["queued", "processing"].includes(job.status) ? "cancel" : ["failed", "cancelled"].includes(job.status) ? "retry" : null;
    if (action) {
      const button = node("button", action === "cancel" ? "Cancel job" : "Retry job");
      button.type = "button";
      button.addEventListener("click", async () => {
        button.disabled = true;
        try { await api("/v1/jobs/" + encodeURIComponent(id) + "/" + action, { method: "POST" }); error(""); await refresh(); }
        catch (exc) { error(exc.message); button.disabled = false; }
      }); actions.append(button);
    }
    body.append(actions); $("detail").replaceChildren(body);
  } catch (exc) { error(exc.message); }
}
function renderWorkers(workers) {
  const signature = JSON.stringify([workers.map(({last_seen, ...worker}) => worker), state.runners, $("show-stopped").checked, state.controlsAvailable]);
  if (signature === state.workersSignature) {
    for (const worker of workers) {
      const seen = state.workerTimes.get(worker.worker_id);
      if (seen) seen.textContent = "Last seen: " + when(worker.last_seen);
    }
    return;
  }
  state.workersSignature = signature;
  const visible = workers.filter(w => w.running || $("show-stopped").checked);
  $("worker-total").textContent = "(" + visible.length + " shown / " + workers.length + " registered)";
  const grid = $("workers"); grid.replaceChildren();
  state.workerTimes.clear();
  if (!visible.length) grid.append(node("p", "No active registrations. Launch a worker above, or show stopped workers.", "empty"));
  for (const worker of visible) {
    const card = node("article", undefined, "worker"), title = node("div", undefined, "worker-title");
    title.append(node("strong", worker.worker_id), badge(worker.state));
    const seen = node("p", "Last seen: " + when(worker.last_seen)); state.workerTimes.set(worker.worker_id, seen);
    card.append(title, node("p", "Queues: " + worker.queues.join(", ")),
      node("p", "Model: " + (worker.model || "Not reported")), seen);
    if (worker.active_job_id) card.append(node("p", "Job: " + worker.active_job_id));
    const actions = node("div", undefined, "worker-actions");
    const launch = node("button", worker.running ? "Configure another" : "Launch again"); launch.type = "button";
    launch.addEventListener("click", () => {
      const form = $("launch-worker"); form.elements.worker_id.value = worker.running ? "" : worker.worker_id;
      form.elements.queues.value = worker.queues.join(", ");
      loadModels().then(() => {
        const available = [...form.elements.model.options].some(option => option.value === worker.model);
        form.elements.model.value = available ? worker.model : "";
        if (worker.model && !available) $("model-status").textContent = "Previous model is not in the current catalog. Choose an available model or the configured default.";
      });
      $("launch-compose").open = true; $("launch-compose").scrollIntoView({block:"start"});
    }); actions.append(launch);
    if (worker.running) {
      const stop = node("button", "Stop worker"); stop.type = "button"; stop.className = "danger";
      stop.disabled = !state.controlsAvailable;
      stop.addEventListener("click", async () => {
        if (worker.active_job_id && !window.confirm("Stop this worker and release its unfinished job?")) return;
        stop.disabled = true;
        try { await api("/v1/workers/" + encodeURIComponent(worker.worker_id) + "/stop", {method:"POST"});
          notice("Stop requested for " + worker.worker_id + "."); await refresh(); }
        catch (exc) { error(exc.message); stop.disabled = false; }
      }); actions.append(stop);
    }
    card.append(actions);
    grid.append(card);
  }
  const launches = $("runners"); launches.replaceChildren();
  for (const run of state.runners) {
    if (run.state === "running" && !run.error) continue;
    const row = node("div", undefined, "runner-status"); row.append(badge(run.state),
      node("span", run.worker_id + (run.error ? " · " + run.error.message : " · " + (run.state === "starting" ? "Checking Codex sign-in…" : "Process exited (" + run.exit_code + ")"))));
    launches.append(row);
  }
}
async function refresh() {
  if (state.busy) { state.queued = true; return; }
  state.busy = true; $("refresh").disabled = true;
  const version = state.viewVersion;
  try {
    const query = new URLSearchParams({ limit: "1" });
    if ($("queue-filter").value) query.set("queue", $("queue-filter").value);
    if ($("status-filter").value) query.set("status", $("status-filter").value);
    if ($("search").value.trim()) query.set("q", $("search").value.trim());
    const [health, queues, workers, first, runners] = await Promise.all([
      api("/v1/health"), api("/v1/queues"), api("/v1/workers"), api("/v1/jobs?" + query),
      api("/v1/runners").catch(exc => {
        if (exc.status === 404) return {runners:[], cwd:"Restart the broker to enable worker controls", unsupported:true};
        throw exc;
      })]);
    if (!health.ok) throw new Error("Broker health check failed.");
    if (version !== state.viewVersion) { state.queued = true; return; }
    state.total = first.total; state.page = Math.min(state.page, Math.max(0, Math.ceil(state.total / pageSize) - 1));
    query.set("offset", String(Math.max(0, state.total - (state.page + 1) * pageSize)));
    query.set("limit", String(Math.max(1, Math.min(pageSize, state.total - state.page * pageSize))));
    const jobs = await api("/v1/jobs?" + query);
    if (version !== state.viewVersion) { state.queued = true; return; }
    state.workers = workers.workers; state.runners = runners.runners;
    state.controlsAvailable = !runners.unsupported;
    $("launch-worker").querySelector("button[type=submit]").disabled = !state.controlsAvailable || !!state.modelsLoading;
    $("search").disabled = !state.controlsAvailable;
    if (!state.controlsAvailable) notice("Restart the broker to load worker launch/stop controls and job search. Existing job views still work.");
    $("runner-cwd").textContent = runners.cwd;
    renderQueues(queues.queues); renderWorkers(workers.workers); renderJobs(jobs.jobs);
    $("connection").className = "badge online"; $("connection").textContent = "Broker online";
    $("updated").textContent = "Updated " + new Date().toLocaleTimeString();
    if (state.connectionError) { error(""); state.connectionError = false; }
    if (!state.selected && jobs.jobs.length) state.selected = jobs.jobs[jobs.jobs.length - 1].id;
    renderJobs(jobs.jobs); await loadDetail();
  } catch (exc) {
    state.connectionError = true;
    $("connection").className = "badge offline"; $("connection").textContent = "Disconnected";
    error("Could not refresh: " + exc.message + " Retrying automatically.");
  } finally {
    state.busy = false; $("refresh").disabled = false;
    if (state.queued) { state.queued = false; refresh(); }
  }
}
$("refresh").addEventListener("click", () => { error(""); refresh(); });
function resetSelection() {
  state.viewVersion++;
  state.selected = null; state.detailSignature = null;
  state.checked.clear();
  $("detail-status").replaceChildren();
  $("detail").replaceChildren(node("p", "Select a job to see its result.", "empty"));
}
for (const id of ["queue-filter", "status-filter"]) $(id).addEventListener("change", () => { state.page = 0; resetSelection(); refresh(); });
$("newer").addEventListener("click", () => { state.page--; resetSelection(); refresh(); });
$("older").addEventListener("click", () => { state.page++; resetSelection(); refresh(); });
$("enqueue").addEventListener("submit", async (event) => {
  event.preventDefault(); const form = event.currentTarget, button = form.querySelector("button[type=submit]");
  button.disabled = true;
  try {
    const data = new FormData(form), raw = String(data.get("context")).trim();
    const payload = { queue: String(data.get("queue")).trim(), instruction: data.get("instruction"),
      result_format: data.get("result_format"), context: raw ? JSON.parse(raw, (_key, value) => {
        if (typeof value === "number" && !Number.isFinite(value)) throw new Error("Context must contain finite JSON numbers.");
        return value;
      }) : null };
    const job = await api("/v1/jobs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    state.viewVersion++;
    state.selected = job.id; state.page = 0; $("queue-filter").value = ""; $("status-filter").value = "";
    form.reset(); $("compose").open = false; error(""); await refresh();
  } catch (exc) { error("Could not submit job: " + exc.message); }
  finally { button.disabled = false; }
});
$("pause").addEventListener("click", () => {
  state.paused = !state.paused; $("pause").textContent = state.paused ? "Resume updates" : "Pause updates";
  $("pause").setAttribute("aria-pressed", String(state.paused));
  if (!state.paused) { notice("Automatic updates resumed."); refresh(); }
  else notice("Automatic updates paused. Refresh still works manually.");
});
$("show-stopped").addEventListener("change", () => renderWorkers(state.workers));
$("clear-filters").addEventListener("click", () => {
  $("queue-filter").value = ""; $("status-filter").value = ""; $("search").value = "";
  state.page = 0; resetSelection(); refresh();
});
let searchTimer;
$("search").addEventListener("input", () => {
  clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.page = 0; resetSelection(); refresh(); }, 300);
});
$("select-page").addEventListener("change", () => {
  for (const job of state.jobs) { if ($("select-page").checked) state.checked.add(job.id); else state.checked.delete(job.id); }
  state.jobsSignature = null; renderJobs(state.jobs);
});
async function bulk(action) {
  if (state.bulkBusy) return;
  const eligible = state.jobs.filter(j => state.checked.has(j.id) && (action === "cancel" ? ["queued", "processing"] : ["failed", "cancelled"]).includes(j.status));
  if (!eligible.length) return;
  if (!window.confirm(action + " " + eligible.length + " selected jobs?")) return;
  state.bulkBusy = true;
  $("bulk-cancel").disabled = true; $("bulk-retry").disabled = true;
  let ok = 0, failed = 0;
  for (const job of eligible) {
    try { await api("/v1/jobs/" + encodeURIComponent(job.id) + "/" + action, {method:"POST"}); ok++; }
    catch { failed++; }
  }
  state.checked.clear(); state.bulkBusy = false; notice(action + ": " + ok + " succeeded" + (failed ? ", " + failed + " rejected because state changed or the broker was unavailable." : "."));
  await refresh();
}
$("bulk-cancel").addEventListener("click", () => bulk("cancel"));
$("bulk-retry").addEventListener("click", () => bulk("retry"));
async function loadModels(force = false) {
  if (state.modelsLoading) return state.modelsLoading;
  if (state.modelsLoaded && !force) return;
  state.modelsLoading = (async () => {
    const select = $("model-select"), previous = select.value;
    select.disabled = true; $("refresh-models").disabled = true;
    $("launch-worker").querySelector("button[type=submit]").disabled = true;
    $("model-status").textContent = "Loading models from Codex…";
    try {
      const catalog = await api("/v1/models" + (force ? "?refresh=1" : ""));
      const fallback = node("option", "Configured default" + (catalog.default_model ? " — " + catalog.default_model : "")); fallback.value = "";
      select.replaceChildren(fallback);
      for (const model of catalog.models) {
        const option = node("option", model.label === model.id ? model.id : model.label + " · " + model.id);
        option.value = model.id; select.append(option);
      }
      select.value = [...select.options].some(option => option.value === previous) ? previous : "";
      state.modelsLoaded = true;
      $("model-status").textContent = catalog.models.length + " models reported by Codex. Selection applies to the new worker.";
    } catch (exc) {
      $("model-status").textContent = (exc.status === 404 ? "Restart the broker to enable the model dropdown." : exc.message) + " Configured default remains available.";
    } finally {
      select.disabled = false; $("refresh-models").disabled = false; state.modelsLoading = null;
      $("launch-worker").querySelector("button[type=submit]").disabled = !state.controlsAvailable;
    }
  })();
  return state.modelsLoading;
}
$("launch-compose").addEventListener("toggle", () => { if ($("launch-compose").open) loadModels(); });
$("refresh-models").addEventListener("click", () => loadModels(true));
$("launch-worker").addEventListener("submit", async event => {
  event.preventDefault(); const form = event.currentTarget, button = form.querySelector("button[type=submit]");
  button.disabled = true;
  try {
    const data = new FormData(form), queues = String(data.get("queues")).split(/[\s,]+/).filter(Boolean);
    const payload = {queues, sandbox: data.get("sandbox"), worker_id: String(data.get("worker_id")).trim() || null, model: String(data.get("model")).trim() || null};
    const run = await api("/v1/runners", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload)});
    notice("Launching " + run.worker_id + ". Its registration will appear after Codex sign-in checks.");
    $("launch-compose").open = false; error(""); await refresh();
  } catch (exc) { error("Could not launch worker: " + exc.message); }
  finally { button.disabled = false; }
});
async function poll() { if (!state.paused) await refresh(); setTimeout(poll, 3000); }
poll();
