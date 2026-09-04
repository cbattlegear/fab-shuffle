/* Fab Shuffle wizard. Plain ES modules-free JS so the container needs no build step. */

const STAGES = ["login", "capacity", "workspace", "review", "progress"];

const state = {
  sessionId: null,
  capacity: null,
  workspace: null,
  workspaces: [],
  preview: null,
  runId: null,
  events: null,
  readiness: null,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

// ------------------------------------------------------------------ plumbing

async function api(path, { method = "GET", body, signal, download = false } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (state.sessionId) headers["X-Fab-Shuffle-Session"] = state.sessionId;

  const response = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  });

  if (download && response.ok) return response.blob();
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(payload.detail || `Request failed with HTTP ${response.status}`);
  }
  return payload;
}

function showError(message) {
  const alert = $("#alert");
  alert.textContent = message;
  alert.hidden = !message;
  if (message) alert.scrollIntoView({ block: "nearest", behavior: "smooth" });
}

function goTo(stage) {
  $$(".panel").forEach((panel) => {
    panel.hidden = panel.dataset.stage !== stage;
  });
  const index = STAGES.indexOf(stage);
  $$(".wizard-nav li").forEach((item) => {
    const position = STAGES.indexOf(item.dataset.stage);
    item.classList.toggle("current", position === index);
    item.classList.toggle("done", position < index);
  });
  showError("");
}

function busy(button, isBusy, labelWhenBusy) {
  if (isBusy) {
    button.dataset.label = button.textContent;
    button.textContent = labelWhenBusy;
    button.disabled = true;
  } else {
    button.textContent = button.dataset.label || button.textContent;
    button.disabled = false;
  }
}

function renderChoices(container, entries, onSelect) {
  container.innerHTML = "";
  if (!entries.length) {
    container.innerHTML = '<p class="hint">Nothing here that this service principal can see.</p>';
    return;
  }
  entries.forEach((entry) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "choice";
    button.setAttribute("role", "radio");
    button.setAttribute("aria-checked", "false");
    button.dataset.id = entry.id;
    button.innerHTML = `<span class="name"></span><span class="meta"></span>`;
    button.querySelector(".name").textContent = entry.label;
    button.querySelector(".meta").textContent = entry.meta || "";
    button.addEventListener("click", () => {
      Array.from(container.children).forEach((child) =>
        child.setAttribute && child.setAttribute("aria-checked", "false")
      );
      button.setAttribute("aria-checked", "true");
      onSelect(entry);
    });
    container.appendChild(button);
  });
}

// --------------------------------------------------------------------- login

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector("button");
  const data = Object.fromEntries(new FormData(form).entries());

  busy(button, true, "Signing in…");
  try {
    const result = await api("/api/login", { method: "POST", body: data });
    state.sessionId = result.sessionId;
    form.reset();
    $("#sign-out").hidden = false;
    await loadCapacities();
    loadLeftovers();
    loadResumable();
    // Needed by the restore-access control on this same step.
    loadWorkspaces().then(fillWorkspaceSelects).catch(() => {});
    goTo("capacity");
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

$("#sign-out").addEventListener("click", async () => {
  try {
    await api("/api/logout", { method: "POST" });
  } catch (_) {
    /* signing out locally is enough */
  }
  if (state.events) state.events.close();
  resetReadiness();
  Object.assign(state, {
    sessionId: null, capacity: null, workspace: null, workspaces: [],
    preview: null, runId: null, events: null,
  });
  $("#sign-out").hidden = true;
  goTo("login");
});

// ------------------------------------------------------------------ capacity

async function loadCapacities() {
  const container = $("#capacity-list");
  container.innerHTML = '<p class="hint">Loading capacities…</p>';
  const { capacities } = await api("/api/capacities");
  renderChoices(
    container,
    capacities.map((capacity) => ({
      id: capacity.id,
      label: capacity.displayName || capacity.id,
      meta: [capacity.region, capacity.sku, capacity.state].filter(Boolean).join(" · "),
      raw: capacity,
    })),
    (entry) => {
      state.capacity = entry.raw;
      $("#capacity-next").disabled = false;
    }
  );
}

$("#capacity-next").addEventListener("click", async () => {
  const button = $("#capacity-next");
  busy(button, true, "Loading…");
  try {
    await loadWorkspaces();
    goTo("workspace");
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

// Run state lives in memory, so a restarted container loses track of a scratch workspace
// that was never cleaned up. Surface any leftovers right after sign-in.
async function loadResumable() {
  try {
    const { runs } = await api("/api/resumable");
    const container = $("#resumable");
    container.hidden = !runs.length;
    if (!runs.length) return;

    const list = container.querySelector("ul");
    list.innerHTML = "";
    runs.forEach((run) => {
      const item = document.createElement("li");

      const what = document.createElement("div");
      what.textContent = `${run.sourceWorkspaceName} → ${run.targetWorkspaceName}`;
      item.appendChild(what);

      const detail = document.createElement("div");
      detail.className = "hint";
      const started = run.startedAt ? new Date(run.startedAt).toLocaleString() : "an earlier run";
      const built = run.itemsCreated === 1 ? "1 item" : `${run.itemsCreated} items`;
      detail.textContent = `Started ${started}. Got as far as ${run.lastPhase || "the beginning"}, ${built} built.`;
      item.appendChild(detail);

      const button = document.createElement("button");
      button.className = "secondary";
      button.textContent = "Pick it up";
      button.addEventListener("click", () => resumeRun(run.runId, button));
      item.appendChild(button);

      list.appendChild(item);
    });
  } catch (_) {
    // Offering an old run back is a convenience; never block sign-in on it.
  }
}

async function resumeRun(runId, button) {
  busy(button, true, "Starting…");
  try {
    const result = await api(`/api/runs/${runId}/resume`, { method: "POST" });
    state.runId = result.runId;
    goTo("progress");
    watchRun(result.runId);
  } catch (error) {
    showError(error.message);
    busy(button, false);
  }
}

async function loadLeftovers() {
  try {
    const { workspaces } = await api("/api/scratch-workspaces");
    const container = $("#leftovers");
    container.hidden = !workspaces.length;
    if (!workspaces.length) return;

    const list = container.querySelector("ul");
    list.innerHTML = "";
    workspaces.forEach((workspace) => {
      const item = document.createElement("li");
      item.textContent = workspace.displayName;
      list.appendChild(item);
    });
  } catch (_) {
    // Discovering leftovers is a convenience; never block sign-in on it.
  }
}

$("#delete-leftovers").addEventListener("click", async () => {
  const button = $("#delete-leftovers");
  busy(button, true, "Deleting…");
  try {
    const result = await api("/api/scratch-workspaces/cleanup", { method: "POST" });
    if (result.warnings.length) showError(result.warnings.join(" "));
    await loadLeftovers();
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

function fillWorkspaceSelects() {
  ["#restore-source", "#restore-target"].forEach((selector) => {
    const select = $(selector);
    select.innerHTML = "";
    state.workspaces.forEach((workspace) => {
      const option = document.createElement("option");
      option.value = workspace.id;
      option.textContent = workspace.displayName || workspace.id;
      select.appendChild(option);
    });
  });
}

$("#restore-access").addEventListener("click", async () => {
  const button = $("#restore-access");
  const source = $("#restore-source").value;
  const target = $("#restore-target").value;

  if (!source || !target || source === target) {
    showError("Pick two different workspaces.");
    return;
  }

  busy(button, true, "Restoring…");
  try {
    const result = await api("/api/workspaces/restore-access", {
      method: "POST",
      body: { source_workspace_id: source, target_workspace_id: target },
    });
    showError(result.warnings.length ? result.warnings.join(" ") : "");
    if (!result.warnings.length) {
      button.textContent = `Granted ${result.granted} admin(s)`;
      setTimeout(() => (button.textContent = "Restore access"), 4000);
    }
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

// ----------------------------------------------------------------- workspace

async function loadWorkspaces() {
  const { workspaces } = await api("/api/workspaces");
  state.workspaces = workspaces;
  renderWorkspaces("");
}

function renderWorkspaces(filter) {
  const needle = filter.trim().toLowerCase();
  const entries = state.workspaces
    .filter((w) => !needle || (w.displayName || "").toLowerCase().includes(needle))
    .map((workspace) => ({
      id: workspace.id,
      label: workspace.displayName || workspace.id,
      meta: workspace.capacityRegion || "",
      raw: workspace,
    }));

  renderChoices($("#workspace-list"), entries, (entry) => {
    state.workspace = entry.raw;
    $("#workspace-next").disabled = false;
  });
}

$("#workspace-filter").addEventListener("input", (event) => {
  state.workspace = null;
  $("#workspace-next").disabled = true;
  renderWorkspaces(event.target.value);
});

$("#workspace-next").addEventListener("click", async () => {
  const button = $("#workspace-next");
  busy(button, true, "Inspecting…");
  const params = new URLSearchParams({
    capacity_id: state.capacity.id,
    source_workspace_id: state.workspace.id,
  });

  // Both requests go out together, and the review screen appears straight away. The
  // dependency check walks the relations API and every connection in the tenant, so waiting
  // for it before showing anything left the wizard looking stuck.
  const dependencies = api(`/api/preview/dependencies?${params}`);
  dependencies.catch(() => {});

  try {
    goTo("review");
    renderReviewPending();
    state.preview = await api(`/api/preview?${params}`);
    renderReview();
  } catch (error) {
    goTo("workspace");
    showError(error.message);
    busy(button, false);
    return;
  }
  busy(button, false);

  await settleAssessment(dependencies);
});

$("#recheck").addEventListener("click", async () => {
  const button = $("#recheck");
  busy(button, true, "Re-checking…");
  const params = new URLSearchParams({ source_workspace_id: state.workspace.id });

  showDependenciesPending();
  setStartEnabled(false);
  try {
    await settleAssessment(api(`/api/preview/dependencies?${params}`));
  } finally {
    busy(button, false);
  }
});

/** Apply the slow half of the assessment, however it was started. */
async function settleAssessment(request) {
  try {
    const result = await request;
    state.preview.dependencies = result.dependencies;
    state.preview.connectionAccess = result.connectionAccess;
  } catch (error) {
    state.preview.dependencies = [`Dependencies could not be checked: ${error.message}`];
    state.preview.connectionAccess = null;
  }
  stopAssessmentTimer();
  renderDependencies();
  renderConnectionAccess();
  // Only now is it known whether anything blocks the run.
  setStartEnabled(state.preview.blockers.length === 0);
}

function setStartEnabled(enabled) {
  const start = $("#start-run");
  start.disabled = !enabled;
  start.title = enabled ? "" : "Waiting for the assessment to finish";
}


// -------------------------------------------------------------------- review

function renderReviewPending() {
  const callout = $("#strategy-callout");
  callout.className = "callout";
  callout.innerHTML = '<strong><span class="spin">◜</span> Inspecting the workspace</strong><p></p>';
  callout.querySelector("p").textContent =
    "Reading the items in the source workspace to work out whether it can be reassigned or " +
    "has to be rebuilt.";
  $("#review-summary").innerHTML = "";
  ["#blockers", "#unsupported", "#dependencies", "#review-warnings", "#connection-access"].forEach(
    (id) => {
      $(id).hidden = true;
    },
  );
  // The name and options depend on the strategy, so they stay hidden until it is known.
  $("#target-name-field").hidden = true;
  $("#rebuild-options").hidden = true;
  $("#start-run").disabled = true;
}

function renderDependencies() {
  const container = $("#dependencies");
  container.classList.remove("pending");
  container.querySelector("h3").textContent = "Needs attention";
  container.querySelector(".hint").hidden = false;
  fillList(container, state.preview.dependencies || []);
}

function renderConnectionAccess() {
  const container = $("#connection-access");
  const access = state.preview.connectionAccess;
  container.hidden = !access;
  if (!access) return;

  const list = container.querySelector("ul");
  list.innerHTML = "";
  access.connections.forEach((entry) => {
    const item = document.createElement("li");

    const label = document.createElement("code");
    label.textContent = entry.label;
    item.appendChild(label);

    const why = document.createElement("div");
    why.className = "why";
    why.textContent = `Needed by ${entry.usedBy.join(", ")}.`;
    item.appendChild(why);

    list.appendChild(item);
  });

  const steps = container.querySelector("ol");
  steps.innerHTML = "";
  access.instructions.forEach((instruction) => {
    const step = document.createElement("li");
    step.textContent = instruction;
    steps.appendChild(step);
  });

  const script = $("#grant-script");
  script.hidden = !access.script;
  if (access.script) {
    script.querySelector("code").textContent = access.script;
    // Collapsed again on every re-check, so a stale expanded script is never mistaken
    // for the current one.
    script.open = false;
  }
}

$("#grant-script").addEventListener("click", async (event) => {
  if (!event.target.classList.contains("copy")) return;
  const button = event.target;
  const script = state.preview.connectionAccess && state.preview.connectionAccess.script;
  if (!script) return;

  try {
    await navigator.clipboard.writeText(script);
    button.textContent = "Copied";
  } catch {
    // Clipboard access needs a secure context, which a plain http:// host is not.
    button.textContent = "Press Ctrl+C";
    const range = document.createRange();
    range.selectNodeContents($("#grant-script").querySelector("code"));
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  }
  setTimeout(() => {
    button.textContent = "Copy";
  }, 2000);
});

function showDependenciesPending() {
  const container = $("#dependencies");
  container.hidden = false;
  container.classList.add("pending");
  container.querySelector("h3").textContent = "Checking dependencies…";
  container.querySelector(".hint").hidden = true;
  const list = container.querySelector("ul");
  list.innerHTML = "";
  const item = document.createElement("li");
  item.id = "assessment-progress";
  list.appendChild(item);
  startAssessmentTimer(item);
}

/** Tick a live count so a slow assessment visibly moves. */
function startAssessmentTimer(element) {
  stopAssessmentTimer();
  const started = Date.now();
  const total = state.preview && state.preview.counts ? itemTotal(state.preview.counts) : 0;
  const scope = total ? `${total} data item(s) plus everything that binds a connection` : "the workspace";

  const tick = () => {
    const seconds = Math.round((Date.now() - started) / 1000);
    element.textContent =
      `Reading references and connections across ${scope} — ${seconds}s elapsed. ` +
      "This walks one API call per item, so it takes a moment on a large workspace.";
  };
  tick();
  state.assessmentTimer = setInterval(tick, 1000);
}

function stopAssessmentTimer() {
  if (state.assessmentTimer) {
    clearInterval(state.assessmentTimer);
    state.assessmentTimer = null;
  }
}

function itemTotal(counts) {
  return Object.values(counts).reduce((sum, value) => sum + (Number(value) || 0), 0);
}

function renderReview() {
  const preview = state.preview;
  const reassign = preview.strategy === "reassign";

  const callout = $("#strategy-callout");
  callout.className = `callout ${reassign ? "good" : ""}`;
  callout.innerHTML = "<strong></strong><p></p>";
  callout.querySelector("strong").textContent = reassign
    ? "This workspace can just be reassigned"
    : "This workspace has to be rebuilt";
  callout.querySelector("p").textContent = reassign
    ? "It only holds Power BI content, so Fab Shuffle moves the existing workspace onto the " +
      "target capacity instead of recreating it. Nothing is copied and no new workspace is made." +
      (preview.largeSemanticModels.length
        ? ` ${preview.largeSemanticModels.length} semantic model(s) use large storage format and will be ` +
          "converted to small for the move, then switched back afterwards."
        : "")
    : "It contains Fabric items, which cannot move across regions on a capacity reassignment. " +
      "Fab Shuffle creates a new workspace in the target region and copies everything it supports.";

  const rows = [
    ["Source workspace", preview.sourceWorkspaceName],
    ["Target capacity", `${preview.capacityName} (${preview.capacityRegion || "unknown region"})`],
  ];
  if (reassign) {
    rows.push(["Large semantic models", String(preview.largeSemanticModels.length)]);
  } else {
    // Every type that will move, in the order the migration creates them.
    (preview.counts || []).forEach((entry) => rows.push([entry.label, String(entry.count)]));
    rows.push(["Items in total", String(preview.migratedTotal ?? 0)]);
  }

  $("#review-summary").innerHTML = rows.map(() => `<div><span class="k"></span><span class="v"></span></div>`).join("");
  $$("#review-summary div").forEach((row, index) => {
    row.querySelector(".k").textContent = rows[index][0];
    row.querySelector(".v").textContent = rows[index][1];
  });

  fillList($("#blockers"), preview.blockers);
  fillList($("#unsupported"), preview.unsupportedSummary);
  fillList($("#review-warnings"), preview.capacityWarning ? [preview.capacityWarning] : []);

  if (preview.dependencies) {
    renderDependencies();
  } else if (reassign) {
    // A reassignment rewrites no references, so there is nothing to check.
    $("#dependencies").hidden = true;
  } else {
    showDependenciesPending();
  }
  renderConnectionAccess();

  // A reassignment keeps the workspace and its name, and copies nothing.
  $("#target-name-field").hidden = reassign;
  $("#rebuild-options").hidden = reassign;
  $("#target-name").value = preview.targetWorkspaceName;

  // The button stays disabled until the dependency and connection assessment has finished,
  // because until then it is not known whether anything blocks the run.
  setStartEnabled(reassign && preview.blockers.length === 0);
  $("#start-run").textContent = reassign ? "Reassign workspace" : "Start migration";
}

function fillList(container, entries) {
  container.hidden = !entries.length;
  if (!entries.length) return;
  const list = container.querySelector("ul");
  list.innerHTML = "";
  entries.forEach((entry) => {
    const item = document.createElement("li");
    item.textContent = entry;
    list.appendChild(item);
  });
}

$("#start-run").addEventListener("click", async () => {
  const button = $("#start-run");
  const reassign = state.preview.strategy === "reassign";
  busy(button, true, reassign ? "Reassigning…" : "Starting…");
  try {
    const result = await api("/api/runs", {
      method: "POST",
      body: {
        capacity_id: state.capacity.id,
        source_workspace_id: state.workspace.id,
        strategy: state.preview.strategy,
        target_workspace_name: reassign ? null : $("#target-name").value.trim() || null,
        include_data: $("#opt-data").checked,
        include_files: $("#opt-files").checked,
        copy_permissions: $("#opt-permissions").checked,
        cleanup_when_done: $("#opt-cleanup").checked,
      },
    });
    state.runId = result.runId;
    goTo("progress");
    watchRun(result.runId);
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

// ------------------------------------------------------------------ progress

const STEP_ICONS = {
  pending: "○",
  running: "◐",
  succeeded: "✓",
  failed: "✕",
  skipped: "–",
};

const RUN_MESSAGES = {
  pending: "Preparing…",
  running: "Migration in progress",
  succeeded: "Migration finished",
  failed: "Migration failed",
  cancelled: "Migration cancelled",
};

function watchRun(runId) {
  if (state.events) state.events.close();
  state.runId = runId;
  resetReadiness(runId);
  const url = `/api/runs/${runId}/events?session_id=${encodeURIComponent(state.sessionId)}`;
  const events = new EventSource(url);
  state.events = events;

  events.onmessage = (message) => {
    if (state.events === events) renderRun(JSON.parse(message.data));
  };
  events.onerror = () => {
    events.close();
    if (state.events !== events) return;
    // The stream ends when the run finishes; fall back to a single fetch for the final state.
    api(`/api/runs/${runId}`).then((run) => {
      if (state.events === events) renderRun(run);
    }).catch(() => {});
  };
}

function renderRun(run) {
  if (run.id && run.id !== state.runId) return;
  observeReadiness(run);
  const banner = $("#run-banner");
  banner.className = `run-banner ${run.status}`;
  const spinner = run.status === "running" ? '<span class="spin">◐</span> ' : "";
  banner.innerHTML = `${spinner}<strong></strong><span></span>`;
  banner.querySelector("strong").textContent = RUN_MESSAGES[run.status] || run.status;
  banner.querySelector("span").textContent = run.targetWorkspace
    ? ` — target workspace: ${run.targetWorkspace.displayName}`
    : "";

  if (run.error) {
    const error = document.createElement("p");
    error.textContent = run.error;
    banner.appendChild(error);
  }

  const list = $("#step-list");
  list.innerHTML = "";
  run.steps.forEach((step) => {
    const item = document.createElement("li");
    item.className = step.status;
    item.innerHTML = `
      <span class="icon"></span>
      <div class="step-body">
        <div class="title"></div>
        <div class="detail"></div>
      </div>`;
    item.querySelector(".icon").textContent = STEP_ICONS[step.status] || "○";
    item.querySelector(".title").textContent = step.title;
    item.querySelector(".detail").textContent = step.detail || "";

    if (step.warnings.length) {
      const warnings = document.createElement("ul");
      warnings.className = "step-warnings";
      step.warnings.forEach((warning) => {
        const entry = document.createElement("li");
        entry.textContent = warning;
        warnings.appendChild(entry);
      });
      item.querySelector(".step-body").appendChild(warnings);
    }
    list.appendChild(item);
  });

  const finished = run.status !== "running" && run.status !== "pending";
  $("#cancel-run").hidden = finished;
  $("#start-over").hidden = !finished;
  $("#cleanup-run").hidden = !finished || run.cleanupDone || !run.scratchWorkspace;
  // Only worth offering when the run got far enough to build a workspace and still left
  // something behind. A clean run has nothing to retry.
  const leftSomething = (run.summary?.warnings || []).length > 0;
  $("#retry-run").hidden = !finished || !run.targetWorkspace || !leftSomething;
}

// ---------------------------------------------------------- cutover readiness

const READINESS_LABELS = { ready: "Ready", needs_attention: "Needs attention", unknown: "Unknown" };
const READINESS_PAGE_SIZE = 25;
const READINESS_REFRESH_MS = 500;

function readinessState(value) {
  return Object.hasOwn(READINESS_LABELS, value) ? value : "unknown";
}

function resetReadiness(runId = null) {
  const previous = state.readiness;
  if (previous) {
    clearTimeout(previous.timer);
    previous.controller?.abort();
    previous.downloadController?.abort();
  }
  state.readiness = runId ? {
    runId, report: null, revision: undefined, runStatus: null, version: 0,
    pending: false, urgent: false, timer: null, controller: null,
    lastFetch: Date.now(), error: "", downloadError: "", downloadController: null,
    page: 1, filter: "all", search: "",
  } : null;
  $("#readiness-filter").value = "all";
  $("#readiness-search").value = "";
  $("#readiness-items").replaceChildren();
  $("#readiness-limits").replaceChildren();
  $("#readiness-controls").hidden = true;
  $("#readiness-pagination").hidden = true;
  $("#readiness-results").textContent = "";
  $("#readiness-empty").hidden = true;
  renderReadinessStatus();
  if (state.readiness) scheduleReadiness();
}

function observeReadiness(run) {
  const current = state.readiness;
  if (!current || current.runId !== state.runId) return;
  const terminal = ["succeeded", "failed", "cancelled"].includes(run.status);
  const finishedNow = terminal && current.runStatus !== run.status;
  const changed = current.runStatus === null || current.revision !== run.readinessRevision;
  current.runStatus = run.status;
  current.revision = run.readinessRevision;
  if (changed || finishedNow) {
    current.version += 1;
    scheduleReadiness(finishedNow);
  }
}

function scheduleReadiness(immediate = false) {
  const current = state.readiness;
  if (!current) return;
  current.pending = true;
  current.urgent ||= immediate;
  if (immediate) {
    clearTimeout(current.timer);
    current.timer = null;
  }
  // Serialize requests. A revision received mid-fetch invalidates that response and
  // queues one trailing fetch; the final snapshot bypasses the normal throttle.
  if (!current.controller && current.timer === null) {
    const delay = current.urgent ? 0 : Math.max(0, READINESS_REFRESH_MS - (Date.now() - current.lastFetch));
    current.timer = setTimeout(() => fetchReadiness(current), delay);
  }
  renderReadinessStatus();
}

async function fetchReadiness(current) {
  if (state.readiness !== current || state.runId !== current.runId) return;
  current.timer = null;
  current.pending = false;
  current.urgent = false;
  current.lastFetch = Date.now();
  current.error = "";
  const version = current.version;
  current.controller = new AbortController();
  renderReadinessStatus();
  try {
    const report = await api(`/api/runs/${encodeURIComponent(current.runId)}/readiness`, {
      signal: current.controller.signal,
    });
    if (state.readiness !== current || version !== current.version) return;
    if (report.schemaVersion !== 1 || report.runId !== current.runId || !Array.isArray(report.items)) {
      throw new Error("The readiness report has an unsupported format. Reload the application and retry.");
    }
    current.report = report;
    renderReadinessItems();
    $("#readiness-limits").replaceChildren();
    (report.limits || []).forEach((limit) => {
      $("#readiness-limits").appendChild(readinessElement("li", limit));
    });
  } catch (error) {
    if (state.readiness === current && version === current.version && error.name !== "AbortError") {
      current.error = error.message;
    }
  } finally {
    if (state.readiness === current) {
      current.controller = null;
      if (current.pending) scheduleReadiness(current.urgent);
      renderReadinessStatus();
    }
  }
}

function renderReadinessStatus() {
  const current = state.readiness;
  $("#cutover-readiness").hidden = !current;
  $(".readiness-jump").hidden = !current;
  if (!current) return;
  const report = current.report;
  const loading = Boolean(current.controller || current.pending);
  const reportedState = current.error ? "unknown" : readinessState(report?.state);
  const label = READINESS_LABELS[reportedState];
  const overall = $("#readiness-overall");
  overall.textContent = report || current.error ? label : "Not yet assessed";
  overall.className = `readiness-state ${reportedState}`;
  $("#readiness-jump").textContent = `Cutover readiness: ${report || current.error ? label : "loading…"}`;
  $("#readiness-counts").textContent = report
    ? `${report.counts?.ready ?? 0} ready · ${report.counts?.needs_attention ?? 0} need attention · ` +
      `${report.counts?.unknown ?? 0} unknown${current.error ? " (last loaded snapshot)" : ""}`
    : "";
  $("#readiness-status").textContent = current.error
    ? (report ? "The latest report could not be loaded. Items below are from the last loaded snapshot."
      : "Readiness could not be established. Retry loading the report.")
    : loading
      ? (report ? "Updating readiness… Showing the last loaded snapshot." : "Loading readiness report…")
      : ["pending", "running"].includes(current.runStatus)
        ? "The migration is still running. Readiness will update as evidence is recorded."
        : "Readiness report loaded.";
  $("#readiness-error").textContent = current.error;
  $("#readiness-error").hidden = !current.error;
  $("#readiness-retry").hidden = !current.error;
  $("#readiness-retry").disabled = loading;
  $("#readiness-loading").hidden = Boolean(report) || !loading;
  $("#readiness-scope").hidden = !(report?.limits || []).length;
  $("#readiness-export").disabled = !report || Boolean(current.downloadController);
  $("#readiness-export").textContent = current.downloadController
    ? "Exporting…" : "Export full report (JSON)";
  $("#readiness-download-error").textContent = current.downloadError;
  $("#readiness-download-error").hidden = !current.downloadError;
}

function readinessElement(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined && text !== null) element.textContent = String(text);
  if (className) element.className = className;
  return element;
}

function readinessTextList(parent, heading, entries) {
  if (!entries?.length) return;
  if (heading) parent.appendChild(readinessElement("h5", heading));
  const list = readinessElement("ul", null, "readiness-text-list");
  entries.forEach((entry) => list.appendChild(readinessElement("li", entry)));
  parent.appendChild(list);
}

function readinessItem(item, key, open) {
  const itemState = readinessState(item.state);
  const row = readinessElement("li", null, "readiness-item");
  row.dataset.readinessKey = key;
  const header = readinessElement("div", null, "readiness-item-header");
  const identity = readinessElement("div");
  identity.appendChild(readinessElement("h4", item.name || item.sourceId || "Unnamed item"));
  const disposition = { created: "Created", adopted: "Adopted", refreshed: "Refreshed", unknown: "Unknown disposition" };
  const dispositionLabel = Object.hasOwn(disposition, item.disposition)
    ? disposition[item.disposition] : disposition.unknown;
  identity.appendChild(readinessElement("p", `${item.itemType || "Unknown type"} · ${dispositionLabel}`, "hint"));
  header.appendChild(identity);
  header.appendChild(readinessElement("span", READINESS_LABELS[itemState], `readiness-state ${itemState}`));
  row.appendChild(header);
  const fallback = {
    ready: "Recorded checks passed. Review the report scope before cutover.",
    needs_attention: "Outstanding work needs review before cutover.",
    unknown: "Readiness has not been established for this item.",
  };
  readinessTextList(row, null, item.reasons?.length ? item.reasons : [fallback[itemState]]);
  readinessTextList(row, "Operator actions", item.actions?.length ? item.actions
    : itemState !== "ready" ? ["Verify this item in the target workspace before cutover."] : []);
  readinessTextList(row, "Unresolved references", item.unresolvedReferences);

  const details = readinessElement("details", null, "readiness-evidence");
  details.open = open;
  const summary = readinessElement("summary", "Evidence and references");
  summary.setAttribute("aria-label", `Evidence and references for ${item.name || item.sourceId || "unnamed item"}`);
  details.appendChild(summary);
  const identifiers = readinessElement("dl", null, "readiness-identifiers");
  [
    ["Source item", item.sourceId], ["Target item", item.targetId],
    ["Source workspace", item.sourceWorkspaceId], ["Target workspace", item.targetWorkspaceId],
  ].forEach(([name, value]) => {
    identifiers.appendChild(readinessElement("dt", name));
    identifiers.appendChild(readinessElement("dd", value || "Not recorded"));
  });
  details.appendChild(identifiers);
  const steps = readinessElement("ul", null, "readiness-evidence-steps");
  (item.steps || []).forEach((step) => {
    const entry = readinessElement("li");
    const labels = { succeeded: "Succeeded", failed: "Failed", skipped: "Skipped", unknown: "Unknown" };
    const stepState = Object.hasOwn(labels, step.state) ? labels[step.state] : labels.unknown;
    entry.appendChild(readinessElement("strong", `${step.step || "Unspecified step"} · ${stepState}`));
    [
      ["Reason", step.reason], ["Action", step.action], ["Error code", step.errorCode],
      ["Service message", step.message], ["Attempt", step.attemptId], ["Target item", step.targetId],
    ].forEach(([name, value]) => {
      if (value) entry.appendChild(readinessElement("p", `${name}: ${value}`));
    });
    steps.appendChild(entry);
  });
  details.appendChild(steps.children.length ? steps
    : readinessElement("p", "No step evidence recorded.", "hint"));
  row.appendChild(details);
  return row;
}

function renderReadinessItems() {
  const current = state.readiness;
  if (!current?.report) return;
  const list = $("#readiness-items");
  const focusedRow = list.contains(document.activeElement)
    ? document.activeElement.closest("[data-readiness-key]")?.dataset.readinessKey : null;
  const openKeys = new Set(Array.from(list.children)
    .filter((row) => row.querySelector("details")?.open).map((row) => row.dataset.readinessKey));
  const needle = current.search.trim().toLocaleLowerCase();
  const items = current.report.items.map((item, index) => ({
    item, key: item.sourceId ? `source:${item.sourceId}` : item.targetId ? `target:${item.targetId}` : `index:${index}`,
  })).filter(({ item }) => {
    if (current.filter !== "all" && readinessState(item.state) !== current.filter) return false;
    const searchable = [
      item.name, item.itemType, item.sourceId, item.targetId, item.sourceWorkspaceId, item.targetWorkspaceId,
      ...(item.reasons || []), ...(item.actions || []), ...(item.unresolvedReferences || []),
    ];
    return !needle || searchable.some((value) => String(value || "").toLocaleLowerCase().includes(needle));
  });
  const pages = Math.max(1, Math.ceil(items.length / READINESS_PAGE_SIZE));
  current.page = Math.max(1, Math.min(current.page, pages));
  const start = (current.page - 1) * READINESS_PAGE_SIZE;
  const visible = items.slice(start, start + READINESS_PAGE_SIZE);
  list.replaceChildren(...visible.map(({ item, key }) => readinessItem(item, key, openKeys.has(key))));
  $("#readiness-controls").hidden = !current.report.items.length;
  $("#readiness-results").textContent = items.length
    ? `Showing ${start + 1}–${Math.min(start + READINESS_PAGE_SIZE, items.length)} of ${items.length} ` +
      `items${items.length !== current.report.items.length ? ` (${current.report.items.length} total)` : ""}`
    : "0 items shown";
  $("#readiness-empty").hidden = Boolean(items.length);
  $("#readiness-empty").textContent = current.report.items.length
    ? "No items match these filters. Choose All states or change your search."
    : "No item evidence is recorded for this run. Review the report scope and validate the target before cutover.";
  $("#readiness-pagination").hidden = pages === 1;
  $("#readiness-page").textContent = `Page ${current.page} of ${pages}`;
  $("#readiness-previous").disabled = current.page === 1;
  $("#readiness-next").disabled = current.page === pages;
  if (focusedRow) {
    const replacement = Array.from(list.children).find((row) => row.dataset.readinessKey === focusedRow);
    (replacement?.querySelector("summary") || $("#readiness-results")).focus({ preventScroll: true });
  }
}

$("#readiness-filter").addEventListener("change", (event) => {
  if (!state.readiness) return;
  state.readiness.filter = event.target.value;
  state.readiness.page = 1;
  renderReadinessItems();
});

$("#readiness-search").addEventListener("input", (event) => {
  if (!state.readiness) return;
  state.readiness.search = event.target.value;
  state.readiness.page = 1;
  renderReadinessItems();
});

[["#readiness-previous", -1], ["#readiness-next", 1]].forEach(([selector, direction]) => {
  $(selector).addEventListener("click", () => {
    if (!state.readiness) return;
    state.readiness.page += direction;
    renderReadinessItems();
    $("#readiness-results").focus({ preventScroll: true });
  });
});

$("#readiness-retry").addEventListener("click", () => scheduleReadiness(true));

$("#readiness-export").addEventListener("click", async () => {
  const current = state.readiness;
  if (!current?.report || current.downloadController) return;
  current.downloadController = new AbortController();
  current.downloadError = "";
  renderReadinessStatus();
  try {
    const blob = await api(`/api/runs/${encodeURIComponent(current.runId)}/readiness?download=true`, {
      download: true, signal: current.downloadController.signal,
    });
    if (state.readiness !== current) return;
    const url = URL.createObjectURL(blob);
    const link = readinessElement("a");
    link.href = url;
    link.download = `fab-shuffle-readiness-${current.runId.replace(/[^a-zA-Z0-9_-]/g, "_")}.json`;
    link.hidden = true;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (error) {
    if (state.readiness === current && error.name !== "AbortError") {
      current.downloadError = `Export failed: ${error.message} Try Export full report again.`;
    }
  } finally {
    if (state.readiness === current) {
      current.downloadController = null;
      renderReadinessStatus();
    }
  }
});

$("#retry-run").addEventListener("click", async () => {
  const button = $("#retry-run");
  busy(button, true, "Starting…");
  try {
    // The same path as picking up an interrupted run: everything already in the new
    // workspace is adopted, so only what did not make it is attempted again.
    const result = await api(`/api/runs/${state.runId}/resume`, { method: "POST" });
    state.runId = result.runId;
    watchRun(result.runId);
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

$("#cancel-run").addEventListener("click", async () => {
  const button = $("#cancel-run");
  busy(button, true, "Cancelling…");
  try {
    await api(`/api/runs/${state.runId}/cancel`, { method: "POST" });
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

$("#cleanup-run").addEventListener("click", async () => {
  const button = $("#cleanup-run");
  busy(button, true, "Cleaning up…");
  try {
    const result = await api(`/api/runs/${state.runId}/cleanup`, { method: "POST" });
    renderRun(result.run);
    if (result.warnings.length) showError(result.warnings.join(" "));
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

$("#start-over").addEventListener("click", () => {
  if (state.events) state.events.close();
  resetReadiness();
  state.runId = null;
  state.events = null;
  state.workspace = null;
  $("#workspace-next").disabled = true;
  goTo("capacity");
});

$$("[data-back]").forEach((button) => {
  button.addEventListener("click", () => goTo(button.dataset.back));
});

goTo("login");
