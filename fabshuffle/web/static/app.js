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
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

// ------------------------------------------------------------------ plumbing

async function api(path, { method = "GET", body } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (state.sessionId) headers["X-Fab-Shuffle-Session"] = state.sessionId;

  const response = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

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
  try {
    const params = new URLSearchParams({
      capacity_id: state.capacity.id,
      source_workspace_id: state.workspace.id,
    });
    state.preview = await api(`/api/preview?${params}`);
    renderReview();
    goTo("review");
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

// -------------------------------------------------------------------- review

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
    rows.push(
      ["Lakehouses", String(preview.counts.lakehouses)],
      ["Warehouses", String(preview.counts.warehouses)],
      ["Eventhouses", String(preview.counts.eventhouses)]
    );
  }

  $("#review-summary").innerHTML = rows.map(() => `<div><span class="k"></span><span class="v"></span></div>`).join("");
  $$("#review-summary div").forEach((row, index) => {
    row.querySelector(".k").textContent = rows[index][0];
    row.querySelector(".v").textContent = rows[index][1];
  });

  fillList($("#blockers"), preview.blockers);
  fillList(
    $("#unsupported"),
    preview.unsupported.map((item) => `${item.type} “${item.name}” — ${item.reason}`)
  );

  // A reassignment keeps the workspace and its name, and copies nothing.
  $("#target-name-field").hidden = reassign;
  $("#rebuild-options").hidden = reassign;
  $("#target-name").value = preview.targetWorkspaceName;

  $("#start-run").disabled = preview.blockers.length > 0;
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
  const url = `/api/runs/${runId}/events?session_id=${encodeURIComponent(state.sessionId)}`;
  const events = new EventSource(url);
  state.events = events;

  events.onmessage = (message) => renderRun(JSON.parse(message.data));
  events.onerror = () => {
    events.close();
    // The stream ends when the run finishes; fall back to a single fetch for the final state.
    api(`/api/runs/${runId}`).then(renderRun).catch(() => {});
  };
}

function renderRun(run) {
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
      <div>
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
      item.querySelector("div:last-child").appendChild(warnings);
    }
    list.appendChild(item);
  });

  const finished = run.status !== "running" && run.status !== "pending";
  $("#cancel-run").hidden = finished;
  $("#start-over").hidden = !finished;
  $("#cleanup-run").hidden = !finished || run.cleanupDone || !run.scratchWorkspace;
}

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
