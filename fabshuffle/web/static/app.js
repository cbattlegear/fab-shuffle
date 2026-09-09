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
  identity: null,
  paired: false,
  crossTenant: false,
  assessmentVersion: 0,
  assessmentPending: false,
  mappingsChecked: false,
  resumeRunId: null,
  resumeReturnStage: "capacity",
  forceRebuild: false,
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
    const detail = payload.detail;
    const message = typeof detail === "string" ? detail
      : Array.isArray(detail) ? detail.map((entry) => `${(entry.loc || []).join(".")}: ${entry.msg}`).join("; ")
      : detail?.message;
    throw new Error(message || `Request failed with HTTP ${response.status}`);
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

function updateLoginMode() {
  const paired = $("#another-tenant").checked;
  $("#destination-credentials").hidden = !paired;
  $("#destination-credentials").disabled = !paired;
  $("#source-credentials-label").textContent = paired ? "Source service principal" : "Service principal";
}

$("#another-tenant").addEventListener("change", updateLoginMode);

function loginBody(form) {
  const data = Object.fromEntries(new FormData(form).entries());
  const body = { tenant_id: data.tenant_id, client_id: data.client_id, client_secret: data.client_secret };
  if ($("#another-tenant").checked) {
    body.destination = {
      tenant_id: data.destination_tenant_id,
      client_id: data.destination_client_id,
      client_secret: data.destination_client_secret,
    };
  }
  return body;
}

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector("button");
  const data = loginBody(form);

  busy(button, true, "Signing in…");
  try {
    const result = await api("/api/login", { method: "POST", body: data });
    invalidateSavedRunActions();
    state.sessionId = result.sessionId;
    state.identity = result;
    state.paired = !!result.paired;
    state.crossTenant = !!result.crossTenant;
    state.assessmentVersion++;
    state.assessmentPending = false;
    state.mappingsChecked = false;
    state.resumeRunId = null;
    state.forceRebuild = false;
    state.capacity = null;
    state.workspace = null;
    state.workspaces = [];
    $("#capacity-next").disabled = true;
    $("#workspace-next").disabled = true;
    $("#opt-permissions").checked = !state.paired;
    $("#opt-write-freeze").checked = false;
    form.reset();
    updateLoginMode();
    renderIdentity();
    $("#sign-out").hidden = false;
    await loadCapacities();
    if (!state.paired) loadLeftovers();
    loadResumable();
    // Needed by the restore-access control on this same step.
    if (!state.paired) loadWorkspaces().then(fillWorkspaceSelects).catch(() => {});
    goTo("capacity");
  } catch (error) {
    showError(error.message);
  } finally {
    busy(button, false);
  }
});

$("#sign-out").addEventListener("click", async () => {
  invalidateSavedRunActions();
  try {
    await api("/api/logout", { method: "POST" });
  } catch (_) {
    /* signing out locally is enough */
  }
  if (state.events) state.events.close();
  resetReadiness();
  stopAssessmentTimer();
  state.assessmentVersion++;
  Object.assign(state, {
    sessionId: null, capacity: null, workspace: null, workspaces: [],
    preview: null, runId: null, events: null, identity: null, paired: false, crossTenant: false,
    resumeRunId: null, forceRebuild: false,
  });
  $("#sign-out").hidden = true;
  goTo("login");
});

function renderIdentity() {
  const identity = state.identity;
  $("#restore-access-tool").hidden = state.paired;
  $("#leftovers").hidden = true;
  $("#destination-context").hidden = !state.paired;
  $("#source-context").hidden = !state.paired;
  if (!state.paired) return;
  $("#destination-context").textContent =
    `Destination tenant: ${identity.targetTenantId} · App: ${identity.destinationPrincipal.client_id}`;
  $("#source-context").textContent =
    `Source tenant: ${identity.sourceTenantId} · App: ${identity.principal.client_id}`;
}

// ------------------------------------------------------------------ capacity

let capacityLoadVersion = 0;

async function loadCapacities() {
  const sessionId = state.sessionId;
  const version = ++capacityLoadVersion;
  const container = $("#capacity-list");
  container.innerHTML = '<p class="hint">Loading capacities…</p>';
  const { capacities } = await api("/api/capacities");
  if (sessionId !== state.sessionId || version !== capacityLoadVersion) return;
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
  if (!capacities.length && state.paired) {
    container.textContent =
      "No destination capacities are visible. Grant the destination app access to a capacity, then sign in again.";
  }
}

$("#capacity-next").addEventListener("click", async () => {
  const button = $("#capacity-next");
  if (button.disabled) return;
  const sessionId = state.sessionId;
  const version = state.assessmentVersion;
  busy(button, true, "Loading…");
  try {
    await loadWorkspaces();
    if (sessionId !== state.sessionId || version !== state.assessmentVersion) return;
    goTo("workspace");
  } catch (error) {
    if (sessionId === state.sessionId && version === state.assessmentVersion) showError(error.message);
  } finally {
    if (sessionId === state.sessionId && version === state.assessmentVersion) busy(button, false);
  }
});

let savedRunsVersion = 0;
let savedRunConfirmation = null;
const savedRunErrors = new Map();

async function loadResumable() {
  const sessionId = state.sessionId;
  const version = ++savedRunsVersion;
  const status = $("#resumable-status");
  status.hidden = !state.paired;
  status.textContent = "Looking for saved runs needing attention for this tenant and app pair…";
  try {
    const { runs } = await api("/api/resumable");
    if (state.sessionId !== sessionId || version !== savedRunsVersion) return;
    const container = $("#resumable");
    container.hidden = !runs.length;
    status.textContent = runs.length
      ? "Only runs bound to this source and destination tenant/app pair are shown."
      : "No saved runs need retrying for this tenant/app pair. To recover another pair, sign in with its original apps.";
    const list = container.querySelector("ul");
    list.innerHTML = "";
    runs.forEach((run) => {
      const item = document.createElement("li");
      item.dataset.runId = run.runId;

      const what = document.createElement("div");
      what.className = "saved-run-name";
      what.textContent = `${run.sourceWorkspaceName} → ${run.targetWorkspaceName}`;
      item.appendChild(what);

      const detail = document.createElement("div");
      detail.className = "hint";
      const started = run.startedAt ? new Date(run.startedAt).toLocaleString() : "an earlier run";
      const built = run.itemsCreated === 1 ? "1 item" : `${run.itemsCreated} items`;
      detail.textContent = `Started ${started}. Got as far as ${run.lastPhase || "the beginning"}, ${built} built.`;
      if (run.status === "succeeded") detail.textContent += " Finished with failed work; retry is available.";
      if (run.sourceTenantId) detail.textContent += ` Tenants: ${run.sourceTenantId} → ${run.targetTenantId}.`;
      if (run.sourceTenantId) {
        detail.textContent += " Keep source writes paused while resuming data or file copies.";
      }
      item.appendChild(detail);

      const pending = run.recoveryAction === "restart_pending";
      if (pending || run.canRestart === false) {
        const reason = document.createElement("p");
        reason.className = "hint";
        reason.textContent = pending
          ? "Full restart is unfinished. Resume is unavailable; finish the full restart before starting a fresh migration."
          : "";
        if (run.canRestart === false) reason.textContent += ` ${run.restartBlockedReason || "Full restart is unavailable. Refresh the saved migrations to check again."}`;
        item.appendChild(reason);
      }

      const actions = document.createElement("div");
      actions.className = "saved-run-actions";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "secondary";
      button.textContent = state.paired ? "Review mappings and resume" : "Resume";
      button.disabled = pending;
      button.addEventListener("click", async () => {
        if (button.disabled || sessionId !== state.sessionId || savedRunConfirmation) return;
        const restore = lockSavedRunRow(item);
        try {
          if (state.paired) await prepareResume(run.runId, button, "capacity");
          else await resumeRun(run.runId, button);
        } finally {
          if (sessionId === state.sessionId) restore();
        }
      });
      actions.appendChild(button);
      for (const kind of ["ignore", "restart"]) {
        const action = document.createElement("button");
        action.type = "button";
        action.className = kind === "restart" ? "secondary destructive" : "secondary";
        action.textContent = kind === "ignore" ? "Ignore" : pending ? "Finish full restart" : "Full restart";
        action.disabled = kind === "restart" && run.canRestart === false;
        action.addEventListener("click", () => {
          if (!action.disabled && sessionId === state.sessionId) confirmSavedRunAction(run, kind, item, action);
        });
        actions.appendChild(action);
      }
      item.appendChild(actions);
      if (savedRunErrors.has(run.runId)) {
        const error = document.createElement("p");
        error.className = "hint saved-run-error";
        error.textContent = savedRunErrors.get(run.runId);
        item.appendChild(error);
      }

      list.appendChild(item);
    });
    return true;
  } catch (error) {
    if (state.sessionId !== sessionId || version !== savedRunsVersion) return;
    status.hidden = false;
    status.textContent = `Could not load unfinished migrations: ${error.message}. Sign in again to retry.`;
    return false;
  }
}

function lockSavedRunRow(row) {
  const buttons = Array.from(row.querySelectorAll("button")).map((button) => [button, button.disabled]);
  buttons.forEach(([button]) => { button.disabled = true; });
  return () => buttons.forEach(([button, disabled]) => { button.disabled = disabled; });
}

function closeSavedRunConfirmation(restoreFocus = false) {
  const confirmation = savedRunConfirmation;
  savedRunConfirmation = null;
  const dialog = $("#saved-run-confirmation");
  if (dialog.open) dialog.close();
  if (restoreFocus && confirmation?.sessionId === state.sessionId) confirmation.opener.focus();
}

function invalidateSavedRunActions() {
  savedRunsVersion++;
  savedRunErrors.clear();
  closeSavedRunConfirmation();
}

function confirmSavedRunAction(run, kind, row, opener) {
  if (savedRunConfirmation) return;
  savedRunConfirmation = { run, kind, row, opener, sessionId: state.sessionId, busy: false };
  const restarting = kind === "restart";
  $("#saved-run-confirm-title").textContent = restarting ? "Full restart of this migration?" : "Ignore this saved migration?";
  $("#saved-run-confirm-name").textContent = `${run.sourceWorkspaceName} → ${run.targetWorkspaceName}`;
  $("#saved-run-confirm-target").textContent = run.targetWorkspaceId
    ? `Destination workspace ID: ${run.targetWorkspaceId}`
    : "No destination workspace was recorded for this migration.";
  $("#saved-run-confirm-description").textContent = restarting
    ? (run.targetWorkspaceId
      ? "The recorded destination workspace and all its contents will be deleted. "
      : "No destination workspace will be deleted because none was recorded. ") +
      "The source workspace is left unchanged. Any temporary run workspace is cleaned up if needed."
    : "Permanently hide this migration from the saved migrations list, including after signing in again.";
  $("#saved-run-confirm-warning").textContent = restarting
    ? "Deletion cannot be undone. After cleanup, choose a capacity and review a fresh migration. Nothing starts automatically, and no previous item IDs, mappings or copy checkpoints are reused."
    : "This does not delete any workspace or data, and does not stop running jobs. The saved journal is kept.";
  $("#saved-run-confirm-cancel").disabled = false;
  const submit = $("#saved-run-confirm-submit");
  submit.disabled = false;
  submit.className = restarting ? "destructive" : "primary";
  submit.textContent = restarting
    ? (run.targetWorkspaceId ? "Delete destination and restart" : "Full restart")
    : "Ignore migration";
  $("#saved-run-confirm-progress").hidden = true;
  $("#saved-run-confirmation").showModal();
  $("#saved-run-confirm-cancel").focus();
}

$("#saved-run-confirm-cancel").addEventListener("click", () => {
  if (!savedRunConfirmation?.busy) closeSavedRunConfirmation(true);
});
$("#saved-run-confirmation").addEventListener("cancel", (event) => {
  event.preventDefault();
  if (!savedRunConfirmation?.busy) closeSavedRunConfirmation(true);
});

$("#saved-run-confirm-submit").addEventListener("click", async () => {
  const confirmation = savedRunConfirmation;
  if (!confirmation || confirmation.busy || confirmation.sessionId !== state.sessionId) return;
  const { run, kind, row, sessionId } = confirmation;
  confirmation.busy = true;
  savedRunsVersion++;
  const restore = lockSavedRunRow(row);
  $("#saved-run-confirm-submit").disabled = true;
  $("#saved-run-confirm-cancel").disabled = true;
  const progress = $("#saved-run-confirm-progress");
  progress.hidden = false;
  progress.textContent = kind === "restart" ? "Cleaning up the recorded migration… Please wait."
    : "Hiding this saved migration…";
  const current = () => savedRunConfirmation === confirmation && state.sessionId === sessionId;
  try {
    const body = kind === "restart"
      ? { confirmed: true, target_workspace_id: run.targetWorkspaceId || "" }
      : { confirmed: true };
    const result = await api(`/api/runs/${encodeURIComponent(run.runId)}/${kind}`, { method: "POST", body });
    if (!current()) return;
    savedRunErrors.delete(run.runId);
    row.remove();
    if (!$("#resumable").querySelector("ul").children.length) $("#resumable").hidden = true;
    if (kind === "restart") {
      resetForFullRestart(result.sourceWorkspace);
      goTo("capacity");
    }
    closeSavedRunConfirmation();
    if (kind === "restart") {
      $("#capacity-title").focus();
      // Cleanup has already succeeded; a discovery failure must not offer deletion again.
      loadCapacities().catch((error) => {
        if (sessionId === state.sessionId) showError(`Full restart cleanup completed. Could not load capacities: ${error.message}. Sign in again to choose a capacity.`);
      });
    }
    await loadResumable();
    if (sessionId !== state.sessionId) return;
    if (kind === "ignore") {
      ($("#resumable").hidden ? $("#capacity-title") : $("#resumable-title")).focus();
    }
  } catch (error) {
    if (!current()) return;
    restore();
    if (kind === "restart") {
      // Until the server reports otherwise, a failed response could mean partial deletion.
      run.recoveryAction = "restart_pending";
      row.querySelector("button").disabled = true;
      confirmation.opener.textContent = "Finish full restart";
    }
    const message = `${error.message} ${kind === "restart"
      ? "Could not confirm that full restart completed. Resolve the error, then try Full restart again."
      : "Could not confirm that the migration was hidden. Resolve the error, then try Ignore again."}`;
    savedRunErrors.set(run.runId, message);
    closeSavedRunConfirmation();
    showError(message);
    confirmation.opener.focus();
    await loadResumable();
    if (sessionId === state.sessionId) {
      const updated = Array.from($("#resumable").querySelector("ul").children)
        .find((item) => item.dataset.runId === run.runId);
      const action = updated?.querySelectorAll("button")[kind === "restart" ? 2 : 1];
      if (action && !action.disabled) action.focus();
      else ($("#resumable").hidden ? $("#capacity-title") : $("#resumable-title")).focus();
    }
  }
});

function resetForFullRestart(sourceWorkspace) {
  if (state.events) state.events.close();
  resetReadiness();
  stopAssessmentTimer();
  state.assessmentVersion++;
  Object.assign(state, {
    capacity: null, workspace: sourceWorkspace, workspaces: [], preview: null,
    runId: null, resumeRunId: null, resumeReturnStage: "capacity", events: null,
    assessmentPending: false, forceRebuild: true,
  });
  resetMappings();
  busy($("#capacity-next"), false);
  busy($("#workspace-next"), false);
  $("#capacity-next").disabled = true;
  $("#workspace-next").disabled = true;
  $("#workspace-filter").value = "";
  $("#workspace-list").innerHTML = "";
  $("#target-name").value = "";
  $("#target-name").disabled = false;
  for (const id of ["opt-data", "opt-files", "opt-cleanup", "opt-permissions"]) {
    $(`#${id}`).checked = id !== "opt-permissions" || !state.paired;
    $(`#${id}`).disabled = id === "opt-permissions" && state.paired;
  }
  $("#resume-artifacts").hidden = true;
  $("#resume-artifacts").open = false;
  for (const id of ["resume-target-id", "resume-item-ids", "source-connection-options",
    "target-connection-options", "review-summary", "run-banner", "step-list"]) {
    $(`#${id}`).textContent = "";
  }
  $("#connection-options-status").textContent = "";
  $("#mapping-status").textContent = "Re-check after editing mappings.";
  $("#review-title").textContent = "Review the move";
  $("#review-back").hidden = false;
  $("#resume-back").hidden = true;
  $("#start-run").textContent = "Start migration";
  $("#start-run").disabled = true;
  for (const id of ["retry-run", "cleanup-run", "cancel-run", "start-over"]) $(`#${id}`).hidden = true;
}

async function resumeRun(runId, button, mappings) {
  const sessionId = state.sessionId;
  const version = state.assessmentVersion;
  busy(button, true, "Starting…");
  try {
    const result = await api(`/api/runs/${runId}/resume`, { method: "POST", body: mappings });
    if (sessionId !== state.sessionId || version !== state.assessmentVersion) return;
    state.resumeRunId = null;
    state.runId = result.runId;
    goTo("progress");
    watchRun(result.runId);
  } catch (error) {
    if (sessionId !== state.sessionId || version !== state.assessmentVersion) return;
    showError(error.message);
    busy(button, false);
    if (state.resumeRunId && state.preview) setStartEnabled(!state.preview.blockers.length);
  }
}

async function prepareResume(runId, button, returnStage = "progress") {
  busy(button, true, "Loading saved mappings…");
  const sessionId = state.sessionId;
  const version = ++state.assessmentVersion;
  try {
    const saved = await api(`/api/runs/${runId}/resume-plan`);
    if (sessionId !== state.sessionId || version !== state.assessmentVersion) return;
    const plan = saved.plan;
    state.resumeRunId = runId;
    state.resumeReturnStage = returnStage;
    state.capacity = { id: plan.capacityId };
    state.workspace = { id: plan.sourceWorkspaceId };
    state.preview = {
      ...plan, blockers: [], unsupportedSummary: [], dependencies: [],
      counts: [], migratedTotal: 0, largeSemanticModels: [],
      targetWorkspaceId: saved.targetWorkspaceId,
    };
    resetMappings();
    $("#opt-data").checked = plan.includeData;
    $("#opt-files").checked = plan.includeFiles;
    $("#opt-permissions").checked = plan.copyPermissions;
    $("#opt-cleanup").checked = saved.cleanupWhenDone;
    for (const [source, target] of Object.entries(plan.connectionMappings || {})) {
      const inputs = addMappingRow("connection").querySelectorAll("input");
      inputs[0].value = source;
      inputs[1].value = target;
    }
    for (const mapping of plan.referenceMappings || []) {
      addMappingRow("reference").querySelectorAll("input").forEach((input) => {
        input.value = mapping[input.dataset.field] || "";
      });
    }
    goTo("review");
    renderReview();
    $("#resume-target-id").textContent = saved.targetWorkspaceId
      ? `Destination workspace ID: ${saved.targetWorkspaceId}`
      : "No destination workspace was recorded yet. Resuming will create it.";
    const items = $("#resume-item-ids");
    items.innerHTML = "";
    for (const item of saved.items) {
      const row = document.createElement("li");
      row.textContent = `${item.name || item.sourceId} (${item.type}) — destination item ID: ${item.targetId}`;
      items.appendChild(row);
    }
    if (!saved.items.length) {
      const row = document.createElement("li");
      row.textContent = "No destination items were recorded. Inspect the migration warnings before resuming.";
      items.appendChild(row);
    }
    loadConnectionOptions();
    await recheckMappings();
  } catch (error) {
    if (sessionId === state.sessionId) showError(error.message);
  } finally {
    if (sessionId === state.sessionId) busy(button, false);
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
  const sessionId = state.sessionId;
  const version = state.assessmentVersion;
  const { workspaces } = await api("/api/workspaces");
  if (sessionId !== state.sessionId || version !== state.assessmentVersion) return;
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
  const selected = entries.find((entry) => entry.id === state.workspace?.id);
  state.workspace = selected?.raw || null;
  $("#workspace-next").disabled = !selected;
  if (selected) {
    const choice = Array.from($("#workspace-list").children).find((entry) => entry.dataset.id === selected.id);
    choice.setAttribute("aria-checked", "true");
  }
  if (!entries.length && state.paired) {
    $("#workspace-list").textContent = needle ? "No matching source workspaces. Clear the filter to see all workspaces."
      : "No source workspaces are visible. Grant the source app workspace access, then sign in again.";
  }
}

$("#workspace-filter").addEventListener("input", (event) => {
  state.workspace = null;
  $("#workspace-next").disabled = true;
  renderWorkspaces(event.target.value);
});

$("#workspace-next").addEventListener("click", async () => {
  const button = $("#workspace-next");
  if (button.disabled) return;
  const sessionId = state.sessionId;
  busy(button, true, "Inspecting…");
  const params = new URLSearchParams({
    capacity_id: state.capacity.id,
    source_workspace_id: state.workspace.id,
  });
  if (state.forceRebuild) params.set("strategy", "rebuild");

  // Both requests go out together, and the review screen appears straight away. The
  // dependency check walks the relations API and every connection in the tenant, so waiting
  // for it before showing anything left the wizard looking stuck.
  state.resumeRunId = null;
  resetMappings();
  state.assessmentPending = true;
  const version = ++state.assessmentVersion;
  const dependencies = state.paired ? null : api(`/api/preview/dependencies?${params}`);
  dependencies?.catch(() => {});

  try {
    goTo("review");
    renderReviewPending();
    const preview = await api(`/api/preview?${params}`);
    if (version !== state.assessmentVersion || sessionId !== state.sessionId) return;
    state.preview = preview;
    renderReview();
  } catch (error) {
    if (version !== state.assessmentVersion || sessionId !== state.sessionId) return;
    goTo("workspace");
    showError(error.message);
    busy(button, false);
    return;
  }
  busy(button, false);

  if (state.paired) {
    loadConnectionOptions();
    await recheckMappings();
  } else await settleAssessment(dependencies, version);
});

$("#recheck").addEventListener("click", async () => {
  const button = $("#recheck");
  busy(button, true, "Re-checking…");
  if (state.paired) {
    await recheckMappings();
    busy(button, false);
    return;
  }
  const params = new URLSearchParams({ source_workspace_id: state.workspace.id });
  if (state.forceRebuild) params.set("strategy", "rebuild");

  showDependenciesPending();
  setStartEnabled(false);
  try {
    await settleAssessment(api(`/api/preview/dependencies?${params}`));
  } finally {
    busy(button, false);
  }
});

/** Apply the slow half of the assessment, however it was started. */
async function settleAssessment(request, version = state.assessmentVersion) {
  try {
    const result = await request;
    if (version !== state.assessmentVersion || !state.preview) return;
    state.preview.dependencies = result.dependencies;
    state.preview.connectionAccess = result.connectionAccess;
    state.preview.mappingBlockers = result.blockers || [];
    state.mappingsChecked = true;
    $("#mapping-status").textContent = result.assessmentNotice ||
      "Destination references checked. Validate copied items and data before cutover.";
  } catch (error) {
    if (version !== state.assessmentVersion || !state.preview) return;
    state.preview.dependencies = [`Dependencies could not be checked: ${error.message}`];
    state.preview.connectionAccess = null;
    state.mappingsChecked = false;
    $("#mapping-status").textContent = `Check failed: ${error.message}. Re-check to retry.`;
  }
  state.assessmentPending = false;
  stopAssessmentTimer();
  renderDependencies();
  renderConnectionAccess();
  // Only now is it known whether anything blocks the run.
  setStartEnabled(state.preview.blockers.length === 0);
}

function setStartEnabled(enabled) {
  const start = $("#start-run");
  const freezeMissing = state.paired && ($("#opt-data").checked || $("#opt-files").checked) &&
    !$("#opt-write-freeze").checked;
  const mappingBlocked = state.paired && (!state.mappingsChecked || state.preview?.mappingBlockers?.length);
  start.disabled = !enabled || state.assessmentPending || freezeMissing || !!mappingBlocked;
  start.title = !enabled || state.assessmentPending ? "Finish the assessment and resolve its blockers"
    : mappingBlocked ? "Re-check destination mappings and resolve their blockers"
    : freezeMissing ? "Confirm the source write freeze, or turn off both data and file copying" : "";
}


// -------------------------------------------------------------------- review

let mappingRowId = 0;
let connectionOptionsVersion = 0;

function invalidateMappings() {
  state.assessmentVersion++;
  state.assessmentPending = false;
  state.mappingsChecked = false;
  stopAssessmentTimer();
  $("#mapping-status").textContent = "Mappings changed. Re-check before starting.";
  setStartEnabled(!!state.preview && !state.preview.blockers.length);
}

function addMappingRow(kind) {
  const row = document.createElement("div");
  row.className = "mapping-row";
  const names = kind === "connection"
    ? [["source_connection_id", "Source connection ID"], ["target_connection_id", "Destination connection ID"]]
    : [["source_workspace_id", "Source workspace ID"], ["source_item_id", "Source item ID"],
      ["target_workspace_id", "Destination workspace ID"], ["target_item_id", "Destination item ID"]];
  names.forEach(([name, title], index) => {
    const label = document.createElement("label");
    label.textContent = title;
    const input = document.createElement("input");
    input.type = "text";
    input.id = `mapping-${++mappingRowId}`;
    input.dataset.field = name;
    input.autocomplete = "off";
    input.spellcheck = false;
    input.maxLength = 128;
    input.placeholder = "00000000-1111-2222-3333-444444444444";
    if (kind === "connection") input.setAttribute("list", `${index ? "target" : "source"}-connection-options`);
    input.addEventListener("input", invalidateMappings);
    label.appendChild(input);
    row.appendChild(label);
  });
  const remove = document.createElement("button");
  remove.type = "button";
  remove.className = "secondary";
  remove.textContent = "Remove mapping";
  remove.addEventListener("click", () => {
    row.remove();
    invalidateMappings();
    $(`#add-${kind}-mapping`).focus();
  });
  row.appendChild(remove);
  $(`#${kind}-mapping-rows`).appendChild(row);
  invalidateMappings();
  return row;
}

function resetMappings() {
  connectionOptionsVersion++;
  $("#connection-mapping-rows").innerHTML = "";
  $("#reference-mapping-rows").innerHTML = "";
  $("#opt-write-freeze").checked = false;
  state.mappingsChecked = false;
}

function mappingOptions() {
  const result = { connection_mappings: {}, reference_mappings: [] };
  if (!state.paired) return result;
  for (const kind of ["connection", "reference"]) {
    for (const row of $(`#${kind}-mapping-rows`).children) {
      const entries = Array.from(row.querySelectorAll("input")).map((input) =>
        [input.dataset.field, (input.value || "").trim()]);
      if (entries.every(([, value]) => !value)) continue;
      if (entries.some(([, value]) => !value)) {
        throw new Error(`Complete every ID in the ${kind} mapping, or remove its row.`);
      }
      const values = Object.fromEntries(entries);
      if (kind === "connection") {
        if (Object.hasOwn(result.connection_mappings, values.source_connection_id)) {
          throw new Error("Each source connection must have only one destination mapping.");
        }
        Object.defineProperty(result.connection_mappings, values.source_connection_id, {
          value: values.target_connection_id, enumerable: true,
        });
      } else result.reference_mappings.push(values);
    }
  }
  return result;
}

function migrationBody() {
  const reassign = state.preview?.strategy === "reassign";
  return {
    capacity_id: state.capacity.id,
    source_workspace_id: state.workspace.id,
    strategy: state.paired ? "rebuild" : state.preview?.strategy,
    target_workspace_name: reassign ? null : $("#target-name").value.trim() || null,
    include_data: $("#opt-data").checked,
    include_files: $("#opt-files").checked,
    copy_permissions: state.paired ? false : $("#opt-permissions").checked,
    cleanup_when_done: $("#opt-cleanup").checked,
    write_freeze_confirmed: $("#opt-write-freeze").checked,
    ...mappingOptions(),
  };
}

async function recheckMappings() {
  const button = $("#recheck-mappings");
  let body;
  try { body = state.resumeRunId ? mappingOptions() : migrationBody(); } catch (error) {
    showError(error.message);
    return;
  }
  const version = ++state.assessmentVersion;
  state.assessmentPending = true;
  state.mappingsChecked = false;
  busy(button, true, "Checking destination references…");
  showDependenciesPending();
  setStartEnabled(false);
  try {
    const path = state.resumeRunId
      ? `/api/runs/${state.resumeRunId}/resume-preview` : "/api/preview/dependencies";
    await settleAssessment(api(path, { method: "POST", body }), version);
  } finally {
    busy(button, false);
  }
}

async function loadConnectionOptions() {
  const sessionId = state.sessionId;
  const version = ++connectionOptionsVersion;
  const status = $("#connection-options-status");
  status.textContent = "Loading source and destination connection IDs…";
  const results = await Promise.all(["source", "target"].map(async (side) => {
    try {
      const result = await api(`/api/connections?side=${side}`);
      if (sessionId !== state.sessionId || version !== connectionOptionsVersion) return "";
      const options = $(`#${side}-connection-options`);
      options.innerHTML = "";
      result.connections.forEach((entry) => {
        const option = document.createElement("option");
        option.value = entry.id;
        option.textContent = entry.displayName || entry.id;
        options.appendChild(option);
      });
      return result.connections.length ? `${side}: ${result.connections.length} available`
        : `${side}: no visible connections; create or share one, then re-check`;
    } catch (error) { return `${side}: ${error.message}. Enter known IDs manually or re-check to retry`; }
  }));
  if (sessionId === state.sessionId && version === connectionOptionsVersion) status.textContent = results.join(". ");
}

$("#add-connection-mapping").addEventListener("click", () => addMappingRow("connection").querySelector("input").focus());
$("#add-reference-mapping").addEventListener("click", () => addMappingRow("reference").querySelector("input").focus());
$("#recheck-mappings").addEventListener("click", () => {
  loadConnectionOptions();
  recheckMappings();
});
["#opt-data", "#opt-files", "#opt-write-freeze"].forEach((id) => {
  $(id).addEventListener("change", () => setStartEnabled(!!state.preview && !state.preview.blockers.length));
});

function renderReviewPending() {
  const callout = $("#strategy-callout");
  callout.className = "callout";
  callout.innerHTML = '<strong><span class="spin">◜</span> Inspecting the workspace</strong><p></p>';
  callout.querySelector("p").textContent = state.forceRebuild
    ? "Reading the source items to plan a fresh workspace. Full restart always rebuilds; the source will not be reassigned."
    : "Reading the items in the source workspace to work out whether it can be reassigned or " +
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
  $("#destination-mappings").hidden = true;
  $("#write-freeze").hidden = true;
  $("#start-run").disabled = true;
}

function renderDependencies() {
  const container = $("#dependencies");
  container.classList.remove("pending");
  container.querySelector("h3").textContent = "Needs attention";
  container.querySelector(".hint").hidden = false;
  container.querySelector(".hint").textContent = state.paired
    ? "Resolve these destination prerequisites. Items with unresolved source references are skipped, " +
      "not created pointing back at the source."
    : "References the migration cannot follow, and connections you will have to grant access to. " +
      "The new workspace will be created, but these need fixing before it works.";
  fillList(container, [...(state.preview.dependencies || []), ...(state.preview.mappingBlockers || [])]);
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
  const resuming = !!state.resumeRunId;
  $("#review-title").textContent = resuming ? "Update mappings and resume" : "Review the move";
  $("#resume-artifacts").hidden = !resuming;
  $("#resume-artifacts").open = resuming;
  $("#review-back").hidden = resuming;
  $("#resume-back").hidden = !resuming;

  const callout = $("#strategy-callout");
  callout.className = `callout ${reassign ? "good" : ""}`;
  callout.innerHTML = "<strong></strong><p></p>";
  callout.querySelector("strong").textContent = reassign
    ? "This workspace can just be reassigned"
    : state.crossTenant ? "Rebuild in the destination tenant"
    : state.paired ? "Rebuild with destination credentials" : "This workspace has to be rebuilt";
  callout.querySelector("p").textContent = reassign
    ? "It only holds Power BI content, so Fab Shuffle moves the existing workspace onto the " +
      "target capacity instead of recreating it. Nothing is copied and no new workspace is made." +
      (preview.largeSemanticModels.length
        ? ` ${preview.largeSemanticModels.length} semantic model(s) use large storage format and will be ` +
          "converted to small for the move, then switched back afterwards."
        : "")
    : state.paired ? "Fab Shuffle creates a new workspace with destination credentials. " +
      "Source permissions are not copied. " +
      "Items with unresolved source references are not created."
    : "It contains Fabric items, which cannot move across regions on a capacity reassignment. " +
      "Fab Shuffle creates a new workspace in the target region and copies everything it supports.";
  if (resuming) {
    callout.querySelector("strong").textContent = "Resume the same migration";
    callout.querySelector("p").textContent =
      "The recorded destination workspace, tenant/app pair, options and copy checkpoints are kept. " +
      "Only connection and external item mappings can be changed here. Unresolved consumers are safely skipped.";
  }

  const rows = [
    ["Source workspace", preview.sourceWorkspaceName],
    ["Target capacity", `${preview.capacityName} (${preview.capacityRegion || "unknown region"})`],
  ];
  if (state.paired) {
    rows.unshift(["Source tenant", preview.sourceTenantId], ["Source app", preview.sourceClientId]);
    rows.push(["Source workspace ID", state.workspace.id],
      ["Destination tenant", preview.targetTenantId], ["Destination app", preview.targetClientId],
      ["Destination capacity ID", state.capacity.id]);
  }
  if (resuming) {
    rows.push(["Destination workspace", preview.targetWorkspaceName],
      ["Destination workspace ID", preview.targetWorkspaceId || "Not created yet"]);
  } else if (reassign) {
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
  fillList($("#review-warnings"), [preview.capacityWarning, preview.assessmentNotice].filter(Boolean));

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
  $("#target-name").disabled = resuming;
  ["#opt-data", "#opt-files", "#opt-cleanup"].forEach((id) => { $(id).disabled = resuming; });
  $("#destination-mappings").hidden = !state.paired || reassign;
  $("#write-freeze").hidden = !state.paired;
  $("#opt-permissions").disabled = state.paired || resuming;
  if (state.paired) $("#opt-permissions").checked = false;
  $("#opt-permissions").title = state.paired ? "Grant access to the destination workspace separately." : "";

  // The button stays disabled until the dependency and connection assessment has finished,
  // because until then it is not known whether anything blocks the run.
  setStartEnabled(reassign && preview.blockers.length === 0);
  $("#start-run").textContent = resuming ? "Resume migration" : reassign ? "Reassign workspace" : "Start migration";
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
  if (button.disabled) return;
  if (state.resumeRunId) {
    try { await resumeRun(state.resumeRunId, button, mappingOptions()); }
    catch (error) { showError(error.message); }
    return;
  }
  const sessionId = state.sessionId;
  const reassign = state.preview.strategy === "reassign";
  busy(button, true, reassign ? "Reassigning…" : "Starting…");
  try {
    const result = await api("/api/runs", {
      method: "POST",
      body: migrationBody(),
    });
    if (sessionId !== state.sessionId) return;

    state.runId = result.runId;
    goTo("progress");
    watchRun(result.runId);
  } catch (error) {
    if (sessionId === state.sessionId) showError(error.message);
  } finally {
    busy(button, false);
    if (sessionId === state.sessionId && state.preview) setStartEnabled(!state.preview.blockers.length);
  }
});

$("#resume-back").addEventListener("click", () => {
  state.resumeRunId = null;
  state.assessmentVersion++;
  stopAssessmentTimer();
  goTo(state.resumeReturnStage);
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
  const spinner = run.status === "running" ? '<span class="spin" aria-hidden="true">◐</span> ' : "";
  banner.innerHTML = `${spinner}<strong></strong><span class="run-target"></span>`;
  banner.querySelector("strong").textContent = RUN_MESSAGES[run.status] || run.status;
  banner.querySelector(".run-target").textContent = run.targetWorkspace
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
  $("#retry-run").textContent = state.paired ? "Update mappings and retry" : "Retry what did not migrate";
}

// ---------------------------------------------------------- cutover readiness

const READINESS_LABELS = { ready: "Ready", needs_attention: "Needs attention", unknown: "Unknown" };
const READINESS_PAGE_SIZE = 25;
const READINESS_REFRESH_MS = 500;

function readinessState(value) {
  return Object.hasOwn(READINESS_LABELS, value) ? value : "unknown";
}

function readinessAvailable(status) {
  return ["succeeded", "failed", "cancelled", "interrupted"].includes(status);
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
}

function observeReadiness(run) {
  const current = state.readiness;
  if (!current || current.runId !== state.runId) return;
  const terminal = readinessAvailable(run.status);
  const finishedNow = terminal && current.runStatus !== run.status;
  const changed = current.runStatus === null || current.revision !== run.readinessRevision;
  current.runStatus = run.status;
  current.revision = run.readinessRevision;
  if (!terminal) {
    if (current.report || current.controller || current.downloadController || current.pending ||
        current.timer !== null || current.error || current.downloadError) {
      resetReadiness(current.runId);
      state.readiness.runStatus = run.status;
      state.readiness.revision = run.readinessRevision;
    }
    renderReadinessStatus();
    return;
  }
  if (changed || finishedNow) {
    current.version += 1;
    scheduleReadiness(finishedNow);
  }
}

function scheduleReadiness(immediate = false) {
  const current = state.readiness;
  if (!current || !readinessAvailable(current.runStatus)) return;
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
  if (state.readiness !== current || state.runId !== current.runId ||
      !readinessAvailable(current.runStatus)) return;
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
  const visible = current && readinessAvailable(current.runStatus);
  $("#cutover-readiness").hidden = !visible;
  $(".readiness-jump").hidden = !visible;
  if (!visible) {
    $("#readiness-export").disabled = true;
    return;
  }
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
  if (!current?.report || !readinessAvailable(current.runStatus) || current.downloadController) return;
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
  if (state.paired) {
    await prepareResume(state.runId, button);
    return;
  }
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
  state.forceRebuild = false;
  $("#workspace-next").disabled = true;
  goTo("capacity");
});

$$("[data-back]").forEach((button) => {
  button.addEventListener("click", () => goTo(button.dataset.back));
});

goTo("login");
