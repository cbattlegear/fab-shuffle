const assert = require("node:assert/strict");
const { fixture } = require("./cross_tenant_ui_checks.js");

const saved = {
  runId: "saved-run",
  sourceWorkspaceName: "Original source",
  targetWorkspaceName: "Recorded destination",
  targetWorkspaceId: "recorded-destination-id",
  itemsCreated: 2,
  lastPhase: "Copy data",
  status: "failed",
  recoveryAction: "",
  canRestart: true,
  restartBlockedReason: "",
};
const source = { id: "original-source-id", displayName: "Original source" };
const row = (f, index = 0) => f.get("resumable").querySelector("ul").children[index];
const buttons = (f, index = 0) => row(f, index).querySelectorAll("button");
const confirm = (f) => f.get("saved-run-confirm-submit").click();
const payload = (f, index) => JSON.parse(f.requests[index].options.body);
const stage = (f, name) => f.document.querySelectorAll(".panel").find((panel) => panel.dataset.stage === name);

async function load(f, runs = [saved]) {
  const request = f.requests.length;
  f.ui.loadResumable();
  await f.reply(request, { runs });
}

async function begin(f, kind = "restart", run = saved) {
  f.ui.state.sessionId = "original-session";
  await load(f, [run]);
  buttons(f)[kind === "restart" ? 2 : 1].click();
}

async function finishRestart(f, request) {
  await f.reply(request, { restartComplete: true, runId: saved.runId, sourceWorkspace: source });
  const capacityRequest = f.requests.findIndex((entry, index) => index > request && entry.url === "/api/capacities");
  const runsRequest = f.requests.findIndex((entry, index) => index > request && entry.url === "/api/resumable");
  assert.ok(capacityRequest >= 0);
  assert.ok(runsRequest >= 0);
  return { capacityRequest, runsRequest };
}

const scenarios = {
  async confirmation_cancel(f) {
    await begin(f, "ignore", { ...saved, targetWorkspaceName: "<img onerror=alert(1)> destination" });
    const dialog = f.get("saved-run-confirmation");
    assert.equal(dialog.open, true);
    assert.equal(f.document.activeElement, f.get("saved-run-confirm-cancel"));
    assert.equal(dialog["aria-labelledby"], "saved-run-confirm-title");
    assert.match(f.get("saved-run-confirm-name").textContent, /Original source.*<img onerror=alert\(1\)>/);
    assert.equal(f.get("saved-run-confirm-name").children.length, 0);
    assert.match(f.get("saved-run-confirm-target").textContent, /recorded-destination-id/);
    assert.match(dialog.textContent, /Permanently hide/);
    assert.match(dialog.textContent, /does not delete any workspace or data/);
    assert.match(dialog.textContent, /does not stop running jobs/);
    assert.equal(f.requests.length, 1);
    f.get("saved-run-confirm-cancel").click();
    assert.equal(dialog.open, false);
    assert.equal(f.document.activeElement, buttons(f)[1]);
    assert.equal(f.requests.length, 1);
    buttons(f)[2].click();
    assert.match(dialog.textContent, /all its contents will be deleted/);
    assert.match(dialog.textContent, /source workspace is left unchanged/);
    assert.match(dialog.textContent, /temporary run workspace/);
    assert.match(dialog.textContent, /cannot be undone/);
    assert.match(dialog.textContent, /choose a capacity/);
    assert.equal(f.document.activeElement, f.get("saved-run-confirm-cancel"));
    let prevented = false;
    dialog.handlers.cancel({ preventDefault() { prevented = true; } });
    assert.equal(prevented, true);
    assert.equal(dialog.open, false);
    assert.equal(f.document.activeElement, buttons(f)[2]);
    assert.equal(f.requests.length, 1);
  },

  async ignore_refresh(f) {
    f.ui.state.sessionId = "original-session";
    const untouched = { id: "keep-workspace" };
    f.ui.state.workspace = untouched;
    f.ui.state.runId = "keep-run";
    await load(f, [saved, { ...saved, runId: "other-run" }]);
    f.ui.loadResumable();
    buttons(f)[1].click();
    confirm(f);
    assert.equal(f.requests[2].url, "/api/runs/saved-run/ignore");
    assert.equal(f.requests[2].options.method, "POST");
    assert.equal(f.requests[2].options.headers["X-Fab-Shuffle-Session"], "original-session");
    assert.deepEqual(payload(f, 2), { confirmed: true });
    assert.ok(buttons(f).every((button) => button.disabled));
    assert.equal(f.get("saved-run-confirm-cancel").disabled, true);
    confirm(f);
    f.get("saved-run-confirm-cancel").click();
    f.get("saved-run-confirmation").handlers.cancel({ preventDefault() {} });
    assert.equal(f.get("saved-run-confirmation").open, true);
    assert.equal(f.requests.length, 3);
    await f.reply(2, { ignored: true, runId: saved.runId });
    assert.equal(f.requests[3].url, "/api/resumable");
    assert.equal(row(f).dataset.runId, "other-run");
    await f.reply(3, { runs: [{ ...saved, runId: "other-run" }] });
    await f.reply(1, { runs: [saved] });
    assert.equal(row(f).dataset.runId, "other-run", "A pre-ignore response must not resurrect the row");
    assert.equal(f.ui.state.workspace, untouched);
    assert.equal(f.ui.state.runId, "keep-run");
    assert.equal(f.streams.length, 0);
    f.ui.state.sessionId = "later-session";
    await load(f, []);
    assert.equal(f.get("resumable").hidden, true);
    assert.equal(f.get("resumable").querySelector("ul").children.length, 0);
    assert.equal(f.requests.filter((request) => request.options.method === "POST").length, 1);
  },

  async ignore_failure(f) {
    await begin(f, "ignore");
    confirm(f);
    await f.reply(1, { detail: "JournalWriteFailed: access denied" }, 500);
    assert.equal(f.get("saved-run-confirmation").open, false);
    assert.equal(row(f).dataset.runId, saved.runId);
    assert.ok(buttons(f).every((button) => !button.disabled));
    await f.reply(2, { runs: [saved] });
    assert.match(row(f).textContent, /JournalWriteFailed: access denied/);
    assert.match(f.get("alert").textContent, /try Ignore again/);
    assert.equal(f.document.activeElement, buttons(f)[1]);
  },

  async full_restart_fresh_state(f) {
    f.paired();
    const identity = { paired: true, tenant: "unchanged-tenant", app: "unchanged-app" };
    f.ui.state.identity = identity;
    f.ui.state.resumeRunId = saved.runId;
    f.ui.state.resumeReturnStage = "progress";
    f.ui.state.workspace = { id: "stale-source" };
    f.ui.watchRun(saved.runId);
    const oldStream = f.streams[0];
    f.ui.state.readiness.report = { items: [{ targetId: "old-ready-target" }] };
    const controller = new AbortController();
    f.ui.state.readiness.controller = controller;
    const connection = f.ui.addMappingRow("connection").querySelectorAll("input");
    connection[0].value = "old-source-connection";
    connection[1].value = "old-destination-connection";
    const reference = f.ui.addMappingRow("reference").querySelectorAll("input");
    ["old-source-ws", "old-source-item", "old-target-ws", "old-target-item"].forEach((value, index) => {
      reference[index].value = value;
    });
    f.get("opt-write-freeze").checked = true;
    f.get("resume-target-id").textContent = "old-target-ws";
    f.get("resume-item-ids").textContent = "old-target-item";
    f.get("target-name").value = "old-target-name";
    f.get("workspace-filter").value = "stale filter";
    const beforeVersion = f.ui.state.assessmentVersion;
    await begin(f);
    confirm(f);
    assert.deepEqual(payload(f, 1), { confirmed: true, target_workspace_id: saved.targetWorkspaceId });
    assert.ok(buttons(f).every((button) => button.disabled));
    const { capacityRequest, runsRequest } = await finishRestart(f, 1);
    assert.equal(f.ui.state.sessionId, "original-session");
    assert.equal(f.ui.state.identity, identity);
    assert.equal(f.ui.state.paired, true);
    assert.equal(f.ui.state.crossTenant, true);
    assert.equal(f.ui.state.forceRebuild, true);
    assert.deepEqual(JSON.parse(JSON.stringify(f.ui.state.workspace)), source);
    for (const key of ["runId", "resumeRunId", "capacity", "preview", "readiness", "events"]) {
      assert.equal(f.ui.state[key], null, `${key} should be cleared`);
    }
    assert.equal(f.ui.state.resumeReturnStage, "capacity");
    assert.equal(f.ui.state.assessmentPending, false);
    assert.equal(f.ui.state.mappingsChecked, false);
    assert.ok(f.ui.state.assessmentVersion > beforeVersion);
    assert.equal(oldStream.closed, true);
    assert.equal(controller.signal.aborted, true);
    assert.equal(f.get("opt-write-freeze").checked, false);
    assert.equal(f.get("opt-permissions").checked, false);
    assert.equal(f.get("opt-permissions").disabled, true);
    assert.equal(f.get("connection-mapping-rows").children.length, 0);
    assert.equal(f.get("reference-mapping-rows").children.length, 0);
    assert.equal(f.get("resume-target-id").textContent, "");
    assert.equal(f.get("resume-item-ids").textContent, "");
    assert.equal(f.get("resume-artifacts").hidden, true);
    assert.equal(f.get("target-name").value, "");
    assert.equal(f.get("target-name").disabled, false);
    assert.equal(f.get("workspace-filter").value, "");
    assert.equal(f.get("capacity-next").disabled, true);
    assert.equal(f.get("start-run").disabled, true);
    assert.equal(stage(f, "capacity").hidden, false);
    assert.equal(f.document.activeElement, f.get("capacity-title"));
    oldStream.onmessage({ data: JSON.stringify({ id: saved.runId, status: "failed", steps: [] }) });
    assert.equal(f.get("run-banner").textContent, "");
    assert.equal(f.requests.filter((request) => request.options.method === "POST").length, 1);

    await f.reply(capacityRequest, { capacities: [{ id: "fresh-capacity", displayName: "Fresh capacity" }] });
    await f.reply(runsRequest, { runs: [] });
    f.get("capacity-list").children[0].click();
    f.get("capacity-next").click();
    await f.reply(f.requests.length - 1, { workspaces: [source, { id: "different-source", displayName: "Other" }] });
    assert.equal(f.ui.state.workspace.id, source.id);
    assert.equal(f.get("workspace-list").children[0]["aria-checked"], "true");
    assert.equal(f.get("workspace-next").disabled, false);
    assert.equal(stage(f, "workspace").hidden, false);
    f.get("workspace-next").click();
    const previewRequest = f.requests.length - 1;
    assert.match(f.requests[previewRequest].url, /capacity_id=fresh-capacity/);
    assert.match(f.requests[previewRequest].url, /source_workspace_id=original-source-id/);
    await f.reply(previewRequest, {
      strategy: "rebuild", capacityName: "Fresh capacity", sourceWorkspaceName: source.displayName,
      targetWorkspaceName: "Fresh target", blockers: [], unsupportedSummary: [], dependencies: [],
      counts: [], largeSemanticModels: [],
    });
    const mappingRequest = f.requests.length - 1;
    assert.equal(f.requests[mappingRequest].url, "/api/preview/dependencies");
    assert.deepEqual(payload(f, mappingRequest).connection_mappings, {});
    assert.deepEqual(payload(f, mappingRequest).reference_mappings, []);
    await f.reply(mappingRequest - 2, { connections: [] });
    await f.reply(mappingRequest - 1, { connections: [] });
    await f.reply(mappingRequest, { blockers: [], dependencies: [], connectionAccess: null });
    f.get("opt-write-freeze").checked = true;
    f.get("opt-write-freeze").handlers.change();
    assert.equal(f.get("start-run").textContent, "Start migration");
    f.get("start-run").click();
    const createRequest = f.requests.length - 1;
    assert.equal(f.requests[createRequest].url, "/api/runs");
    assert.equal(payload(f, createRequest).capacity_id, "fresh-capacity");
    assert.equal(payload(f, createRequest).source_workspace_id, source.id);
    assert.ok(!f.requests[createRequest].options.body.includes("old-"));
    assert.ok(!f.requests.some((request) => /\/resume(?:-plan|-preview)?$/.test(request.url)));
    await f.reply(createRequest, { runId: "genuinely-new-run" });
    assert.equal(f.ui.state.runId, "genuinely-new-run");
  },

  async restart_forces_rebuild(f) {
    await begin(f);
    assert.equal(f.ui.state.forceRebuild, false);
    confirm(f);
    const { capacityRequest, runsRequest } = await finishRestart(f, 1);
    assert.equal(f.ui.state.forceRebuild, true);
    await f.reply(capacityRequest, { capacities: [{ id: "new-capacity", displayName: "New capacity" }] });
    await f.reply(runsRequest, { runs: [] });
    f.get("capacity-list").children[0].click();
    f.get("capacity-next").click();
    await f.reply(f.requests.length - 1, { workspaces: [source] });
    f.get("workspace-next").click();
    const previewRequest = f.requests.length - 1;
    const dependenciesRequest = previewRequest - 1;
    assert.match(f.requests[previewRequest].url, /^\/api\/preview\?/);
    assert.match(f.requests[dependenciesRequest].url, /^\/api\/preview\/dependencies\?/);
    for (const index of [previewRequest, dependenciesRequest]) {
      assert.equal(new URL(f.requests[index].url, "http://fixture").searchParams.get("strategy"), "rebuild");
    }
    assert.match(f.get("strategy-callout").textContent, /source will not be reassigned/);
    await f.reply(previewRequest, {
      strategy: "rebuild", capacityName: "New capacity", sourceWorkspaceName: source.displayName,
      targetWorkspaceName: "Fresh Power BI workspace", blockers: [], unsupportedSummary: [],
      counts: [{ label: "Reports", count: 1 }], largeSemanticModels: [],
    });
    await f.reply(dependenciesRequest, { blockers: [], dependencies: [], connectionAccess: null });
    assert.equal(f.get("start-run").textContent, "Start migration");
    f.get("recheck").click();
    const recheckRequest = f.requests.length - 1;
    assert.equal(new URL(f.requests[recheckRequest].url, "http://fixture").searchParams.get("strategy"), "rebuild");
    await f.reply(recheckRequest, { blockers: [], dependencies: [], connectionAccess: null });
    f.get("start-run").click();
    const startRequest = f.requests.length - 1;
    assert.equal(f.requests[startRequest].url, "/api/runs");
    assert.equal(payload(f, startRequest).strategy, "rebuild");
    assert.equal(payload(f, startRequest).source_workspace_id, source.id);
    assert.equal(payload(f, startRequest).target_workspace_name, "Fresh Power BI workspace");
    await f.reply(startRequest, { runId: "fresh-power-bi-copy" });
    assert.equal(f.ui.state.runId, "fresh-power-bi-copy");
  },

  async ordinary_preview_not_forced(f) {
    Object.assign(f.ui.state, { sessionId: "session", workspace: source, capacity: { id: "capacity" } });
    f.get("workspace-next").disabled = false;
    f.get("workspace-next").click();
    assert.equal(f.requests.length, 2);
    f.requests.forEach((request) => {
      assert.equal(new URL(request.url, "http://fixture").searchParams.has("strategy"), false);
    });
    await f.reply(1, {
      strategy: "reassign", capacityName: "Capacity", sourceWorkspaceName: source.displayName,
      targetWorkspaceName: source.displayName, blockers: [], unsupportedSummary: [],
      counts: [], largeSemanticModels: [],
    });
    await f.reply(0, { blockers: [], dependencies: [], connectionAccess: null });
    assert.equal(f.get("start-run").textContent, "Reassign workspace");
    assert.equal(f.ui.migrationBody().strategy, "reassign");
    f.get("recheck").click();
    assert.equal(new URL(f.requests[2].url, "http://fixture").searchParams.has("strategy"), false);
    await f.reply(2, { blockers: [], dependencies: [], connectionAccess: null });
  },

  async start_over_clears_forced_rebuild(f) {
    f.ui.state.forceRebuild = true;
    f.get("start-over").click();
    assert.equal(f.ui.state.forceRebuild, false);
    assert.equal(stage(f, "capacity").hidden, false);
  },

  async restart_no_destination(f) {
    await begin(f, "restart", { ...saved, targetWorkspaceId: "" });
    assert.match(f.get("saved-run-confirm-target").textContent, /No destination workspace was recorded/);
    assert.match(f.get("saved-run-confirm-description").textContent, /No destination workspace will be deleted/);
    confirm(f);
    assert.deepEqual(payload(f, 1), { confirmed: true, target_workspace_id: "" });
    const { capacityRequest, runsRequest } = await finishRestart(f, 1);
    await f.reply(capacityRequest, { capacities: [] });
    await f.reply(runsRequest, { runs: [] });
    assert.equal(f.ui.state.workspace.id, source.id);
    assert.equal(f.ui.state.runId, null);
  },

  async pending_and_blocked(f) {
    f.ui.state.sessionId = "original-session";
    await load(f, [
      { ...saved, recoveryAction: "restart_pending" },
      { ...saved, runId: "blocked", canRestart: false, restartBlockedReason: "CopyJobStillRunning: wait for job 123 to finish" },
    ]);
    assert.equal(buttons(f)[0].disabled, true);
    assert.equal(buttons(f)[2].textContent, "Finish full restart");
    assert.equal(buttons(f)[2].disabled, false);
    assert.equal(buttons(f)[1].disabled, false);
    buttons(f)[0].click();
    assert.equal(f.requests.length, 1);
    assert.match(row(f, 1).textContent, /CopyJobStillRunning: wait for job 123 to finish/);
    assert.equal(buttons(f, 1)[2].disabled, true);
    buttons(f, 1)[2].click();
    assert.equal(f.get("saved-run-confirmation").open, false);
    buttons(f)[1].click();
    assert.equal(f.get("saved-run-confirmation").open, true);
  },

  async restart_failure_retry(f) {
    const oldCapacity = { id: "old-capacity" };
    f.ui.state.capacity = oldCapacity;
    await begin(f);
    confirm(f);
    await f.reply(1, { detail: "WorkspaceDeletionFailed: Retry-After 30" }, 503);
    assert.equal(row(f).dataset.runId, saved.runId);
    assert.equal(buttons(f)[0].disabled, true);
    assert.equal(buttons(f)[2].disabled, false);
    assert.equal(f.ui.state.capacity, oldCapacity);
    await f.reply(2, { runs: [{ ...saved, recoveryAction: "restart_pending" }] });
    assert.equal(buttons(f)[0].disabled, true);
    assert.match(row(f).textContent, /WorkspaceDeletionFailed: Retry-After 30/);
    assert.equal(buttons(f)[2].textContent, "Finish full restart");
    assert.equal(f.document.activeElement, buttons(f)[2]);
    buttons(f)[2].click();
    confirm(f);
    assert.deepEqual(payload(f, 3), payload(f, 1));
    const { capacityRequest, runsRequest } = await finishRestart(f, 3);
    await f.reply(capacityRequest, { capacities: [] });
    await f.reply(runsRequest, { runs: [] });
    assert.equal(f.get("resumable").hidden, true);
  },

  async restart_failure_refresh_failure(f) {
    await begin(f);
    confirm(f);
    await f.reply(1, { detail: "ScratchCleanupFailed: AccessDenied" }, 503);
    await f.reply(2, { detail: "DiscoveryUnavailable" }, 500);
    assert.equal(row(f).dataset.runId, saved.runId);
    assert.equal(buttons(f)[0].disabled, true);
    assert.equal(buttons(f)[2].disabled, false);
    assert.equal(buttons(f)[2].textContent, "Finish full restart");
    assert.match(f.get("alert").textContent, /ScratchCleanupFailed: AccessDenied/);
    assert.match(f.get("resumable-status").textContent, /DiscoveryUnavailable/);
  },

  async stale_session_success() {
    for (const kind of ["ignore", "restart"]) {
      const f = fixture();
      await begin(f, kind);
      confirm(f);
      f.get("sign-out").click();
      await f.reply(2, {});
      Object.assign(f.ui.state, { sessionId: "new-session", runId: "new-session-run", capacity: { id: "new-capacity" } });
      f.get("alert").textContent = "New session message";
      f.get("capacity-list").textContent = "New session capacities";
      await f.reply(1, kind === "restart"
        ? { restartComplete: true, sourceWorkspace: source }
        : { ignored: true, runId: saved.runId });
      assert.equal(f.ui.state.sessionId, "new-session");
      assert.equal(f.ui.state.runId, "new-session-run");
      assert.equal(f.ui.state.capacity.id, "new-capacity");
      assert.equal(f.get("alert").textContent, "New session message");
      assert.equal(f.get("capacity-list").textContent, "New session capacities");
      assert.equal(f.requests.length, 3);
      assert.equal(f.get("saved-run-confirmation").open, false);
    }
  },

  async stale_session_failure() {
    for (const kind of ["ignore", "restart"]) {
      const f = fixture();
      await begin(f, kind);
      confirm(f);
      f.get("sign-out").click();
      await f.reply(2, {});
      f.ui.state.sessionId = "new-session";
      f.get("alert").textContent = "New session message";
      await f.reply(1, { detail: "Old session error" }, 500);
      assert.equal(f.get("alert").textContent, "New session message");
      assert.equal(f.requests.length, 3);
    }
  },

  async logout_during_action(f) {
    await begin(f);
    f.ui.state.capacity = { id: "unchanged-until-logout" };
    confirm(f);
    f.get("sign-out").click();
    await f.reply(1, { restartComplete: true, sourceWorkspace: source });
    assert.equal(f.ui.state.capacity.id, "unchanged-until-logout");
    assert.equal(f.requests.length, 3);
    await f.reply(2, {});
    assert.equal(f.ui.state.sessionId, null);
    assert.equal(stage(f, "login").hidden, false);
  },

  async stale_discovery_after_restart(f) {
    await begin(f);
    confirm(f);
    const { capacityRequest, runsRequest } = await finishRestart(f, 1);
    f.get("sign-out").click();
    await f.reply(f.requests.length - 1, {});
    f.ui.state.sessionId = "new-session";
    f.get("capacity-list").textContent = "New session capacities";
    f.get("resumable").querySelector("ul").textContent = "New session migrations";
    await f.reply(capacityRequest, { capacities: [{ id: "old-capacity", displayName: "Must not show" }] });
    await f.reply(runsRequest, { runs: [saved] });
    assert.equal(f.get("capacity-list").textContent, "New session capacities");
    assert.equal(f.get("resumable").querySelector("ul").textContent, "New session migrations");
  },

  async stale_preview_after_restart(f) {
    f.paired();
    await begin(f);
    f.get("saved-run-confirm-cancel").click();
    f.get("workspace-next").disabled = false;
    f.get("workspace-next").click();
    const oldPreviewRequest = f.requests.length - 1;
    buttons(f)[2].click();
    confirm(f);
    const restartRequest = f.requests.length - 1;
    const { capacityRequest, runsRequest } = await finishRestart(f, restartRequest);
    await f.reply(oldPreviewRequest, { targetWorkspaceName: "Stale preview" });
    assert.equal(f.ui.state.preview, null);
    assert.equal(stage(f, "capacity").hidden, false);
    assert.equal(f.get("workspace-next").textContent, "Continue");
    await f.reply(capacityRequest, { capacities: [] });
    await f.reply(runsRequest, { runs: [] });
  },

  async stale_workspace_discovery(f) {
    f.paired();
    await begin(f);
    f.get("saved-run-confirm-cancel").click();
    f.get("capacity-next").disabled = false;
    f.get("capacity-next").click();
    const workspaceRequest = f.requests.length - 1;
    buttons(f)[2].click();
    confirm(f);
    const { capacityRequest, runsRequest } = await finishRestart(f, f.requests.length - 1);
    await f.reply(workspaceRequest, { workspaces: [{ id: "previous-source", displayName: "Previous source" }] });
    assert.equal(f.ui.state.workspace.id, source.id);
    assert.equal(stage(f, "capacity").hidden, false);
    assert.equal(f.get("capacity-next").disabled, true);
    assert.equal(f.get("capacity-next").textContent, "Continue");
    await f.reply(capacityRequest, { capacities: [] });
    await f.reply(runsRequest, { runs: [] });
  },

  async stale_resume_after_restart() {
    for (const paired of [false, true]) {
      const f = fixture();
      if (paired) f.paired();
      f.ui.state.sessionId = "original-session";
      await load(f, [{ ...saved, runId: "other-run" }, saved]);
      buttons(f)[0].click();
      const resumeRequest = f.requests.length - 1;
      buttons(f, 1)[2].click();
      confirm(f);
      const { capacityRequest, runsRequest } = await finishRestart(f, f.requests.length - 1);
      await f.reply(resumeRequest, paired ? { plan: {} } : { runId: "other-resumed-run" });
      assert.equal(f.ui.state.runId, null);
      assert.equal(f.ui.state.resumeRunId, null);
      assert.equal(f.ui.state.preview, null);
      assert.equal(stage(f, "capacity").hidden, false);
      await f.reply(capacityRequest, { capacities: [] });
      await f.reply(runsRequest, { runs: [] });
    }
  },

  async stale_connections_after_restart(f) {
    f.paired();
    await begin(f);
    f.ui.loadConnectionOptions();
    const sourceRequest = f.requests.length - 2;
    const targetRequest = f.requests.length - 1;
    confirm(f);
    const { capacityRequest, runsRequest } = await finishRestart(f, f.requests.length - 1);
    await f.reply(sourceRequest, { connections: [{ id: "old-source-connection" }] });
    await f.reply(targetRequest, { connections: [{ id: "old-target-connection" }] });
    assert.equal(f.get("source-connection-options").children.length, 0);
    assert.equal(f.get("target-connection-options").children.length, 0);
    assert.equal(f.get("connection-options-status").textContent, "");
    await f.reply(capacityRequest, { capacities: [] });
    await f.reply(runsRequest, { runs: [] });
  },

  async missing_source_reselection(f) {
    await begin(f);
    confirm(f);
    const { capacityRequest, runsRequest } = await finishRestart(f, 1);
    await f.reply(capacityRequest, { capacities: [{ id: "new-capacity" }] });
    await f.reply(runsRequest, { runs: [] });
    f.get("capacity-list").children[0].click();
    f.get("capacity-next").click();
    await f.reply(f.requests.length - 1, { workspaces: [{ id: "available-source", displayName: "Available source" }] });
    assert.equal(f.ui.state.workspace, null);
    assert.equal(f.get("workspace-next").disabled, true);
    f.get("workspace-list").children[0].click();
    assert.equal(f.ui.state.workspace.id, "available-source");
    assert.equal(f.get("workspace-next").disabled, false);
  },

  async resume_locks_row(f) {
    f.ui.state.sessionId = "original-session";
    await load(f);
    assert.equal(buttons(f)[0].textContent, "Resume");
    buttons(f)[0].click();
    assert.equal(f.requests[1].url, "/api/runs/saved-run/resume");
    assert.ok(buttons(f).every((button) => button.disabled));
    buttons(f)[1].click();
    buttons(f)[2].click();
    assert.equal(f.get("saved-run-confirmation").open, false);
    await f.reply(1, { detail: "RunBusy: wait for the current worker" }, 409);
    assert.ok(buttons(f).every((button) => !button.disabled));
  },

  async saved_review_back(f) {
    f.paired();
    f.ui.state.resumeRunId = saved.runId;
    f.ui.state.resumeReturnStage = "capacity";
    f.ui.renderReview();
    f.get("resume-back").click();
    assert.equal(f.ui.state.resumeRunId, null);
    assert.equal(stage(f, "capacity").hidden, false);
    assert.equal(f.requests.length, 0);
  },
};

const name = process.argv[2];
assert.ok(scenarios[name], `Unknown saved migration UI check: ${name}`);
scenarios[name](fixture()).then(() => console.log(`Passed ${name}`)).catch((error) => {
  console.error(error); process.exitCode = 1;
});
