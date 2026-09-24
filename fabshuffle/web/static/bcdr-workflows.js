/* Operator journeys select existing guarded actions; the backend remains authoritative. */

const bcdrJourneys = {
  overview: { title: "Manage standby", description: "Choose the job you need to do. Loading status may resume the metadata capacity.", stages: [] },
  setup: { title: "Set up standby", description: "Choose workspaces or a name rule. Your saved recovery settings are reused.",
    stages: [["select", "Choose workspaces"], ["schedule", "Scheduled sync"]] },
  settings: { title: "Recovery settings", description: "One-time environment configuration and advanced controls. Ordinary standby setup only selects workspaces.",
    stages: [["environment", "Environment"], ["routing", "Default destination"], ["protection", "Data protection"], ["advanced", "Advanced mappings"]] },
  test: { title: "DR Test", description: "Exercise the existing standby with recovery owners only. Production stays primary.",
    stages: [["review", "Review standby"], ["exercise", "Exercise & validate"], ["end", "End test"]] },
  incident: { title: "I'm currently down", description: "Recover from saved metadata. This path does not discover or capture the primary.",
    stages: [["assess", "Assess recovery"], ["prepare", "Prepare & enable"], ["cutover", "Approve cutover"], ["return", "Return to primary"]] },
};

function bcdrWorkflowFacts() {
  const workflow = bcdr.latest.details?.workflow;
  const sync = bcdr.latest.details?.sync_summary;
  const test = bcdr.latest.details?.dr_test || workflow?.test;
  const mode = bcdr.latest.mode;
  return {
    workflow, sync, test, mode,
    baseline: sync?.metadata_ready ?? workflow?.metadata_baseline_ready ?? null,
    testActive: ["testing", "ending_test"].includes(mode),
    incidentActive: ["enabling_recovery", "active_recovery", "failing_back", "rearming"].includes(mode),
    observed: !!mode,
  };
}

function bcdrJourney(journey, stage, focus = true) {
  if (journey === "setup" && ["home", "sync"].includes(stage)) stage = "select";
  if (journey === "setup" && stage === "protection") journey = "settings";
  if (!bcdrJourneys[journey]) journey = "overview";
  bcdr.journey = journey;
  const definition = bcdrJourneys[journey];
  bcdr.journeyStage = definition.stages.some(([value]) => value === stage) ? stage : definition.stages[0]?.[0];
  if (globalThis.history && globalThis.location) {
    globalThis.history.replaceState(null, "", `#bcdr/${journey}${bcdr.journeyStage ? `/${bcdr.journeyStage}` : ""}`);
  }
  bcdrRefreshJourney();
  if (journey === "setup" && bcdr.journeyStage === "select" && !bcdr.scope?.loaded) bcdrLoadStandbyScope(true);
  if (journey === "settings" && bcdr.journeyStage === "routing") bcdrLoadStandbyScope(false);
  if (focus) $("#bcdr-journey-title")?.focus({ preventScroll: true });
}

function bcdrBuildJourneyShell(content, commands) {
  $("#bcdr-open").hidden = true;
  bcdr.journeyCommands = commands;
  bcdr.commandSections = new Map();
  bcdr.journey = "overview";
  bcdr.journeyStage = null;
  const route = globalThis.location?.hash.split("/");
  if (route?.[0] === "#bcdr" && bcdrJourneys[route[1]]) {
    bcdr.journey = route[1];
    const stages = bcdrJourneys[route[1]].stages;
    bcdr.journeyStage = stages.some(([key]) => key === route[2]) ? route[2] : stages[0]?.[0];
  }
  bcdr.resultJourney = null;
  bcdr.standbyTargets = null;
  const navigation = bcdrElement("nav", undefined, "bcdr-journey-nav");
  navigation.setAttribute("aria-label", "Standby and recovery workflows");
  for (const [key, label] of [["overview", "Overview"], ["setup", "Set up standby"], ["test", "DR Test"], ["incident", "I'm currently down"]]) {
    const button = bcdrElement("button", label, "secondary");
    button.type = "button";
    button.dataset.journey = key;
    button.addEventListener("click", () => bcdrJourney(key));
    navigation.appendChild(button);
  }
  const settings = bcdrElement("button", "Settings", "link-button");
  settings.type = "button";
  settings.dataset.journey = "settings";
  settings.addEventListener("click", () => bcdrJourney("settings"));
  navigation.appendChild(settings);
  content.appendChild(navigation);
  const heading = bcdrElement("h3", "Manage standby");
  heading.id = "bcdr-journey-title";
  heading.tabIndex = -1;
  content.appendChild(heading);
  const description = bcdrElement("p", "", "hint");
  description.id = "bcdr-journey-description";
  content.appendChild(description);
  const status = commands.find((command) => command.name === "status");
  if (status) {
    const load = bcdrElement("button", "Load recovery status", "secondary");
    load.type = "button";
    const statusHost = bcdrElement("div", undefined, "bcdr-status-action");
    statusHost.id = "bcdr-status-action";
    statusHost.appendChild(load);
    content.appendChild(statusHost);
    load.addEventListener("click", () => bcdrSubmit(status, undefined, load));
    statusHost.appendChild(bcdrElement("p",
      "Reads the saved catalog using the recovery identity. May resume the metadata capacity; does not capture the primary.", "hint"));
  }
  const facts = bcdrElement("section");
  facts.id = "bcdr-workflow-facts";
  facts.setAttribute("aria-label", "Observed standby state");
  content.appendChild(facts);
  const stages = bcdrElement("nav", undefined, "bcdr-stage-nav");
  stages.id = "bcdr-stage-nav";
  stages.setAttribute("aria-label", "Current workflow steps");
  content.appendChild(stages);
  const help = bcdrElement("section", undefined, "bcdr-journey-help");
  help.id = "bcdr-journey-help";
  content.appendChild(help);
  content.appendChild(bcdrBuildStandbyScope());
}

function bcdrJourneyButton(container, label, journey, stage, primary = false) {
  const button = bcdrElement("button", label, primary ? "primary" : "secondary");
  button.type = "button";
  button.addEventListener("click", () => bcdrJourney(journey, stage));
  container.appendChild(button);
}

function bcdrRefreshJourney() {
  const facts = $("#bcdr-workflow-facts");
  if (!facts) return;
  const current = bcdrJourneys[bcdr.journey] || bcdrJourneys.overview;
  const state = bcdrWorkflowFacts();
  const statusAction = $("#bcdr-status-action");
  if (statusAction) statusAction.hidden = bcdr.journey === "setup" && bcdr.journeyStage === "select";
  facts.hidden = !(bcdr.journey === "overview" ||
    (bcdr.journey === "test" && bcdr.journeyStage === "review") ||
    (bcdr.journey === "incident" && bcdr.journeyStage === "assess"));
  const scheduling = bcdr.journey === "setup" && bcdr.journeyStage === "schedule";
  $("#bcdr-journey-title").textContent = scheduling ? "Schedule standby sync" : current.title;
  $("#bcdr-journey-description").textContent = scheduling ?
    "Prepare scheduling instructions for the reviewed scope. No job is created or enabled here." : current.description;
  for (const button of document.querySelectorAll(".bcdr-journey-nav button")) {
    button.setAttribute("aria-current", button.dataset.journey === bcdr.journey ? "page" : "false");
  }
  facts.replaceChildren();
  const list = bcdrElement("dl", undefined, "summary");
  const last = state.sync || state.workflow?.last_sync;
  for (const [label, value] of [
    ["Metadata home", bcdr.savedSetup?.warehouseName || "Not configured or not yet observed"],
    ["Observed state", state.mode ? bcdrLabel(state.mode) : "Unknown - load recovery status"],
    ["Metadata baseline", state.baseline == null ? "Load status to check" :
      state.baseline ? "Established" : "Initial sync or repair required"],
    ["Latest metadata sync", last?.completed_at || "No completed run observed"],
    ["Data recovery", state.workflow || state.sync || bcdr.latest.groups?.length ?
      `${state.sync?.data_gap_groups?.length ?? state.workflow?.recovery_gap_groups?.length ??
        bcdr.latest.groups?.filter((group) => !group.data_ready).length ?? 0} group(s) with unresolved data readiness; inspect evidence` :
      "Not yet checked"],
    ["Automatic sync", state.testActive ? "On hold - DR Test active" : state.incidentActive ?
      "On hold - incident recovery active" : "Deployment not verified; inspect the last scheduled result"],
    ["Last scheduled metadata sync", state.workflow?.last_scheduled_sync?.completed_at || "No completed scheduled run observed"],
  ]) {
    list.appendChild(bcdrElement("dt", label));
    list.appendChild(bcdrElement("dd", value, "v"));
  }
  facts.appendChild(list);
  if (state.workflow?.pending_operations || state.workflow?.controller_busy) {
    facts.appendChild(bcdrElement("p", "Interrupted or owned work needs reconciliation before a new action.", "alert"));
  }
  const stages = $("#bcdr-stage-nav");
  stages.replaceChildren();
  for (const [key, label] of current.stages) {
    const button = bcdrElement("button", label, "secondary");
    button.type = "button";
    button.setAttribute("aria-current", key === bcdr.journeyStage ? "step" : "false");
    button.addEventListener("click", () => bcdrJourney(bcdr.journey, key));
    stages.appendChild(button);
  }
  stages.hidden = !current.stages.length || (bcdr.journey === "setup" && bcdr.journeyStage === "select");
  const help = $("#bcdr-journey-help");
  help.replaceChildren();
  let visible = [];
  if (bcdr.journey === "overview") {
    help.appendChild(bcdrElement("p", state.testActive ? "A DR Test is open. Continue it or end it safely before synchronization." :
      state.incidentActive ? "Recovery is active. Continue the incident workflow; do not restart setup." :
      state.baseline ? "Your metadata standby exists. Review data gaps separately from automatic synchronization." :
      "Start or resume setup to establish standby. If setup already exists, load status before choosing the next step."));
    if (state.testActive) bcdrJourneyButton(help, "Resume DR Test", "test", "exercise", true);
    else if (["failing_back", "rearming"].includes(state.mode)) {
      bcdrJourneyButton(help, "Continue return to primary", "incident", "return", true);
    } else if (state.incidentActive) bcdrJourneyButton(help, "Continue recovery", "incident", "prepare", true);
    else {
      bcdrJourneyButton(help, state.baseline ? "Choose workspaces to sync" : "Set up standby", "setup", "select", true);
      if (state.baseline) bcdrJourneyButton(help, "Set up scheduled sync", "setup", "schedule");
    }
    bcdrJourneyButton(help, "Run a DR Test", "test", "review");
    bcdrJourneyButton(help, "I'm currently down", "incident", "assess");
  } else if (bcdr.journey === "setup") {
    visible = bcdr.journeyStage === "schedule" ? ["schedule-guide"] : [];
    if (state.testActive || state.incidentActive) {
      help.appendChild(bcdrElement("p", "Setup and source synchronization are unavailable during a test or incident. Return to that workflow.", "alert"));
      visible = [];
    } else if (bcdr.journeyStage === "schedule" &&
      (!state.baseline || state.workflow?.schedule_eligible === false)) {
      visible = [];
      help.appendChild(bcdrElement("p",
        "Load status, complete the metadata baseline and reconcile active work before preparing the schedule.", "hint"));
    }
    if (bcdr.journeyStage === "select" && state.baseline) {
      bcdrJourneyButton(help, "Set up scheduled sync", "setup", "schedule");
    }
    if (bcdr.journeyStage === "schedule") {
      help.appendChild(bcdrElement("p", "Both controller and job need the same identity, shared files, remote lease and production image. Local-only Docker does not establish unattended scheduling.", "hint"));
      bcdrJourneyButton(help, "Skip for now - manage standby", "overview");
    }
  } else if (bcdr.journey === "settings") {
    visible = {
      environment: ["setup"], routing: ["configure-standby-defaults"],
      protection: ["configure-protection"], advanced: ["plan", "synchronize"],
    }[bcdr.journeyStage] || [];
    help.appendChild(bcdrElement("p",
      "These are administrator settings, reused by the workspace-only setup flow. No infrastructure is chosen implicitly."));
    if (bcdr.journeyStage === "routing") {
      const status = bcdrElement("p", bcdr.scope?.settingsStatus || "Loading approved recovery capacities...", "hint");
      status.id = "bcdr-settings-load-status";
      status.setAttribute("role", "status");
      help.appendChild(status);
    }
    if (state.testActive || state.incidentActive) {
      visible = [];
      help.appendChild(bcdrElement("p", "Finish the active test or recovery before changing settings.", "alert"));
    }
    if (bcdr.savedSetup?.warehousePhase === "ready") {
      bcdrJourneyButton(help, "Choose workspaces to protect", "setup", "select", true);
    }
  } else if (bcdr.journey === "test") {
    help.appendChild(bcdrElement("p",
      "This uses the existing standby and configured recovery owners. No primary pause, production ACL replay, consumer routing or writer handoff. Automatic sync is held until End test completes."));
    if (state.incidentActive) {
      help.appendChild(bcdrElement("p", "Production recovery is already active. A DR Test cannot start here.", "alert"));
      bcdrJourneyButton(help, "Continue incident recovery", "incident", "prepare");
    } else if (bcdr.journeyStage === "review") {
      help.appendChild(bcdrElement("p", "Load recovery status, review the captured recovery point and gaps, then choose the groups to test. No fresh source capture is required."));
      bcdrJourneyButton(help, state.testActive ? "Continue recorded test" : "Choose test groups", "test", "exercise", true);
    } else if (bcdr.journeyStage === "exercise") {
      visible = state.mode === "ending_test" ? ["end-dr-test"] :
        state.testActive ? ["continue-dr-test"] : ["start-dr-test"];
      if (!state.testActive && state.workflow?.test_eligible === false) {
        visible = [];
        help.appendChild(bcdrElement("p", "Complete metadata synchronization and reconcile pending work before starting a test.", "hint"));
      }
      if (!state.observed) help.appendChild(bcdrElement("p", "Load recovery status first to pin the recovery point and groups.", "hint"));
      if (state.testActive) bcdrJourneyButton(help, "End test", "test", "end");
    } else if (bcdr.journeyStage === "end") {
      visible = state.testActive ? ["end-dr-test"] : [];
      help.appendChild(bcdrElement("p", "End test verifies the shared standby and retains its resources. Uncertain operations or changed targets must be reconciled; closing this page does not end the test."));
    }
    if (state.test) {
      const summary = bcdrElement("section", undefined, "bcdr-test-summary");
      summary.appendChild(bcdrElement("h4", `Recorded test: ${bcdrLabel(state.test.phase)}`));
      summary.appendChild(bcdrElement("p", `Started: ${state.test.started_at || "recorded"}. Production cutover: no.`));
      const outcomes = bcdrElement("ul");
      for (const item of state.test.results || []) {
        outcomes.appendChild(bcdrElement("li", `${bcdrGroupName(item.group_id)}: ${bcdrLabel(item.outcome)}. ${(item.messages || []).join(" ")}`));
      }
      summary.appendChild(outcomes);
      summary.appendChild(bcdrElement("p", "Owner test evidence is not production-user readiness or actual regional-failover qualification.", "hint"));
      help.appendChild(summary);
    }
  } else if (bcdr.journey === "incident") {
    if (state.testActive) {
      help.appendChild(bcdrElement("p", "A DR Test is active. Reconcile its pending work and End test safely before entering incident recovery. This handoff does not require primary discovery.", "alert"));
      bcdrJourneyButton(help, "Review and end the DR Test", "test", "end");
    } else {
      visible = { assess: [], prepare: ["configure-replica", "enable-recovery"],
        cutover: ["cutover"], return: ["plan-failback", "execute-failback", "cutback", "rearm"] }[bcdr.journeyStage] || [];
      help.appendChild(bcdrElement("p", {
        assess: "Load the existing catalog with the recovery identity. Review recovery point, selected groups and data gaps. No healthy-source discovery or new setup runs here.",
        prepare: "Prepare protected data and explicitly enable eligible recovery access. Missing data or unverified references are blockers, not successful recovery.",
        cutover: "Approve cutover only with current data, reference and identity evidence and an explicit primary-writer fence. Workload activation and consumer routing remain separate operator actions.",
        return: "Use this only when the primary is available. Plan failback explicitly checks the primary, then reconcile return targets, approve cutback and rearm.",
      }[bcdr.journeyStage]));
      if (bcdr.journeyStage === "assess") bcdrJourneyButton(help, "Review recovery preparation", "incident", "prepare", true);
      if (bcdr.journeyStage === "return" && !state.incidentActive) {
        visible = [];
        help.appendChild(bcdrElement("p", "Return to primary becomes relevant after real recovery, not after a DR Test.", "hint"));
      }
    }
  }
  if (state.workflow?.pending_operations || state.workflow?.controller_busy) visible.push("reconcile-operation");
  for (const [name, section] of bcdr.commandSections || []) {
    section.hidden = !visible.includes(name);
    section.open = visible.length === 1 || ["setup", "synchronize", "schedule-guide", "start-dr-test", "continue-dr-test", "end-dr-test", "enable-recovery"].includes(name);
  }
  const preparation = $("#bcdr-preparation");
  if (preparation) preparation.hidden = bcdr.journey !== "settings" || !["environment", "advanced"].includes(bcdr.journeyStage) ||
    state.testActive || state.incidentActive;
  const scope = $("#bcdr-standby-scope");
  if (scope) scope.hidden = bcdr.journey !== "setup" || bcdr.journeyStage !== "select" ||
    state.testActive || state.incidentActive;
  if (bcdr.scope) bcdr.scope.start.textContent = state.baseline ? "Update standby" : "Create standby";
  const result = $("#bcdr-result");
  if (result) result.hidden = bcdr.resultJourney !== bcdr.journey;
}

function bcdrBuildStandbyScope() {
  const section = bcdrElement("section", undefined, "bcdr-scope");
  section.id = "bcdr-standby-scope";
  section.setAttribute("role", "form");
  section.setAttribute("aria-label", "Workspace selection");
  const status = bcdrElement("p", "Loading your saved recovery settings...", "hint");
  status.setAttribute("role", "status");
  status.setAttribute("aria-live", "polite");
  section.appendChild(status);
  const modeLabel = bcdrElement("label", "Selection method");
  const mode = bcdrElement("select");
  mode.name = "selection_mode";
  for (const [value, text] of [["workspaces", "Choose from the list"], ["pattern", "Match workspace names"]]) {
    const option = bcdrElement("option", text);
    option.value = value;
    mode.appendChild(option);
  }
  mode.value = "workspaces";
  modeLabel.appendChild(mode);
  section.appendChild(modeLabel);
  const entries = bcdrElement("fieldset", undefined, "bcdr-scope-list");
  entries.appendChild(bcdrElement("legend", "Workspaces"));
  section.appendChild(entries);
  const patternLabel = bcdrElement("label", "Workspace name contains");
  const pattern = bcdrElement("input");
  pattern.type = "text";
  pattern.name = "name_pattern";
  pattern.maxLength = 256;
  pattern.placeholder = "For example: sales";
  patternLabel.appendChild(pattern);
  patternLabel.appendChild(bcdrElement("span",
    "Contains this text, ignoring case. No wildcards or regular expressions.", "hint"));
  patternLabel.hidden = true;
  section.appendChild(patternLabel);
  const preview = bcdrElement("section", undefined, "bcdr-scope-preview");
  preview.setAttribute("aria-live", "polite");
  section.appendChild(preview);
  const actions = bcdrElement("div", undefined, "actions");
  const review = bcdrElement("button", "Review selection", "primary");
  review.type = "button";
  review.disabled = true;
  const start = bcdrElement("button", "Create standby", "primary");
  start.type = "button";
  start.hidden = true;
  const reload = bcdrElement("button", "Reload workspaces", "secondary");
  reload.type = "button";
  actions.appendChild(review);
  actions.appendChild(start);
  actions.appendChild(reload);
  section.appendChild(actions);
  const settings = bcdrElement("button", "Recovery environment settings", "link-button");
  settings.type = "button";
  settings.addEventListener("click", () => bcdrJourney("settings", "environment"));
  section.appendChild(settings);
  const scope = { section, status, mode, entries, pattern, patternLabel, preview, review, start, reload,
    selected: new Set(), loaded: false, loading: false, revision: 0, dirty: false, reviewed: null };
  bcdr.scope = scope;
  const change = () => {
    scope.dirty = true;
    ++scope.revision;
    scope.reviewed = null;
    preview.replaceChildren();
    start.hidden = true;
    review.className = "primary";
    const feedback = actions.querySelector(".bcdr-feedback");
    if (feedback) {
      feedback.querySelector(".bcdr-action-error").hidden = true;
      feedback.querySelector(".bcdr-action-progress").textContent = "";
      feedback.querySelector(".bcdr-action-warnings").replaceChildren();
      feedback.querySelector(".bcdr-action-warnings").hidden = true;
    }
    entries.hidden = mode.value !== "workspaces";
    patternLabel.hidden = mode.value !== "pattern";
  };
  mode.addEventListener("change", change);
  pattern.addEventListener("input", change);
  pattern.addEventListener("keydown", (event) => {
    if (event.key === "Enter") { event.preventDefault(); review.click(); }
  });
  scope.change = change;
  reload.addEventListener("click", () => bcdrLoadStandbyScope(true));
  review.addEventListener("click", () => bcdrReviewStandbyScope());
  start.addEventListener("click", async () => {
    if (!scope.reviewed || scope.loading || !scope.loaded || bcdr.pending) return;
    const result = await bcdrSubmit({ name: "create-standby", label: start.textContent, method: "POST", path: "/api/bcdr/create-standby",
      confirmation: "Apply this reviewed workspace selection using the saved recovery settings." },
    { ...scope.reviewed.request, expected_configuration: scope.reviewed.configuration }, start);
    if (scope !== bcdr.scope) return;
    scope.reviewed = null;
    start.hidden = true;
    review.className = result?.details?.sync_summary?.metadata_ready ? "secondary" : "primary";
    const next = result?.details?.sync_summary?.metadata_ready ?
      scope.section.querySelectorAll("button").find((button) => button.textContent === "Set up scheduled sync") : review;
    next?.focus({ preventScroll: true });
  });
  return section;
}

async function bcdrLoadStandbyScope(includeWorkspaces) {
  const scope = bcdr.scope;
  if (!scope || bcdr.pending || !bcdrCurrentSession(state.sessionId)) return;
  if (scope.loading) { scope.queuedRead = includeWorkspaces; return; }
  const stateFacts = bcdrWorkflowFacts();
  if (stateFacts.testActive || stateFacts.incidentActive) return;
  const session = state.sessionId;
  scope.loading = true;
  scope.review.disabled = true;
  scope.start.disabled = true;
  if (includeWorkspaces) {
    ++scope.revision;
    scope.reviewed = null;
    scope.start.hidden = true;
    scope.preview.replaceChildren();
  }
  scope.reload.disabled = true;
  const status = (message) => {
    if (includeWorkspaces) scope.status.textContent = message;
    else {
      scope.settingsStatus = message;
      const output = $("#bcdr-settings-load-status");
      if (output) output.textContent = message;
    }
  };
  status(includeWorkspaces ?
    "Loading saved recovery settings and workspace names. This may resume the metadata capacity." :
    "Loading approved recovery capacities...");
  try {
    const result = await api(`/api/bcdr/standby-scope?include_workspaces=${includeWorkspaces ? "true" : "false"}`);
    if (!bcdrCurrentSession(session) || scope !== bcdr.scope) return;
    if (includeWorkspaces && (bcdr.journey !== "setup" || bcdr.journeyStage !== "select")) return;
    if (result.needs_configuration) {
      status(result.message);
      scope.loaded = false;
      scope.reviewed = null;
      scope.start.hidden = true;
      scope.review.disabled = true;
      if (includeWorkspaces) {
        scope.mode.parentElement.hidden = true;
        scope.entries.hidden = true;
        scope.patternLabel.hidden = true;
        scope.preview.hidden = true;
        scope.review.hidden = true;
        scope.reload.textContent = "Check configuration";
      }
      return;
    }
    const data = result.details?.standby_scope;
    if (!data || !Array.isArray(data.target_capacities) || !Array.isArray(data.workspaces)) {
      throw new Error("The saved configuration response is incomplete");
    }
    bcdr.standbyTargets = data.target_capacities;
    bcdr.bindings.forEach((refresh) => refresh("discovery"));
    if (!includeWorkspaces) {
      status("Choose a default only for source capacities without an existing approved route.");
      return;
    }
    scope.loaded = true;
    scope.review.disabled = false;
    scope.mode.parentElement.hidden = false;
    scope.preview.hidden = false;
    scope.review.hidden = false;
    scope.reload.textContent = "Reload workspaces";
    if (!scope.dirty && data.saved_selection) {
      scope.mode.value = data.saved_selection.selection_mode;
      scope.selected = new Set(data.saved_selection.workspace_ids || []);
      scope.pattern.value = data.saved_selection.name_pattern || "";
    }
    scope.entries.replaceChildren(bcdrElement("legend", "Workspaces"));
    const entries = [...data.workspaces];
    for (const id of scope.selected) {
      if (!entries.some((entry) => entry.id === id)) entries.push({ id, displayName: "Unavailable saved workspace" });
    }
    const text = (row) => [row.displayName || "Unnamed workspace", row.capacityRegion, row.description]
      .filter(Boolean).join(" - ");
    const labels = new Map();
    entries.forEach((entry) => labels.set(text(entry), (labels.get(text(entry)) || 0) + 1));
    for (const entry of entries) {
      const ambiguous = labels.get(text(entry)) > 1;
      const label = bcdrElement("label", undefined, "bcdr-check");
      const checkbox = bcdrElement("input");
      checkbox.type = "checkbox";
      checkbox.value = entry.id;
      checkbox.checked = scope.selected.has(entry.id);
      checkbox.disabled = ambiguous && !checkbox.checked;
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) scope.selected.add(entry.id); else scope.selected.delete(entry.id);
        if (ambiguous && !checkbox.checked) checkbox.disabled = true;
        scope.change();
      });
      label.appendChild(checkbox);
      label.appendChild(bcdrElement("span", text(entry) + (ambiguous ?
        " (same name: use a rule to include all matches, or disambiguate names/descriptions in Fabric)" : "")));
      scope.entries.appendChild(label);
    }
    scope.entries.hidden = scope.mode.value !== "workspaces";
    scope.patternLabel.hidden = scope.mode.value !== "pattern";
    scope.status.textContent = data.workspaces.length ?
      `${data.workspaces.length} workspace${data.workspaces.length === 1 ? "" : "s"} available in your configured source scope.` :
      "No accessible workspaces were returned in the configured source scope. Check source access and settings.";
  } catch (error) {
    if (bcdrCurrentSession(session) && scope === bcdr.scope) {
      status(`Could not load saved configuration: ${error.message}`);
      if (includeWorkspaces) {
        scope.loaded = false;
        scope.review.disabled = true;
      }
    }
  } finally {
    if (scope === bcdr.scope) {
      scope.loading = false;
      scope.reload.disabled = false;
      scope.start.disabled = false;
      scope.review.disabled = !scope.loaded || !!scope.reviewing;
      const queued = scope.queuedRead;
      scope.queuedRead = undefined;
      if (queued !== undefined && queued !== includeWorkspaces && bcdrCurrentSession(session) &&
        (queued ? bcdr.journey === "setup" && bcdr.journeyStage === "select" :
          bcdr.journey === "settings" && bcdr.journeyStage === "routing")) {
        bcdrLoadStandbyScope(queued);
      }
    }
  }
}

async function bcdrReviewStandbyScope() {
  const scope = bcdr.scope;
  if (!scope || !scope.loaded || scope.loading || scope.reviewing || bcdr.pending ||
    !bcdrCurrentSession(state.sessionId)) return;
  const request = scope.mode.value === "pattern" ?
    { selection_mode: "pattern", name_pattern: scope.pattern.value.trim() } :
    { selection_mode: "workspaces", workspace_ids: Array.from(scope.selected) };
  if (request.selection_mode === "pattern" ? !request.name_pattern : !request.workspace_ids.length) {
    bcdrError("Choose at least one workspace or enter a name rule.", scope.review);
    return;
  }
  const revision = scope.revision;
  const session = state.sessionId;
  scope.review.disabled = true;
  scope.reviewing = true;
  scope.reviewed = null;
  scope.start.hidden = true;
  bcdrError("", scope.review);
  scope.preview.textContent = "Checking the matching workspaces and saved routing...";
  try {
    const result = await api("/api/bcdr/preview-standby", { method: "POST", body: request });
    if (!bcdrCurrentSession(session) || scope !== bcdr.scope || revision !== scope.revision ||
      bcdr.journey !== "setup" || bcdr.journeyStage !== "select") return;
    const preview = result.details?.standby_preview;
    if (!preview?.configuration || !Array.isArray(preview.workspaces)) throw new Error("Selection preview is incomplete");
    scope.reviewed = { request, configuration: preview.configuration };
    scope.review.className = "secondary";
    scope.preview.replaceChildren();
    scope.preview.appendChild(bcdrElement("h4",
      `${preview.workspaces.length} workspace${preview.workspaces.length === 1 ? "" : "s"} to protect`));
    const names = bcdrElement("ul");
    preview.workspaces.forEach((entry) => names.appendChild(bcdrElement("li", entry.displayName)));
    scope.preview.appendChild(names);
    const targets = (preview.target_capacity_ids || []).map((id) =>
      bcdr.standbyTargets?.find((entry) => entry.id === id)?.displayName || "Configured recovery capacity");
    if (targets.length) scope.preview.appendChild(bcdrElement("p", `Recovery destination: ${targets.join(", ")}.`));
    scope.preview.appendChild(bcdrElement("p", preview.message));
    if (request.selection_mode === "pattern") scope.preview.appendChild(bcdrElement("p",
      "Future matching workspaces in the configured source scope are included on subsequent syncs.", "hint"));
    scope.start.hidden = false;
  } catch (error) {
    if (bcdrCurrentSession(session) && scope === bcdr.scope && revision === scope.revision) {
      scope.preview.replaceChildren();
      bcdrError(error.message, scope.review);
    }
  } finally {
    if (scope === bcdr.scope) {
      scope.review.disabled = false;
      scope.reviewing = false;
      if (!scope.reviewed && scope.preview.textContent === "Checking the matching workspaces and saved routing...") {
        scope.preview.textContent = "Review the current selection before creating standby.";
      }
    }
  }
}

function bcdrGroupName(groupOrId) {
  const group = typeof groupOrId === "string" ? (bcdr.latest.groups || []).find((entry) => entry.group_id === groupOrId) : groupOrId;
  const items = bcdr.latest.details?.inventory?.items || [];
  const names = (group?.items || []).map((identity) => items.find((entry) =>
    entry.identity.workspace_id === identity.workspace_id && entry.identity.item_id === identity.item_id)?.display_name).filter(Boolean);
  return names.length ? names.join(", ") : `${group?.items?.length || "Selected"} item(s)`;
}

function bcdrRenderScheduleGuide(container, guide) {
  const section = bcdrElement("section", undefined, "bcdr-schedule-guide");
  section.appendChild(bcdrElement("h4", "Scheduled sync: deployment handoff"));
  section.appendChild(bcdrElement("p", "Instructions prepared. Job deployment is not verified.", "bcdr-status"));
  section.appendChild(bcdrElement("p", `Reviewed workspaces: ${(guide.workspace_names || []).join(", ") || "See approved scope"}.`));
  if (!guide.remote_lease_configured) section.appendChild(bcdrElement("p",
    "A shared remote lease is not configured here. Configure it on both controller and scheduler before unattended execution.", "alert"));
  const steps = bcdrElement("ol");
  (guide.steps || []).forEach((step) => steps.appendChild(bcdrElement("li", step)));
  section.appendChild(steps);
  section.appendChild(bcdrElement("p", `UTC schedule to review: ${guide.schedule_utc}`));
  section.appendChild(bcdrElement("pre", guide.command));
  const download = bcdrElement("a", "Download approved sync settings", "secondary bcdr-download");
  download.download = guide.request_filename;
  download.href = `data:application/json;charset=utf-8,${encodeURIComponent(JSON.stringify(guide.request, null, 2))}`;
  section.appendChild(download);
  const reference = bcdrElement("a", "Open scheduler deployment reference");
  reference.href = guide.template_url;
  reference.target = "_blank";
  reference.rel = "noopener noreferrer";
  section.appendChild(reference);
  container.appendChild(section);
}

if (globalThis.location?.hash.startsWith("#bcdr/")) {
  $("#login-workflow").value = "bcdr";
  updateLoginMode();
}
