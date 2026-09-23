/* Operator journeys select existing guarded actions; the backend remains authoritative. */

const bcdrJourneys = {
  overview: { title: "Manage standby", description: "Choose the job you need to do. Loading status may resume the metadata capacity.", stages: [] },
  setup: { title: "Set up standby", description: "Establish a metadata baseline, then prepare automatic synchronization.",
    stages: [["home", "Metadata & scope"], ["protection", "Data protection"], ["sync", "Initial sync"], ["schedule", "Scheduled sync"]] },
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
  if (!bcdrJourneys[journey]) journey = "overview";
  bcdr.journey = journey;
  const definition = bcdrJourneys[journey];
  bcdr.journeyStage = definition.stages.some(([value]) => value === stage) ? stage : definition.stages[0]?.[0];
  if (globalThis.history && globalThis.location) {
    globalThis.history.replaceState(null, "", `#bcdr/${journey}${bcdr.journeyStage ? `/${bcdr.journeyStage}` : ""}`);
  }
  bcdrRefreshJourney();
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
  const navigation = bcdrElement("nav", undefined, "bcdr-journey-nav");
  navigation.setAttribute("aria-label", "Standby and recovery workflows");
  for (const [key, label] of [["overview", "Overview"], ["setup", "Set up standby"], ["test", "DR Test"], ["incident", "I'm currently down"]]) {
    const button = bcdrElement("button", label, "secondary");
    button.type = "button";
    button.dataset.journey = key;
    button.addEventListener("click", () => bcdrJourney(key));
    navigation.appendChild(button);
  }
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
  if (statusAction) statusAction.hidden = bcdr.journey === "setup" &&
    bcdr.journeyStage === "home" && !bcdr.savedSetup;
  facts.hidden = !(bcdr.journey === "overview" ||
    (bcdr.journey === "test" && bcdr.journeyStage === "review") ||
    (bcdr.journey === "incident" && bcdr.journeyStage === "assess"));
  $("#bcdr-journey-title").textContent = current.title;
  $("#bcdr-journey-description").textContent = current.description;
  for (const button of document.querySelectorAll(".bcdr-journey-nav button")) {
    button.setAttribute("aria-current", button.dataset.journey === bcdr.journey ? "page" : "false");
    if (button.dataset.journey === "setup") button.textContent = state.baseline ? "Standby settings" : "Set up standby";
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
  stages.hidden = !current.stages.length;
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
      bcdrJourneyButton(help, state.baseline ? "Sync standby now" : "Start or resume setup", "setup", state.baseline ? "sync" : "home", true);
      if (state.baseline) bcdrJourneyButton(help, "Set up scheduled sync", "setup", "schedule");
    }
    bcdrJourneyButton(help, "Run a DR Test", "test", "review");
    bcdrJourneyButton(help, "I'm currently down", "incident", "assess");
  } else if (bcdr.journey === "setup") {
    const descriptions = {
      home: "Choose source scope and the metadata home. Continue the saved Warehouse setup instead of recreating it.",
      protection: "Optional protection is separate from metadata sync. Configure qualified data inputs for workloads that need them.",
      sync: "Review named routes and scope, then run the initial metadata sync. Resources remain stopped; this is not cutover.",
      schedule: "Prepare a reviewed handoff after metadata sync. This does not create a job, and does not prove data recovery readiness.",
    };
    help.appendChild(bcdrElement("p", descriptions[bcdr.journeyStage]));
    visible = { home: ["setup"], protection: ["configure-protection"],
      sync: ["plan", "synchronize"], schedule: ["schedule-guide"] }[bcdr.journeyStage] || [];
    if (state.testActive || state.incidentActive) {
      help.appendChild(bcdrElement("p", "Setup and source synchronization are unavailable during a test or incident. Return to that workflow.", "alert"));
      visible = [];
    } else if (bcdr.journeyStage === "schedule" &&
      (!state.baseline || state.workflow?.schedule_eligible === false)) {
      visible = [];
      help.appendChild(bcdrElement("p",
        "Load status, complete the metadata baseline and reconcile active work before preparing the schedule.", "hint"));
    }
    if (bcdr.journeyStage === "home" && bcdr.savedSetup?.warehousePhase === "ready") {
      bcdrJourneyButton(help, "Continue to initial sync", "setup", "sync", true);
    } else if (bcdr.journeyStage === "protection") {
      bcdrJourneyButton(help, "Continue to initial sync", "setup", "sync");
    } else if (bcdr.journeyStage === "sync" && state.baseline) {
      bcdrJourneyButton(help, "Set up scheduled sync", "setup", "schedule", true);
    }
    if (bcdr.journeyStage === "schedule") {
      help.appendChild(bcdrElement("p", "Both controller and job need the same identity, shared files, remote lease and production image. Local-only Docker does not establish unattended scheduling.", "hint"));
      bcdrJourneyButton(help, "Skip for now - manage standby", "overview");
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
  if (preparation) preparation.hidden = bcdr.journey !== "setup" || !["home", "sync"].includes(bcdr.journeyStage) ||
    state.testActive || state.incidentActive;
  const result = $("#bcdr-result");
  if (result) result.hidden = bcdr.resultJourney !== bcdr.journey;
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
