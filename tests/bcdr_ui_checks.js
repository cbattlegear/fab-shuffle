const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { fixture } = require("./cross_tenant_ui_checks");

function setup() {
  const f = fixture();
  vm.runInContext(fs.readFileSync(path.join(__dirname, "..", "fabshuffle", "web", "static",
    "bcdr-workflows.js"), "utf8"), f.context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, "..", "fabshuffle", "web", "static", "bcdr.js"), "utf8") + `
    globalThis.product = { bcdr, bcdrField, bcdrRenderForms, bcdrRenderResult, bcdrSubmit, bcdrConfirm, bcdrJourney };
  `, f.context);
  f.product = f.context.product;
  Object.assign(f.ui.state, { sessionId: "same-tenant-session" });
  f.product.bcdr.sessionId = "same-tenant-session";
  return f;
}

const schema = {
  type: "object",
  properties: {
    schema_version: { const: 1 },
    generation_id: { type: "string" },
    group_ids: { type: "array", items: { type: "string" } },
    park: { type: "boolean", default: true },
    include_workspace_ids: { type: "array", items: { type: "string" }, default: [] },
  },
  required: ["generation_id", "group_ids"],
};
const command = {
  name: "synchronize", label: "Sync standby", method: "POST", path: "/api/bcdr/synchronize",
  description: "Prepare inactive standby items.", schema, confirmation: "Sync is NOT Enable recovery.",
};
const result = {
  mode: "standby", generation_id: "generation-1", outcome: "partial", exit_code: 2,
  warnings: ["Orders: supply an off-region export."],
  groups: [{
    group_id: "group-1", items: [{ item_id: "orders" }], metadata_applied: true,
    data_ready: false, access_enabled: false, ready_for_cutover: false, active: false,
    blockers: ["Orders is unprotected."],
  }],
};
const reconcileSchema = {
  type: "object",
  properties: {
    operation_id: { type: "string" }, expected_controller_id: { type: "string" },
    expected_epoch: { type: "integer", minimum: 1 },
    previous_controller_stopped: { type: "boolean" },
    fencing_evidence: { type: "string" }, target_quiescence_evidence: { type: "string" },
  },
  required: ["operation_id", "expected_controller_id", "expected_epoch", "previous_controller_stopped",
    "fencing_evidence", "target_quiescence_evidence"],
};
const reconcileCommand = {
  name: "reconcile-operation", label: "Reconcile interrupted operation", method: "POST",
  path: "/api/bcdr/reconcile-operation", schema: reconcileSchema,
  description: "Inspect the exact service receipt.", confirmation: "Fence the previous controller before takeover.",
};

const armCapacity = "/subscriptions/demo/resourceGroups/qa/providers/Microsoft.Fabric/capacities/recovery";
const discovery = {
  source: {
    capacities: [{ id: "source-capacity-guid", displayName: "Primary", region: "East US", sku: "F4" }],
    workspaces: [{ id: "source-workspace-guid", displayName: "Source work", capacityId: "source-capacity-guid" }],
    errors: {},
  },
  recovery: {
    capacities: [{ id: "recovery-capacity-guid", displayName: "Recovery", region: "West US", sku: "F4" }],
    workspaces: [{ id: "control-workspace-guid", displayName: "Control", capacityId: "recovery-capacity-guid" }],
    capacityChoices: [{
      id: "recovery-capacity-guid", displayName: "Recovery", region: "West US", sku: "F4",
      arm_resource_id: armCapacity, matchStatus: "matched", matchMethod: "name_region",
      subscriptionName: "QA subscription", resourceGroup: "qa",
      matchMessage: "Matched by name and region.",
    }],
    errors: {},
  },
};

const recoveryCapacitySchema = {
  type: "object", properties: {
    fabric_capacity_id: { type: "string" }, arm_resource_id: { type: "string" },
    dedicated_recovery: { type: "boolean" }, authorized_for_suspend: { type: "boolean" },
  }, required: ["fabric_capacity_id", "arm_resource_id", "dedicated_recovery", "authorized_for_suspend"],
};

const warehouseSetupCommand = {
  name: "setup", label: "Set up metadata", path: "/api/bcdr/setup", method: "POST",
  confirmation: "Check the selected Warehouse before initializing its catalog.",
  schema: { type: "object", properties: {
    control_workspace_id: { type: "string" }, control_workspace_name: { type: "string" },
    warehouse_action: { type: "string", enum: ["continue", "existing", "create"] },
    warehouse_id: { type: "string" }, expected_setup_revision: { type: "integer" },
    warehouse_name: { type: "string", default: "New metadata" },
  } },
};

const simpleScope = { mode: "standby", details: { standby_scope: {
  workspaces: [{ id: "source-workspace-guid", displayName: "Sales workspace", capacityRegion: "East US" }],
  target_capacities: [{ id: "recovery-capacity-guid", displayName: "Recovery", region: "West US" }],
  default_target_id: "recovery-capacity-guid", saved_selection: null,
} } };

function discoverButton(f) {
  return f.get("bcdr-content").querySelectorAll("button").find((button) =>
    button.textContent === "Discover setup choices" || button.textContent === "Discovering choices...");
}

const scenarios = {
  async normal_standby_setup_only_selects_workspace_scope(f) {
    f.product.bcdrRenderForms([warehouseSetupCommand, command]);
    f.product.bcdrJourney("setup", "select");
    assert.equal(f.requests[0].url, "/api/bcdr/standby-scope?include_workspaces=true");
    await f.reply(0, simpleScope);
    const scope = f.product.bcdr.scope;
    assert.equal(scope.section.hidden, false);
    assert.equal(f.get("bcdr-preparation").hidden, true);
    assert.equal(f.product.bcdr.commandSections.get("setup").hidden, true);
    assert.equal(f.product.bcdr.commandSections.get("synchronize").hidden, true);
    assert.doesNotMatch(scope.section.textContent, /source-workspace-guid|recovery-capacity-guid|ARM resource/);
    const checkbox = scope.entries.querySelector("input");
    checkbox.checked = true;
    checkbox.handlers.change();
    scope.review.click();
    assert.deepEqual(JSON.parse(f.requests[1].options.body), {
      selection_mode: "workspaces", workspace_ids: ["source-workspace-guid"],
    });
    await f.reply(1, { details: { standby_preview: {
      configuration: "a".repeat(64), workspaces: simpleScope.details.standby_scope.workspaces,
      message: "Saved recovery settings will be reused.",
    } } });
    assert.match(scope.preview.textContent, /Sales workspace/);
    assert.equal(scope.start.hidden, false);
    scope.start.click();
    const request = JSON.parse(f.requests[2].options.body);
    assert.equal(request.confirmation, "create-standby");
    assert.equal(request.request.expected_configuration, "a".repeat(64));
    assert.equal(request.request.warehouse_name, undefined);
    assert.equal(request.request.recovery_capacities, undefined);
    await f.reply(2, { ...result, details: { sync_summary: { metadata_ready: true, data_gap_groups: [] } } });
    assert.match(scope.section.textContent, /Set up scheduled sync/);
  },
  async standby_name_rule_invalidates_stale_selection_preview(f) {
    f.product.bcdrRenderForms([warehouseSetupCommand]);
    f.product.bcdrJourney("setup");
    await f.reply(0, simpleScope);
    const scope = f.product.bcdr.scope;
    scope.mode.value = "pattern";
    scope.mode.handlers.change();
    scope.pattern.value = "sales";
    scope.pattern.handlers.input();
    scope.review.click();
    assert.deepEqual(JSON.parse(f.requests[1].options.body), { selection_mode: "pattern", name_pattern: "sales" });
    scope.pattern.value = "finance";
    scope.pattern.handlers.input();
    await f.reply(1, { details: { standby_preview: {
      configuration: "a".repeat(64), workspaces: simpleScope.details.standby_scope.workspaces,
    } } });
    assert.equal(scope.start.hidden, true);
    assert.equal(scope.reviewed, null);
    scope.start.click();
    assert.equal(f.requests.length, 2);
  },
  async standby_prerequisites_belong_in_settings_not_the_selection_form(f) {
    f.product.bcdrRenderForms([warehouseSetupCommand]);
    f.product.bcdrJourney("setup");
    await f.reply(0, { needs_configuration: true, message: "Configure the environment once in Settings." });
    const scope = f.product.bcdr.scope;
    assert.equal(scope.review.disabled, true);
    assert.match(scope.status.textContent, /once in Settings/);
    scope.section.querySelectorAll("button").find((button) => button.textContent === "Recovery environment settings").click();
    assert.equal(f.product.bcdr.journey, "settings");
    assert.equal(f.product.bcdr.commandSections.get("setup").hidden, false);
    assert.equal(scope.section.hidden, true);
    assert.equal(f.requests.length, 1, "Settings environment does not implicitly discover the source");
  },
  async late_workspace_choices_do_not_populate_an_incident(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("setup");
    f.product.bcdrJourney("incident", "assess");
    await f.reply(0, simpleScope);
    assert.equal(f.product.bcdr.scope.loaded, false);
    assert.equal(f.product.bcdr.scope.section.hidden, true);
    assert.equal(f.requests.length, 1);
  },
  async destination_settings_load_without_source_discovery(f) {
    f.product.bcdrRenderForms([{ ...command, name: "configure-standby-defaults",
      schema: { type: "object", properties: { target_capacity_id: { type: "string" } },
        required: ["target_capacity_id"] } }]);
    f.product.bcdrJourney("settings", "routing");
    assert.equal(f.requests[0].url, "/api/bcdr/standby-scope?include_workspaces=false");
    await f.reply(0, simpleScope);
    const form = f.get("bcdr-content").querySelector("form");
    const select = form.querySelector("select");
    assert.match(select.textContent, /Recovery - West US/);
    assert.doesNotMatch(select.textContent, /recovery-capacity-guid/);
  },
  async settings_load_is_not_lost_behind_pending_workspace_discovery(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("setup");
    f.product.bcdrJourney("settings", "routing");
    assert.equal(f.requests.length, 1);
    await f.reply(0, simpleScope);
    assert.equal(f.requests[1].url, "/api/bcdr/standby-scope?include_workspaces=false");
    await f.reply(1, simpleScope);
    assert.match(f.get("bcdr-settings-load-status").textContent, /Choose a default/);
    assert.equal(f.product.bcdr.scope.loading, false);
  },
  async ambiguous_workspace_names_are_not_selected_by_guessing(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("setup");
    const data = JSON.parse(JSON.stringify(simpleScope));
    data.details.standby_scope.workspaces.push({
      ...data.details.standby_scope.workspaces[0], id: "second-workspace-guid",
    });
    await f.reply(0, data);
    const scope = f.product.bcdr.scope;
    assert.ok(scope.entries.querySelectorAll("input").every((input) => input.disabled && !input.checked));
    assert.match(scope.entries.textContent, /use a rule to include all matches/);
    assert.equal(scope.start.hidden, true);
  },
  async scope_loading_errors_never_leave_an_enabled_noop_review_button(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("setup");
    assert.equal(f.product.bcdr.scope.review.disabled, true);
    await f.reply(0, { detail: "AccessDenied: Grant source read access" }, 403);
    const scope = f.product.bcdr.scope;
    assert.match(scope.status.textContent, /AccessDenied/);
    assert.equal(scope.review.disabled, true);
    assert.equal(scope.reload.disabled, false);
    scope.reload.click();
    await f.reply(1, simpleScope);
    assert.equal(scope.review.disabled, false);
    assert.equal(scope.loaded, true);
  },
  async late_settings_error_does_not_disable_a_loaded_workspace_selection(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("setup");
    await f.reply(0, simpleScope);
    f.product.bcdrJourney("settings", "routing");
    f.product.bcdrJourney("setup");
    await f.reply(1, { detail: "AccessDenied: Check destination access" }, 403);
    assert.equal(f.product.bcdr.scope.loaded, true);
    assert.equal(f.product.bcdr.scope.review.disabled, false);
    assert.match(f.product.bcdr.scope.status.textContent, /workspace.*available/);
  },
  async journeys_are_separate_and_navigation_never_reads_the_source(f) {
    const commands = ["setup", "status", "synchronize", "schedule-guide", "start-dr-test",
      "continue-dr-test", "end-dr-test", "enable-recovery", "cutover", "plan-failback", "reconcile-operation"]
      .map((name) => ({ ...command, name, label: name, path: `/api/bcdr/${name}` }));
    f.product.bcdrRenderForms(commands);
    assert.equal(f.product.bcdr.journey, "overview");
    assert.ok(Array.from(f.product.bcdr.commandSections.values()).every((section) => section.hidden));
    f.product.bcdrJourney("incident", "assess");
    assert.equal(f.get("bcdr-preparation").hidden, true);
    assert.equal(f.product.bcdr.commandSections.get("setup").hidden, true);
    assert.equal(f.product.bcdr.commandSections.get("synchronize").hidden, true);
    assert.match(f.get("bcdr-journey-help").textContent, /No healthy-source discovery/);
    f.product.bcdrJourney("test", "exercise");
    assert.equal(f.product.bcdr.commandSections.get("start-dr-test").hidden, false);
    assert.equal(f.product.bcdr.commandSections.get("cutover").hidden, true);
    f.product.bcdrJourney("settings", "environment");
    assert.equal(f.get("bcdr-preparation").hidden, false);
    assert.equal(f.product.bcdr.commandSections.get("setup").hidden, false);
    assert.equal(f.requests.length, 0);
  },
  async initial_metadata_sync_offers_schedule_despite_data_gaps(f) {
    f.product.bcdrRenderForms([{ ...command, name: "synchronize" },
      { ...command, name: "schedule-guide", label: "Schedule" }]);
    f.product.bcdrJourney("setup", "sync");
    await f.reply(0, simpleScope);
    f.product.bcdrRenderResult({ ...result, details: {
      sync_summary: { metadata_ready: true, completed_at: "2026-09-23T18:00:00Z", data_gap_groups: ["group-1"] },
    } });
    const next = f.get("bcdr-journey-help").querySelectorAll("button")
      .find((button) => button.textContent === "Set up scheduled sync");
    assert.ok(next, "Data gaps must not hide the scheduling handoff");
    next.click();
    assert.equal(f.product.bcdr.journeyStage, "schedule");
    assert.equal(f.product.bcdr.commandSections.get("schedule-guide").hidden, false);
    assert.match(f.get("bcdr-workflow-facts").textContent, /1 group.*unresolved data readiness/s);
    assert.match(f.get("bcdr-journey-help").textContent, /same identity/);
    assert.equal(f.requests.length, 1);
  },
  async test_state_blocks_setup_and_incident_controls_without_claiming_production(f) {
    f.product.bcdrRenderForms(["setup", "synchronize", "start-dr-test", "continue-dr-test",
      "end-dr-test", "enable-recovery", "cutover"].map((name) => ({ ...command, name })));
    f.product.bcdrRenderResult({ ...result, mode: "testing", details: { dr_test: {
      test_id: "test-1", phase: "testing", production_cutover: false,
      results: [{ group_id: "group-1", outcome: "not_tested", messages: ["Supply owner evidence."] }],
    } } });
    f.product.bcdrJourney("test", "exercise");
    assert.equal(f.product.bcdr.commandSections.get("continue-dr-test").hidden, false);
    assert.equal(f.product.bcdr.commandSections.get("start-dr-test").hidden, true);
    assert.match(f.get("bcdr-journey-help").textContent, /Production cutover: no/);
    const test = f.product.bcdrField("test_id", { type: "string" }, {}, true);
    assert.equal(test.read(), "test-1");
    f.product.bcdrJourney("setup", "sync");
    assert.equal(f.product.bcdr.commandSections.get("synchronize").hidden, true);
    f.product.bcdrJourney("incident", "prepare");
    assert.equal(f.product.bcdr.commandSections.get("enable-recovery").hidden, true);
    assert.match(f.get("bcdr-journey-help").textContent, /End test safely/);
  },
  async schedule_handoff_is_downloadable_not_a_deployment_claim(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("setup", "schedule");
    const request = { capacity_routes: [], capture: true, park: true };
    f.product.bcdrRenderResult({ ...result, details: { schedule_guide: {
      request, request_filename: "scheduled-sync-request.json", schedule_utc: "0 2 * * *",
      remote_lease_configured: false, workspace_names: ["Sales"], steps: ["Review the shared storage."],
      command: "python -m fabshuffle.bcdr scheduled-sync",
      template_url: "https://github.com/cbattlegear/fab-shuffle/blob/main/deploy/azuredeploy-sync-job.json",
    } } });
    const section = f.get("bcdr-result").querySelector(".bcdr-schedule-guide");
    assert.match(section.textContent, /Job deployment is not verified/);
    assert.match(section.textContent, /shared remote lease is not configured/);
    const link = section.querySelectorAll("a").find((entry) => entry.download);
    assert.equal(link.download, "scheduled-sync-request.json");
    assert.deepEqual(JSON.parse(decodeURIComponent(link.href.split(",").slice(1).join(","))), request);
    assert.equal(f.requests.length, 0);
  },
  async warehouse_list_signout_drops_late_choices(f) {
    f.product.bcdr.savedSetup = { revision: 1, workspaceId: "control-workspace-guid",
      warehouseName: "Saved metadata", warehousePhase: "accepted" };
    f.product.bcdrRenderForms([warehouseSetupCommand]);
    const form = f.get("bcdr-content").querySelector("form");
    form.querySelectorAll("button").find((button) => button.textContent.startsWith("List Warehouses")).click();
    f.get("sign-out").click();
    await f.reply(0, { workspaceId: "control-workspace-guid", savedSetup: {},
      warehouses: [{ id: "stale", displayName: "Old session metadata" }] });
    assert.equal(f.product.bcdr.warehouseChoices.length, 0);
    assert.equal(f.product.bcdr.savedSetup, null);
    assert.equal(f.get("bcdr-content").textContent, "");
  },
  async warehouse_setup_prefills_saved_name_and_uses_selected_exact_item(f) {
    f.product.bcdr.savedSetup = { revision: 12, workspaceId: "control-workspace-guid",
      workspaceName: "Recovery metadata", warehouseName: "BCDR_Metadata",
      warehousePhase: "accepted", warehouseId: null, hasOperation: true };
    f.product.bcdrRenderForms([warehouseSetupCommand]);
    const form = f.get("bcdr-content").querySelector("form");
    const selects = form.querySelectorAll("select");
    const workspace = selects.find((row) => row.name === "control_workspace_id");
    const action = selects.find((row) => row.name === "warehouse_action");
    const item = selects.find((row) => row.name === "warehouse_id");
    assert.equal(workspace.value, "control-workspace-guid");
    assert.match(workspace.textContent, /Recovery metadata/);
    assert.equal(workspace.disabled, true);
    assert.equal(action.value, "continue");
    assert.equal(form.querySelectorAll("input").find((row) => row.name === "warehouse_name").value, "BCDR_Metadata");
    assert.ok(action.children.find((row) => row.value === "create").disabled);
    assert.equal(f.requests.length, 0, "Opening setup must not read SQL or start capacity");
    form.querySelectorAll("button").find((button) => button.textContent.startsWith("List Warehouses")).click();
    assert.equal(f.requests[0].url, "/api/bcdr/workspaces/control-workspace-guid/warehouses");
    await f.reply(0, { workspaceId: "control-workspace-guid", workspaceName: "Recovery metadata",
      savedSetup: f.product.bcdr.savedSetup,
      warehouses: [{ id: "selected-wh-guid", displayName: "BCDR_Metadata", recorded: false, endpointReported: true }] });
    assert.equal(item.value, "", "A name match is never selected automatically");
    action.value = "existing";
    action.handlers.change();
    item.value = "selected-wh-guid";
    form.handlers.submit({ preventDefault() {} });
    const dialog = f.document.querySelectorAll("dialog").find((node) => node.className.includes("bcdr-confirm"));
    assert.match(dialog.textContent, /BCDR_Metadata/);
    assert.doesNotMatch(dialog.textContent, /selected-wh-guid/);
    dialog.querySelectorAll("button")[1].click();
    const body = JSON.parse(f.requests[1].options.body).request;
    assert.equal(body.warehouse_id, "selected-wh-guid");
    assert.equal(body.warehouse_action, "existing");
    assert.equal(body.expected_setup_revision, 12);
    await f.reply(1, { ...result, details: { setup_phase: "catalog_ready", setup_revision: 15,
      warehouse_name: "BCDR_Metadata", control_warehouse_id: "selected-wh-guid",
      control_workspace_id: "control-workspace-guid", next_action: "Preview standby selection." } });
    assert.match(form.querySelector(".bcdr-action-progress").textContent, /BCDR_Metadata is ready.*Preview/);
  },
  async warehouse_list_errors_and_workspace_changes_do_not_select_stale_items(f) {
    f.product.bcdr.discovered = JSON.parse(JSON.stringify(discovery));
    f.product.bcdr.discovered.recovery.workspaces.push({ id: "second-workspace", displayName: "Second metadata" });
    f.product.bcdrRenderForms([warehouseSetupCommand]);
    const form = f.get("bcdr-content").querySelector("form");
    const selects = form.querySelectorAll("select");
    const mode = selects[0];
    mode.value = "existing";
    mode.handlers.change();
    const workspace = selects.find((row) => row.name === "control_workspace_id");
    workspace.value = "control-workspace-guid";
    workspace.handlers.change();
    assert.equal(f.requests.length, 1, "Selecting a workspace loads its Warehouses");
    workspace.value = "second-workspace";
    workspace.handlers.change();
    assert.equal(f.requests.length, 2);
    await f.reply(0, { workspaceId: "control-workspace-guid", warehouses: [{ id: "old", displayName: "Old" }] });
    assert.equal(f.product.bcdr.warehouseChoices.length, 0);
    await f.reply(1, { detail: "AccessDenied: Request read access" }, 403);
    assert.match(form.textContent, /Could not list Warehouses.*AccessDenied/);
    form.querySelectorAll("button").find((button) => button.textContent.startsWith("List Warehouses")).click();
    await f.reply(2, { workspaceId: "second-workspace", warehouses: [], savedSetup: null });
    assert.match(form.textContent, /No Warehouses were returned/);
    const action = selects.find((row) => row.name === "warehouse_action");
    action.value = "create";
    action.handlers.change();
    const name = form.querySelectorAll("input").find((row) => row.name === "warehouse_name");
    name.value = "Fresh metadata";
    form.handlers.submit({ preventDefault() {} });
    const dialog = f.document.querySelectorAll("dialog").find((node) => node.className.includes("bcdr-confirm"));
    dialog.querySelectorAll("button")[1].click();
    const body = JSON.parse(f.requests[3].options.body).request;
    assert.equal(body.warehouse_action, "create");
    assert.equal(body.warehouse_id, undefined);
    assert.equal(body.control_workspace_id, "second-workspace");
    await f.reply(3, result);
  },
  async recovery_capacity_names_submit_exact_pair_and_catalog(f) {
    f.product.bcdr.discovered = JSON.parse(JSON.stringify(discovery));
    const setupCommand = { name: "setup", label: "Setup", method: "POST", path: "/api/bcdr/setup",
      confirmation: "Review the selected capacities.", schema: { type: "object", properties: {
        recovery_capacities: { type: "array", items: recoveryCapacitySchema },
        catalog_capacity_id: { type: "string" },
      }, required: ["recovery_capacities", "catalog_capacity_id"] } };
    f.product.bcdrRenderForms([setupCommand]);
    const form = f.get("bcdr-content").querySelector("form");
    const catalog = form.querySelectorAll("select").find((select) => select.name === "catalog_capacity_id");
    assert.equal(catalog.children.length, 1, "Only selected recovery capacities can host the catalog");
    form.querySelectorAll("button").find((button) => button.textContent === "Add dedicated recovery capacities").click();
    const capacity = form.querySelectorAll("select").find((select) => select.name === "fabric_capacity_id");
    assert.match(capacity.textContent, /Recovery - West US - F4 - QA subscription - qa/);
    assert.equal(capacity.value, "");
    capacity.value = "recovery-capacity-guid";
    capacity.handlers.change();
    assert.ok(catalog.children.some((option) => option.value === armCapacity));
    catalog.value = armCapacity;
    catalog.handlers.change();
    const approvals = form.querySelectorAll("input").filter((input) => input.type === "checkbox");
    assert.equal(approvals.length, 2);
    assert.ok(approvals.every((input) => !input.checked));
    form.handlers.submit({ preventDefault() {} });
    assert.match(form.querySelector(".bcdr-action-error").textContent, /Confirm dedicated recovery use/);
    assert.equal(f.get("bcdr-error").hidden, true);
    approvals.forEach((input) => { input.checked = true; });
    form.handlers.submit({ preventDefault() {} });
    const dialog = f.document.querySelectorAll("dialog").find((node) => node.className.includes("bcdr-confirm"));
    assert.doesNotMatch(dialog.textContent, /recovery-capacity-guid|\/subscriptions\//);
    assert.match(dialog.textContent, /Recovery - West US/);
    assert.ok(!form.querySelectorAll("input").some((input) => ["arm_resource_id", "fabric_capacity_id", "catalog_capacity_id"].includes(input.name)));
    dialog.querySelectorAll("button")[1].click();
    const body = JSON.parse(f.requests[0].options.body).request;
    assert.equal(body.catalog_capacity_id, armCapacity);
    assert.deepEqual(body.recovery_capacities, [{
      arm_resource_id: armCapacity, fabric_capacity_id: "recovery-capacity-guid",
      dedicated_recovery: true, authorized_for_suspend: true,
    }]);
    await f.reply(0, result);
    form.querySelectorAll("button").find((button) => button.textContent === "Remove entry").click();
    assert.equal(f.product.bcdr.recoveryRows.size, 0);
    assert.ok(!catalog.children.some((option) => option.value === armCapacity && !option.disabled));
  },
  async capacity_match_refresh_preserves_only_same_identity_approvals(f) {
    f.product.bcdr.discovered = JSON.parse(JSON.stringify(discovery));
    const field = f.product.bcdrField("recovery", recoveryCapacitySchema, recoveryCapacitySchema, true);
    const select = field.element.querySelector("select");
    select.value = "recovery-capacity-guid";
    select.handlers.change();
    const approvals = field.element.querySelectorAll("input");
    approvals.forEach((input) => { input.checked = true; });
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.equal(field.read().arm_resource_id, armCapacity);
    assert.ok(approvals.every((input) => input.checked));
    f.product.bcdr.discovered.recovery.capacityChoices[0].arm_resource_id = armCapacity.replace("/qa/", "/other/");
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.ok(approvals.every((input) => !input.checked));
    assert.equal(select.value, "");
    assert.match(field.element.textContent, /match changed or disappeared/);
    assert.throws(() => field.read(), /Choose an available/);
  },
  async capacity_routes_are_named_and_unmatched_choices_are_disabled(f) {
    f.product.bcdr.discovered = JSON.parse(JSON.stringify(discovery));
    const source = f.product.bcdrField("source_capacity_id", { type: "string" }, {}, true);
    const target = f.product.bcdrField("target_capacity_id", { type: "string" }, {}, true);
    assert.match(source.element.textContent, /Primary - East US/);
    assert.match(target.element.textContent, /Recovery - West US/);
    assert.equal(source.element.querySelectorAll("input").length, 0);
    source.element.querySelector("select").value = "source-capacity-guid";
    target.element.querySelector("select").value = "recovery-capacity-guid";
    assert.equal(source.read(), "source-capacity-guid");
    assert.equal(target.read(), "recovery-capacity-guid");
    f.product.bcdr.discovered.recovery.capacityChoices[0].matchStatus = "ambiguous";
    f.product.bcdr.discovered.recovery.capacityChoices[0].matchMessage = "Resolve duplicate name and region.";
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.throws(() => target.read(), /Choose an available/);
    assert.match(target.element.textContent, /Resolve duplicate/);
    f.product.bcdr.discovered.recovery.errors.capacityMapping = "AuthorizationFailed: Grant read access";
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.throws(() => target.read(), /Refresh capacity discovery/);
    assert.match(target.element.textContent, /AuthorizationFailed/);
  },
  async discovery_feedback_and_preserved_selections(f) {
    f.product.bcdrRenderForms([command]);
    const field = f.product.bcdrField("source_capacity_ids", { type: "array", items: { type: "string" } }, {}, true);
    f.get("bcdr-content").appendChild(field.element);
    const text = f.product.bcdrField("warehouse_name", { type: "string" }, {}, true);
    text.element.querySelector("input").value = "Keep my Warehouse name";
    const button = discoverButton(f);
    button.click();
    assert.match(f.get("bcdr-discovery-status").textContent, /Reading source and recovery/);
    button.click();
    assert.equal(f.requests.length, 1, "Repeated click must not issue concurrent discovery");
    await f.reply(0, discovery);
    const status = f.get("bcdr-discovery-status");
    assert.equal(status.parentElement, button.parentElement, "Feedback must be beside the button");
    assert.equal(status["aria-live"], "polite");
    assert.match(status.textContent, /Discovery complete.*Source: 1 workspaces.*Recovery: 1 capacities/s);
    assert.match(status.textContent, /No configuration was saved/);
    assert.equal(field.element.querySelectorAll("textarea").length, 0);
    assert.doesNotMatch(field.element.textContent, /source-capacity-guid/);
    field.element.querySelector("input").checked = true;
    button.click();
    await f.reply(1, discovery);
    assert.equal(field.element.querySelector("input").checked, true);
    assert.equal(field.read()[0], "source-capacity-guid");
    assert.equal(text.read(), "Keep my Warehouse name");
    assert.ok(f.requests.every((request) => request.url === "/api/bcdr/discovery"));
  },
  async discovery_empty_partial_and_failed(f) {
    f.product.bcdrRenderForms([command]);
    const button = discoverButton(f);
    button.click();
    await f.reply(0, { source: { capacities: [], workspaces: [] }, recovery: { capacities: [], workspaces: [] } });
    assert.match(f.get("bcdr-discovery-status").textContent, /No resources are visible.*access/);
    button.click();
    await f.reply(1, discovery);
    button.click();
    await f.reply(2, {
      source: discovery.source,
      recovery: { capacities: [], errors: { workspaces: "AccessDenied: Grant read access" } },
    });
    assert.match(f.get("bcdr-discovery-status").textContent, /Discovery incomplete.*AccessDenied.*Previous choices/s);
    assert.equal(f.product.bcdr.discovered.recovery.workspaces[0].id, "control-workspace-guid");
    button.click();
    await f.reply(3, { detail: "ServiceBusy: Try again later" }, 503);
    assert.match(f.get("bcdr-discovery-status").textContent, /Discovery failed.*ServiceBusy.*not changed/s);
    assert.equal(button.disabled, false);
    assert.equal(f.product.bcdr.discovered.source.capacities[0].id, "source-capacity-guid");
    button.click();
    await f.reply(4, {});
    assert.match(f.get("bcdr-discovery-status").textContent, /Discovery failed.*response is incomplete/s);
    assert.equal(f.product.bcdr.discovered.source.capacities[0].id, "source-capacity-guid");
  },
  async discovery_signout_ignores_late_result(f) {
    f.product.bcdrRenderForms([command]);
    const button = discoverButton(f);
    button.click();
    f.get("sign-out").click();
    await f.reply(0, discovery);
    assert.equal(f.get("bcdr-content").textContent, "");
    assert.equal(Object.keys(f.product.bcdr.discovered).length, 0);
    assert.equal(f.product.bcdr.discovering, false);
    assert.equal(f.get("bcdr-progress").textContent, "");
    f.ui.state.sessionId = "another-session";
    button.click();
    assert.equal(f.requests.length, 2, "Only discovery and logout; stale controls cannot query the new session");
  },
  async existing_workspace_picker_uses_recovery_names(f) {
    f.product.bcdr.discovered = discovery;
    const setupSchema = { type: "object", properties: {
      control_workspace_name: { type: "string" }, control_workspace_id: { type: "string" },
      warehouse_name: { type: "string", default: "Catalog" },
    } };
    const setupCommand = { name: "setup", label: "Setup", path: "/api/bcdr/setup", method: "POST",
      schema: setupSchema, confirmation: "Create the catalog." };
    f.product.bcdrRenderForms([setupCommand]);
    const form = f.get("bcdr-content").querySelector("form");
    const selects = form.querySelectorAll("select");
    selects[0].value = "existing";
    selects[0].handlers.change();
    const select = selects[1];
    assert.equal(select.value, "", "No workspace is selected automatically");
    assert.match(select.textContent, /Control - Recovery - West US/);
    assert.doesNotMatch(select.textContent, /guid|Source work/);
    assert.ok(!form.querySelectorAll("input").some((input) => input.name === "control_workspace_id"));
    select.value = "control-workspace-guid";
    select.handlers.change();
    form.handlers.submit({ preventDefault() {} });
    const dialog = f.document.querySelectorAll("dialog").find((node) => node.className.includes("bcdr-confirm"));
    assert.match(dialog.textContent, /Control - Recovery/);
    assert.doesNotMatch(dialog.textContent, /control-workspace-guid/);
    dialog.querySelectorAll("button")[1].click();
    const payload = JSON.parse(f.requests[0].options.body).request;
    assert.equal(payload.control_workspace_id, "control-workspace-guid");
    assert.equal(payload.control_workspace_name, undefined);
    await f.reply(0, result);
  },
  async workspace_picker_rejects_missing_ambiguous_and_stale_choices(f) {
    f.product.bcdr.discovered = JSON.parse(JSON.stringify(discovery));
    const field = f.product.bcdrField("control_workspace_id", { type: "string" }, {}, true);
    const select = field.element.querySelector("select");
    assert.throws(() => field.read(), /select an available/);
    select.value = "control-workspace-guid";
    select.handlers.change();
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.equal(field.read(), "control-workspace-guid");
    f.product.bcdr.discovered.recovery.workspaces.push({
      id: "another-guid", displayName: "Control", capacityId: "recovery-capacity-guid",
    });
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.throws(() => field.read(), /select an available/);
    assert.match(field.element.textContent, /ambiguous/);
    f.product.bcdr.discovered.recovery.workspaces = [];
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.throws(() => field.read(), /select an available/);
    assert.match(field.element.textContent, /unavailable/);
    f.product.bcdr.discovered.recovery.workspaces = discovery.recovery.workspaces;
    f.product.bcdr.discovered.recovery.errors = { workspaces: "Denied" };
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.throws(() => field.read(), /Refresh recovery workspace/);
  },
  async named_choices_reject_missing_resources(f) {
    f.product.bcdr.discovered = JSON.parse(JSON.stringify(discovery));
    const field = f.product.bcdrField("source_capacity_ids", { type: "array", items: { type: "string" } }, {}, true);
    field.element.querySelector("input").checked = true;
    f.product.bcdr.discovered.source.capacities = [];
    f.product.bcdr.bindings.forEach((refresh) => refresh("discovery"));
    assert.throws(() => field.read(), /remove unavailable/);
    assert.match(field.element.textContent, /Primary.*unavailable/s);
    assert.doesNotMatch(field.element.textContent, /source-capacity-guid/);
    field.element.querySelector("input").checked = false;
    assert.throws(() => field.read(), /Select at least one/);
  },
  async discovery_keeps_pending_operation_choice(f) {
    f.product.bcdrRenderForms([reconcileCommand]);
    f.product.bcdrRenderResult({ ...result, details: {
      controller_id: "controller", controller_epoch: 9,
      pending_operations: [{ operation_id: "op", kind: "item-apply", state: "ambiguous" }],
    } });
    const select = f.get("bcdr-content").querySelector("form").querySelector("select");
    select.value = "op";
    select.handlers.change();
    discoverButton(f).click();
    await f.reply(0, discovery);
    assert.equal(select.value, "op", "Discovery must not reset an unrelated catalog operation choice");
  },
  async reconciliation_pins_controller_not_writer_epoch(f) {
    f.product.bcdrRenderForms([reconcileCommand]);
    const field = f.product.bcdrField("reconciliation", reconcileSchema, reconcileSchema, true);
    f.get("bcdr-content").appendChild(field.element);
    assert.throws(() => field.read(), /recorded operation/);
    f.product.bcdrRenderResult({
      ...result, details: { controller_id: "controller-1", controller_epoch: 7, writer: { epoch: 2 },
        pending_operations: [{ operation_id: "op-1", kind: "item-apply", state: "ambiguous",
          source: { workspace_id: "ws-1", item_id: "item-1" }, service_operation_id: "receipt-1",
          error_code: "TimedOut", message: "Inspect the recorded receipt." }] },
    });
    assert.match(f.get("bcdr-result").textContent, /TimedOut: Inspect the recorded receipt/);
    const select = field.element.querySelector("select");
    assert.equal(select.value, "", "No operation selected automatically");
    select.value = "op-1";
    select.handlers.change();
    let payload = field.read();
    assert.equal(payload.expected_epoch, 7);
    assert.equal(payload.expected_controller_id, "controller-1");
    assert.equal(payload.previous_controller_stopped, false, "Fencing attestation is never prechecked");
    assert.equal(payload.operation_id, "op-1");
    f.product.bcdrRenderResult({ ...result, details: { reconciled_operation: "op-1" } });
    assert.throws(() => field.read(), /recorded operation/);
    assert.match(f.get("bcdr-result").textContent, /Read status again/);
  },
  async reconciliation_confirmation_uses_exact_action(f) {
    f.product.bcdrRenderForms([reconcileCommand]);
    f.product.bcdrRenderResult({ ...result, details: {
      controller_id: "controller", controller_epoch: 9, writer: { epoch: 1 },
      pending_operations: [{ operation_id: "op", kind: "item-apply", state: "ambiguous" }],
    } });
    const form = f.get("bcdr-content").querySelector("form");
    form.querySelector("select").value = "op";
    form.querySelectorAll("input").filter((input) => !input.readOnly && input.type !== "checkbox")
      .forEach((input) => { if (input.name) input.value = "independent evidence"; });
    const fence = form.querySelectorAll("input").find((input) => input.name === "previous_controller_stopped");
    fence.checked = true;
    form.handlers.submit({ preventDefault() {} });
    const dialog = f.document.querySelectorAll("dialog").find((node) => node.className.includes("bcdr-confirm"));
    assert.equal(f.requests.length, 0);
    dialog.querySelectorAll("button")[1].click();
    const payload = JSON.parse(f.requests[0].options.body);
    assert.equal(payload.confirmation, "reconcile-operation");
    assert.equal(payload.request.expected_epoch, 9);
    assert.equal(payload.request.operation_id, "op");
    await f.reply(0, { ...result, details: { reconciled_operation: "op" } });
  },
  async readiness_pins_issuer_generation_and_writer(f) {
    f.product.bcdr.identity = { tenant_id: "tenant", object_id: "authenticated-spn" };
    f.product.bcdr.latest = {
      generation_id: "original-lineage-generation",
      details: {
        writer: { epoch: 4 }, controller_epoch: 19,
        readiness_context: { generation_id: "return-generation", writer_epoch: 4 },
      },
    };
    const schema = { type: "object", properties: {
      generation_id: { type: "string" }, writer_epoch: { type: "integer" },
      issuer: { type: "object" }, effective_principals: { type: "array", items: { type: "object" } },
    }, required: ["generation_id", "writer_epoch", "issuer"] };
    const field = f.product.bcdrField("readiness", schema, schema, true);
    const payload = field.read();
    assert.equal(payload.generation_id, "return-generation");
    assert.equal(payload.writer_epoch, 4);
    assert.equal(payload.issuer.object_id, "authenticated-spn");
    assert.equal(payload.issuer.kind, "ServicePrincipal");
    assert.equal(payload.effective_principals.length, 0, "Never infer effective access from issuer");
  },
  async missing_readiness_context_never_uses_lineage_generation(f) {
    f.product.bcdr.latest = { generation_id: "source-lineage", details: { writer: { epoch: 4 } } };
    const schema = { type: "object", properties: {
      generation_id: { type: "string" }, writer_epoch: { type: "integer" },
    }, required: ["generation_id", "writer_epoch"] };
    const field = f.product.bcdrField("readiness", schema, schema, true);
    assert.throws(() => field.read(), /Read status or preview first/);
  },
  async qualification_is_not_generated_by_defaults(f) {
    const field = f.product.bcdrField("source_paths_verified_local", { const: true, type: "boolean" }, {}, true);
    assert.throws(() => field.read(), /Confirm/);
    const checkbox = field.element.querySelector("input");
    assert.equal(checkbox.checked, false);
    checkbox.checked = true;
    assert.equal(field.read(), true);
    const evidence = f.product.bcdrField("enforcement_reference", { type: "string" }, {}, true);
    assert.equal(evidence.element.querySelector("input").value, "");
    const hash = f.product.bcdrField("binding_sha256", { type: "string" }, {}, true);
    assert.equal(hash.element.querySelector("input").value, "");
  },
  async temporary_attachment_retains_source_and_is_not_ready(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrRenderResult({ ...result, details: {
      temporary_attachments: [{
        attachments: [{
          binding: {
            source: { workspace_id: "original-ws", item_id: "original-item" }, source_path: "Tables/dbo/orders",
            consumer: { workspace_id: "dr-ws", item_id: "dr-item" },
          },
          attachment_verified: true, data_ready: false,
        }],
        data_ready: false, endpoint_ready: false,
      }],
    } });
    const text = f.get("bcdr-result").textContent;
    assert.match(text, /retain the source/);
    assert.match(text, /original-ws\/original-item\/Tables\/dbo\/orders/);
    assert.match(text, /Attachment verified: yes; data ready: no/);
    assert.match(text, /not independent recovery/);
  },
  async shipped_schemas(f) {
    const commands = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
    f.product.bcdrRenderForms(commands);
    assert.equal(f.get("bcdr-content").querySelectorAll("form").length, commands.length);
    assert.ok(f.get("bcdr-content").querySelectorAll("input").length > 10);
    assert.ok(f.get("bcdr-content").querySelectorAll("select").length > 0);
    assert.match(f.get("bcdr-content").textContent, /Restricted standby access/);
    assert.match(f.get("bcdr-content").textContent, /Authenticated recovery service principal/);
    assert.match(f.get("bcdr-content").textContent, /Reconcile interrupted operation/);
    assert.match(f.get("bcdr-content").textContent, /Configure qualified temporary attachments/);
    const forms = f.get("bcdr-content").querySelectorAll("form");
    const protection = forms[commands.findIndex((entry) => entry.name === "configure-protection")];
    const descriptor = protection.querySelectorAll("select").find((select) =>
      select.children.some((option) => option.textContent === "LakehouseProtection"));
    assert.ok(descriptor, "Independent Lakehouse must be a real public descriptor choice");
    descriptor.value = descriptor.children.find((option) => option.textContent === "LakehouseProtection").value;
    descriptor.handlers.change();
    const verified = protection.querySelectorAll("input").find((input) => input.name === "source_paths_verified_local");
    assert.ok(verified && !verified.checked, "Independent source-path attestation requires explicit confirmation");
    assert.match(protection.textContent, /snapshot qualification reference/);
    assert.match(protection.textContent, /Directories/);
    const replica = forms[commands.findIndex((entry) => entry.name === "configure-replica")];
    replica.querySelectorAll("button").find((button) => button.textContent === "Add attachments").click();
    assert.ok(replica.querySelectorAll("input").some((input) => input.name === "binding_sha256"));
    assert.ok(replica.querySelectorAll("input").some((input) => input.name === "enforcement_reference"));
  },
  async open_is_read_only(f) {
    f.get("bcdr-open").click();
    assert.equal(f.requests[0].url, "/api/bcdr/forms");
    await f.reply(0, { commands: [command] });
    assert.equal(f.requests.length, 1, "Opening must not resume capacity or read source/catalog");
    assert.equal(f.get("bcdr-panel").hidden, false);
    assert.equal(f.document.querySelector(".wizard-nav").hidden, true);
    assert.equal(f.document.activeElement, f.get("bcdr-title"));
    f.get("bcdr-back").click();
    assert.equal(f.document.querySelector(".wizard-nav").hidden, false);
    assert.equal(f.document.activeElement, f.get("bcdr-open"));
  },
  async recovery_login_skips_source_discovery(f) {
    f.get("login-workflow").value = "bcdr";
    const form = f.get("login-form");
    const login = form.handlers.submit({ preventDefault() {}, currentTarget: form });
    await f.reply(0, { sessionId: "recovery-session", principal: { client_id: "recovery-app" } });
    await login;
    assert.equal(f.requests[1].url, "/api/bcdr/forms");
    assert.ok(f.requests.every((request) =>
      !["/api/workspaces", "/api/capacities", "/api/scratch-workspaces"].includes(request.url)));
    await f.reply(1, { commands: [command] });
  },
  async returning_to_migration_cannot_discover_for_a_new_session(f) {
    f.product.bcdr.returnStage = "login";
    const back = f.get("bcdr-back").click();
    assert.equal(f.requests[0].url, "/api/capacities");
    f.ui.state.sessionId = "new-recovery-session";
    f.product.bcdr.sessionId = "new-recovery-session";
    await f.reply(0, { capacities: [] });
    await back;
    assert.equal(f.requests.length, 1, "An old navigation must not discover source workspaces in a new session");
  },
  async controls_pin_backend_ids(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrRenderResult(result);
    const form = f.get("bcdr-content").querySelector("form");
    const generation = form.querySelectorAll("input").find((input) => input.readOnly);
    assert.equal(generation.value, "generation-1");
    const group = form.querySelectorAll("input").find((input) => input.value === "group-1");
    assert.equal(group.checked, false, "Groups need explicit selection");
    group.checked = true;
    form.handlers.submit({ preventDefault() {} });
    const dialog = f.document.querySelector("dialog.bcdr-confirm") ||
      f.document.querySelectorAll("dialog").find((element) => element.className.includes("bcdr-confirm"));
    assert.equal(dialog.open, true);
    assert.equal(f.requests.length, 0, "Opening confirmation must not start work");
    const buttons = dialog.querySelectorAll("button");
    assert.equal(f.document.activeElement, buttons[0]);
    buttons[1].click();
    const payload = JSON.parse(f.requests[0].options.body);
    assert.equal(payload.confirmation, "synchronize");
    assert.equal(payload.request.generation_id, "generation-1");
    assert.deepEqual(payload.request.group_ids, ["group-1"]);
    assert.equal(payload.request.park, true);
    await f.reply(0, result);
  },
  async partial_is_not_ready(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrRenderResult(result);
    const output = f.get("bcdr-result").textContent;
    assert.match(output, /Partial/);
    assert.match(output, /Metadata applied: yes/);
    assert.match(output, /Data ready: no/);
    assert.match(output, /Ready for cutover: no/);
    assert.match(output, /off-region export/);
    assert.match(output, /Orders is unprotected/);
    assert.doesNotMatch(output, /all recovered/i);
  },
  async capacity_state_is_not_inferred_from_mode(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrRenderResult({
      ...result, mode: "standby", details: { capacities: [{
        capacity_id: "capacity-1", arm_resource_id: "/subscriptions/demo/providers/Microsoft.Fabric/capacities/dr",
        state: "Active", provisioning_state: "Succeeded", observed_at: "2026-09-17T18:00:00Z",
      }] },
    });
    assert.match(f.get("bcdr-result").textContent, /Active - dr/);
    assert.doesNotMatch(f.get("bcdr-result").textContent, /Paused - dr|capacity-1|\/subscriptions\//);
    assert.match(f.get("bcdr-result").textContent, /not an inference from standby mode/);
  },
  async service_errors_and_pending_controls(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrJourney("settings", "advanced");
    const form = f.get("bcdr-content").querySelector("form");
    const button = form.querySelector("button");
    const pending = f.product.bcdrSubmit(command, { generation_id: "pinned" }, button);
    assert.equal(button.disabled, true);
    assert.match(form.querySelector(".bcdr-action-progress").textContent, /does not cancel/);
    await f.reply(0, { detail: "CapacityNotActive: resume failed; retry the recorded operation." }, 409);
    await pending;
    assert.equal(button.disabled, false);
    assert.equal(f.get("bcdr-error").hidden, true);
    assert.match(form.querySelector(".bcdr-action-error").textContent, /CapacityNotActive/);
    assert.equal(f.document.activeElement, button);
  },
  async feedback_stays_with_the_action_and_marks_only_its_fields(f) {
    const schema = { type: "object", properties: { evidence: { type: "string" } } };
    f.product.bcdrRenderForms([{ ...command, schema, name: "plan" }, { ...command, schema }]);
    f.product.bcdrJourney("settings", "advanced");
    const forms = f.get("bcdr-content").querySelectorAll("form");
    const buttons = forms.map((form) => form.querySelector("button"));
    const first = f.product.bcdrSubmit(command, {}, buttons[0]);
    await f.reply(0, { detail: [{ loc: ["body", "request", "evidence"], msg: "Supply evidence", type: "missing" }] }, 422);
    await first;
    const error = forms[0].querySelector(".bcdr-action-error");
    assert.match(error.textContent, /Supply evidence/);
    const field = forms[0].querySelector("input");
    assert.equal(field["aria-errormessage"], error.id);
    assert.equal(field["aria-invalid"], "true");
    assert.notEqual(forms[1].querySelector("input")["aria-invalid"], "true");
    assert.equal(f.get("bcdr-error").hidden, true);
    const second = f.product.bcdrSubmit(command, {}, buttons[1]);
    await f.reply(1, { ...result, warnings: ["Control workspace: New owner has Admin access; review Manage access."] });
    await second;
    assert.equal(forms[1].querySelector(".bcdr-action-error").hidden, true);
    assert.match(forms[1].querySelector(".bcdr-action-warnings").textContent, /New owner/);
    assert.match(error.textContent, /Supply evidence/, "Another form must not clear this action's error");
    assert.equal(field["aria-invalid"], "true");
    assert.equal(f.document.activeElement, buttons[1]);
  },
  async native_validation_is_reported_at_the_action(f) {
    f.product.bcdrRenderForms([command]);
    const form = f.get("bcdr-content").querySelector("form");
    const button = form.querySelector("button");
    assert.equal(form.noValidate, true);
    form.checkValidity = () => false;
    const field = form.querySelector("input");
    field.name = "generation_id";
    field.validity = { valid: false };
    field.validationMessage = "Select a captured generation.";
    button.focus();
    form.handlers.submit({ preventDefault() {} });
    assert.match(form.querySelector(".bcdr-action-error").textContent, /Select a captured generation/);
    assert.equal(field["aria-errormessage"], form.querySelector(".bcdr-action-error").id);
    assert.equal(f.document.activeElement, button);
    assert.equal(f.requests.length, 0);
    assert.equal(f.get("bcdr-error").hidden, true);
  },
  async stale_session_never_renders_old_results(f) {
    f.product.bcdrRenderForms([command]);
    const button = f.get("bcdr-content").querySelector("button");
    const pending = f.product.bcdrSubmit(command, {}, button);
    f.ui.state.sessionId = "new-session";
    await f.reply(0, result);
    await pending;
    assert.equal(f.get("bcdr-result").hidden, true);
    assert.equal(f.get("bcdr-error").hidden, true);
  },
  async signout_clears_sensitive_results(f) {
    f.product.bcdrRenderForms([command]);
    f.product.bcdrRenderResult(result);
    const signout = f.get("sign-out").click();
    assert.equal(f.get("bcdr-content").textContent, "");
    assert.equal(f.product.bcdr.sessionId, null);
    assert.deepEqual(JSON.parse(JSON.stringify(f.product.bcdr.latest)), {});
    await f.reply(0, { ok: true });
    await signout;
    assert.equal(f.ui.state.sessionId, null);
    assert.equal(f.get("bcdr-open").hidden, true);
  },
  async signout_ignores_late_errors(f) {
    f.product.bcdrRenderForms([command]);
    const button = f.get("bcdr-content").querySelector("button");
    const pending = f.product.bcdrSubmit(command, {}, button);
    const signout = f.get("sign-out").click();
    assert.equal(f.product.bcdr.sessionId, null);
    await f.reply(0, { detail: "Old session catalog details" }, 409);
    await pending;
    assert.equal(f.get("bcdr-error").hidden, true);
    assert.equal(f.get("bcdr-progress").textContent, "");
    await f.reply(1, { ok: true });
    await signout;
  },
  async optional_object_is_omitted_unless_selected(f) {
    const field = f.product.bcdrField("storage", {
      anyOf: [{ type: "object", properties: { source_region: { type: "string" } }, required: ["source_region"] },
        { type: "null" }], default: null,
    }, {});
    assert.equal(field.read(), undefined);
    const checkbox = field.element.querySelector("input");
    checkbox.checked = true;
    checkbox.handlers.change();
    const region = field.element.querySelectorAll("input").find((input) => input.name === "source_region");
    region.value = "eastus";
    assert.equal(field.read().source_region, "eastus");
  },
  async schema_controls_are_native_not_json(f) {
    const field = f.product.bcdrField("setup", {
      type: "object", properties: {
        warehouse_name: { type: "string" },
        owners: { type: "array", items: { $ref: "#/$defs/Owner" } },
      }, required: ["warehouse_name"],
    }, { $defs: { Owner: {
      type: "object", properties: {
        object_id: { type: "string" }, role: { type: "string", enum: ["Admin", "Viewer"] },
      },
    } } });
    f.get("bcdr-content").appendChild(field.element);
    const inputs = field.element.querySelectorAll("input");
    inputs[0].value = "RecoveryCatalog";
    assert.equal(inputs[0].required, true);
    const add = field.element.querySelector("button");
    add.click();
    assert.ok(field.element.querySelector("select"));
    const owner = field.element.querySelectorAll("input").find((input) => input.name === "object_id");
    owner.value = "owner-id";
    const body = field.read();
    assert.equal(body.warehouse_name, "RecoveryCatalog");
    assert.equal(body.owners[0].object_id, "owner-id");
    assert.equal(field.element.querySelectorAll("textarea").length, 0);
  },
};

const name = process.argv[2];
assert.ok(scenarios[name], `Unknown BCDR UI check: ${name}`);
scenarios[name](setup()).then(() => console.log(`Passed ${name}`)).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
