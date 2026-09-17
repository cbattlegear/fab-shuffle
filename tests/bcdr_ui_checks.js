const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { fixture } = require("./cross_tenant_ui_checks");

function setup() {
  const f = fixture();
  vm.runInContext(fs.readFileSync(path.join(__dirname, "..", "fabshuffle", "web", "static", "bcdr.js"), "utf8") + `
    globalThis.product = { bcdr, bcdrField, bcdrRenderForms, bcdrRenderResult, bcdrSubmit, bcdrConfirm };
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

const scenarios = {
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
    assert.match(f.get("bcdr-result").textContent, /Active - capacity-1/);
    assert.doesNotMatch(f.get("bcdr-result").textContent, /Paused - capacity-1/);
    assert.match(f.get("bcdr-result").textContent, /not an inference from standby mode/);
  },
  async service_errors_and_pending_controls(f) {
    f.product.bcdrRenderForms([command]);
    const button = f.get("bcdr-content").querySelector("button");
    const pending = f.product.bcdrSubmit(command, { generation_id: "pinned" }, button);
    assert.equal(button.disabled, true);
    assert.match(f.get("bcdr-progress").textContent, /does not cancel/);
    await f.reply(0, { detail: "CapacityNotActive: resume failed; retry the recorded operation." }, 409);
    await pending;
    assert.equal(button.disabled, false);
    assert.equal(f.get("bcdr-error").hidden, false);
    assert.match(f.get("bcdr-error").textContent, /CapacityNotActive/);
    assert.equal(f.document.activeElement, f.get("bcdr-error"));
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
