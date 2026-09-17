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

const scenarios = {
  async shipped_schemas(f) {
    const commands = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
    f.product.bcdrRenderForms(commands);
    assert.equal(f.get("bcdr-content").querySelectorAll("form").length, commands.length);
    assert.ok(f.get("bcdr-content").querySelectorAll("input").length > 10);
    assert.ok(f.get("bcdr-content").querySelectorAll("select").length > 0);
    assert.match(f.get("bcdr-content").textContent, /Restricted standby access/);
    assert.match(f.get("bcdr-content").textContent, /Authenticated recovery service principal/);
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
