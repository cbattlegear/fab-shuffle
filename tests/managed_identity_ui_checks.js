const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { fixture } = require("./cross_tenant_ui_checks");

const options = {
  easyAuth: true,
  operator: { tenantId: "tenant", objectId: "operator" },
  managedIdentity: { tenant_id: "tenant", client_id: "managed-client" },
  servicePrincipal: true,
};

function load(f) {
  vm.runInContext(fs.readFileSync(
    path.join(__dirname, "..", "fabshuffle", "web", "static", "auth.js"), "utf8",
  ), f.context);
}

const scenarios = {
  async availability_and_selection(f) {
    load(f);
    assert.equal(f.requests[0].url, "/api/auth/options");
    assert.equal(f.get("managed-identity-option").disabled, true);
    await f.reply(0, options);
    assert.equal(f.get("managed-identity-option").disabled, false);
    assert.equal(f.get("azure-sign-out").hidden, false);
    assert.match(f.get("operator-context").textContent, /operator/);
    f.get("another-tenant").checked = true;
    f.get("fabric-auth-mode").value = "managed_identity";
    f.ui.updateLoginMode();
    assert.equal(f.get("source-credentials").disabled, true);
    assert.equal(f.get("source-credentials").hidden, true);
    assert.equal(f.get("tenant-mode").disabled, true);
    assert.equal(f.get("another-tenant").checked, false);
    assert.equal(f.get("destination-credentials").disabled, true);
    assert.equal(f.get("managed-identity-context").hidden, false);
    assert.match(f.get("managed-identity-context").textContent, /managed-client/);
    f.get("fabric-auth-mode").value = "service_principal";
    f.ui.updateLoginMode();
    assert.equal(f.get("source-credentials").disabled, false);
    assert.equal(f.get("tenant-mode").disabled, false);
    assert.equal(f.get("managed-identity-context").hidden, true);
  },
  async managed_login_is_secretless(f) {
    load(f);
    await f.reply(0, options);
    f.get("fabric-auth-mode").value = "managed_identity";
    f.get("login-workflow").value = "migration";
    f.ui.updateLoginMode();
    const form = f.get("login-form");
    form.querySelectorAll("input").forEach((input) => { input.value = "must-not-be-submitted"; });
    const pending = form.handlers.submit({ preventDefault() {}, currentTarget: form });
    assert.equal(f.requests[1].url, "/api/login/managed-identity");
    assert.deepEqual(JSON.parse(f.requests[1].options.body), {});
    assert.equal(f.requests[1].options.headers["X-Fab-Shuffle-Request"], "1");
    await f.reply(1, {
      sessionId: "managed-session", principal: options.managedIdentity,
      managedIdentity: true, paired: false, crossTenant: false,
    });
    assert.equal(f.requests[2].url, "/api/capacities");
    await f.reply(2, { capacities: [] });
    await pending;
    assert.equal(f.ui.state.sessionId, "managed-session");
    assert.equal(f.ui.state.paired, false);
    assert.equal(f.ui.state.crossTenant, false);
    assert.equal(form.querySelectorAll("input").find((input) => input.name === "client_secret").value, "");
  },
  async local_and_failed_options_cannot_enable_identity(f) {
    load(f);
    await f.reply(0, { easyAuth: false, operator: null, managedIdentity: null, servicePrincipal: true });
    assert.equal(f.get("managed-identity-option").disabled, true);
    assert.equal(f.get("azure-sign-out").hidden, true);
    f.get("fabric-auth-mode").value = "managed_identity";
    const form = f.get("login-form");
    await form.handlers.submit({ preventDefault() {}, currentTarget: form });
    assert.equal(f.requests.length, 1, "A manipulated selector cannot invoke ambient identity");
    assert.match(f.get("alert").textContent, /not available/);
    const pending = vm.runInContext("loadAuthenticationOptions()", f.context);
    await f.reply(1, { detail: "Identity configuration is incomplete" }, 503);
    await pending;
    assert.equal(f.ui.state.managedIdentityAvailable, false);
    assert.equal(f.get("fabric-auth-mode").value, "service_principal");
    assert.match(f.get("auth-options-status").textContent, /configuration is incomplete/);
  },
  async expired_easyauth_is_actionable(f) {
    load(f);
    await f.reply(0, options);
    f.get("fabric-auth-mode").value = "managed_identity";
    const form = f.get("login-form");
    const pending = form.handlers.submit({ preventDefault() {}, currentTarget: form });
    f.requests[1].resolve({
      ok: true, status: 200, redirected: true,
      text: async () => "<html>Microsoft sign in</html>",
    });
    await pending;
    assert.match(f.get("alert").textContent, /Azure sign-in has expired/);
    assert.equal(f.ui.state.sessionId, null);
  },
};

const name = process.argv[2];
assert.ok(scenarios[name], `Unknown managed identity UI scenario: ${name}`);
scenarios[name](fixture()).then(() => console.log(`Passed ${name}`)).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
