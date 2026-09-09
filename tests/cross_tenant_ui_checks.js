const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function fixture() {
  const requests = [], streams = [];
  class Element {
    constructor(tag) {
      this.tagName = tag; this.children = []; this.dataset = {}; this.handlers = {};
      this.className = ""; this._text = ""; this.value = ""; this.hidden = false;
      this.disabled = false; this.checked = false; this.open = false;
      this.classList = {
        add: (name) => { this.className += ` ${name}`; },
        remove: (name) => { this.className = this.className.split(" ").filter((n) => n !== name).join(" "); },
        toggle: () => {},
      };
    }
    set textContent(value) { this._text = String(value); this.children = []; }
    get textContent() { return this._text + this.children.map((child) => child.textContent).join(""); }
    set innerHTML(html) { this.children = []; this._text = ""; parse(html, this); }
    appendChild(child) { child.parentElement = this; this.children.push(child); return child; }
    replaceChildren(...children) { this.children = []; this._text = ""; children.forEach((c) => this.appendChild(c)); }
    setAttribute(name, value) {
      if (name === "class") this.className = value;
      else if (name.startsWith("data-")) this.dataset[name.slice(5)] = value;
      else if (["checked", "hidden", "disabled"].includes(name)) this[name] = true;
      else this[name] = value;
    }
    matches(selector) {
      if (selector.startsWith("#")) return this.id === selector.slice(1);
      if (selector.startsWith(".")) return this.className.split(" ").includes(selector.slice(1));
      return this.tagName === selector;
    }
    closest(selector) { return this.matches(selector) ? this : this.parentElement?.closest(selector); }
    querySelectorAll(selector) {
      const parts = selector.split(" "), results = [];
      const visit = (element) => element.children.forEach((child) => {
        if (child.matches(parts.at(-1)) && (parts.length === 1 || child.parentElement.closest(parts[0]))) results.push(child);
        visit(child);
      });
      visit(this);
      return results;
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    addEventListener(type, handler) { this.handlers[type] = handler; }
    focus() { document.activeElement = this; }
    showModal() { this.open = true; }
    close() { this.open = false; }
    scrollIntoView() {}
    remove() { this.parentElement.children = this.parentElement.children.filter((child) => child !== this); }
    click() { return this.handlers.click?.({ target: this, currentTarget: this }); }
    reset() {
      this.querySelectorAll("input").forEach((input) => { input.value = ""; input.checked = false; });
    }
  }
  function parse(html, root) {
    const stack = [root], voids = new Set(["input", "link", "meta", "br"]);
    for (const token of html.matchAll(/<!--[\s\S]*?-->|<\/([a-z0-9-]+)>|<([a-z0-9-]+)\b([^>]*)>|([^<]+)/gi)) {
      if (token[1]) {
        if (stack.at(-1).tagName === token[1]) stack.pop();
      } else if (token[2]) {
        const element = new Element(token[2]);
        for (const attr of token[3].matchAll(/([a-z-]+)(?:="([^"]*)")?/g)) element.setAttribute(attr[1], attr[2] || "");
        stack.at(-1).appendChild(element);
        if (!voids.has(element.tagName)) stack.push(element);
      } else if (token[4]) stack.at(-1)._text += token[4];
    }
  }
  const document = new Element("document");
  document.createElement = (tag) => new Element(tag);
  const root = path.join(__dirname, "..", "fabshuffle", "web");
  parse(fs.readFileSync(path.join(root, "templates", "index.html"), "utf8"), document);
  document.body = document.querySelector("body");
  document.activeElement = document.body;
  const context = vm.createContext({
    document, console, URLSearchParams, AbortController,
    setTimeout: () => 1, clearTimeout() {}, setInterval: () => 1, clearInterval() {},
    FormData: class {
      constructor(form) { this.form = form; }
      entries() { return this.form.querySelectorAll("input").filter((i) => i.name).map((i) => [i.name, i.value]); }
    },
    EventSource: class {
      constructor(url) { this.url = url; streams.push(this); }
      close() { this.closed = true; }
    },
    fetch(url, options) {
      return new Promise((resolve, reject) => requests.push({ url, options, resolve, reject }));
    },
  });
  vm.runInContext(fs.readFileSync(path.join(root, "static", "app.js"), "utf8") + `
    globalThis.ui = { state, loginBody, updateLoginMode, renderReview, renderIdentity, migrationBody,
      mappingOptions, addMappingRow, recheckMappings, setStartEnabled, resumeRun, prepareResume,
      invalidateMappings, loadResumable, loadConnectionOptions, watchRun, resetReadiness };`, context);
  const flush = async () => { for (let i = 0; i < 18; i++) await Promise.resolve(); };
  async function reply(index, data, status = 200) {
    requests[index].resolve({ ok: status < 400, status, text: async () => JSON.stringify(data) });
    await flush();
  }
  const get = (id) => document.querySelector(`#${id}`);
  const ui = context.ui;
  function paired() {
    Object.assign(ui.state, {
      sessionId: "bound-session", paired: true, crossTenant: true, mappingsChecked: true,
      capacity: { id: "capacity" }, workspace: { id: "workspace" },
      preview: {
        strategy: "rebuild", capacityName: "Destination", capacityRegion: "eastus",
        sourceWorkspaceName: "Source", targetWorkspaceName: "Source-copy", blockers: [],
        sourceTenantId: "source-tenant", targetTenantId: "target-tenant",
        sourceClientId: "source-app", targetClientId: "target-app",
        counts: [], unsupportedSummary: [], dependencies: [], largeSemanticModels: [],
      },
    });
    ui.renderReview();
    ui.setStartEnabled(true);
  }
  return { get, ui, document, requests, streams, reply, flush, paired };
}

const scenarios = {
  async login(f) {
    f.ui.state.forceRebuild = true;
    const form = f.get("login-form");
    form.querySelectorAll("input").filter((i) => i.name).forEach((input) => { input.value = input.name; });
    assert.equal(f.ui.loginBody(form).destination, undefined);
    f.get("another-tenant").checked = true;
    f.ui.updateLoginMode();
    assert.equal(f.get("destination-credentials").hidden, false);
    assert.equal(f.get("destination-credentials").disabled, false);
    assert.equal(f.get("source-credentials-label").textContent, "Source service principal");
    form.handlers.submit({ preventDefault() {}, currentTarget: form });
    assert.deepEqual(JSON.parse(f.requests[0].options.body).destination, {
      tenant_id: "destination_tenant_id", client_id: "destination_client_id", client_secret: "destination_client_secret",
    });
    await f.reply(0, {
      sessionId: "bound", paired: true, crossTenant: true,
      sourceTenantId: "source-tenant", targetTenantId: "target-tenant",
      principal: { client_id: "source-app" }, destinationPrincipal: { client_id: "target-app" },
    });
    assert.equal(form.querySelectorAll("input").find((i) => i.name === "destination_client_secret").value, "");
    await f.reply(1, { capacities: [] });
    assert.equal(f.ui.state.paired, true);
    assert.equal(f.ui.state.forceRebuild, false);
    assert.equal(f.get("restore-access-tool").hidden, true);
    assert.match(f.get("destination-context").textContent, /target-tenant/);
    assert.ok(f.requests.every((r) => r.url !== "/api/scratch-workspaces"));
    const resumable = f.requests.findIndex((r) => r.url === "/api/resumable");
    await f.reply(resumable, { runs: [] });
    assert.match(f.get("resumable-status").textContent, /No saved runs need retrying/);
  },
  async freeze_and_review(f) {
    f.paired();
    assert.equal(f.get("opt-permissions").disabled, true);
    assert.equal(f.get("opt-permissions").checked, false);
    assert.equal(f.get("write-freeze").hidden, false);
    assert.match(f.get("review-summary").textContent, /source-tenant.*source-app/s);
    assert.match(f.get("review-summary").textContent, /target-tenant.*target-app/s);
    assert.equal(f.get("start-run").disabled, true);
    f.get("opt-write-freeze").checked = true;
    f.get("opt-write-freeze").handlers.change();
    assert.equal(f.get("start-run").disabled, false);
    f.get("opt-write-freeze").checked = false;
    f.get("opt-data").checked = false;
    f.get("opt-files").checked = false;
    f.get("opt-files").handlers.change();
    assert.equal(f.get("start-run").disabled, false);
  },
  async same_tenant_pair_requires_freeze(f) {
    f.paired();
    f.ui.state.crossTenant = false;
    f.ui.state.preview.targetTenantId = f.ui.state.preview.sourceTenantId;
    f.ui.renderReview();
    f.ui.setStartEnabled(true);
    assert.equal(f.get("write-freeze").hidden, false);
    assert.equal(f.get("start-run").disabled, true);
    assert.equal(f.get("opt-permissions").disabled, true);
    assert.match(f.get("strategy-callout").textContent, /Rebuild with destination credentials/);
    f.get("opt-write-freeze").checked = true;
    f.get("opt-write-freeze").handlers.change();
    assert.equal(f.get("start-run").disabled, false);
    assert.equal(f.ui.migrationBody().strategy, "rebuild");
    assert.equal(f.ui.migrationBody().copy_permissions, false);
    f.ui.state.paired = false;
    f.get("opt-permissions").checked = true;
    f.ui.renderReview();
    assert.equal(f.get("opt-permissions").disabled, false);
    assert.equal(f.ui.migrationBody().copy_permissions, true);
  },
  async mappings_and_execution(f) {
    f.paired();
    f.get("opt-write-freeze").checked = true;
    const connection = f.ui.addMappingRow("connection").querySelectorAll("input");
    connection[0].value = "source-connection";
    assert.throws(() => f.ui.migrationBody(), /Complete every ID/);
    connection[1].value = "target-connection";
    const external = f.ui.addMappingRow("reference").querySelectorAll("input");
    ["source-ws", "source-item", "target-ws", "target-item"].forEach((id, index) => { external[index].value = id; });
    f.ui.recheckMappings();
    const check = JSON.parse(f.requests[0].options.body);
    assert.equal(check.connection_mappings["source-connection"], "target-connection");
    assert.equal(check.reference_mappings[0].target_item_id, "target-item");
    assert.equal(f.requests[0].options.headers["X-Fab-Shuffle-Session"], "bound-session");
    await f.reply(0, { blockers: [], dependencies: [], connectionAccess: null });
    assert.equal(f.get("start-run").disabled, false);
    f.get("start-run").click();
    assert.equal(f.requests[1].url, "/api/runs");
    assert.deepEqual(JSON.parse(f.requests[1].options.body), check);
    await f.reply(1, { runId: "new-run" });
    assert.match(f.streams[0].url, /new-run\/events\?session_id=bound-session/);
  },
  async stale_and_failed_assessment(f) {
    f.paired();
    f.get("opt-write-freeze").checked = true;
    f.ui.recheckMappings();
    f.ui.invalidateMappings();
    await f.reply(0, { blockers: [], dependencies: [], connectionAccess: null });
    assert.equal(f.get("start-run").disabled, true);
    f.ui.recheckMappings();
    await f.reply(1, { detail: "Destination ConnectionAccessDenied: share it with target-app" }, 403);
    assert.equal(f.get("start-run").disabled, true);
    assert.match(f.get("mapping-status").textContent, /ConnectionAccessDenied.*Re-check/);
    f.ui.recheckMappings();
    await f.reply(2, { blockers: ["Map source connection to an accessible destination"], dependencies: [] });
    assert.equal(f.get("start-run").disabled, true);
    assert.match(f.get("dependencies").textContent, /Map source connection/);
  },
  async paired_resume(f) {
    f.paired();
    f.ui.resumeRun("previous", f.get("retry-run"));
    assert.equal(f.requests[0].url, "/api/runs/previous/resume");
    assert.equal(f.requests[0].options.headers["X-Fab-Shuffle-Session"], "bound-session");
    await f.reply(0, { runId: "resumed", resumedFrom: "previous" });
    assert.equal(f.ui.state.runId, "resumed");
    assert.match(f.streams[0].url, /session_id=bound-session/);
  },
  async edit_mappings_before_retry(f) {
    f.paired();
    f.ui.state.runId = "previous";
    f.get("retry-run").click();
    assert.equal(f.requests[0].url, "/api/runs/previous/resume-plan");
    await f.reply(0, {
      plan: {
        ...f.ui.state.preview, capacityId: "capacity", sourceWorkspaceId: "workspace",
        includeData: false, includeFiles: false, copyPermissions: false,
        connectionMappings: { "source-connection": "old-destination-connection" },
        referenceMappings: [],
      },
      cleanupWhenDone: true,
      targetWorkspaceId: "created-workspace",
      items: [{ name: "Bronze", type: "Lakehouse", sourceId: "source-lakehouse", targetId: "new-lakehouse" }],
    });
    assert.match(f.get("resume-target-id").textContent, /created-workspace/);
    assert.match(f.get("resume-item-ids").textContent, /Bronze.*new-lakehouse/);
    assert.equal(f.get("target-name").disabled, true);
    assert.equal(f.get("opt-data").disabled, true);
    assert.equal(f.get("start-run").textContent, "Resume migration");
    assert.equal(f.requests[3].url, "/api/runs/previous/resume-preview");
    await f.reply(1, { connections: [] });
    await f.reply(2, { connections: [] });
    await f.reply(3, { dependencies: ["Create the replacement connection"], blockers: [], connectionAccess: null });
    const inputs = f.get("connection-mapping-rows").querySelectorAll("input");
    assert.equal(inputs[0].value, "source-connection");
    assert.equal(inputs[1].value, "old-destination-connection");
    inputs[1].value = "new-destination-connection";
    inputs[1].handlers.input();
    assert.equal(f.get("start-run").disabled, true);
    f.ui.recheckMappings();
    assert.deepEqual(JSON.parse(f.requests[4].options.body), {
      connection_mappings: { "source-connection": "new-destination-connection" }, reference_mappings: [],
    });
    await f.reply(4, { dependencies: [], blockers: [], connectionAccess: null });
    f.get("target-name").value = "must-not-change";
    f.get("start-run").click();
    assert.equal(f.requests[5].url, "/api/runs/previous/resume");
    assert.deepEqual(JSON.parse(f.requests[5].options.body), JSON.parse(f.requests[4].options.body));
    await f.reply(5, { runId: "resumed-with-mappings" });
    assert.equal(f.ui.state.runId, "resumed-with-mappings");
    assert.match(f.streams[0].url, /resumed-with-mappings\/events/);
  },
};

module.exports = { fixture };

if (require.main === module) {
  const name = process.argv[2];
  assert.ok(scenarios[name], `Unknown paired UI check: ${name}`);
  scenarios[name](fixture()).then(() => console.log(`Passed ${name}`)).catch((error) => {
    console.error(error); process.exitCode = 1;
  });
}
