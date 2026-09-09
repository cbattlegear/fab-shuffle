const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

// A deliberately small DOM fixture: real app code, controllable fetches and time,
// and native-looking elements sufficient for the wizard's readiness interactions.
function fixture() {
  let document;
  const downloads = [];
  class Element {
    constructor(tag) {
      this.tagName = tag.toLowerCase();
      this.children = [];
      this.dataset = {};
      this.attributes = {};
      this.handlers = {};
      this.className = "";
      this._text = "";
      this.hidden = false;
      this.disabled = false;
      this.open = false;
      this.classList = { toggle() {} };
    }
    set textContent(text) { this._text = String(text); this.children = []; }
    get textContent() { return this._text + this.children.map((child) => child.textContent).join(""); }
    set innerHTML(html) { this.replaceChildren(); parse(html, this); }
    appendChild(child) { child.parentElement = this; this.children.push(child); return child; }
    replaceChildren(...children) {
      this.children.forEach((child) => { child.parentElement = null; });
      this.children = [];
      this._text = "";
      children.forEach((child) => this.appendChild(child));
    }
    setAttribute(name, value) {
      this.attributes[name] = value;
      if (name === "class") this.className = value;
      if (name === "id") this.id = value;
      if (name.startsWith("data-")) {
        this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = value;
      }
      if (name === "hidden") this.hidden = true;
    }
    matches(selector) {
      if (selector.startsWith("#")) return this.id === selector.slice(1);
      if (selector.startsWith(".")) return this.className.split(" ").includes(selector.slice(1));
      if (selector === "[data-readiness-key]") return this.dataset.readinessKey !== undefined;
      return this.tagName === selector;
    }
    querySelectorAll(selector) {
      const parts = selector.split(" ");
      const results = [];
      const visit = (element) => {
        element.children.forEach((child) => {
          if (child.matches(parts[parts.length - 1]) &&
              (parts.length === 1 || child.parentElement.closest(parts[0]))) results.push(child);
          visit(child);
        });
      };
      visit(this);
      return results;
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    closest(selector) { return this.matches(selector) ? this : this.parentElement?.closest(selector); }
    contains(element) { return element === this || this.children.some((child) => child.contains(element)); }
    focus() { document.activeElement = this; }
    scrollIntoView() {}
    addEventListener(type, handler) { this.handlers[type] = handler; }
    click() {
      if (this.tagName === "a") downloads.push({ href: this.href, download: this.download });
      return this.handlers.click?.({ target: this, currentTarget: this });
    }
    remove() { this.parentElement.children = this.parentElement.children.filter((child) => child !== this); }
  }
  function parse(html, root) {
    const stack = [root];
    const voids = new Set(["input", "link", "meta", "br"]);
    const tokens = html.matchAll(/<!--[\s\S]*?-->|<\/([a-z0-9-]+)>|<([a-z0-9-]+)\b([^>]*)>|([^<]+)/gi);
    for (const token of tokens) {
      if (token[1]) {
        if (stack.length > 1 && stack[stack.length - 1].tagName === token[1]) stack.pop();
      } else if (token[2]) {
        const element = new Element(token[2]);
        for (const attr of token[3].matchAll(/([a-z-]+)(?:="([^"]*)")?/g)) {
          element.setAttribute(attr[1], attr[2] ?? "");
        }
        stack[stack.length - 1].appendChild(element);
        if (!voids.has(element.tagName)) stack.push(element);
      } else if (token[4]) stack[stack.length - 1]._text += token[4];
    }
  }
  document = new Element("document");
  document.createElement = (tag) => new Element(tag);
  const root = path.join(__dirname, "..", "fabshuffle", "web");
  parse(fs.readFileSync(path.join(root, "templates", "index.html"), "utf8"), document);
  document.body = document.querySelector("body");
  document.activeElement = document.body;
  const requests = [];
  const streams = [];
  const timers = new Map();
  let now = 0;
  let nextTimer = 0;
  let exportedBlob;
  const revoked = [];
  const context = vm.createContext({
    document, console, AbortController,
    Date: class extends Date { static now() { return now; } },
    setTimeout(callback, delay) {
      const id = ++nextTimer;
      timers.set(id, { callback, at: now + delay });
      return id;
    },
    clearTimeout(id) { timers.delete(id); },
    EventSource: class {
      constructor(url) { this.url = url; streams.push(this); }
      close() { this.closed = true; }
    },
    URL: {
      createObjectURL(blob) { exportedBlob = blob; return "blob:readiness"; },
      revokeObjectURL(url) { revoked.push(url); },
    },
    fetch(url, options) {
      return new Promise((resolve, reject) => requests.push({ url, options, resolve, reject }));
    },
  });
  vm.runInContext(fs.readFileSync(path.join(root, "static", "app.js"), "utf8") + `
    globalThis.ui = { state, resetReadiness, observeReadiness, renderReadinessItems,
      renderReadinessStatus, watchRun, renderRun, scheduleReadiness };`, context);
  const ui = context.ui;
  const get = (id) => document.querySelector(`#${id}`);
  const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
  async function tick(ms) {
    const end = now + ms;
    while (true) {
      const next = Array.from(timers.entries()).filter(([, timer]) => timer.at <= end)
        .sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break;
      now = next[1].at;
      timers.delete(next[0]);
      next[1].callback();
      await flush();
    }
    now = end;
    await flush();
  }
  async function reply(index, data, status = 200) {
    requests[index].resolve({
      ok: status >= 200 && status < 300, status,
      text: async () => JSON.stringify(data),
      blob: async () => data,
    });
    await flush();
  }
  function begin(id = "one", status = "succeeded") {
    ui.state.sessionId = "private-session";
    ui.state.runId = id;
    ui.resetReadiness(id);
    if (status) ui.observeReadiness({ status, readinessRevision: 0 });
  }
  function change(id, value, type = "input") {
    const element = get(id);
    element.value = value;
    return element.handlers[type]({ target: element });
  }
  return {
    ui, get, document, requests, streams, downloads, revoked, timers, flush, tick, reply, begin, change,
    exportedBlob: () => exportedBlob,
  };
}

function item(index, overrides = {}) {
  return {
    sourceId: `source-${index}`, targetId: `target-${index}`,
    sourceWorkspaceId: "source-workspace", targetWorkspaceId: "target-workspace",
    name: `Item ${index}`, itemType: "Lakehouse", disposition: "created",
    state: ["ready", "needs_attention", "unknown"][index % 3],
    reasons: [`Reason ${index}`], actions: [`Action ${index}`],
    unresolvedReferences: [], steps: [], history: [], ...overrides,
  };
}

function report(runId = "one", items = [item(0)], overrides = {}) {
  const counts = { ready: 0, needs_attention: 0, unknown: 0 };
  items.forEach((entry) => { counts[entry.state] = (counts[entry.state] || 0) + 1; });
  return {
    schemaVersion: 1, runId, lineageId: "lineage", runStatus: "succeeded",
    state: "needs_attention", counts, limits: ["Recorded evidence only; verify the target."],
    items, ...overrides,
  };
}

function connectionEntry(overrides = {}) {
  return {
    connectionId: "conn-1", connectionName: "Bronze SQL", connectivityType: "ShareableCloud",
    type: "SQL", path: "src.example.com;bronze", matchedSourceItems: ["item-1"],
    matchBasis: "sql_server_database", usageState: "not_checked", ...overrides,
  };
}

function advisories(overrides = {}) {
  return {
    scanState: "complete", sourceWorkspaceId: "src-ws", targetWorkspaceId: "tgt-ws",
    generatedAt: "2024-01-01T00:00:00Z", connections: [connectionEntry()],
    message: "1 tenant-visible connection(s) still reference the source workspace.",
    limits: ["Scope is limited by literal metadata matching; not proven exhaustive."],
    ...overrides,
  };
}

const scenarios = {
  async hidden_until_halted(f) {
    f.begin("one", null);
    assert.equal(f.get("cutover-readiness").hidden, true);
    assert.equal(f.get("readiness-jump").parentElement.hidden, true);
    assert.equal(f.get("readiness-export").disabled, true);
    assert.equal(f.get("connection-advisories").hidden, true);
    await f.tick(1000);
    for (const status of ["pending", "running"]) {
      f.ui.observeReadiness({ status, readinessRevision: 1, cancelled: true });
      f.ui.scheduleReadiness(true);
      await f.tick(1000);
      assert.equal(f.requests.length, 0);
      assert.equal(f.get("cutover-readiness").hidden, true);
      assert.equal(f.get("connection-advisories").hidden, true);
    }
    for (const status of ["succeeded", "failed", "cancelled", "interrupted"]) {
      const index = f.requests.length;
      f.begin(status, null);
      f.ui.observeReadiness({ status, readinessRevision: 1 });
      assert.equal(f.get("cutover-readiness").hidden, false);
      assert.equal(f.get("readiness-jump").parentElement.hidden, false);
      await f.tick(0);
      assert.equal(f.requests.length, index + 1);
      await f.reply(index, report(status, [item(0)], { runStatus: status }));
      assert.equal(f.get("readiness-export").disabled, false);
      // No connectionAdvisories field on this report: shown, but as Unknown, not a false green.
      assert.equal(f.get("connection-advisories").hidden, false);
      assert.equal(f.get("connection-advisories-status").textContent.trim(), "Unknown");
    }
  },
  async active_again(f) {
    f.begin();
    await f.tick(0);
    f.ui.observeReadiness({ status: "running", readinessRevision: 1 });
    assert.equal(f.requests[0].options.signal.aborted, true);
    await f.reply(0, report());
    await f.tick(1000);
    assert.equal(f.requests.length, 1);
    assert.equal(f.ui.state.readiness.report, null);
    assert.equal(f.get("cutover-readiness").hidden, true);
    f.ui.observeReadiness({ status: "failed", readinessRevision: 2 });
    await f.tick(0);
    await f.reply(1, report("one", [item(0)], { runStatus: "failed" }));
    assert.equal(f.get("readiness-items").children.length, 1);
    f.ui.observeReadiness({ status: "running", readinessRevision: 3 });
    assert.equal(f.get("readiness-items").children.length, 0);
    assert.equal(f.get("readiness-jump").parentElement.hidden, true);
    await f.get("readiness-export").click();
    assert.equal(f.requests.length, 2);
    assert.equal(f.ui.state.runId, "one");
    assert.equal(f.ui.state.sessionId, "private-session");
  },
  async target_is_not_spinner(f) {
    f.begin("one", null);
    const name = '<img src=x onerror="alert(1)"> workspace';
    for (const status of ["running", "pending", "succeeded", "failed", "cancelled"]) {
      f.ui.renderRun({
        id: "one", status, steps: [], summary: {},
        targetWorkspace: { displayName: name }, readinessRevision: 1,
      });
      const banner = f.get("run-banner");
      const target = banner.querySelector(".run-target");
      assert.equal(target.textContent, ` — target workspace: ${name}`);
      assert.equal(target.className.split(" ").includes("spin"), false);
      assert.equal(target.querySelector("img"), null);
      const spinner = banner.querySelector(".spin");
      if (status === "running") {
        assert.equal(spinner.textContent, "◐");
        assert.equal(spinner.attributes["aria-hidden"], "true");
      } else assert.equal(spinner, null);
    }
    f.ui.renderRun({ id: "one", status: "running", steps: [], summary: {}, readinessRevision: 2 });
    assert.equal(f.get("run-banner").querySelector(".spin").textContent, "◐");
    assert.equal(f.get("run-banner").querySelector(".run-target").textContent, "");
  },
  async throttle(f) {
    f.begin();
    assert.equal(f.get("readiness-loading").hidden, false);
    await f.tick(0);
    assert.equal(f.requests.length, 1);
    assert.equal(f.requests[0].options.headers["X-Fab-Shuffle-Session"], "private-session");
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 2 });
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 3 });
    await f.reply(0, report("one", [item(0, { name: "stale" })]));
    assert.equal(f.ui.state.readiness.report, null);
    await f.tick(499);
    assert.equal(f.requests.length, 1);
    await f.tick(1);
    await f.reply(1, report());
    assert.equal(f.get("readiness-items").children.length, 1);
    assert.equal(f.get("readiness-loading").hidden, true);
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 3 });
    await f.tick(2000);
    assert.equal(f.requests.length, 2);
  },
  async terminal(f) {
    f.begin("one", null);
    f.ui.observeReadiness({ status: "running", readinessRevision: 7 });
    await f.tick(500);
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 7 });
    assert.equal(f.requests.length, 0);
    await f.tick(0);
    assert.equal(f.requests.length, 1);
    await f.reply(0, report("one", [item(0)], { state: "unknown", runStatus: "succeeded" }));
    assert.equal(f.get("readiness-overall").textContent, "Unknown");
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 7 });
    await f.tick(1000);
    assert.equal(f.requests.length, 1);
  },
  async run_races(f) {
    f.begin("one", null);
    f.ui.watchRun("one");
    const oldStream = f.streams[0];
    oldStream.onmessage({ data: JSON.stringify({
      id: "one", status: "succeeded", steps: [], summary: {}, readinessRevision: 1,
    }) });
    await f.tick(0);
    oldStream.onerror();
    assert.equal(f.requests[1].url, "/api/runs/one");
    f.ui.watchRun("two");
    assert.equal(f.requests[0].options.signal.aborted, true);
    oldStream.onmessage({ data: JSON.stringify({ id: "one", status: "succeeded" }) });
    await f.reply(0, report("one"));
    await f.reply(1, { id: "one", status: "succeeded" });
    assert.equal(f.ui.state.readiness.runId, "two");
    assert.equal(f.ui.state.readiness.runStatus, null);
    assert.equal(f.ui.state.readiness.report, null);
    await f.tick(500);
    assert.equal(f.requests.length, 2);
    f.streams[1].onmessage({ data: JSON.stringify({
      id: "two", status: "succeeded", steps: [], summary: {}, readinessRevision: 1,
    }) });
    await f.tick(0);
    await f.reply(2, report("two"));
    assert.equal(f.ui.state.readiness.report.runId, "two");
    await f.get("start-over").click();
    assert.equal(f.ui.state.readiness, null);
    assert.equal(f.get("cutover-readiness").hidden, true);
  },
  async filters(f) {
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", Array.from({ length: 62 }, (_, index) => item(index))));
    assert.equal(f.get("readiness-items").children.length, 25);
    assert.equal(f.get("readiness-page").textContent, "Page 1 of 3");
    f.get("readiness-next").click();
    assert.equal(f.get("readiness-items").children[0].dataset.readinessKey, "source:source-25");
    assert.equal(f.document.activeElement, f.get("readiness-results"));
    f.change("readiness-filter", "unknown", "change");
    assert.equal(f.ui.state.readiness.page, 1);
    assert.equal(f.get("readiness-items").children.length, 20);
    assert.equal(f.get("readiness-pagination").hidden, true);
    f.change("readiness-search", "Item 61");
    assert.equal(f.get("readiness-items").children.length, 0);
    assert.match(f.get("readiness-empty").textContent, /No items match/);
    f.change("readiness-filter", "all", "change");
    assert.equal(f.get("readiness-items").children.length, 1);
    assert.match(f.get("readiness-results").textContent, /62 total/);
    assert.match(f.get("readiness-counts").textContent, /21 ready/);
    f.change("readiness-search", "Action 0");
    assert.equal(f.get("readiness-items").children.length, 1);
  },
  async safe_text_and_focus(f) {
    const hostile = '<img src=x onerror="alert(1)">';
    const data = item(1, {
      name: hostile, reasons: [hostile], actions: [hostile], unresolvedReferences: [hostile],
      steps: [{ step: hostile, state: "failed", reason: hostile, action: hostile,
        errorCode: "FabricRejected", message: hostile, attemptId: "attempt-1" }],
    });
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", [data]));
    const row = f.get("readiness-items").children[0];
    assert.equal(row.querySelector("h4").textContent, hostile);
    assert.equal(row.querySelector("img"), null);
    assert.match(row.textContent, /Error code: FabricRejected/);
    const details = row.querySelector("details");
    details.open = true;
    details.querySelector("summary").focus();
    f.ui.state.readiness.report = report("one", [item(1, { targetId: "new-target" })]);
    f.ui.renderReadinessItems();
    const replacement = f.get("readiness-items").children[0];
    assert.equal(replacement.querySelector("details").open, true);
    assert.equal(f.document.activeElement, replacement.querySelector("summary"));
    f.ui.state.readiness.report = report("one", [item(1, { state: "__proto__", reasons: [], actions: [] })]);
    f.ui.renderReadinessItems();
    assert.match(f.get("readiness-items").textContent, /Unknown/);
    assert.match(f.get("readiness-items").textContent, /Verify this item/);
  },
  async connection_advisories_render(f) {
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", [item(0)], { connectionAdvisories: advisories() }));
    assert.equal(f.get("connection-advisories").hidden, false);
    assert.equal(f.get("connection-advisories-status").textContent.trim(),
      "Reviewed 1 tenant-visible connection(s) still reference the source workspace.");
    assert.equal(f.get("connection-advisories-scope").hidden, false);
    assert.equal(f.get("connection-advisories-limits").children.length, 1);
    assert.match(f.get("connection-advisories-limits").textContent, /not proven exhaustive/);
    const rows = f.get("connection-advisories-items").children;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].querySelector("h5").textContent, "Bronze SQL");
    assert.match(rows[0].textContent, /ID: conn-1/);
    assert.match(rows[0].textContent, /ShareableCloud/);
    assert.match(rows[0].textContent, /src\.example\.com;bronze/);
    assert.match(rows[0].textContent, /item-1/);
    assert.match(rows[0].textContent, /Matched SQL server and database together/);
    assert.match(rows[0].textContent, /Current consumers have not been checked/);
    assert.match(rows[0].textContent, /not proof that manual recreation is needed/);
    assert.equal(f.get("connection-advisories-empty").hidden, true);
    assert.equal(f.get("connection-lookup-script").hidden, false);

    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 2 });
    await f.tick(500);
    await f.reply(1, report("one", [item(0)], {
      connectionAdvisories: advisories({ connections: [], message: "No tenant-visible connection's path was found to reference the source workspace." }),
    }));
    assert.equal(f.get("connection-advisories-items").children.length, 0);
    assert.equal(f.get("connection-advisories-empty").hidden, false);
    assert.match(f.get("connection-advisories-empty").textContent, /No tenant-visible connection/);
    // Zero matches is not the same as nothing to look up: the script still falls back to
    // listing every visible connection, so it stays available.
    assert.equal(f.get("connection-lookup-script").hidden, false);
  },
  async connection_advisories_states(f) {
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", [item(0)], {
      connectionAdvisories: advisories({ scanState: "not_applicable", connections: [], message: "Reassignment moves the existing workspace." }),
    }));
    assert.equal(f.get("connection-advisories-status").textContent.trim(),
      "Not applicable Reassignment moves the existing workspace.");
    assert.equal(f.get("connection-advisories-empty").hidden, true);
    // A reassign never scans, and there is genuinely nothing to look up by hand either.
    assert.equal(f.get("connection-lookup-script").hidden, true);

    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 2 });
    await f.tick(500);
    await f.reply(1, report("one", [item(0)], {
      connectionAdvisories: advisories({
        scanState: "stale", message: "This scan is from an earlier attempt and has not been refreshed by this one.",
      }),
    }));
    assert.match(f.get("connection-advisories-status").textContent, /^Stale/);
    assert.match(f.get("connection-advisories-status").textContent, /earlier attempt/);
    assert.equal(f.get("connection-lookup-script").hidden, false);

    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 3 });
    await f.tick(500);
    await f.reply(2, report("one", [item(0)], {
      connectionAdvisories: advisories({
        scanState: "incomplete", connections: [],
        message: "The tenant's connections could not be listed.",
      }),
    }));
    assert.match(f.get("connection-advisories-status").textContent, /^Incomplete/);
    assert.equal(f.get("connection-advisories-empty").hidden, false);
    assert.match(f.get("connection-advisories-empty").textContent, /lists every connection/);
    assert.equal(f.get("connection-lookup-script").hidden, false);

    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 4 });
    await f.tick(500);
    await f.reply(3, report("one", [item(0)], {
      connectionAdvisories: advisories({ scanState: "unknown", connections: [], message: "", limits: [] }),
    }));
    assert.match(f.get("connection-advisories-status").textContent, /^Unknown/);
    assert.equal(f.get("connection-advisories-scope").hidden, true);
    assert.equal(f.get("connection-lookup-script").hidden, false);
  },
  async connection_advisories_personal_and_retired(f) {
    f.begin();
    await f.tick(500);
    const action = "Personal cloud connection; review semantic model bindings, not automatic recreation.";
    await f.reply(0, report("one", [], { connectionAdvisories: advisories({
      connections: [connectionEntry({ connectivityType: "PersonalCloud", action })],
    }) }));
    assert.match(f.get("connection-advisories-items").textContent, /review semantic model bindings/);
    assert.doesNotMatch(f.get("connection-advisories-items").textContent, /Repoint or recreate this connection/);
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 2 });
    await f.tick(500);
    await f.reply(1, report("one", [], { connectionAdvisories: advisories({
      scanState: "stale", connections: [],
      message: "The retired matcher results have been withheld, not revalidated.",
      action: "Use the lookup script to inspect server/database pairs.",
    }) }));
    assert.equal(f.get("connection-advisories-items").children.length, 0);
    assert.match(f.get("connection-advisories-status").textContent, /^Stale/);
    assert.match(f.get("connection-advisories-action").textContent, /server\/database pairs/);
    assert.match(f.get("connection-advisories-empty").textContent, /No verified connection matches/);
    assert.equal(f.get("connection-lookup-script").hidden, false);
  },
  async connection_advisories_safe_text(f) {
    const hostile = '<img src=x onerror="alert(1)">';
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", [item(0)], {
      connectionAdvisories: advisories({
        connections: [connectionEntry({
          connectionId: hostile, connectionName: hostile, path: hostile, matchedSourceItems: [hostile],
        })],
      }),
    }));
    const row = f.get("connection-advisories-items").children[0];
    assert.equal(row.querySelector("h5").textContent, hostile);
    assert.equal(row.querySelector("img"), null);
    assert.match(row.textContent, new RegExp(`ID: ${hostile.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`));
  },
  async connection_lookup_script_flow(f) {
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", [item(0)], { connectionAdvisories: advisories() }));
    assert.equal(f.get("connection-lookup-script").hidden, false);

    const fetching = f.get("connection-lookup-fetch").click();
    assert.equal(f.requests[1].url, "/api/runs/one/connections/script");
    await f.reply(1, { text: async () => "#Requires -Version 7.0\nWrite-Host 'hi'" });
    await fetching;
    assert.equal(f.get("connection-lookup-pre").hidden, false);
    assert.match(f.get("connection-lookup-pre").querySelector("code").textContent, /Requires -Version 7/);
    assert.equal(f.get("connection-lookup-download").href, "blob:readiness");
    assert.match(f.get("connection-lookup-download").download, /connection-lookup-one\.ps1/);
    assert.equal(f.get("connection-lookup-fetch").disabled, false);

    const failing = f.get("connection-lookup-fetch").click();
    assert.equal(f.get("connection-lookup-fetch").disabled, true);
    await f.reply(2, { detail: "No connection advisory results are recorded for this run to look up." }, 404);
    await failing;
    assert.match(f.get("connection-lookup-error").textContent, /No connection advisory results/);
    assert.equal(f.get("connection-lookup-error").hidden, false);
    assert.equal(f.get("connection-lookup-fetch").disabled, false);
    f.ui.resetReadiness("two");
    assert.deepEqual(f.revoked, ["blob:readiness"]);
  },
  async connection_lookup_races(f) {
    f.begin();
    await f.tick(500);
    await f.reply(0, report("one", [item(0)], { connectionAdvisories: advisories() }));
    const fetching = f.get("connection-lookup-fetch").click();
    let resolveText;
    await f.reply(1, { text: () => new Promise((resolve) => { resolveText = resolve; }) });
    f.begin("two", "running");
    assert.equal(f.requests[1].options.signal.aborted, true);
    resolveText("# Stale script from the previous run");
    await fetching;
    assert.equal(f.get("connection-lookup-pre").hidden, true);
    assert.equal(f.get("connection-lookup-pre").querySelector("code").textContent, "");
    assert.equal(f.get("connection-lookup-download").href, "");
    assert.equal(f.get("connection-advisories").hidden, true);
    const count = f.requests.length;
    await f.get("connection-lookup-fetch").click();
    assert.equal(f.requests.length, count);
  },
  async errors_and_empty(f) {
    f.begin();
    await f.tick(500);
    await f.reply(0, { detail: "SessionExpired: Sign in again." }, 401);
    assert.equal(f.get("readiness-overall").textContent, "Unknown");
    assert.equal(f.get("readiness-error").textContent, "SessionExpired: Sign in again.");
    assert.equal(f.get("readiness-retry").hidden, false);
    assert.equal(f.get("readiness-loading").hidden, true);
    f.get("readiness-retry").click();
    await f.tick(0);
    await f.reply(1, report("one", [], { state: "unknown" }));
    assert.equal(f.get("readiness-empty").hidden, false);
    assert.match(f.get("readiness-empty").textContent, /No item evidence/);
    assert.equal(f.get("readiness-error").hidden, true);
    f.ui.observeReadiness({ status: "succeeded", readinessRevision: 2 });
    await f.tick(500);
    await f.reply(2, report("wrong-run"));
    assert.match(f.get("readiness-error").textContent, /unsupported format/);
    assert.match(f.get("readiness-status").textContent, /last loaded snapshot/);
    assert.equal(f.ui.state.readiness.report.items.length, 0);
  },
  async export(f) {
    f.begin();
    await f.tick(500);
    const full = report("one", [item(0), item(1), item(2)]);
    await f.reply(0, full);
    f.change("readiness-filter", "ready", "change");
    assert.equal(f.get("readiness-items").children.length, 1);
    const exporting = f.get("readiness-export").click();
    assert.equal(f.get("readiness-export").disabled, true);
    assert.equal(f.requests[1].url, "/api/runs/one/readiness?download=true");
    assert.equal(f.requests[1].options.headers["X-Fab-Shuffle-Session"], "private-session");
    await f.reply(1, full);
    await exporting;
    assert.equal(f.exportedBlob().items.length, 3);
    assert.equal(f.downloads[0].download, "fab-shuffle-readiness-one.json");
    await f.tick(1000);
    assert.deepEqual(f.revoked, ["blob:readiness"]);
    const failed = f.get("readiness-export").click();
    await f.reply(2, { detail: "DownloadUnavailable: retry later." }, 503);
    await failed;
    assert.match(f.get("readiness-download-error").textContent, /DownloadUnavailable: retry later/);
    assert.equal(f.get("readiness-export").disabled, false);
    const abandoned = f.get("readiness-export").click();
    f.ui.watchRun("two");
    assert.equal(f.requests[3].options.signal.aborted, true);
    await f.reply(3, full);
    await abandoned;
    assert.equal(f.downloads.length, 1);
  },
  async sign_out(f) {
    f.begin();
    await f.tick(500);
    const logout = f.get("sign-out").click();
    await f.reply(1, {});
    await logout;
    assert.equal(f.ui.state.sessionId, null);
    assert.equal(f.ui.state.readiness, null);
    assert.equal(f.requests[0].options.signal.aborted, true);
    await f.reply(0, report());
    assert.equal(f.get("readiness-items").children.length, 0);
    assert.equal(f.get("cutover-readiness").hidden, true);
  },
};

const scenario = process.argv[2];
assert.ok(Object.hasOwn(scenarios, scenario), `Unknown UI check: ${scenario}`);
scenarios[scenario](fixture()).catch((error) => { console.error(error); process.exitCode = 1; });
