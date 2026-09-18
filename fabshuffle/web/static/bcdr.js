/* BCDR is a separate operation surface, sharing only the wizard's authenticated session. */

const bcdr = {
  sessionId: null,
  pending: false,
  returnStage: "capacity",
  forms: null,
  sequence: 0,
  latest: {},
  identity: {},
  discovered: {},
  bindings: [],
};

function bcdrElement(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}

function bcdrCurrentSession(sessionId) {
  return !!sessionId && sessionId === state.sessionId && sessionId === bcdr.sessionId;
}

function bcdrLabel(name) {
  const labels = {
    recovery_set_id: "Recovery set ID",
    control_workspace_id: "Control workspace ID",
    control_workspace_name: "New control workspace name",
    warehouse_name: "New metadata Warehouse name",
    source_capacity_ids: "Source capacity IDs",
    source_capacity_id: "Source capacity ID",
    target_capacity_ids: "Dedicated recovery capacity IDs",
    target_capacity_id: "Dedicated recovery capacity ID",
    connection_mappings: "Approved connection routes",
    return_connection_mappings: "Approved return connection routes",
    connection_id: "Connection ID",
    recovery_capacities: "Dedicated recovery capacities",
    fabric_capacity_id: "Fabric capacity ID",
    catalog_capacity_id: "Control Warehouse capacity ARM resource ID",
    dedicated_recovery: "This capacity is dedicated to recovery",
    authorized_for_suspend: "I authorize suspension of this dedicated capacity",
    access_policy: "Restricted standby access",
    owners: "Designated owners",
    recovery_spn: "Authenticated recovery service principal",
    arm_resource_id: "Azure ARM capacity resource ID",
    capacity_id: "Fabric capacity ID",
    object_id: "Principal object ID (not application ID)",
    client_id: "Application (client) ID",
    include_workspace_ids: "Include exact workspace IDs",
    exclude_workspace_ids: "Exclude exact workspace IDs",
    keywords: "Positive workspace-name keywords",
    approved_dependency_workspace_ids: "Approved dependency workspace additions",
    approved_addition_ids: "Approved dependency workspace additions",
    approved_acl_ids: "Approved deferred ACL IDs",
    group_ids: "Selected dependency group IDs",
    selected_group_ids: "Selected dependency group IDs",
    generation_id: "Captured generation ID",
    writer_fence: "Writer-fence evidence",
    readiness: "Readiness evidence",
    target_observed_sha256: "Observed target content hash (SHA-256)",
    writers_stopped: "I attest the named side's external writers are stopped",
    conflicting_writes: "Conflicting writes were observed",
    data_verified: "Data checks passed for this target",
    references_verified: "Reference checks passed for this target",
    security_verified: "Effective-access checks passed for this target",
    effective_principals: "Principals used for effective-access checks",
    evidence: "Evidence reference (operator or runtime record)",
    confirmed_by: "Operator confirming the writer fence",
    expected_controller_id: "Recorded controller ID",
    operation_id: "Recorded operation",
    previous_controller_stopped: "I have stopped and fenced the previous controller",
    fencing_evidence: "Evidence that the previous controller is fenced",
    target_quiescence_evidence: "Evidence that destination activity is stopped",
    issuer: "Authenticated readiness evidence issuer",
    writer_epoch: "Observed writer epoch",
    qualification_evidence: "Independent incident qualification reference",
    access_evidence: "Independently verified access enforcement",
    enforcement_reference: "Read-only enforcement evidence reference",
    qualification_id: "Independent qualification ID",
    binding_sha256: "Independently verified binding hash (SHA-256)",
    expected_configuration_sha256: "Existing configuration hash (for same-scope renewal only)",
    source_paths_verified_local: "Snapshot paths independently verified as local source data",
    snapshot_qualification_ref: "Independent snapshot qualification reference",
    storage_read_approval_ref: "Approved OneLake snapshot access reference",
    read_only_verified: "Independent checks verified read-only access (attestation, not enforcement)",
    retention_acknowledged: "I will retain the original source identities and data while attachments exist",
  };
  return labels[name] || name.replace(/_/g, " ").replace(/^./, (letter) => letter.toUpperCase());
}

function bcdrResolve(schema, root) {
  if (!schema.$ref) return schema;
  const prefix = "#/$defs/";
  if (!schema.$ref.startsWith(prefix)) throw new Error("Unsupported form schema reference.");
  const resolved = root.$defs?.[schema.$ref.slice(prefix.length)];
  if (!resolved) throw new Error("The server returned an incomplete request schema.");
  return { ...resolved, ...schema, $ref: undefined };
}

function bcdrField(name, source, root, required = false, context = []) {
  let schema = bcdrResolve(source, root);
  if (schema.anyOf || schema.oneOf) {
    const alternatives = schema.anyOf || schema.oneOf;
    const nullable = alternatives.some((choice) => choice.type === "null");
    const choices = alternatives.filter((choice) => choice.type !== "null");
    if (nullable && choices.length === 1 && bcdrResolve(choices[0], root).type === "object") {
      const group = bcdrElement("div", undefined, "bcdr-wide");
      const label = bcdrElement("label", undefined, "bcdr-check");
      const enabled = bcdrElement("input");
      enabled.type = "checkbox";
      enabled.checked = false;
      label.appendChild(enabled);
      label.appendChild(bcdrElement("span", `Supply ${bcdrLabel(name).toLowerCase()} (when applicable)`));
      group.appendChild(label);
      const fields = bcdrElement("fieldset");
      fields.disabled = true;
      fields.hidden = true;
      const field = bcdrField(name, choices[0], root, true, context);
      fields.appendChild(field.element);
      group.appendChild(fields);
      enabled.addEventListener("change", () => {
        fields.disabled = !enabled.checked;
        fields.hidden = !enabled.checked;
      });
      return { element: group, read: () => enabled.checked ? field.read() : required ? null : undefined };
    }
    if (choices.length > 1) {
      const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
      fieldset.appendChild(bcdrElement("legend", bcdrLabel(name)));
      const label = bcdrElement("label", "Choose the applicable record type");
      const select = bcdrElement("select");
      select.required = required;
      const empty = bcdrElement("option", "Choose an option");
      empty.value = "";
      select.appendChild(empty);
      choices.forEach((choice, index) => {
        const resolved = bcdrResolve(choice, root);
        const option = bcdrElement("option", resolved.title || bcdrLabel(resolved.type || String(index + 1)));
        option.value = String(index);
        select.appendChild(option);
      });
      label.appendChild(select);
      fieldset.appendChild(label);
      const holder = bcdrElement("div");
      fieldset.appendChild(holder);
      let field;
      select.addEventListener("change", () => {
        holder.replaceChildren();
        field = select.value === "" ? undefined : bcdrField(name, choices[Number(select.value)], root, true, context);
        if (field) holder.appendChild(field.element);
      });
      return {
        element: fieldset,
        read: () => {
          if (!field && required) throw new Error(`Choose the ${bcdrLabel(name).toLowerCase()} record type.`);
          return field?.read();
        },
      };
    }
    schema = { ...schema, ...bcdrResolve(choices[0], root) };
  }
  const id = `bcdr-field-${++bcdr.sequence}`;
  const controllerEpoch = name === "expected_epoch" && !!root.properties?.expected_controller_id;
  const title = controllerEpoch ? "Recorded controller epoch (not writer epoch)" : bcdrLabel(name);
  if (name === "recovery_spn" || name === "issuer") {
    const group = bcdrElement("div", undefined, "bcdr-wide");
    group.appendChild(bcdrElement("p",
      `${title}: ${bcdr.identity.object_id || "Identity unavailable; sign in again"}. ` +
      `Tenant ${bcdr.identity.tenant_id || "unknown"}.`, "identity-context"));
    return {
      element: group,
      read: () => {
        if (!bcdr.identity.object_id || !bcdr.identity.tenant_id) {
          throw new Error("Sign in again with a recovery principal whose tenant and object ID can be verified.");
        }
        return { tenant_id: bcdr.identity.tenant_id, object_id: bcdr.identity.object_id, kind: "ServicePrincipal" };
      },
    };
  }
  if (name === "operation_id" && root.properties?.expected_controller_id) {
    const group = bcdrElement("div", undefined, "bcdr-wide");
    const label = bcdrElement("label", title);
    const select = bcdrElement("select");
    select.id = id;
    select.name = name;
    label.htmlFor = id;
    label.appendChild(select);
    group.appendChild(label);
    const advanced = bcdrElement("details");
    advanced.appendChild(bcdrElement("summary", "Use an exact recorded ID instead"));
    const exactLabel = bcdrElement("label", "Exact operation ID from the catalog or saved result");
    const exact = bcdrElement("input");
    exact.type = "text";
    exact.id = `${id}-exact`;
    exactLabel.htmlFor = exact.id;
    exactLabel.appendChild(exact);
    advanced.appendChild(exactLabel);
    advanced.appendChild(bcdrElement("p",
      "Use this for a completed receipt whose catalog mapping was interrupted. Never use a target name or create a new ID.",
      "hint"));
    group.appendChild(advanced);
    select.addEventListener("change", () => { exact.value = ""; });
    exact.addEventListener("input", () => { select.value = ""; });
    const refresh = () => {
      select.replaceChildren();
      const empty = bcdrElement("option", "Read status, then select an operation");
      empty.value = "";
      select.appendChild(empty);
      for (const operation of bcdr.latest.details?.pending_operations || []) {
        const option = bcdrElement("option",
          `${operation.kind}: ${operation.source?.item_id || operation.target?.item_id || operation.operation_id}` +
          ` - ${operation.state} - ${operation.operation_id}`);
        option.value = operation.operation_id;
        select.appendChild(option);
      }
      select.value = "";
      exact.value = "";
    };
    bcdr.bindings.push(refresh);
    refresh();
    return {
      element: group,
      read: () => {
        const value = exact.value.trim() || select.value;
        if (!value) throw new Error("Select the exact recorded operation after reading recovery status.");
        return value;
      },
    };
  }
  if (["generation_id", "plan_id", "expected_epoch", "writer_epoch", "tenant_id", "expected_controller_id"].includes(name)) {
    const label = bcdrElement("label", title);
    const input = bcdrElement("input");
    input.type = "text";
    input.readOnly = true;
    input.id = id;
    label.htmlFor = id;
    input.placeholder = name === "plan_id" ? "Create a failback plan first" : "Read status or preview first";
    label.appendChild(input);
    const refresh = () => {
      input.value = String(name === "expected_controller_id" ? bcdr.latest.details?.controller_id || "" :
        controllerEpoch ? bcdr.latest.details?.controller_epoch ?? "" :
        name === "writer_epoch" ? bcdr.latest.details?.readiness_context?.writer_epoch ?? "" :
        name === "generation_id" && context.includes("readiness") ?
          bcdr.latest.details?.readiness_context?.generation_id || "" :
        name === "expected_epoch" ? bcdr.latest.details?.writer?.epoch ?? "" :
        name === "tenant_id" ? bcdr.identity.tenant_id || "" : bcdr.latest[name] || "");
    };
    bcdr.bindings.push(refresh);
    refresh();
    return {
      element: label,
      read: () => {
        if (!input.value && required) throw new Error(`${input.placeholder} to pin the ${title.toLowerCase()}.`);
        return input.value === "" ? undefined :
          ["expected_epoch", "writer_epoch"].includes(name) ? Number(input.value) : input.value;
      },
    };
  }
  if (name === "group_ids") {
    const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
    fieldset.appendChild(bcdrElement("legend", title));
    const choices = bcdrElement("div");
    fieldset.appendChild(choices);
    let controls = [];
    const refresh = () => {
      choices.replaceChildren();
      controls = [];
      const groups = bcdr.latest.groups || [];
      if (!groups.length) choices.appendChild(bcdrElement("p",
        "Preview or read status to choose exact dependency groups. No groups are selected automatically.", "hint"));
      for (const group of groups) {
        const label = bcdrElement("label", undefined, "bcdr-check");
        const input = bcdrElement("input");
        input.type = "checkbox";
        input.value = group.group_id;
        input.checked = false;
        label.appendChild(input);
        const description = `${group.group_id} (${group.items.length} items)` +
          (group.blockers.length ? ` - ${group.blockers.join("; ")}` : "");
        label.appendChild(bcdrElement("span", description));
        choices.appendChild(label);
        controls.push(input);
      }
    };
    bcdr.bindings.push(refresh);
    refresh();
    return { element: fieldset, read: () => controls.filter((input) => input.checked).map((input) => input.value) };
  }
  if (["include_workspace_ids", "exclude_workspace_ids", "approved_addition_ids", "source_capacity_ids"].includes(name)) {
    const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
    fieldset.appendChild(bcdrElement("legend", title));
    const choices = bcdrElement("div");
    const manual = bcdrElement("details");
    manual.appendChild(bcdrElement("summary", "Enter exact IDs instead"));
    const label = bcdrElement("label", `${title}, one per line`);
    const input = bcdrElement("textarea");
    input.id = id;
    label.htmlFor = id;
    label.appendChild(input);
    manual.appendChild(label);
    fieldset.appendChild(choices);
    fieldset.appendChild(manual);
    let controls = [];
    const refresh = () => {
      choices.replaceChildren();
      controls = [];
      const captured = bcdr.latest.details?.inventory?.workspaces || [];
      const entries = name === "source_capacity_ids" ?
        (bcdr.discovered.source?.capacities || []).map((entry) => [entry.id, entry.displayName]) :
        captured.length ? captured.map((entry) => [entry.identity.workspace_id, entry.display_name]) :
        name === "approved_addition_ids" ? [] :
        (bcdr.discovered.source?.workspaces || []).map((entry) => [entry.id, entry.displayName]);
      if (name === "approved_addition_ids") choices.appendChild(bcdrElement("p",
        "Approve only the required additions named by the latest preview. Exclusions are not overridden silently.",
        "hint"));
      if (!entries.length) {
        choices.appendChild(bcdrElement("p",
          name === "source_capacity_ids" ? "Discover setup choices or enter the exact source capacity IDs." :
          "Workspace choices appear after discovery or catalog status. Empty inclusion uses only the configured source-capacity scope.",
          "hint"));
      }
      for (const [value, displayName] of entries) {
        const label = bcdrElement("label", undefined, "bcdr-check");
        const checkbox = bcdrElement("input");
        checkbox.type = "checkbox";
        checkbox.value = value;
        checkbox.checked = false;
        label.appendChild(checkbox);
        label.appendChild(bcdrElement("span", `${displayName} - ${value}`));
        choices.appendChild(label);
        controls.push(checkbox);
      }
    };
    bcdr.bindings.push(refresh);
    refresh();
    return {
      element: fieldset,
      read: () => [...new Set([
        ...controls.filter((control) => control.checked).map((control) => control.value),
        ...input.value.split(/\r?\n/).map((value) => value.trim()).filter(Boolean),
      ])],
    };
  }
  if (name === "approved_acl_ids") {
    const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
    fieldset.appendChild(bcdrElement("legend", title));
    const choices = bcdrElement("div");
    fieldset.appendChild(choices);
    let controls = [];
    const refresh = () => {
      choices.replaceChildren();
      controls = [];
      const acls = bcdr.latest.details?.desired_acls || [];
      if (!acls.length) choices.appendChild(bcdrElement("p",
        "Read status or preview to inspect captured ACL intents. No general access is approved by default.", "hint"));
      for (const acl of acls) {
        const label = bcdrElement("label", undefined, "bcdr-check");
        const input = bcdrElement("input");
        input.type = "checkbox";
        input.value = acl.acl_id;
        input.checked = false;
        const target = acl.item?.item_id || acl.workspace?.workspace_id || acl.connection?.connection_id;
        label.appendChild(input);
        if (["control_workspace_id", "fabric_capacity_id", "source_capacity_id", "target_capacity_id"].includes(name)) {
          const choices = bcdrElement("datalist");
          choices.id = `${id}-choices`;
          input.setAttribute("list", choices.id);
          label.appendChild(choices);
          const refresh = () => {
            const side = name === "source_capacity_id" ? bcdr.discovered.source : bcdr.discovered.recovery;
            const entries = name === "control_workspace_id" ? side?.workspaces || [] : side?.capacities || [];
            choices.replaceChildren();
            for (const entry of entries) {
              const option = bcdrElement("option");
              option.value = entry.id;
              option.label = entry.displayName;
              choices.appendChild(option);
            }
          };
          bcdr.bindings.push(refresh);
          refresh();
        }
        label.appendChild(bcdrElement("span",
          `${acl.principal.kind} ${acl.principal.object_id}: ${acl.permission} on ${acl.scope} ${target}` +
          (acl.securable ? ` (${acl.securable})` : "") + ` - ACL ${acl.acl_id}`));
        choices.appendChild(label);
        controls.push(input);
      }
    };
    bcdr.bindings.push(refresh);
    refresh();
    return { element: fieldset, read: () => controls.filter((input) => input.checked).map((input) => input.value) };
  }
  if (schema.const !== undefined) {
    if (typeof schema.const === "boolean") {
      const label = bcdrElement("label", title, "bcdr-check");
      const input = bcdrElement("input");
      input.type = "checkbox";
      input.name = name;
      input.id = id;
      input.checked = false;
      input.required = true;
      label.htmlFor = id;
      label.appendChild(input);
      return {
        element: label,
        read: () => {
          if (!input.checked) throw new Error(`Confirm: ${title}.`);
          return schema.const;
        },
      };
    }
    return { element: document.createDocumentFragment(), read: () => schema.const };
  }
  if (schema.type === "object") {
    const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
    fieldset.appendChild(bcdrElement("legend", title));
    if (schema.description) fieldset.appendChild(bcdrElement("p", schema.description, "hint"));
    const fields = bcdrElement("div", undefined, "bcdr-fields");
    const readers = [];
    let workspace;
    if (schema.properties?.control_workspace_id && schema.properties?.control_workspace_name) {
      const group = bcdrElement("fieldset", undefined, "bcdr-wide");
      group.appendChild(bcdrElement("legend", "Control workspace"));
      const label = bcdrElement("label", "Create or choose the restricted control workspace");
      const select = bcdrElement("select");
      for (const [value, text] of [["create", "Create a new control workspace"], ["existing", "Use an existing control workspace"]]) {
        const option = bcdrElement("option", text);
        option.value = value;
        select.appendChild(option);
      }
      select.value = "create";
      label.appendChild(select);
      group.appendChild(label);
      const create = bcdrField("control_workspace_name", schema.properties.control_workspace_name, root, true);
      const existing = bcdrField("control_workspace_id", schema.properties.control_workspace_id, root, true);
      const createFields = bcdrElement("fieldset");
      const existingFields = bcdrElement("fieldset");
      createFields.appendChild(create.element);
      existingFields.appendChild(existing.element);
      group.appendChild(createFields);
      group.appendChild(existingFields);
      const change = () => {
        createFields.disabled = createFields.hidden = select.value !== "create";
        existingFields.disabled = existingFields.hidden = select.value !== "existing";
      };
      select.addEventListener("change", change);
      change();
      fields.appendChild(group);
      workspace = () => select.value === "create" ?
        { control_workspace_name: create.read() } : { control_workspace_id: existing.read() };
    }
    for (const [key, value] of Object.entries(schema.properties || {})) {
      if (workspace && ["control_workspace_name", "control_workspace_id"].includes(key)) continue;
      const field = bcdrField(key, value, root, (schema.required || []).includes(key), [...context, name]);
      fields.appendChild(field.element);
      readers.push([key, field.read]);
    }
    fieldset.appendChild(fields);
    return {
      element: fieldset,
      read: () => ({
        ...Object.fromEntries(readers.map(([key, read]) => [key, read()]).filter(([, value]) => value !== undefined)),
        ...(workspace ? workspace() : {}),
      }),
    };
  }
  if (schema.type === "array" && bcdrResolve(schema.items || {}, root).type === "object") {
    const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
    fieldset.appendChild(bcdrElement("legend", title));
    if (schema.description) fieldset.appendChild(bcdrElement("p", schema.description, "hint"));
    const entries = bcdrElement("div");
    const readers = new Map();
    const add = bcdrElement("button", `Add ${title.toLowerCase()}`, "secondary");
    add.type = "button";
    add.addEventListener("click", () => {
      const row = bcdrElement("div");
      const field = bcdrField(name, schema.items, root, true, context);
      row.appendChild(field.element);
      const remove = bcdrElement("button", "Remove entry", "secondary");
      remove.type = "button";
      remove.addEventListener("click", () => { readers.delete(row); row.remove(); add.focus(); });
      row.appendChild(remove);
      entries.appendChild(row);
      readers.set(row, field.read);
      row.querySelector("input, select, textarea")?.focus();
    });
    fieldset.appendChild(entries);
    fieldset.appendChild(add);
    return { element: fieldset, read: () => Array.from(readers.values(), (read) => read()) };
  }
  const label = bcdrElement("label", title);
  label.htmlFor = id;
  let input;
  if (schema.enum) {
    input = bcdrElement("select");
    if (!required) input.appendChild(bcdrElement("option", ""));
    schema.enum.forEach((value) => {
      const option = bcdrElement("option", bcdrLabel(String(value)));
      option.value = value;
      input.appendChild(option);
    });
  } else {
    input = bcdrElement(schema.type === "array" ? "textarea" : "input");
    if (schema.type === "boolean") input.type = "checkbox";
    else if (["integer", "number"].includes(schema.type)) input.type = "number";
    else if (schema.format === "date-time") input.type = "datetime-local";
    else input.type = "text";
  }
  input.id = id;
  input.name = name;
  input.autocomplete = "off";
  input.spellcheck = false;
  if (schema.type === "boolean") {
    label.className = "bcdr-check";
    input.checked = schema.default === true;
  } else {
    input.required = required;
    if (schema.default !== undefined && schema.default !== null) {
      input.value = Array.isArray(schema.default) ? schema.default.join("\n") : String(schema.default);
    }
  }
  if (schema.minimum !== undefined) input.min = schema.minimum;
  if (schema.maximum !== undefined) input.max = schema.maximum;
  if (schema.minLength !== undefined) input.minLength = schema.minLength;
  if (schema.maxLength !== undefined) input.maxLength = schema.maxLength;
  label.appendChild(input);
  if (schema.type === "array" || schema.description || schema.format === "date-time") {
    const help = bcdrElement("span", schema.description || (schema.format === "date-time" ?
      "Enter local date and time. The request records the equivalent UTC timestamp." :
      "One exact value per line. Leave empty only if the selection policy permits it."), "hint");
    help.id = `${id}-help`;
    input.setAttribute("aria-describedby", help.id);
    label.appendChild(help);
  }
  return {
    element: label,
    read: () => {
      if (schema.type === "boolean") return input.checked;
      const value = name === "suffix" ? input.value : input.value.trim();
      if (!value && !required && schema.type !== "array") return undefined;
      if (schema.type === "array") return value.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
      if (["integer", "number"].includes(schema.type)) return Number(value);
      if (schema.format === "date-time" && value) return new Date(value).toISOString();
      return value;
    },
  };
}

function bcdrError(message) {
  const error = $("#bcdr-error");
  error.textContent = message;
  error.hidden = !message;
  if (message) { error.tabIndex = -1; error.focus(); }
}

function bcdrRenderResult(result) {
  bcdr.latest = { ...bcdr.latest, ...result };
  bcdr.bindings.forEach((refresh) => refresh());
  const output = $("#bcdr-result");
  output.replaceChildren();
  output.appendChild(bcdrElement("h3", "Operation result"));
  const status = result.outcome || result.status || result.mode;
  if (status) {
    const summary = bcdrElement("p", bcdrLabel(status), "bcdr-status");
    summary.dataset.outcome = status;
    output.appendChild(summary);
  }
  output.appendChild(bcdrElement("p",
    "A captured generation is not an applied standby generation. Restored/stopped is not ready for cutover.",
    "hint"));
  const facts = bcdrElement("dl", undefined, "summary");
  for (const [label, value] of [
    ["Recovery mode", result.mode],
    ["Captured generation", result.generation_id],
    ["Failback plan", result.plan_id],
  ]) {
    if (!value) continue;
    facts.appendChild(bcdrElement("dt", label));
    facts.appendChild(bcdrElement("dd", value, "v"));
  }
  output.appendChild(facts);
  if (result.details?.readiness_context) {
    const context = result.details.readiness_context;
    output.appendChild(bcdrElement("h4", "Backend readiness context"));
    output.appendChild(bcdrElement("p",
      `Evidence generation ${context.generation_id}; writer epoch ${context.writer_epoch}. ` +
      "This may differ from the original failover generation during failback. " +
      "Collect current target and intended-principal evidence; these identifiers do not prove readiness.", "hint"));
    const evidence = bcdrElement("details");
    evidence.appendChild(bcdrElement("summary", "Issuer and intended runtime principals"));
    evidence.appendChild(bcdrElement("pre", JSON.stringify(context, null, 2)));
    output.appendChild(evidence);
  }
  if (result.details?.configuration_sha256) {
    output.appendChild(bcdrElement("p", `Configuration hash: ${result.details.configuration_sha256}`, "hint"));
  }
  if (result.details?.temporary_configurations?.length) {
    const section = bcdrElement("details");
    section.appendChild(bcdrElement("summary", "Recorded temporary configurations for same-scope renewal"));
    const list = bcdrElement("ul", undefined, "bcdr-results");
    for (const configuration of result.details.temporary_configurations) {
      const source = configuration.request.source;
      list.appendChild(bcdrElement("li",
        `${source.workspace_id}/${source.item_id}: ${configuration.sha256}. ` +
        `Qualification valid until ${configuration.request.valid_until}.`));
    }
    section.appendChild(list);
    output.appendChild(section);
  }
  if (result.details?.temporary_attachments?.length) {
    const section = bcdrElement("section", undefined, "warnings");
    section.appendChild(bcdrElement("h4", "Temporary attachments: retain the source"));
    section.appendChild(bcdrElement("p",
      "Attached data is not independent recovery. Do not delete the original identities or backing data. " +
      "Collect fresh post-attachment engine, data, reference and effective-access evidence."));
    const list = bcdrElement("ul", undefined, "bcdr-results");
    for (const item of result.details.temporary_attachments) {
      for (const attachment of item.attachments) {
        const binding = attachment.binding;
        const entry = bcdrElement("li",
          `${binding.source.workspace_id}/${binding.source.item_id}/${binding.source_path} -> ` +
          `${binding.consumer.workspace_id}/${binding.consumer.item_id}. ` +
          `Attachment verified: ${attachment.attachment_verified ? "yes" : "no"}; ` +
          `data ready: ${attachment.data_ready ? "yes" : "no"}.`);
        list.appendChild(entry);
      }
    }
    section.appendChild(list);
    output.appendChild(section);
  }
  if (result.details?.pending_operations?.length) {
    output.appendChild(bcdrElement("h4", "Interrupted operations to inspect"));
    output.appendChild(bcdrElement("p",
      "Read the exact service receipt before reconciling. Stop and fence the previous controller; " +
      "an interrupted operation is not permission to repeat a create.", "hint"));
    const list = bcdrElement("ul", undefined, "bcdr-results");
    for (const operation of result.details.pending_operations) {
      const entry = bcdrElement("li");
      entry.appendChild(bcdrElement("strong", `${operation.kind} - ${operation.state}`));
      entry.appendChild(bcdrElement("p", `Operation ${operation.operation_id}`));
      if (operation.source) entry.appendChild(bcdrElement("p",
        `Source item ${operation.source.workspace_id}/${operation.source.item_id}`));
      if (operation.service_operation_id) entry.appendChild(bcdrElement("p",
        `Service receipt ${operation.service_operation_id}`));
      if (operation.error_code || operation.message) entry.appendChild(bcdrElement("p",
        [operation.error_code, operation.message].filter(Boolean).join(": ")));
      list.appendChild(entry);
    }
    output.appendChild(list);
  }
  if (result.details?.reconciled_operation) {
    output.appendChild(bcdrElement("p",
      `Reconciled operation ${result.details.reconciled_operation}. This does not enable recovery or approve cutover. ` +
      "Read status again before selecting another operation.", "hint"));
  }
  if (result.details?.capacities?.length) {
    output.appendChild(bcdrElement("h4", "Observed recovery capacity state"));
    output.appendChild(bcdrElement("p",
      "These are service observations, not an inference from standby mode. Reading status resumes the catalog capacity.",
      "hint"));
    const capacities = bcdrElement("ul", undefined, "bcdr-results");
    for (const capacity of result.details.capacities) {
      const entry = bcdrElement("li");
      entry.appendChild(bcdrElement("strong", `${capacity.state || "Unreported"} - ${capacity.capacity_id}`));
      entry.appendChild(bcdrElement("p",
        `Provisioning: ${capacity.provisioning_state || "Unreported"}. Observed: ${capacity.observed_at}.`));
      const resource = bcdrElement("details");
      resource.appendChild(bcdrElement("summary", "Exact ARM resource"));
      resource.appendChild(bcdrElement("p", capacity.arm_resource_id));
      entry.appendChild(resource);
      capacities.appendChild(entry);
    }
    output.appendChild(capacities);
  }
  if (result.warnings?.length) {
    const warnings = bcdrElement("div", undefined, "warnings");
    warnings.appendChild(bcdrElement("h4", "Actions and protection warnings"));
    const list = bcdrElement("ul");
    result.warnings.forEach((warning) => list.appendChild(bcdrElement("li", warning)));
    warnings.appendChild(list);
    output.appendChild(warnings);
  }
  if (result.groups?.length) {
    output.appendChild(bcdrElement("h4", "Dependency group outcomes"));
    const groups = bcdrElement("ul", undefined, "bcdr-results");
    result.groups.forEach((group) => {
      const entry = bcdrElement("li");
      entry.appendChild(bcdrElement("strong", group.group_id));
      const state = [
        `Metadata applied: ${group.metadata_applied ? "yes" : "no"}`,
        `Data ready: ${group.data_ready ? "yes" : "no"}`,
        `Access enabled: ${group.access_enabled ? "yes" : "no"}`,
        `Ready for cutover: ${group.ready_for_cutover ? "yes" : "no"}`,
        `Active: ${group.active ? "yes" : "no"}`,
      ];
      entry.appendChild(bcdrElement("p", state.join(" | ")));
      group.blockers.forEach((blocker) => entry.appendChild(bcdrElement("p", blocker, "bcdr-status")));
      groups.appendChild(entry);
    });
    output.appendChild(groups);
  }
  const inventory = result.details?.inventory;
  const unresolved = (inventory?.items || []).filter((item) => item.unresolved.length);
  if (unresolved.length) {
    const section = bcdrElement("details");
    section.open = true;
    section.appendChild(bcdrElement("summary", "Captured unresolved references"));
    section.appendChild(bcdrElement("p",
      "This captured inventory may include unselected items. The backend group outcomes determine their impact.",
      "hint"));
    const list = bcdrElement("ul");
    unresolved.forEach((item) => list.appendChild(bcdrElement("li",
      `${item.display_name}: ${item.unresolved.join("; ")}`)));
    section.appendChild(list);
    output.appendChild(section);
  }
  if (result.details?.applied_items?.length) {
    const applied = bcdrElement("details");
    applied.appendChild(bcdrElement("summary", "Applied standby generations by item"));
    const list = bcdrElement("ul", undefined, "bcdr-results");
    for (const item of result.details.applied_items) {
      const source = inventory?.items?.find((entry) =>
        entry.identity.workspace_id === item.source.workspace_id && entry.identity.item_id === item.source.item_id);
      list.appendChild(bcdrElement("li",
        `${source?.display_name || item.source.item_id}: ${item.outcome}. ` +
        `Applied generation ${item.capture_generation_id}; target ${item.target.workspace_id}/${item.target.item_id}.`));
    }
    applied.appendChild(list);
    output.appendChild(applied);
  }
  const details = bcdrElement("details");
  details.open = !result.groups?.length;
  details.appendChild(bcdrElement("summary", "Generation, group outcomes, warnings and evidence"));
  const data = bcdrElement("pre", JSON.stringify(result, null, 2));
  data.tabIndex = 0;
  details.appendChild(data);
  output.appendChild(details);
  output.hidden = false;
}

async function bcdrSubmit(command, body, button) {
  if (bcdr.pending) return;
  if (!state.sessionId || state.sessionId !== bcdr.sessionId) {
    bcdrError("The sign-in changed. Reopen Standby & recovery and review the request again.");
    return;
  }
  const sessionId = state.sessionId;
  bcdr.pending = true;
  bcdrError("");
  const panel = $("#bcdr-panel");
  panel.setAttribute("aria-busy", "true");
  const controls = Array.from(panel.querySelectorAll("button, input, select, textarea"));
  const disabled = controls.map((control) => control.disabled);
  controls.forEach((control) => {
    control.setAttribute("aria-invalid", "false");
    control.disabled = true;
  });
  $("#bcdr-progress").textContent =
    `${command.label} is in progress. Closing this view does not cancel server-side work.`;
  try {
    const payload = command.confirmation ? { confirmation: command.name, request: body } : body;
    const result = await api(command.path, { method: command.method, body: payload });
    if (!bcdrCurrentSession(sessionId)) return;
    bcdrRenderResult(result);
    $("#bcdr-progress").textContent = `${command.label}: result received. Review all group outcomes below.`;
  } catch (error) {
    if (bcdrCurrentSession(sessionId)) {
      for (const entry of error.details || []) {
        const name = entry.loc?.at(-1);
        controls.filter((control) => control.name === name).forEach((control) => {
          control.setAttribute("aria-invalid", "true");
          control.setAttribute("aria-errormessage", "bcdr-error");
        });
      }
      bcdrError(error.message);
      $("#bcdr-progress").textContent = "The operation did not report success. Review the error before retrying.";
    }
  } finally {
    bcdr.pending = false;
    panel.setAttribute("aria-busy", "false");
    controls.forEach((control, index) => { control.disabled = disabled[index]; });
    if (bcdrCurrentSession(sessionId) && !$("#bcdr-error").hidden) $("#bcdr-error").focus();
    else if (bcdrCurrentSession(sessionId)) button.focus();
  }
}

function bcdrConfirm(command, body, trigger) {
  const sessionId = state.sessionId;
  const dialog = bcdrElement("dialog", undefined, "bcdr bcdr-confirm");
  const heading = bcdrElement("h2", command.label);
  heading.id = "bcdr-confirm-title";
  dialog.setAttribute("aria-labelledby", heading.id);
  dialog.appendChild(heading);
  const description = bcdrElement("p", command.confirmation);
  description.id = "bcdr-confirm-description";
  dialog.setAttribute("aria-describedby", description.id);
  dialog.appendChild(description);
  const chosen = bcdrElement("details");
  chosen.appendChild(bcdrElement("summary", "Review exact request IDs and approvals"));
  chosen.appendChild(bcdrElement("pre", JSON.stringify(body, null, 2)));
  dialog.appendChild(chosen);
  const actions = bcdrElement("div", undefined, "actions");
  const cancel = bcdrElement("button", "Cancel", "secondary");
  cancel.type = "button";
  cancel.autofocus = true;
  const confirm = bcdrElement("button", command.label, "primary");
  confirm.type = "button";
  actions.appendChild(cancel);
  actions.appendChild(confirm);
  dialog.appendChild(actions);
  document.body.appendChild(dialog);
  dialog.addEventListener("close", () => { dialog.remove(); trigger.focus(); });
  cancel.addEventListener("click", () => dialog.close());
  confirm.addEventListener("click", () => {
    dialog.close();
    if (!bcdrCurrentSession(sessionId)) {
      bcdrError("The sign-in changed. Review this operation again in the current session.");
      return;
    }
    bcdrSubmit(command, body, trigger);
  });
  dialog.showModal();
  cancel.focus();
}

function bcdrRenderForms(commands) {
  const content = $("#bcdr-content");
  content.replaceChildren();
  bcdr.bindings = [];
  const preparation = bcdrElement("details");
  preparation.appendChild(bcdrElement("summary", "Healthy-source preparation: discover named setup choices"));
  preparation.appendChild(bcdrElement("p",
    "This optional read contacts the source and recovery Fabric APIs. Do not use it during a source outage; " +
    "recovery actions use captured catalog inventory instead. It does not resume capacity or enable recovery.", "hint"));
  const discover = bcdrElement("button", "Discover setup choices", "secondary");
  discover.type = "button";
  discover.addEventListener("click", async () => {
    if (bcdr.pending) return;
    const sessionId = state.sessionId;
    busy(discover, true, "Discovering choices...");
    bcdrError("");
    try {
      const inventory = await api("/api/bcdr/discovery");
      if (!bcdrCurrentSession(sessionId)) return;
      bcdr.discovered = inventory;
      bcdr.bindings.forEach((refresh) => refresh());
      $("#bcdr-progress").textContent = "Named setup choices loaded. Review exact IDs; no configuration has been saved.";
    } catch (error) {
      if (bcdrCurrentSession(sessionId)) bcdrError(error.message);
    } finally {
      busy(discover, false);
    }
  });
  preparation.appendChild(discover);
  content.appendChild(preparation);
  for (const command of commands) {
    const section = bcdrElement("details");
    section.open = command.name === "setup";
    section.appendChild(bcdrElement("summary", command.label));
    section.appendChild(bcdrElement("p", command.description, "hint"));
    const form = bcdrElement("form");
    form.setAttribute("aria-label", command.label);
    const field = bcdrField(command.label, command.schema, command.schema, true);
    field.element.querySelector("legend")?.remove();
    form.appendChild(field.element);
    const button = bcdrElement("button", command.label, command.primary ? "primary" : "secondary");
    button.type = "submit";
    form.appendChild(button);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (bcdr.pending || !form.reportValidity()) return;
      try {
        const body = command.method === "GET" ? undefined : field.read();
        if (command.confirmation) bcdrConfirm(command, body, button);
        else bcdrSubmit(command, body, button);
      } catch (error) {
        bcdrError(error.message);
      }
    });
    section.appendChild(form);
    content.appendChild(section);
  }
  const result = bcdrElement("section");
  result.id = "bcdr-result";
  result.hidden = true;
  content.appendChild(result);
}

$("#bcdr-open").addEventListener("click", async () => {
  if (!state.sessionId || state.crossTenant) return;
  bcdr.returnStage = Array.from(document.querySelectorAll(".panel")).find(
    (panel) => !panel.hidden && panel.dataset.stage !== "bcdr"
  )?.dataset.stage || "capacity";
  goTo("bcdr");
  $(".wizard-nav").hidden = true;
  $("#bcdr-title").focus();
  if (bcdr.sessionId === state.sessionId && bcdr.forms) return;
  bcdr.sessionId = state.sessionId;
  bcdr.forms = null;
  bcdr.latest = {};
  bcdr.identity = {};
  bcdr.discovered = {};
  $("#bcdr-content").replaceChildren();
  bcdrError("");
  $("#bcdr-progress").textContent = "Loading operation forms. Recovery is not being enabled.";
  const sessionId = state.sessionId;
  try {
    const response = await api("/api/bcdr/forms");
    if (!bcdrCurrentSession(sessionId)) return;
    bcdr.identity = response.identity || {};
    bcdrRenderForms(response.commands);
    bcdr.forms = response.commands;
    $("#bcdr-progress").textContent = "Choose setup or read the saved recovery status. No operation has started.";
  } catch (error) {
    if (bcdrCurrentSession(sessionId)) {
      bcdrError(error.message);
      $("#bcdr-progress").textContent = "The recovery forms could not be loaded.";
    }
  }
});

$("#sign-out").addEventListener("click", () => {
  bcdr.sessionId = null;
  bcdr.forms = null;
  bcdr.latest = {};
  bcdr.identity = {};
  bcdr.discovered = {};
  bcdr.bindings = [];
  $("#bcdr-content").replaceChildren();
  $("#bcdr-progress").textContent = "";
  bcdrError("");
  document.querySelectorAll(".bcdr-confirm").forEach((dialog) => dialog.close());
});

$("#bcdr-back").addEventListener("click", async () => {
  const sessionId = state.sessionId;
  $(".wizard-nav").hidden = false;
  const fromRecoverySignIn = bcdr.returnStage === "login";
  goTo(fromRecoverySignIn ? "capacity" : bcdr.returnStage);
  $("#bcdr-open").focus();
  if (fromRecoverySignIn) {
    try {
      await loadCapacities();
      if (!bcdrCurrentSession(sessionId)) return;
      loadResumable();
      if (!state.paired) {
        loadLeftovers();
        loadWorkspaces().then(() => {
          if (bcdrCurrentSession(sessionId)) fillWorkspaceSelects();
        }).catch((error) => {
          if (bcdrCurrentSession(sessionId)) showError(error.message);
        });
      }
    } catch (error) {
      if (bcdrCurrentSession(sessionId)) showError(error.message);
    }
  }
});
