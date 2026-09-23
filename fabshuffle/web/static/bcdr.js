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
  discoverySequence: 0,
  discovering: false,
  bindings: [],
  capacityBindings: [],
  recoveryRows: new Map(),
  savedSetup: null,
  warehouseChoices: [],
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
    control_workspace_id: "Existing control workspace",
    control_workspace_name: "New control workspace name",
    warehouse_name: "Metadata Warehouse name",
    warehouse_id: "Selected metadata Warehouse",
    warehouse_action: "Warehouse action",
    source_capacity_ids: "Source capacities",
    source_capacity_id: "Source capacity",
    target_capacity_ids: "Dedicated recovery capacities",
    target_capacity_id: "Dedicated recovery capacity",
    connection_mappings: "Approved connection routes",
    return_connection_mappings: "Approved return connection routes",
    connection_id: "Connection ID",
    recovery_capacities: "Dedicated recovery capacities",
    fabric_capacity_id: "Recovery capacity",
    catalog_capacity_id: "Control Warehouse capacity",
    dedicated_recovery: "This capacity is dedicated to recovery",
    authorized_for_suspend: "I authorize suspension of this dedicated capacity",
    access_policy: "Restricted standby access",
    owners: "Designated owners",
    recovery_spn: "Authenticated recovery service principal",
    arm_resource_id: "Azure ARM capacity resource ID",
    capacity_id: "Fabric capacity ID",
    object_id: "Principal object ID (not application ID)",
    client_id: "Application (client) ID",
    include_workspace_ids: "Include workspaces",
    exclude_workspace_ids: "Exclude workspaces",
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

function bcdrResourceChoices(name) {
  if (name === "warehouse_id" || name === "control_warehouse_id") {
    const choices = bcdr.warehouseChoices.map((entry) => ({ id: entry.id, label: entry.displayName }));
    if (bcdr.savedSetup?.warehouseId && !choices.some((entry) => entry.id === bcdr.savedSetup.warehouseId)) {
      choices.push({ id: bcdr.savedSetup.warehouseId, label: bcdr.savedSetup.warehouseName || "Saved Warehouse" });
    }
    return choices;
  }
  if (["capacity_id", "target_capacity_ids"].includes(name)) {
    return [...(bcdr.discovered.source?.capacities || []), ...(bcdr.discovered.recovery?.capacities || [])]
      .map((entry) => ({ id: entry.id, label: [entry.displayName, entry.region].filter(Boolean).join(" - ") }));
  }
  if (["source_capacity_ids", "source_capacity_id"].includes(name)) {
    return (bcdr.discovered.source?.capacities || []).map((entry) => ({
      id: entry.id,
      label: [entry.displayName || "Unnamed capacity", entry.region, entry.sku].filter(Boolean).join(" - "),
    }));
  }
  if (["fabric_capacity_id", "target_capacity_id", "catalog_capacity_id", "arm_resource_id"].includes(name)) {
    const selected = new Set(Array.from(bcdr.recoveryRows.values(), (read) => read()).filter(Boolean));
    return (bcdr.discovered.recovery?.capacityChoices || [])
      .filter((entry) => name !== "catalog_capacity_id" || selected.has(entry.id))
      .map((entry) => ({
        id: ["catalog_capacity_id", "arm_resource_id"].includes(name) ? entry.arm_resource_id : entry.id,
        label: [entry.displayName || "Unnamed capacity", entry.region, entry.sku,
          entry.subscriptionName, entry.resourceGroup].filter(Boolean).join(" - "),
        unavailable: entry.matchStatus !== "matched",
        message: entry.matchMessage,
      }));
  }
  const captured = bcdr.latest.details?.inventory?.workspaces || [];
  if (name !== "control_workspace_id" && captured.length) {
    return captured.map((entry) => ({
      id: entry.identity.workspace_id,
      label: entry.display_name || "Unnamed workspace",
    }));
  }
  if (name === "approved_addition_ids") return [];
  const side = name === "control_workspace_id" ? bcdr.discovered.recovery : bcdr.discovered.source;
  const choices = (side?.workspaces || []).map((entry) => {
    const capacity = side?.capacities?.find((row) => row.id === entry.capacityId);
    return {
      id: entry.id,
      label: [entry.displayName || "Unnamed workspace", capacity?.displayName,
        entry.capacityRegion || capacity?.region, entry.description].filter(Boolean).join(" - "),
    };
  });
  if (name === "control_workspace_id" && bcdr.savedSetup?.workspaceId &&
    !choices.some((entry) => entry.id === bcdr.savedSetup.workspaceId)) {
    choices.push({ id: bcdr.savedSetup.workspaceId,
      label: bcdr.savedSetup.workspaceName || "Saved recovery metadata workspace" });
  }
  return choices;
}

function bcdrCapacitySelect(name, id, required) {
  const label = bcdrElement("label", bcdrLabel(name));
  const select = bcdrElement("select");
  select.id = id;
  select.name = name;
  select.required = required;
  label.htmlFor = id;
  label.appendChild(select);
  const hint = bcdrElement("span", "", "hint");
  hint.id = `${id}-help`;
  select.setAttribute("aria-describedby", hint.id);
  label.appendChild(hint);
  let available = new Set();
  let selectedLabel = "";
  const refresh = () => {
    const selected = select.value;
    const entries = bcdrResourceChoices(name);
    available = new Set();
    select.replaceChildren();
    const empty = bcdrElement("option", "Choose a capacity");
    empty.value = "";
    select.appendChild(empty);
    for (const entry of entries) {
      const ambiguous = entries.filter((other) => other.label === entry.label).length > 1;
      const option = bcdrElement("option", entry.label +
        (entry.unavailable ? ` - ${entry.message}` : ambiguous ? " (ambiguous name)" : ""));
      option.value = entry.id || "";
      option.disabled = entry.unavailable || ambiguous || !entry.id;
      if (!option.disabled) available.add(entry.id);
      select.appendChild(option);
    }
    if (selected && !available.has(selected)) {
      const missing = bcdrElement("option", `${selectedLabel || "Previous capacity"} (unavailable)`);
      missing.value = selected;
      missing.disabled = true;
      select.appendChild(missing);
    }
    select.value = selected;
    const error = name === "source_capacity_id" ? bcdr.discovered.source?.errors?.capacities :
      bcdr.discovered.recovery?.errors?.capacityMapping || bcdr.discovered.recovery?.errors?.capacities;
    hint.textContent = error ? `Discovery incomplete: ${error}. Resolve access and retry discovery.` :
      selected && !available.has(selected) ? "This selection is no longer available. Choose again; it has not been replaced." :
      name === "catalog_capacity_id" ? "Choose one of the dedicated recovery capacities selected above." :
      name === "source_capacity_id" ? "Discover setup choices to load source capacity names." :
      "Azure resources are matched by name and region, not by a shared ID. Review the subscription and resource group.";
  };
  select.addEventListener("change", () => {
    selectedLabel = bcdrResourceChoices(name).find((entry) => entry.id === select.value)?.label || "";
    bcdr.capacityBindings.forEach((update) => update());
  });
  bcdr.bindings.push(refresh);
  bcdr.capacityBindings.push(refresh);
  refresh();
  return {
    element: label, select,
    dispose: () => {
      bcdr.bindings = bcdr.bindings.filter((entry) => entry !== refresh);
      bcdr.capacityBindings = bcdr.capacityBindings.filter((entry) => entry !== refresh);
    },
    read: () => {
      if (!select.value && !required) return undefined;
      const errors = name === "source_capacity_id" ? bcdr.discovered.source?.errors : bcdr.discovered.recovery?.errors;
      if (errors?.capacities || (name !== "source_capacity_id" && errors?.capacityMapping)) {
        throw new Error("Refresh capacity discovery successfully before submitting this selection.");
      }
      if (!available.has(select.value)) throw new Error(`Choose an available ${bcdrLabel(name).toLowerCase()} by name.`);
      return select.value;
    },
  };
}

function bcdrRecoveryCapacity(id, schema, root, context) {
  const group = bcdrElement("fieldset", undefined, "bcdr-wide");
  group.appendChild(bcdrElement("legend", "Dedicated recovery capacity"));
  const picker = bcdrCapacitySelect("fabric_capacity_id", id, true);
  group.appendChild(picker.element);
  const dedicated = bcdrField("dedicated_recovery", schema.properties.dedicated_recovery, root, true, context);
  const suspend = bcdrField("authorized_for_suspend", schema.properties.authorized_for_suspend, root, true, context);
  group.appendChild(dedicated.element);
  group.appendChild(suspend.element);
  const notice = bcdrElement("p", "", "hint");
  notice.setAttribute("role", "status");
  group.appendChild(notice);
  let boundArm = null;
  const choice = () => (bcdr.discovered.recovery?.capacityChoices || [])
    .find((entry) => entry.id === picker.select.value && entry.matchStatus === "matched");
  bcdr.recoveryRows.set(id, () => boundArm && boundArm === choice()?.arm_resource_id ? picker.select.value : "");
  const resetApprovals = () => {
    dedicated.element.querySelector("input").checked = false;
    suspend.element.querySelector("input").checked = false;
  };
  picker.select.addEventListener("change", () => {
    boundArm = choice()?.arm_resource_id || null;
    notice.textContent = "";
    resetApprovals();
    bcdr.capacityBindings.forEach((refresh) => refresh());
  });
  const refreshMatch = () => {
    if (boundArm && boundArm !== choice()?.arm_resource_id) {
      boundArm = null;
      picker.select.value = "";
      resetApprovals();
      notice.textContent = "The Azure match changed or disappeared. Select the capacity again and review both approvals.";
      bcdr.capacityBindings.forEach((refresh) => refresh());
    }
  };
  bcdr.bindings.push(refreshMatch);
  return {
    element: group,
    dispose: () => {
      bcdr.recoveryRows.delete(id);
      picker.dispose();
      bcdr.bindings = bcdr.bindings.filter((entry) => entry !== refreshMatch);
      bcdr.capacityBindings.forEach((refresh) => refresh());
    },
    read: () => {
      const fabricId = picker.read();
      if (!boundArm || boundArm !== choice()?.arm_resource_id) throw new Error("Review the updated recovery capacity match.");
      if (Array.from(bcdr.recoveryRows.values(), (read) => read()).filter((value) => value === fabricId).length > 1) {
        throw new Error("Choose each dedicated recovery capacity only once.");
      }
      if (!dedicated.read() || !suspend.read()) throw new Error("Confirm dedicated recovery use and suspension authorization.");
      return { fabric_capacity_id: fabricId, arm_resource_id: boundArm,
        dedicated_recovery: true, authorized_for_suspend: true };
    },
  };
}

function bcdrNamedWorkspace(name, id, required) {
  const label = bcdrElement("label", bcdrLabel(name));
  label.htmlFor = id;
  const select = bcdrElement("select");
  select.id = id;
  select.name = name;
  select.required = required;
  label.appendChild(select);
  const hint = bcdrElement("span", "", "hint");
  hint.id = `${id}-help`;
  select.setAttribute("aria-describedby", hint.id);
  label.appendChild(hint);
  let available = new Set();
  let selectedLabel = "";
  const refresh = () => {
    const selected = select.value;
    const entries = bcdrResourceChoices(name);
    available = new Set();
    select.replaceChildren();
    const placeholder = bcdrElement("option", "Choose a workspace");
    placeholder.value = "";
    select.appendChild(placeholder);
    for (const entry of entries) {
      const ambiguous = entries.filter((other) => other.label === entry.label).length > 1;
      const option = bcdrElement("option", entry.label + (ambiguous ? " (ambiguous name)" : ""));
      option.value = entry.id;
      option.disabled = ambiguous;
      if (!ambiguous) available.add(entry.id);
      select.appendChild(option);
    }
    if (selected && !available.has(selected)) {
      const missing = bcdrElement("option", `${selectedLabel || "Previous workspace"} (unavailable)`);
      missing.value = selected;
      missing.disabled = true;
      select.appendChild(missing);
    }
    select.value = selected;
    hint.textContent = selected && !available.has(selected) ?
      "The selected workspace is no longer available unambiguously. Choose another or refresh discovery." :
      !entries.length ? "Discover setup choices while the source is healthy to load recovery workspace names." :
      "Select the restricted control workspace by name. Ambiguous names must be resolved before selection.";
  };
  select.addEventListener("change", () => {
    selectedLabel = bcdrResourceChoices(name).find((entry) => entry.id === select.value)?.label || "";
    refresh();
  });
  bcdr.bindings.push(refresh);
  refresh();
  return {
    element: label, select,
    setValue: (value) => {
      select.value = value;
      selectedLabel = bcdrResourceChoices(name).find((entry) => entry.id === value)?.label || "";
      refresh();
    },
    read: () => {
      if (!select.value && !required) return undefined;
      if (bcdr.discovered.recovery?.errors?.workspaces) {
        throw new Error("Refresh recovery workspace discovery successfully before choosing the control workspace.");
      }
      if (!available.has(select.value)) throw new Error("Discover setup choices, then select an available control workspace.");
      return select.value;
    },
  };
}

function bcdrWarehouseSetup(workspaceId, schema) {
  const group = bcdrElement("fieldset", undefined, "bcdr-wide");
  group.appendChild(bcdrElement("legend", "Metadata Warehouse: choose or create"));
  const recorded = bcdrElement("p", "", "hint");
  group.appendChild(recorded);
  const actionLabel = bcdrElement("label", "How would you like to continue?");
  const action = bcdrElement("select");
  action.name = "warehouse_action";
  const options = {};
  for (const [value, text] of [
    ["continue", "Continue saved setup"], ["existing", "Use an existing Warehouse"], ["create", "Create a new Warehouse"],
  ]) {
    const option = bcdrElement("option", text);
    option.value = value;
    options[value] = option;
    action.appendChild(option);
  }
  actionLabel.appendChild(action);
  group.appendChild(actionLabel);
  const load = bcdrElement("button", "List Warehouses in this workspace", "secondary");
  load.type = "button";
  group.appendChild(load);
  const status = bcdrElement("p",
    "Choose an existing metadata workspace, then list its Warehouses. Listing does not resume capacity or initialize SQL.",
    "hint");
  status.setAttribute("role", "status");
  status.setAttribute("aria-live", "polite");
  group.appendChild(status);
  const existing = bcdrElement("label", "Existing metadata Warehouse");
  const select = bcdrElement("select");
  select.name = "warehouse_id";
  existing.appendChild(select);
  group.appendChild(existing);
  const create = bcdrField("warehouse_name", schema.properties.warehouse_name, schema, true);
  group.appendChild(create.element);
  const name = create.element.querySelector("input");
  const saved = () => bcdr.savedSetup?.workspaceId === workspaceId() ? bcdr.savedSetup : null;
  let loadedWorkspace = null;
  let sequence = 0;
  let loading = false;
  const show = () => {
    existing.hidden = select.disabled = action.value !== "existing";
    select.required = action.value === "existing";
    create.element.hidden = name.disabled = action.value !== "create";
    name.required = action.value === "create";
  };
  const renderSaved = () => {
    const previous = saved();
    options.continue.disabled = !previous?.warehousePhase;
    options.existing.disabled = !workspaceId();
    options.create.disabled = !!previous?.warehousePhase;
    load.disabled = loading || !workspaceId();
    recorded.textContent = previous?.warehousePhase ?
      `Saved setup: ${previous.warehouseName || "Metadata Warehouse"} - ${bcdrLabel(previous.warehousePhase)}. ` +
        (previous.warehouseId ? "Continue using the recorded Warehouse, or list it below. " :
          "A prior creation was recorded. If its operation is unavailable, list Warehouses and explicitly select the intended item. ") +
        "Existing catalog compatibility is checked before initialization; no Warehouse is adopted by name." :
      "Use an empty existing Warehouse or create a new metadata Warehouse. Unrelated contents are never overwritten.";
  };
  const reset = () => {
    ++sequence;
    loading = false;
    loadedWorkspace = null;
    bcdr.warehouseChoices = [];
    select.replaceChildren();
    const empty = bcdrElement("option", "List Warehouses, then choose one");
    empty.value = "";
    select.appendChild(empty);
    select.value = "";
    name.value = saved()?.warehouseName || schema.properties.warehouse_name.default || "";
    action.value = saved()?.warehousePhase ? "continue" : workspaceId() ? "existing" : "create";
    renderSaved();
    show();
  };
  const list = async () => {
    if (loading || bcdr.pending || !bcdrCurrentSession(state.sessionId) || !workspaceId()) return;
    const sessionId = state.sessionId;
    const workspace = workspaceId();
    const version = ++sequence;
    const previous = select.value;
    loading = true;
    loadedWorkspace = null;
    load.disabled = true;
    status.textContent = "Reading Warehouses in the selected metadata workspace...";
    try {
      const result = await api(`/api/bcdr/workspaces/${encodeURIComponent(workspace)}/warehouses`);
      if (!bcdrCurrentSession(sessionId) || version !== sequence || workspace !== workspaceId()) return;
      if (result.workspaceId !== workspace || !Array.isArray(result.warehouses)) {
        throw new Error("The service returned an incomplete Warehouse list");
      }
      bcdr.savedSetup = result.savedSetup;
      if (bcdr.savedSetup && result.workspaceName) bcdr.savedSetup.workspaceName = result.workspaceName;
      bcdr.warehouseChoices = result.warehouses;
      select.replaceChildren();
      const empty = bcdrElement("option", "Choose a Warehouse");
      empty.value = "";
      select.appendChild(empty);
      for (const entry of result.warehouses) {
        const option = bcdrElement("option", `${entry.displayName} - ` +
          (entry.recorded ? "saved selection" : entry.endpointReported ? "SQL endpoint reported" : "endpoint not reported yet"));
        option.value = entry.id;
        select.appendChild(option);
      }
      select.value = result.warehouses.some((entry) => entry.id === previous) ? previous : "";
      loadedWorkspace = workspace;
      status.textContent = result.warehouses.length ?
        `Found ${result.warehouses.length} Warehouse(s). Choose Use an existing Warehouse, then select one by name. ` +
          "Catalog compatibility will be checked before setup; nothing was changed." :
        "No Warehouses were returned. Check workspace access or choose Create a new Warehouse when no setup is pending.";
      renderSaved();
      show();
    } catch (error) {
      if (bcdrCurrentSession(sessionId) && version === sequence) {
        status.textContent = `Could not list Warehouses: ${error.message}. Retry this read; no setup was started.`;
      }
    } finally {
      if (version === sequence) {
        loading = false;
        load.disabled = !workspaceId();
      }
    }
  };
  action.addEventListener("change", show);
  load.addEventListener("click", list);
  reset();
  bcdr.bindings.push(renderSaved);
  return {
    element: group,
    contextChanged: (loadChoices = true) => {
      reset();
      status.textContent = "Choose or create a Warehouse for this metadata workspace.";
      if (workspaceId() && loadChoices !== false) list();
    },
    read: () => {
      const result = { warehouse_action: action.value };
      if (loading) throw new Error("Wait for the Warehouse list to finish loading.");
      if (options[action.value]?.disabled) throw new Error("Choose an available Warehouse setup action.");
      if (action.value === "existing") {
        const entry = bcdr.warehouseChoices.find((row) => row.id === select.value);
        if (loadedWorkspace !== workspaceId() || !entry) throw new Error("List Warehouses and select the intended item.");
        result.warehouse_id = entry.id;
        result.warehouse_name = entry.displayName;
      } else if (action.value === "continue") {
        result.warehouse_name = saved().warehouseName;
      } else {
        result.warehouse_name = create.read();
      }
      if (saved()) result.expected_setup_revision = saved().revision;
      return result;
    },
  };
}

function bcdrReviewRequest(value, key = "") {
  if (["control_workspace_id", "source_capacity_ids", "include_workspace_ids",
    "exclude_workspace_ids", "approved_addition_ids", "source_capacity_id", "target_capacity_id",
    "fabric_capacity_id", "catalog_capacity_id", "arm_resource_id", "capacity_id", "target_capacity_ids",
    "warehouse_id", "control_warehouse_id"].includes(key)) {
    const entries = bcdrResourceChoices(key);
    const display = (id) => entries.find((entry) => entry.id === id)?.label ||
      (key === "arm_resource_id" && typeof id === "string" ? id.split("/").at(-1) : "Name unavailable; see server logs");
    return Array.isArray(value) ? value.map(display) : value == null ? value : display(value);
  }
  if (Array.isArray(value)) return value.map((entry) => bcdrReviewRequest(entry));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([name, entry]) => [name, bcdrReviewRequest(entry, name)]));
  }
  return value;
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
  if (name === "control_workspace_id") return bcdrNamedWorkspace(name, id, required);
  if (["source_capacity_id", "target_capacity_id", "catalog_capacity_id"].includes(name)) {
    return bcdrCapacitySelect(name, id, required);
  }
  if (schema.type === "object" && schema.properties?.arm_resource_id && schema.properties?.fabric_capacity_id) {
    return bcdrRecoveryCapacity(id, schema, root, context);
  }
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
    const refresh = (reason) => {
      if (reason === "discovery") return;
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
    const refresh = (reason) => {
      if (reason === "discovery") return;
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
    fieldset.appendChild(choices);
    let controls = [];
    const refresh = () => {
      const selected = new Map(controls.filter((control) => control.checked)
        .map((control) => [control.value, control.dataset.label]));
      choices.replaceChildren();
      controls = [];
      const entries = bcdrResourceChoices(name);
      if (name === "approved_addition_ids") choices.appendChild(bcdrElement("p",
        "Approve only the required additions named by the latest preview. Exclusions are not overridden silently.",
        "hint"));
      if (!entries.length) {
        choices.appendChild(bcdrElement("p",
          name === "source_capacity_ids" ? "Discover setup choices to select source capacities by name." :
          "Workspace choices appear after discovery or catalog status. Empty inclusion uses only the configured source-capacity scope.",
          "hint"));
      }
      const rows = entries.map((entry) => ({
        ...entry, unavailable: entries.filter((other) => other.label === entry.label).length > 1,
      }));
      for (const [value, label] of selected) {
        if (!entries.some((entry) => entry.id === value)) rows.push({ id: value, label, unavailable: true });
      }
      for (const entry of rows) {
        const label = bcdrElement("label", undefined, "bcdr-check");
        const checkbox = bcdrElement("input");
        checkbox.type = "checkbox";
        checkbox.value = entry.id;
        checkbox.dataset.label = entry.label;
        checkbox.dataset.unavailable = entry.unavailable ? "true" : "";
        checkbox.checked = selected.has(entry.id);
        checkbox.disabled = entry.unavailable && !checkbox.checked;
        label.appendChild(checkbox);
        label.appendChild(bcdrElement("span", entry.label +
          (entry.unavailable ? " (unavailable or ambiguous; uncheck or refresh discovery)" : "")));
        choices.appendChild(label);
        controls.push(checkbox);
      }
    };
    bcdr.bindings.push(refresh);
    refresh();
    return {
      element: fieldset,
      read: () => {
        const selected = controls.filter((control) => control.checked);
        const captured = name !== "source_capacity_ids" && bcdr.latest.details?.inventory?.workspaces?.length;
        const kind = name === "source_capacity_ids" ? "capacities" : "workspaces";
        if (selected.length && !captured && bcdr.discovered.source?.errors?.[kind]) {
          throw new Error(`Refresh source ${kind} successfully before submitting these selections.`);
        }
        if (selected.some((control) => control.dataset.unavailable)) {
          throw new Error(`${title}: remove unavailable selections or refresh discovery.`);
        }
        if (required && !selected.length) throw new Error(`Select at least one of the ${title.toLowerCase()}.`);
        return selected.map((control) => control.value);
      },
    };
  }
  if (name === "approved_acl_ids") {
    const fieldset = bcdrElement("fieldset", undefined, "bcdr-wide");
    fieldset.appendChild(bcdrElement("legend", title));
    const choices = bcdrElement("div");
    fieldset.appendChild(choices);
    let controls = [];
    const refresh = (reason) => {
      if (reason === "discovery") return;
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
    let warehouse;
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
      select.value = bcdr.savedSetup?.workspaceId ? "existing" : "create";
      select.disabled = !!bcdr.savedSetup?.workspaceId;
      label.appendChild(select);
      group.appendChild(label);
      const create = bcdrField("control_workspace_name", schema.properties.control_workspace_name, root, true);
      const existing = bcdrField("control_workspace_id", schema.properties.control_workspace_id, root, true);
      if (bcdr.savedSetup?.workspaceId) existing.setValue(bcdr.savedSetup.workspaceId);
      else if (bcdr.savedSetup?.workspaceName) create.element.querySelector("input").value = bcdr.savedSetup.workspaceName;
      if (bcdr.savedSetup?.workspaceId) existing.select.disabled = true;
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
      if (schema.properties.warehouse_action) {
        warehouse = bcdrWarehouseSetup(() => select.value === "existing" ? existing.select.value : null, root);
        fields.appendChild(warehouse.element);
        select.addEventListener("change", warehouse.contextChanged);
        existing.select.addEventListener("change", warehouse.contextChanged);
        const syncSavedWorkspace = () => {
          const savedId = bcdr.savedSetup?.workspaceId;
          if (!savedId) return;
          if (select.value !== "existing" || existing.select.value !== savedId) {
            select.value = "existing";
            existing.setValue(savedId);
            change();
            warehouse.contextChanged(false);
          }
          group.disabled = true;
        };
        bcdr.bindings.push(syncSavedWorkspace);
        syncSavedWorkspace();
      }
      workspace = () => select.value === "create" ?
        { control_workspace_name: create.read() } : { control_workspace_id: existing.read() };
    }
    for (const [key, value] of Object.entries(schema.properties || {})) {
      if (workspace && ["control_workspace_name", "control_workspace_id"].includes(key)) continue;
      if (warehouse && ["warehouse_name", "warehouse_action", "warehouse_id", "expected_setup_revision"].includes(key)) continue;
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
        ...(warehouse ? warehouse.read() : {}),
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
      remove.addEventListener("click", () => {
        readers.delete(row);
        field.dispose?.();
        row.remove();
        add.focus();
      });
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

function bcdrActionFeedback(trigger) {
  const host = trigger.closest("form") || trigger.parentElement;
  let feedback = host.querySelector(".bcdr-feedback");
  if (!feedback) {
    feedback = bcdrElement("div", undefined, "bcdr-feedback");
    const error = bcdrElement("p", "", "alert bcdr-action-error");
    error.id = `bcdr-action-error-${++bcdr.sequence}`;
    error.setAttribute("role", "alert");
    error.hidden = true;
    const progress = bcdrElement("p", "", "hint bcdr-action-progress");
    progress.setAttribute("role", "status");
    progress.setAttribute("aria-live", "polite");
    const warnings = bcdrElement("div", "", "warnings bcdr-action-warnings");
    warnings.setAttribute("role", "status");
    warnings.setAttribute("aria-live", "polite");
    warnings.hidden = true;
    feedback.appendChild(error);
    feedback.appendChild(progress);
    feedback.appendChild(warnings);
    host.appendChild(feedback);
  }
  return {
    error: feedback.querySelector(".bcdr-action-error"),
    progress: feedback.querySelector(".bcdr-action-progress"),
    warnings: feedback.querySelector(".bcdr-action-warnings"),
  };
}

function bcdrError(message, trigger) {
  const error = trigger ? bcdrActionFeedback(trigger).error : $("#bcdr-error");
  error.textContent = message;
  error.hidden = !message;
  if (message && !trigger) { error.tabIndex = -1; error.focus(); }
}

function bcdrRenderResult(result) {
  if (result.details?.setup_phase === "catalog_ready") {
    bcdr.savedSetup = { ...bcdr.savedSetup, workspaceId: result.details.control_workspace_id,
      warehouseId: result.details.control_warehouse_id, warehouseName: result.details.warehouse_name,
      warehousePhase: "ready", revision: result.details.setup_revision };
  }
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
      const name = bcdrResourceChoices("capacity_id").find((row) => row.id === capacity.capacity_id)?.label ||
        capacity.arm_resource_id?.split("/").at(-1) || "Recovery capacity";
      entry.appendChild(bcdrElement("strong", `${capacity.state || "Unreported"} - ${name}`));
      entry.appendChild(bcdrElement("p",
        `Provisioning: ${capacity.provisioning_state || "Unreported"}. Observed: ${capacity.observed_at}.`));
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
  const data = bcdrElement("pre", JSON.stringify(bcdrReviewRequest(result), null, 2));
  data.tabIndex = 0;
  details.appendChild(data);
  output.appendChild(details);
  output.hidden = false;
}

async function bcdrSubmit(command, body, button) {
  if (bcdr.pending) return;
  if (!state.sessionId || state.sessionId !== bcdr.sessionId) {
    bcdrError("The sign-in changed. Reopen Standby & recovery and review the request again.", button);
    return;
  }
  const sessionId = state.sessionId;
  bcdr.pending = true;
  bcdrError("");
  bcdrError("", button);
  const feedback = bcdrActionFeedback(button);
  feedback.warnings.replaceChildren();
  feedback.warnings.hidden = true;
  $("#bcdr-progress").textContent = "Progress, errors and warnings are shown beside the selected action.";
  const panel = $("#bcdr-panel");
  panel.setAttribute("aria-busy", "true");
  const controls = Array.from(panel.querySelectorAll("button, input, select, textarea"));
  const disabled = controls.map((control) => control.disabled);
  const actionControls = Array.from((button.closest("form") || button.parentElement)
    .querySelectorAll("input, select, textarea"));
  actionControls.forEach((control) => {
    control.setAttribute("aria-invalid", "false");
    control.setAttribute("aria-errormessage", "");
  });
  controls.forEach((control) => { control.disabled = true; });
  feedback.progress.textContent =
    `${command.label} is in progress. Closing this view does not cancel server-side work.`;
  try {
    const payload = command.confirmation ? { confirmation: command.name, request: body } : body;
    const result = await api(command.path, { method: command.method, body: payload });
    if (!bcdrCurrentSession(sessionId)) return;
    bcdrRenderResult(result);
    feedback.progress.textContent = `${command.label}: result received. Review all group outcomes below.`;
    if (result.details?.setup_phase === "catalog_ready") {
      feedback.progress.textContent = `Metadata Warehouse ${result.details.warehouse_name} is ready. ${result.details.next_action}`;
    }
    if (result.warnings?.length) {
      feedback.warnings.appendChild(bcdrElement("strong", "Review these warnings"));
      const list = bcdrElement("ul");
      result.warnings.forEach((warning) => list.appendChild(bcdrElement("li", warning)));
      feedback.warnings.appendChild(list);
      feedback.warnings.hidden = false;
    }
  } catch (error) {
    if (bcdrCurrentSession(sessionId)) {
      for (const entry of error.details || []) {
        const name = entry.loc?.at(-1);
        actionControls.filter((control) => control.name === name).forEach((control) => {
          control.setAttribute("aria-invalid", "true");
          control.setAttribute("aria-errormessage", feedback.error.id);
        });
      }
      bcdrError(error.message, button);
      feedback.progress.textContent = "The operation did not report success. Review the error here before retrying.";
    }
  } finally {
    bcdr.pending = false;
    panel.setAttribute("aria-busy", "false");
    controls.forEach((control, index) => { control.disabled = disabled[index]; });
    if (bcdrCurrentSession(sessionId)) button.focus({ preventScroll: true });
  }
}

function bcdrConfirm(command, body, trigger) {
  bcdrError("", trigger);
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
  chosen.appendChild(bcdrElement("summary", "Review selected resources and approvals"));
  chosen.appendChild(bcdrElement("pre", JSON.stringify(bcdrReviewRequest(body), null, 2)));
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
      bcdrError("The sign-in changed. Review this operation again in the current session.", trigger);
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
  bcdr.capacityBindings = [];
  bcdr.recoveryRows = new Map();
  const preparation = bcdrElement("details");
  preparation.appendChild(bcdrElement("summary", "Healthy-source preparation: discover named setup choices"));
  preparation.appendChild(bcdrElement("p",
    "This optional read contacts the source and recovery Fabric APIs. Do not use it during a source outage; " +
    "recovery actions use captured catalog inventory instead. It does not resume capacity or enable recovery.", "hint"));
  const discover = bcdrElement("button", "Discover setup choices", "secondary");
  discover.type = "button";
  const discoveryStatus = bcdrElement("p", "", "hint");
  discoveryStatus.id = "bcdr-discovery-status";
  discoveryStatus.setAttribute("role", "status");
  discoveryStatus.setAttribute("aria-live", "polite");
  discover.setAttribute("aria-describedby", discoveryStatus.id);
  discover.addEventListener("click", async () => {
    if (bcdr.pending || bcdr.discovering || !bcdrCurrentSession(state.sessionId)) return;
    const sessionId = state.sessionId;
    const sequence = ++bcdr.discoverySequence;
    bcdr.discovering = true;
    busy(discover, true, "Discovering choices...");
    discoveryStatus.textContent = "Reading source and recovery workspace and capacity names. No setup is being saved.";
    bcdrError("");
    try {
      const inventory = await api("/api/bcdr/discovery");
      if (!bcdrCurrentSession(sessionId) || sequence !== bcdr.discoverySequence) return;
      if (!["source", "recovery"].every((side) => inventory[side] &&
        ["workspaces", "capacities"].every((kind) =>
          Array.isArray(inventory[side][kind]) || typeof inventory[side].errors?.[kind] === "string"))) {
        throw new Error("The discovery response is incomplete. Inspect the server logs before retrying");
      }
      if ("savedSetup" in inventory) bcdr.savedSetup = inventory.savedSetup;
      const summaries = [];
      const failures = [];
      for (const side of ["source", "recovery"]) {
        const found = inventory[side] || {};
        bcdr.discovered[side] = { ...bcdr.discovered[side], ...found, errors: found.errors || {} };
        for (const kind of ["workspaces", "capacities"]) {
          const error = found.errors?.[kind];
          if (error) failures.push(`${bcdrLabel(side)} ${kind}: ${error}`);
          else summaries.push(`${bcdrLabel(side)}: ${(found[kind] || []).length} ${kind}`);
        }
      }
      const mappingError = inventory.recovery.errors?.capacityMapping;
      if (mappingError) failures.push(`Recovery capacity matching: ${mappingError}`);
      const matches = inventory.recovery.capacityChoices || [];
      if (!mappingError && matches.length) {
        summaries.push(`${matches.filter((entry) => entry.matchStatus === "matched").length} recovery capacities matched by name/region`);
        const unavailable = matches.filter((entry) => entry.matchStatus !== "matched");
        if (unavailable.length) failures.push(`${unavailable.length} recovery capacities have no unique Azure name/region match. Review the capacity choices below.`);
      }
      bcdr.bindings.forEach((refresh) => refresh("discovery"));
      const empty = !["source", "recovery"].some((side) =>
        inventory[side]?.workspaces?.length || inventory[side]?.capacities?.length);
      discoveryStatus.textContent = `${failures.length ? "Discovery incomplete. " : "Discovery complete. "}` +
        (summaries.length ? summaries.join("; ") + ". " : "") +
        (failures.length ? `${failures.join(" ")} Previous choices are retained where refresh failed. Retry discovery after resolving the error. ` :
          empty ? "No resources are visible. Check the source and recovery principals' workspace and capacity access, then retry discovery. " :
          "Choose the named resources below. Existing valid selections have been kept. ") +
        "No configuration was saved and no capacity operation was started.";
      $("#bcdr-progress").textContent = "Discovery results are shown beside Discover setup choices.";
    } catch (error) {
      if (bcdrCurrentSession(sessionId) && sequence === bcdr.discoverySequence) {
        discoveryStatus.textContent = `Discovery failed: ${error.message}. Existing choices were not changed. ` +
          "Resolve the error and retry. No configuration was saved and no capacity operation was started.";
      }
    } finally {
      if (sequence === bcdr.discoverySequence) {
        bcdr.discovering = false;
        busy(discover, false);
      }
    }
  });
  preparation.appendChild(discover);
  preparation.appendChild(discoveryStatus);
  content.appendChild(preparation);
  for (const command of commands) {
    const section = bcdrElement("details");
    section.open = command.name === "setup";
    section.appendChild(bcdrElement("summary", command.label));
    section.appendChild(bcdrElement("p", command.description, "hint"));
    const form = bcdrElement("form");
    form.noValidate = true;
    form.setAttribute("aria-label", command.label);
    const field = bcdrField(command.label, command.schema, command.schema, true);
    field.element.querySelector("legend")?.remove();
    form.appendChild(field.element);
    const button = bcdrElement("button", command.label, command.primary ? "primary" : "secondary");
    button.type = "submit";
    form.appendChild(button);
    bcdrActionFeedback(button);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (bcdr.pending) return;
      if (!form.checkValidity()) {
        const feedback = bcdrActionFeedback(button);
        const invalid = Array.from(form.querySelectorAll("input, select, textarea"))
          .filter((control) => control.validity?.valid === false);
        invalid.forEach((control) => {
          control.setAttribute("aria-invalid", "true");
          control.setAttribute("aria-errormessage", feedback.error.id);
        });
        const messages = invalid.map((control) =>
          `${bcdrLabel(control.name || "field")}: ${control.validationMessage}`);
        bcdrError(messages.join(" ") || "Complete the required fields in this form before continuing.", button);
        return;
      }
      try {
        const body = command.method === "GET" ? undefined : field.read();
        if (command.confirmation) bcdrConfirm(command, body, button);
        else bcdrSubmit(command, body, button);
      } catch (error) {
        bcdrError(error.message, button);
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
  bcdr.discovering = false;
  ++bcdr.discoverySequence;
  $("#bcdr-content").replaceChildren();
  bcdrError("");
  $("#bcdr-progress").textContent = "Loading operation forms. Recovery is not being enabled.";
  const sessionId = state.sessionId;
  try {
    const response = await api("/api/bcdr/forms");
    if (!bcdrCurrentSession(sessionId)) return;
    bcdr.identity = response.identity || {};
    bcdr.savedSetup = response.savedSetup || null;
    bcdr.warehouseChoices = [];
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
  bcdr.discovering = false;
  ++bcdr.discoverySequence;
  bcdr.bindings = [];
  bcdr.capacityBindings = [];
  bcdr.recoveryRows = new Map();
  bcdr.savedSetup = null;
  bcdr.warehouseChoices = [];
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
