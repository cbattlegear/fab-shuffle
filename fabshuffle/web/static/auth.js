/* EasyAuth authorizes the operator; the selected Fabric identity authorizes the work. */
async function loadAuthenticationOptions() {
  const status = $("#auth-options-status");
  const option = $("#managed-identity-option");
  state.managedIdentityAvailable = false;
  option.disabled = true;
  option.hidden = true;
  status.textContent = "Checking available authentication methods...";
  try {
    const options = await api("/api/auth/options");
    const identity = options.managedIdentity;
    const available = !!(options.easyAuth && options.operator && identity);
    state.managedIdentityAvailable = available;
    option.disabled = !available;
    option.hidden = !available;
    $("#azure-sign-out").hidden = !options.easyAuth;
    const context = $("#operator-context");
    context.hidden = !options.operator;
    context.textContent = options.operator
      ? `Azure operator: ${options.operator.objectId}. This sign-in is separate from Fabric access.`
      : "";
    $("#managed-identity-context").textContent = available
      ? `Use the configured identity ${identity.client_id} in tenant ${identity.tenant_id}. ` +
        "No client secret is required. This option accesses workspaces in the same tenant only."
      : "";
    status.textContent = available
      ? "Managed identity is available. Its Fabric and Azure permissions must be granted separately."
      : "Use a service principal with access to the source workspace and destination capacity.";
    updateLoginMode();
  } catch (error) {
    status.textContent = `Authentication options could not be loaded: ${error.message}`;
    $("#fabric-auth-mode").value = "service_principal";
    updateLoginMode();
  }
}

loadAuthenticationOptions();
