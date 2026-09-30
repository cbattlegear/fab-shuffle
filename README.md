# Fab Shuffle

This solves two scenarios: 
1) I need to move a workspace from a capacity in Region 1 to a capacity in Region 2.
2) I need to move a workspace from Tenant 1 to Tenant 2.

It does this by automating the API calls, data copy processes, and ID remapping needed to get the workspace items from point A to point B. 

## Setup prerequisites

Configure access **before deploying or running Fab Shuffle**:

1. **Choose the Fabric runtime identity.** Normally,
   [register a Microsoft Entra application and create a client secret](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal).
   Keep its **tenant ID, application (client) ID, and client secret value** for the wizard.
   Azure deployments can instead attach an [existing user-assigned managed identity](#optional-managed-identity).
   Add the chosen principal to an [Entra security group](https://learn.microsoft.com/en-us/entra/fundamentals/how-to-manage-groups).
2. **Allow normal Fabric APIs.** Have a Fabric administrator open **Admin portal > Tenant settings > Developer settings**.
   Enable **Service principals can call Fabric public APIs** and
   **Service principals can create workspaces, connections, and deployment pipelines** for that security group.
   See [Developer tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-developer).
3. **Assign it to the source workspace.** In the Fabric workspace you want to migrate,
   open **Manage access**, add the service principal, and give it the **Admin** workspace role.
   Enabling the tenant settings does not grant workspace access.

Also grant the principal **Contributor or Admin on the destination Fabric capacity**
([required for workspace creation](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/create-workspace));
Azure capacity Reader alone is not sufficient. Share the [connections](#connections) used by
the selected items and shortcuts with it.

**Optional admin API fallback:** workspace role discovery normally uses the
[workspace-scoped API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspace-role-assignments).
If that read is denied, the tool tries an admin endpoint. To permit the fallback, a Fabric
administrator can enable **Service principals can access read-only admin APIs** for the
security group under **Tenant settings > Admin API settings**. This grants broader,
tenant-wide read visibility; it is not a blanket prerequisite for workspace-scoped migration.
Follow the [admin API setup guidance](https://learn.microsoft.com/en-us/fabric/admin/enable-service-principal-admin-apis),
including its restriction on admin-consent-required Power BI application permissions.

For [cross-tenant migrations](#cross-tenant-migrations), use one principal per tenant:
configure the applicable tenant settings in each, source-workspace access for the source
principal, and destination-capacity access for the destination principal.

## Run it

Choose a local Docker container or Azure Container Apps. **Run and test through the Docker
image, not a host Python preview.** It includes the required AzCopy, SqlPackage,
UnpackDacPac, bcp and ODBC tools. SQL authentication uses service-principal access tokens,
not your desktop identity.

### Locally

Install Docker and use **Linux containers** mode on Windows, then run:

```bash
docker run --rm --platform linux/amd64 -p 8080:8080 -v fab-shuffle-scratch:/app/local \
  ghcr.io/cbattlegear/fab-shuffle:latest
```

Open <http://localhost:8080>. The volume preserves staging files and recovery journals when
the container is replaced; without it, they are lost with the container.

### In Azure

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fcbattlegear%2Ffab-shuffle%2Fmain%2Fdeploy%2Fazuredeploy.json)

Deploys [`deploy/azuredeploy.json`](deploy/azuredeploy.json): a Container App running the
same public image, its environment, a Log Analytics workspace for the container's logs,
and a storage account with a classic Azure Files SMB share mounted at `/app/local`, plus a
private Blob container used **only for BCDR coordination**. The central Fabric Warehouse
remains the sole metadata catalog. Staging files, controller bootstrap and recovery journals
use the share; session credentials and active workers remain in memory.

**Entra EasyAuth is mandatory in this Azure template**, whether the runtime uses a managed
identity or a service-principal secret. It replaces the old optional client-IP restriction.
Browser operator sign-in and authorization are separate from the identity calling Fabric:
signing in does not grant the runtime access to Fabric. Only explicitly listed user object
IDs can use the console. Do not add all tenant users or treat recovery workspace ACL groups
as the operator allowlist.

#### Register operator sign-in before deploying

Follow [Container Apps authentication with an existing Entra registration](https://learn.microsoft.com/en-us/azure/container-apps/authentication-entra#option-2-use-an-existing-registration-created-separately).
This template does **not** create an app registration or update Microsoft Graph.

1. Create a separate **single-tenant** Entra app registration for the web console (accounts
   in this organizational directory only). Record its tenant and application/client GUIDs.
   Set its Application ID URI to `api://<client-id>` and add the `user_impersonation` scope
   described in Learn. Enable **ID tokens (used for implicit and hybrid flows)** under
   Authentication. Create a client secret and retain its **value**, not its secret ID.
   The template requires this secret for the documented hybrid flow; it never falls back
   to secretless implicit flow. This registration is not the Fabric runtime principal.
2. Deploy with `easyAuthTenantId`, `easyAuthClientId`, `easyAuthClientSecret` and a nonempty
   `allowedOperatorObjectIds` array of **individual user object-ID GUIDs in that tenant**.
   No group IDs, client IDs, emails or wildcards. Enter the secret through the secure portal
   field or a secret-backed deployment parameter, never a checked-in parameter file or
   command-line literal. ARM stores it as the Container App's `fab-shuffle-easyauth` secret;
   it is not passed to Python or returned as an output.
3. Wait for the **entire** deployment to succeed, then run its `verifyAuthenticationCommand`
   output. Inspect the actual auth configuration: platform and Entra provider enabled,
   `RedirectToLoginPage`, your tenant-specific `/v2.0` issuer and application audiences,
   the exact nonempty allowed operator **identities**, `fab-shuffle-easyauth` secret reference,
   and only `/api/health` excluded. Do not continue after a failed or incomplete auth deployment.
4. Run `enablePublicIngressCommand` **while `FAB_SHUFFLE_EASYAUTH_READY=false`**. The template
   starts with internal ingress and the application closed even to internal callers with
   forged principal headers. This supported ingress-only update does not replace the app's
   environment, identity or volume configuration. Then run `readPublicFqdnCommand` to read
   the actual external hostname; internal hostnames have an extra `.internal.` segment.
5. Set the registration's **Web redirect URI** to
   `https://<actual-external-fqdn>/.auth/login/aad/callback`. It must match the
   `easyAuthCallbackUrl` output, which uses Azure's reported default environment domain,
   not the temporary internal hostname. The generated domain is not known before deployment,
   so registration deliberately has a postdeployment step. Do not guess a hostname or use a
   wildcard callback. The template never writes Microsoft Graph.
6. **Last**, run `activateApplicationCommand` (sets only
   `FAB_SHUFFLE_EASYAUTH_READY=true`) and wait for its new revision to be ready. Open the `url`
   output and verify an allowed operator can enter, a non-allowlisted user cannot, and an
   unauthenticated request is redirected. Until activation, application routes return `503`
   even with a valid or forged principal; health remains available. Rotate the registration
   secret and Container App secret together before expiry; never disable authentication to
   work around an expired secret or wrong callback.

After activation, the application requires the platform principal headers and matches
tenant/object ID against the same allowlist on every protected request. Only the exact
`/api/health` probe path is excluded; `/api` is not public. The token store is disabled:
operator authorization uses the platform identity claims, not access-token headers or
persisted sign-in tokens. Use a released image containing these guards, not an older image
that ignores their environment variables. Pin a published production image digest.
Every ARM redeployment intentionally resets internal ingress and readiness to false:
finish active work first, then repeat verification and activation. There is no public or
ready-by-default parameter that can bypass the initial auth deployment.

**App Name** and **Location** are optional overrides. Leave them blank to generate the
app name and use the selected resource group's region; ARM resolves these defaults during
deployment instead of showing expressions in the form. To update an existing deployment,
keep its app name (or leave blank if it used the generated default) and deployment region.
If you override Location, enter an Azure region code such as `canadacentral`.

Worth knowing before you use it:

- **It runs as exactly one replica, deliberately.** Sessions and run progress are held in the
  process, so a second replica would not know about your sign-in or your migration. Do not
  raise `maxReplicas`.
- **Clean up after the migration.** Compute, logs, and storage incur charges. Retain the share
  while you need recovery journals or staged files; deleting the storage account or resource
  group deletes those too.
- The region you deploy into has nothing to do with the region you are migrating *to*. That
  comes from the capacity you pick in the wizard.

#### Optional Managed Identity

Set `managedIdentityResourceId` to the full ARM ID of an **existing user-assigned managed
identity (UAMI)**, or leave it empty to retain runtime service-principal sign-in. No identity
is silently created and no system-assigned identity is used. A shared UAMI survives an app
recreation and can be attached to both the web controller and scheduled job. Do not recreate
the UAMI or change its tenant/client/object ID after initializing a recovery bootstrap;
the stored recovery identity is not rewritten to adopt a different principal.

The template resolves the UAMI's actual **client ID, principal/object ID and tenant ID**
through ARM and returns them as outputs. The resource ID is not any of those GUIDs.
Only when a UAMI is attached does the container receive
`FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID` and `FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID`.
Token acquisition uses the Container Apps identity endpoint; it does not fall back to
developer credentials or a secretly configured service principal. Managed Identity is for
the same-tenant runtime, not a substitute for separate principals in a cross-tenant move.
Every allowed operator who uses it can exercise its granted runtime permissions: keep both
the operator list and those permissions narrow.

Grant the identity the [Fabric prerequisites above](#setup-prerequisites): applicable tenant
settings/security-group membership, workspace access, destination capacity permissions,
and required connection access. SQL/Warehouse and OneLake authorization are separate checks;
attaching an identity does not create database users or grant data access. For BCDR capacity
pause/resume, separately assign an Azure custom role with the
[documented capacity actions](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume#prerequisites)
(`Microsoft.Fabric/capacities/read`, `write`, `suspend/action`, `resume/action`), scoped only
to the **explicit dedicated recovery capacity ARM resources**. The template does not assign
subscription Contributor or any Fabric/capacity role.

The one automatic data role is **Storage Blob Data Contributor**, assigned to the UAMI on
the dedicated `fab-shuffle-locks` **container only**, for the BCDR remote lease. Deployment
therefore needs permission to assign the existing UAMI and
`Microsoft.Authorization/roleAssignments/write` at that container scope, in addition to
creating the app/storage and listing the Azure Files account key. Without a UAMI, an
administrator must grant the runtime SP that same container-scoped data role before using
BCDR; use the `bcdrLeaseContainerResourceId` output, not a subscription-wide grant.
Allow for [RBAC propagation](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access);
inspect the service error and retry after the grant takes effect, not by bypassing the lease.

#### Scheduled metadata sync job

[![Deploy scheduled sync job to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fcbattlegear%2Ffab-shuffle%2Fmain%2Fdeploy%2Fazuredeploy-sync-job.json)

This **separate** [`deploy/azuredeploy-sync-job.json`](deploy/azuredeploy-sync-job.json)
deployment adds a scheduled Container Apps Job, not another web console or Fabric notebook.
First initialize BCDR with the UAMI and persist its approved sync request. Reuse the web
deployment's **same environment/resource group/location, Azure Files storage, UAMI, bootstrap,
lease URL and production image digest**. No second Warehouse or storage account is created.
The job has no ingress, browser EasyAuth or runtime client secret; it runs only the guarded
`scheduled-sync` command. The default schedule is **02:00 UTC daily** with a four-hour timeout,
one replica per execution and no automatic retries. Follow the
[job preparation and interruption runbook](docs/bcdr.md#azure-scheduled-metadata-sync-job)
before deploying; successful deployment is not proof of a successful Fabric sync.

#### Storage and lifecycle

The `shareQuotaGiB` parameter defaults to **100 GiB** and accepts **1 through 102400 GiB**
(100 TiB). This is persistent Azure Files storage, **not Azure Blob storage and not unlimited
disk**. File-size, IOPS and throughput limits still apply, and storage operations, capacity
and data transfer have costs. Size the share for concurrent staging, retained interrupted
runs and journals; monitor free space and usage. See
[Azure Files scale targets](https://learn.microsoft.com/en-us/azure/storage/files/storage-files-scale-targets).
The separate template settings `maxDiskStagingGiB` (default **10**) and `maxMemoryMiB`
(default **1024**) control bounded temporary artifacts and in-memory processing respectively.
Neither resizes the share nor changes the container's RAM allocation.

#### Choosing the Azure Files size

Size for **peak concurrent staging plus retained files and headroom**, not simply the largest
data store. Single-principal lakehouse copies stage whole `Files/` areas; bcp stages one SQL
table export at a time. Paired-credential transfers stream data and primarily need schema
and Delta-checkpoint staging instead.

Run [`Get-FabShuffleStorageEstimate.ps1`](scripts/Get-FabShuffleStorageEstimate.ps1) in
PowerShell 7+ with **Az.Accounts** and **Az.Storage**, using your user account with access
to every source item and file:

```powershell
.\scripts\Get-FabShuffleStorageEstimate.ps1 -TenantId <tenant-guid> -WorkspaceId <workspace-guid>
```

Use `-Mode Paired` for two-principal migrations, even within one tenant. The script reports
`SuggestedShareQuotaGiB` using paged Files metadata, staging allowances and **25% headroom**;
it downloads no file contents and changes no resources. Match `-FileConcurrency`,
`-SchemaConcurrency` and `-DiskStagingGiB` to the run, use `-SkipFiles`/`-SkipData` as needed,
and add `-ExistingStagingGiB` for a reused share.

If a single-principal run includes SQLDatabase data, supply `-LargestBcpTableGiB`: the
largest measured or conservatively estimated native table export, **not compressed database
storage size**. This size is not available from Fabric item metadata. Missing measurements
or access failures produce **Incomplete**, with no quota recommendation. Metadata scans
can take time and incur read costs; the estimate is not a guarantee against source growth.

The template mounts the share through the Container Apps environment's `AzureFile` storage
configuration, using an account key obtained by ARM `listKeys`. The key is not an application
environment variable or a template output. The deployment identity needs permission to
create the storage resources and list the account keys. Restrict management access to the
account and environment, and update the environment storage configuration when rotating the
key. This simple template uses authenticated public storage endpoints; the wizard's
EasyAuth does not firewall storage. A private deployment needs compatible VNet, DNS,
storage firewall and SMB connectivity (TCP 445), not just a deny rule on the account.
See [Container Apps storage mounts](https://learn.microsoft.com/en-us/azure/container-apps/storage-mounts).

Replacing a revision does not erase the share, but **wait for active migrations to finish
before deploying an update**: an in-memory worker can still be interrupted. After a restart,
sign in again to resume from the retained journals. Do not point a second independent
controller at the same share. The scheduled job is an explicitly coordinated worker of
the original controller, using the same remote lease, not a second controller.
Existing deployments without a mount need their old journals
copied to the share before switching; adding the mount does not copy the old ephemeral data.

### The wizard

1. **Sign in** with the service principal's tenant ID, client ID, and secret, or use the
   configured Azure Managed Identity. In Azure, first pass the separate Entra operator sign-in.
2. **Pick the target capacity** — its region is the destination region.
3. **Pick the source workspace.**
4. **Review** the plan. Fab Shuffle tells you whether the workspace can simply be reassigned
   or has to be rebuilt, lists anything it cannot move, and refuses to start if there is a
   blocker.
5. **Migrate**, watching each step report progress live, then delete the temporary artifacts.

Session client secrets and the token cache are held in memory, not persisted for recovery.
The single-principal bcp path does write a SQL access token to an owner-only temporary file
and removes it afterward. Protect the container filesystem; an abrupt process termination
can bypass normal cleanup.

### Standby and recovery

Choose **Operate standby & recovery (same tenant)** at sign-in, or **Standby & recovery**
from an existing same-tenant session, for the separate multi-workspace BCDR workflow.
It uses one central metadata Warehouse and a durable non-secret bootstrap descriptor;
it does not use Git integration or a metadata lakehouse/Spark session.

**Sync standby is not Enable recovery.** Standby synchronization updates inactive items
with SPN/owner-only access and parks dedicated recovery capacity only when safe. Recovery
enablement, deferred ACL replay, writer-fenced cutover, failback and rearm are explicit
operations. Captured metadata and restored definitions are not proof of application
readiness; inspect partial group outcomes and optional-data protection warnings.

The standby UI separates **Set up standby**, **DR Test**, and **I'm currently down**.
Normal setup only selects workspace names or a case-insensitive name-contains rule;
recovery infrastructure is configured once under **Settings** and reused.
Initial metadata synchronization leads to a guided scheduled-sync handoff, not automatic
job creation. DR Test uses the existing standby with recovery owners, holds synchronization,
and never cuts over production; incident recovery uses saved state without primary discovery.

See [the BCDR operator guide](docs/bcdr.md) for controller setup, capacity/metadata costs,
source-retention obligations, qualification limits, API routes and the Linux-container
`python -m fabshuffle.bcdr` scheduler entrypoint. Existing migration modes remain unchanged.

For review and preproduction qualification, see the
[BCDR research and design rationale](docs/bcdr-viability.md) and
[live end-to-end testing runbook](docs/bcdr-live-validation.md).

## Migration modes

Fab Shuffle is a Linux-container application with a web wizard. It either reassigns a
Power BI-only workspace or rebuilds supported content on a destination capacity. Published
versions and upgrade notes are listed under [Releases](https://github.com/cbattlegear/fab-shuffle/releases).

| Sign-in mode | Strategy | Access assignments |
| --- | --- | --- |
| One principal, same tenant | Reassign Power BI-only workspaces; otherwise rebuild | Source admins are preserved during rebuild; remaining workspace roles are optional |
| Two principals, same or different tenants (*paired*) | Always rebuild with separate source/destination credentials | Not copied; arrange destination access separately |

### Cross-tenant migrations

Select **Another tenant** on the sign-in screen and enter separate source and destination
service principals. The wizard lists source workspaces with the source identity and
destination capacities with the destination identity. Neither principal needs access to the
other tenant. This creates a new destination workspace; it never reassigns or deletes the source.

Prepare access in each tenant before starting. Both principals need the Fabric API tenant
settings and the item/data-plane permissions for their side of the migration. The destination
principal also needs permission to create workspaces and contributor/admin access to the
destination Fabric capacity, as documented by
[Create Workspace](https://learn.microsoft.com/rest/api/fabric/core/workspaces/create-workspace).
Azure capacity Reader alone is not proof of those Fabric permissions.

**Access assignments are not copied.** This includes workspace admins, connection sharing,
SQL principals/grants/memberships, and semantic-model role memberships. Model role/filter
definitions are retained. Destination administrators must arrange human access and any
workspace identities or gateway infrastructure themselves. Sensitivity labels and tenant
policies require destination configuration; encrypted report/model labels can prevent export.

**Stop source writes before copying data or files.** The review screen requires confirmation.
The freeze must cover scheduled jobs, ingestion, maintenance and expiration processes, and
must remain in place through reconciliation and any retries. If the source changes, start a
fresh migration rather than treating old checkpoints as a new snapshot. Metadata-only
migrations can omit data and files without this confirmation.

On the review screen, map source connection IDs to connections configured in the destination.
For dependencies outside the migrating workspace, provide both source and destination
workspace/item IDs. The tool checks destinations with destination credentials; a source
connection being visible does not make it usable in the destination. It does not export or
reuse source connection secrets.

Some connections cannot be prepared until the new data stores exist. Their consumers are
left uncreated, with the required destination coordinates reported. Use **Retry** to edit
the saved mappings, recheck them, and resume into the same destination. Changed mappings
invalidate affected bindings so existing consumers are refreshed instead of retaining old IDs.

Paired transfers stream OneLake files, SQL rows, KQL results and Cosmos DB documents with
independent source/destination authentication; they do not use native cross-tenant Copy Jobs.
See the [mode-aware support table](#what-gets-migrated) and
[data movement reference](docs/migration-details.md#credential-modes-and-data-movement).

`FAB_SHUFFLE_MAX_MEMORY_BYTES` defaults to **1073741824** (1 GiB) for in-memory
rows, documents, metadata and script processing. `FAB_SHUFFLE_MAX_DISK_STAGING_BYTES`
defaults to **10737418240** (10 GiB) for bounded schema artifacts and on-disk Delta
checkpoints. Raising the disk budget does not increase streaming buffers or the memory
budget. These are per-operation application checks, not aggregate RAM/filesystem quotas.
Schema tools can briefly overshoot between checks and are stopped when excess is detected.
SQL schema transfers use private, disk-budgeted staging in both credential modes and remove
those artifacts after the transfer; script processing keeps its separate memory budget.
The older single-principal AzCopy/bcp disk-staging paths remain uncapped by these settings;
size the share for their complete staging workload.
Delta preflight checks managed Tables and unmanaged Delta tables under Files for unsafe
references; it does not rewrite logs to conceal dependencies. Unsupported features and
budget failures are reported with corrective actions. See
[Delta and SQL staging](docs/migration-details.md#delta-and-sql-staging) for the mechanics.

Definitions are recreated only after known references have destination mappings. Mirrors,
Activator rules and Databricks catalog sync retain their stopped-arrival behavior.
**Eventstreams are manual in paired migrations:** an inactive-create contract could not be
confirmed for every node, so the tool does not create a potentially active second stream.
Recreate those with ingestion deactivated, then reconnect their dependent items. Airflow
configuration and DAG files are copied without the tool invoking jobs; inspect DAG pause
settings and runtime dependencies before enabling the destination scheduler. Import-mode
semantic models need destination credentials and refresh; definition creation does not copy
their cached data.

Recovery is bound to the ordered tenant and application pair. Paired journals live separately
under `local/journal-paired-v1`, so older single-client builds cannot accidentally resume them.
Sign back in with the same applications to resume, inspect a run, cancel it, or clean up its
owned temporary resources. Global scratch sweeps and source-admin restoration are unavailable
in paired mode.

The cutover report remains advisory: transferred content, unmeasured runtime behavior,
manual activation and omitted access configuration are distinct. Keep the source until an
operator has checked destination queries, data and application access. No live two-tenant
service qualification is implied by the automated test suite.

For API clients, see [API integration](docs/migration-details.md#api-integration) and the
interactive schema at `/api/docs`. Omitting destination credentials selects the
single-principal workflow; it does not force reassignment for a workspace with Fabric items.

### Two ways to move a workspace

For single-principal same-tenant moves, Fab Shuffle inspects the workspace and picks the
cheaper of two strategies. Paired migrations always rebuild.

**Reassign** — if the workspace holds only Power BI content (reports, paginated reports,
semantic models, dashboards), there is nothing to rebuild. The cross-region restriction on
`assignToCapacity` only applies to Fabric items, so Fab Shuffle simply assigns the existing
workspace to a capacity in the target region. Nothing is copied and no new workspace appears.

Large semantic models are the wrinkle: they are backed by Azure Premium Files, which pins the
workspace to its region. Fab Shuffle converts each large model to the small storage format,
performs the reassignment, then switches them back. If a model can't be converted, or the
target region [doesn't support large models](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-large-models#region-availability),
the move is refused up front. If conversion or assignment fails, restoration is attempted
for models already converted; any restoration failure is reported for manual recovery.

**Rebuild** — as soon as a single Fabric item is present, the workspace cannot be reassigned
across regions. Fab Shuffle creates a new workspace in the target region and recreates and
copies everything it supports.

### What gets migrated

The columns describe **rebuild** behavior. Paired includes two-principal runs within the same
tenant. A supported path can still be blocked by missing access, unavailable definitions or
unresolved dependencies; the review screen and per-item results report those separately.

| Item or feature | Single-principal rebuild | Paired rebuild | Limits / operator work |
| --- | --- | --- | --- |
| Lakehouse | Schema; tables via Copy Jobs; Files via AzCopy | Schema; tables and Files via OneLake streaming | Schema-enabled lakehouses supported; shortcuts excluded from table-data copy |
| Lakehouse SQL analytics endpoint | Refresh and schema transfer | Refresh and schema transfer | Views, procedures and functions; catalog readiness is separate from byte-copy success |
| Warehouse | T-SQL schema and Copy Job data | T-SQL schema and streamed rows | Collation preserved; paired transfer excludes source security assignments |
| Fabric SQL database | Definition schema and bcp rows | T-SQL schema and streamed rows | Native bcp stages one table export at a time; paired schema excludes source security assignments |
| Cosmos DB database | Container definitions and SDK document copy | Container definitions and independently authenticated document copy | Destination connectivity and data access required |
| Eventhouse | Recreated | Recreated | Created before its KQL databases |
| KQL database (`ReadWrite`) | Definition and cross-cluster data copy | Definition and streamed data | Paired target update policies remain disabled; table shortcuts reconciled separately |
| KQL follower database | Recreated where leader identity is resolvable | Recreated with validated destination references | [Follower constraints](docs/migration-details.md#kql-follower-databases) apply |
| Mirrored database | Definition; optional start | Definition; optional start | Created stopped; start does not prove replication catch-up |
| Eventstream | Definition and rebinding | Manual | Paired inactive creation is not established for every node |
| KQL queryset / dashboard | Definition and rebinding | Definition and rebinding | Points at migrated data stores |
| Semantic model | Definition and rebinding | Definition and rebinding, without role memberships | Includes former default models; cached data is not copied |
| Report | Definition and model rebinding | Definition and model rebinding | Verify destination model and report access |
| Notebook | Definition and rebinding | Definition and rebinding | Default lakehouse and environment attachment remapped |
| Environment | Libraries and Spark settings | Libraries and Spark settings | Publish before running dependent jobs |
| Dataflow Gen2 (CI/CD) | Definition and rebinding | Definition and rebinding | Gen1 and classic Gen2 are not supported |
| Data pipeline / Copy Job | Definition and rebinding | Definition and rebinding | Referenced source-bound connections need explicit replacement mappings |
| Apache Airflow job | Configuration and DAG files | Configuration and DAG files with reference checks | Inspect scheduler pause settings and runtime dependencies before enabling |
| OneLake / KQL table shortcuts | Create missing; reuse identical | Create missing; reuse identical after destination checks | No overwrite of conflicting targets |
| Workspace folders | Recreated hierarchy and placement | Recreated hierarchy and placement | Item identities are remapped |
| Custom Spark pools / workspace Spark settings | Recreated or updated | Recreated or updated | Capacity-level pools and capacity limits need operator review |
| Spark job definition | V2 definition and supported files | V2 definition and supported files | JVM/JAR payloads require manual handling |
| API for GraphQL | Definition and source rebinding | Definition and validated source rebinding | External dependencies need mode-appropriate review/mappings |
| Variable library | Definition and item rebinding | Definition and item rebinding | Known references must resolve |
| Mounted data factory | Definition | Definition | Still refers to the Azure Data Factory; it is not copied |
| Graph model / query set | Definition and rebinding | Definition and rebinding | Rebuild/refresh the graph index |
| Map | Definition and rebinding | Definition and rebinding | Lakehouse and KQL sources remapped |
| Activator (Reflex) | Definition with rules disabled | Definition with rules disabled | Enable rules manually |
| Mirrored Azure Databricks catalog | Definition with auto-sync disabled | Definition with auto-sync disabled | Enable sync manually |
| Snowflake database | Recreated reference | Recreated reference with destination connection checks | Snowflake data stays in Snowflake |
| Workspace permissions | Source admins preserved; other roles optional | Not copied | Destination administrators arrange paired-run access |
| Dashboard / paginated report | Not rebuilt | Not rebuilt | Retained only when the existing Power BI-only workspace is reassigned |

Unsupported user items are reported on the review screen and in run warnings. Derived and
system/monitoring items have separate handling; use the actual assessment rather than
assuming that every entry in the raw Fabric inventory is independently migrated.

In single-principal rebuilds, source **Admin** assignments are attempted during workspace
creation even if **Copy workspace permissions** is off; that option controls the remaining
roles. Paired runs copy neither, including when both principals are in the same tenant.

**Former default semantic models are included**, not skipped because of their names.
See [semantic-model history](docs/migration-details.md#semantic-models) for why they are
independent of the destination lakehouse or warehouse.

On a rebuild, models whose source uses large (`PremiumFiles`) storage have that setting
confirmed or restored in the destination. A destination already configured as large is left
alone. Missing source format metadata and failures are reported explicitly, with storage
evidence in the cutover report; importing a definition does not prove data/query readiness.

**Some items need manual preparation or activation.** Upgrade unsupported dataflows to
Gen2 (CI/CD), publish environments, and rebuild graph indexes. Database mirrors are created
stopped, Databricks catalog auto-sync is disabled, and Activator rules arrive disabled.
Do not enable a second copy until its dependencies and intended cutover behavior are ready.

**Start destination database mirrors** is an off-by-default option on the review screen,
including when retrying a saved migration. Selecting it authorizes a separate start action
after each `MirroredDatabase` is created, before shortcuts are reconciled. This can run a
second replica against the same upstream database, adding load and cost. The original mirror
is never stopped or changed. Activator rules and Databricks catalog sync are not enabled.
Confirm this option again on each retry; a previously recorded choice does not silently
authorize another attempt.

The start uses destination credentials and waits for Fabric to report **Running**. Already
running mirrors are not started twice. An accepted start is not proof of replicated data:
check initial synchronization and table availability before cutover. A shortcut into a newly
started mirror can still need retrying while its table arrives. Cancellation stops the
migration worker, **not replication already started**. With the option off, mirroring remains
operator-controlled, and retry reads the destination state rather than claiming a manually
started mirror is still stopped. See
[Start Mirroring](https://learn.microsoft.com/en-us/rest/api/fabric/mirroreddatabase/mirroring/start-mirroring)
and [Get Mirroring Status](https://learn.microsoft.com/en-us/rest/api/fabric/mirroreddatabase/mirroring/get-mirroring-status).

**Review destination capacity limits.** A smaller capacity may not fit the source's Spark
pools or semantic models. Fab Shuffle warns when SKUs differ; that warning is not proof
that the destination workload will run successfully.

### Cutover readiness (advisory)

The progress/results screen separates **migration completion** from **cutover readiness**.
A successful run can still leave data uncopied, dependencies unresolved, or activation work
for the operator. Readiness never changes the run's success, cancellation or failure status.
The report and its export controls appear only after the migration finishes, fails, is cancelled,
or is interrupted. They remain hidden while a run is pending or running.

| State | Meaning |
| --- | --- |
| Ready | Every required migration step has recorded success for this target incarnation. |
| Needs attention | Recorded failures, skipped work, unresolved dependencies or manual activation remain. |
| Unknown | Evidence is missing, stale or insufficient, including legacy journals. |

Filter or search the per-item report to see source/target identities, created/adopted/refreshed
dispositions, step evidence and operator actions. Data and file options skipped at admission
remain visible. An explicitly empty table/file inventory is different from an unmeasured copy.
Environment publishing, Activator rule enablement, catalog auto-sync and graph refresh remain
operator tasks. Mirroring startup requires the explicit review-screen opt-in or a manual
start; generating a report never starts it. Running-state evidence does not prove replication
has caught up.

**Ready is not permission to delete the source.** This report does not perform row-count/hash
reconciliation, live execution or activation checks, permission parity, external dependency
availability checks, or source-change consistency checks. Semantic model data/refresh readiness
is unknown until checked outside this tool. Evidence describes migration-time observations,
not the target's current live state.

**Export JSON** downloads the full report, independently of visible filters, without external
services. The same authenticated snapshot is available at
`GET /api/runs/{run_id}/readiness`; add `?download=true` for an attachment and use the existing
`X-Fab-Shuffle-Session` header. Saved journals support this endpoint after a process restart.
No definition payloads or credentials are included. Retention follows the run journals.

The journal retains target-bound evidence and attempt identities. Recovery invalidates
readiness when a target or dependency changes, preserving older evidence as history instead
of promoting it into the recreated item. Recovery checkpoints still decide what is safe to
repeat; they are not a substitute for missing lifecycle evidence.

### Dependency order

Stores are created before their consumers, environments before notebooks, and semantic
models before reports. Shortcuts are reconciled after their targets exist. The
[implementation reference](docs/migration-details.md#dependency-order) documents the full
phase sequence and the single-principal versus paired differences.

### Connections

Fab Shuffle never creates, adopts by name, or deletes a connection on the operator's behalf,
in any mode. A connection id is tenant scoped: it can be reused within that tenant when its
target and access are still appropriate, but it does not resolve in another tenant.
Existing connection credentials are not exported. Automatic connection recreation was
deliberately removed; see the [reference-safety rationale](docs/migration-details.md#reference-and-connection-safety).

Supply replacement connection IDs through `connection_mappings`, keyed by source connection
ID. They are read back with destination credentials and validated once the relevant stores
exist. Changed or removed mappings invalidate the old binding instead of silently preserving
it. Without supplied mappings there is no connection-replacement phase.

Migration dependency checks apply only to connections that a *migrated* item or shortcut
actually references: a data pipeline, Copy Job, eventstream, mirrored database or Activator whose exported
definition names one, or a lakehouse/KQL database shortcut whose target does (a shortcut's
connection lives on the shortcut target, not in the item's own definition). Separately,
cutover advisories report visible connections pointing into the source workspace, including
ones no migrated item uses. That read-only inventory does not recreate connections or block
the creation of unrelated items.

A dependent item whose referenced connection still points into the source workspace, with no
supplied `connection_mappings` entry for it, is refused rather than created against a
connection that stops resolving once the source workspace is gone. The warning names the
connection and the item and asks for a destination replacement to be created and mapped by
hand in `connection_mappings`; it does not create one and does not promise that doing so will
fix the underlying data source.

In single-principal mode, referenced connections that target something outside the moving
workspace can be reused without recreation. Paired mode still needs explicit destination
connection mappings. Connection checks report:

- connections the service principal **cannot see**, requiring an access check before their consumers run;
- **personal cloud** connections, which cannot be shared;
- connections routed through a **gateway**. A virtual network gateway in particular stays in
  its original region, so it may no longer be the right path to the data.

#### Connection cutover advisories

After a rebuild, **Connections pointing at the source workspace** appears in the cutover
report. It lists connection names where returned, IDs, redacted paths, matching source item
IDs and the destination workspace. **SQL matches require both server and database** from
source data-store metadata. A matching database name or GUID on a different server is not a
source connection. Hostname casing, `tcp:` and the default port are normalized; database-name
case is preserved, except for GUID casing. Missing or unrecognised SQL path formats are
unverified, not matches. SQL destination suggestions require exact mappings for both
coordinates; changing only the catalog while retaining the old server is never suggested.
Other path suggestions must remove every recognised source reference.

Review current consumers before deciding whether to change anything. **PersonalCloud does
not prove default-semantic-model ownership**, and an **Automatic** connection is an implicit/
SSO binding, not a shared connection that necessarily needs recreation. Check the consuming
model's data-source and Gateway and cloud connections settings. The report does not infer
ownership or usage from a connection's name, SQL target or lack of a display name.
A source connection stays listed even if the migration used an
explicit replacement: consumers outside this migration may still use the original.

This is a point-in-time, read-only inventory, not live cutover validation. The scan matches
known literal identifiers and endpoints in metadata returned to the source principal. It
cannot discover connections that principal cannot list, dynamic references or all external
consumers. Service errors remain visible; missing, incomplete and older-attempt scans are
not clean results. A run stopped before the scan keeps unknown or stale evidence rather
than making additional calls after cancellation. The snapshot survives restart and is
included as `connectionAdvisories` in the downloadable readiness JSON. Reassignment does
not need this scan because it retains the original workspace identity.
Snapshots from the retired identifier-only matcher are marked **Stale** and their matches
and path suggestions withheld, including in JSON downloads; their journals remain unchanged.
Use the user lookup script for manual inspection without rerunning the migration. A future
migration attempt records a new versioned scan.

Under **Look up connection names**, choose **View script**, then **Copy** or **Download
.ps1**. Run it yourself in **PowerShell 7+ with the Az.Accounts module**, signing into the
source tenant with your **user account**, not the migration service principal. It reads
recorded connection IDs; `-ConnectionId` can add IDs. With no recorded or supplied IDs, it
lists all connections your account can see, including additional pages. Only IDs, names
and connection types are printed, along with service errors for failed lookups. No access
is granted, no connection is changed, and names the API does not return remain unavailable.
[List Connections](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connections)
and [Get Connection](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/get-connection)
require connection access and, for user authentication, a delegated `Connection.Read.All`
or `Connection.ReadWrite.All` scope. A name lookup does not fix missing access.

### Airflow files

Airflow configuration and supported UTF-8 text files are checked before creating the target
job. Known literal workspace/item GUIDs and endpoint/path references are rebound; known
unmapped dependencies refuse creation and name what must migrate first. Text uses the same
extension allowlist as item definitions, including Python, JSON, YAML, SQL and plain text.
Binary and other opaque files are preserved byte-for-byte. Unreadable or oversized files
must be fixed before retrying.

Literal references to a fresh job's own source ID are refused too: its destination ID is
not available before creation. Remove those hardcoded self references, retry, then configure
the destination job ID before running it. An adopted job with a known destination ID can
have its literal self references rebound.

This is literal-reference checking, not arbitrary Python analysis. Review references built
dynamically from environment variables, imports, string fragments or opaque supporting
files before running the new job.

### Dependency checking

The tool reads [upstream relations](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-upstream-relations(beta))
and inspects known references in definitions and shortcuts. It reports dependencies outside
the migration and dependencies on unsupported items. Paired moves require explicit
destination references; single-principal external dependencies still need operator review.

The relations API is beta. If unavailable, the report says the graph could not be checked;
that is not evidence that dependencies are safe. Definition/reference checks still apply.

## Usage

### Picking up a migration that stopped

Rebuilding a large workspace takes hours, so a run keeps a journal of what it has done: the
plan it was given, every item it created, and every table and file set it moved. If the
container is restarted, or a step fails, the unfinished run is offered back on the capacity
screen the next time you sign in. Picking it up reuses the workspace it was building and
carries on, rather than starting again.

A resumed run redoes as little as possible. Items that are still in the new workspace are
adopted; data that had finished moving is left alone. Anything the journal claims but that is
no longer there is built again, because the journal records what an attempt *did*, not what is
there now.

Shortcuts are reconciled against destination inventory before creating anything. An exact
parent path and name plus the remapped target must match (including connection IDs; KQL also
compares query acceleration). A matching shortcut is reused without another create request;
the same name in another folder is a different shortcut. A mismatched shortcut, table or
folder is not overwritten or deleted. A 409 is only treated as recovered when a follow-up
read proves the existing shortcut is identical; otherwise the service error remains visible.

Completion checkpoints belong to the target item that received the data. If that item was
deleted, the replacement receives a fresh copy; existing consumers are rebound to its new
identifiers before recovery can succeed. Checkpoints and diagnostics carry across every
attempt, including a second or later resume.

Only the latest inactive attempt is offered for recovery. Concurrent resumes and cleanup of
a related active migration are refused. Unfinished Copy Jobs are retained and polled on
resume rather than started twice; if a submission has no confirmed job or instance ID, stop
and reconcile it in Fabric before resolving its journal record. Do not delete its scratch
workspace while completion is unknown.

The same machinery retries a run that *did* finish but left items behind — a connection that
was not shared yet, say, or a workspace that could not be read. Fix the cause, press **Retry
what did not migrate** on the progress screen to review and resume the saved attempt. Missing
work is retried; retained targets and bindings are checked rather than blindly skipped. The
scratch workspace, when needed for single-principal Copy Jobs, is recreated if it was removed.
Completed runs with recorded failed steps also appear under **Saved migrations needing
attention** after a container restart, so a runtime handoff does not lose the retry controls.
Resume and Retry open a review screen for that attempt's mirror-start choice. Existing
capacity, copied-data options and target workspaces are retained. The API option is
`start_database_mirrors: true` on `POST /api/runs` or `POST /api/runs/{id}/resume`;
omitting it on resume leaves automatic starts off even if a prior attempt opted in.

Each saved migration offers two confirmed actions in addition to resuming:

- **Ignore** hides that migration from the list permanently, including after a container
  restart. It preserves its journal and leaves all workspaces, data and remote jobs alone.
- **Full restart** deletes the recorded destination workspace and its contents, along with
  the run's recorded scratch workspace if it still exists, then returns to destination
  capacity selection. The source is retained. The next migration is a fresh run: no old item
  mappings, completed-copy checkpoints, destination capacity or write-freeze confirmation
  are reused.

Neither action is available while a related migration is running. Full restart is refused
for reassignment runs (their workspace is the source), mismatched destination confirmations,
or unresolved Copy Jobs. Reconcile those jobs first; Ignore does not stop them.
If deletion fails midway, the entry remains available to **finish the full restart**, but
cannot resume the old copy into a partly deleted destination. Only confirmed workspace
deletions are skipped on the next restart attempt. Service errors are retained in the message.

Two things follow from this:

- **Resuming after a process restart needs a new sign-in.** Session credentials are not
  persisted for automatic recovery.
- **Journals live on the volume**, under `local/journal` for single-principal runs or
  `local/journal-paired-v1` for paired runs. They hold workspace and item ids and
  names — the same things the screen shows — and no credentials. The hundred most recent are
  kept, along with the latest recoverable attempts and unresolved Copy Jobs.

Without a mounted volume the journal goes when the container does, and there is nothing to
pick up. That is the main reason the `docker run` line above mounts one.

### Building the image yourself

Use the [container validation workflow](#development) before publishing a custom build.

```bash
docker build --platform linux/amd64 --target production -t fab-shuffle .
docker run --rm --platform linux/amd64 -p 8080:8080 -v fab-shuffle-scratch:/app/local fab-shuffle
```

The image supports `linux/amd64` and `linux/arm64`. Its Python base is pinned by patch
version and multi-architecture digest; `uv.lock`, the hashed `requirements/*.txt` exports,
and `tools.lock.json` record the dependency and external-tool versions.
Every image build checks installed versions, imports the application, and starts it on
loopback to check HTTP health and packaged UI assets without tenant credentials.

**ARM64 builds and smoke checks are not a claim of vendor-supported SQL schema transfer.**
[SqlPackage's Linux support list](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-download#supported-operating-systems)
names x64 only. Use `--platform linux/amd64` for vendor-supported SqlPackage use. Neither
architecture's version/import/health smoke checks exercise a live database or Fabric tenant.

For exact .NET, ODBC, SQL-tool and AzCopy versions, use [tools.lock.json](tools.lock.json)
rather than a second version list in this README.

Two build args exist for networks that block the public package feeds:

| Build arg | Default | Purpose |
| --- | --- | --- |
| `NUGET_SOURCE` | `https://api.nuget.org/v3/index.json` | Feed for `sqlpackage` and `unpackdacpac` |
| `PIP_INDEX_URL` | `https://pypi.org/simple/` | Index for the Python dependencies |

```bash
docker build \
  --build-arg NUGET_SOURCE=https://your-proxy.example/nuget/v3/index.json \
  --build-arg PIP_INDEX_URL=https://your-proxy.example/pypi/simple/ \
  -t fab-shuffle .
```

If `sqlpackage`, `unpackdacpac`, or `azcopy` are missing at runtime, the affected item is
reported as a warning and the rest of the migration continues.

Python mirrors must serve the locked wheels unchanged: installs enforce hashes and refuse
source distributions rather than fetching unpinned build dependencies. `PIP_INDEX_URL`
still selects the download index; the exported requirements deliberately contain no index
or artifact URLs. NuGet mirrors must supply the exact pinned package archives: their bytes
are hash checked after installation as well. Missing versions and checksum mismatches fail
the build; there is no fallback to latest.

### Configuration

Every setting has a sensible default; override with environment variables when needed.

| Variable | Default | Purpose |
| --- | --- | --- |
| `FAB_SHUFFLE_PORT` | `8080` | Web UI port |
| `FAB_SHUFFLE_HOST` | `0.0.0.0` | Bind address |
| `FAB_SHUFFLE_SCRATCH` | `/app/local` | Local staging directory |
| `FAB_SHUFFLE_MAX_MEMORY_BYTES` | `1073741824` (1 GiB) | Per-operation in-memory processing budget; not a container RAM cap |
| `FAB_SHUFFLE_MAX_DISK_STAGING_BYTES` | `10737418240` (10 GiB) | Per-operation bounded schema/checkpoint disk budget; not a share quota or legacy AzCopy/bcp cap |
| `FAB_SHUFFLE_FILE_CONCURRENCY` | `2` | Concurrent file transfers; include their combined staging in share sizing |
| `FAB_SHUFFLE_SCHEMA_CONCURRENCY` | `2` | Concurrent schema transfers |
| `FAB_SHUFFLE_MAX_RETRIES` | `6` | Retries for throttled/transient Fabric calls |
| `FAB_SHUFFLE_COPY_JOB_TIMEOUT_SECONDS` | `43200` | Copy Job budget |
| `FAB_SHUFFLE_LRO_TIMEOUT_SECONDS` | `3600` | Long-running-operation budget |
| `FAB_SHUFFLE_SQL_ENDPOINT_TIMEOUT_SECONDS` | `1800` | Wait for SQL endpoint provisioning |

`FAB_SHUFFLE_MAX_STAGING_BYTES` is retained as a compatibility fallback: when set, it
supplies both budgets unless the corresponding new setting explicitly overrides it.
Use the new variables for independent control. Values must be positive byte counts;
`0` does not mean unlimited.

## How it works

See the [migration implementation reference](docs/migration-details.md) for mode-specific
data movement, dependency ordering, reference safety, staging, item-specific history and
API integration. The [support matrix](fabshuffle/fabric/support.py) and
[orchestrator](fabshuffle/orchestrator.py) are the implementation authorities.

## Development

**Use Docker for application execution, tests and lint, on every host OS.** The validation
target extends the same pinned Debian/Python runtime as the production image, including
AzCopy, SqlPackage, UnpackDacPac, bcp and ODBC. It adds hash-locked development dependencies
and digest-pinned Node and PowerShell for the JavaScript UI and generated-script tests.

```bash
docker build --platform linux/amd64 --target test -t fab-shuffle:test .
docker run --rm --platform linux/amd64 --network none fab-shuffle:test
```

Use Docker's **Linux containers** mode on Windows. No host Python, PowerShell, Node,
virtualenv, nested Docker daemon or daemon-socket mount is needed. The container entrypoint
checks required tools, installed dependencies, lock exports, runtime/import/HTTP smoke,
Ruff, then pytest; missing tools fail instead of silently skipping coverage.
Tests run with no external network, credentials, published ports or persistent recovery
volume. PowerShell authentication/API calls are mocked, not live tenant operations.

To select related tests, append their paths or pytest selectors to the same command:

```bash
docker run --rm --platform linux/amd64 --network none fab-shuffle:test tests/test_shortcut_retry.py tests/test_mirror_activation.py
```

The image contains a snapshot of the checkout: rebuild the `test` target after edits.
`.github/workflows/tests.yml` uses exactly this build/run path on an Ubuntu Docker host,
not a native Ubuntu/Windows Python matrix. Its successful result gates the production
image build/publication; shared BuildKit caches avoid rebuilding unchanged runtime layers.
The final/default Dockerfile target is `production`, so published images never contain
the test dependencies or validation entrypoint. Release builds retain AMD64/ARM64 smoke
coverage; the full suite targets Linux AMD64, the vendor-supported SQL tooling platform.

Use the checked-in resolution, not `pip install -e ".[dev]"`, which would resolve the
compatibility ranges afresh. `uv.lock` remains universal and retains platform markers;
that is dependency metadata, not a promise of native Windows application support.

### Deliberate release upgrades

See [Development and release maintenance](docs/development.md) for dependency refreshes,
artifact verification, version alignment, publication and reproducibility boundaries.
Scheduled rebuilds do not update dependency pins, and a merge alone does not update the
stable `latest` tag.

## Planned features

- Coordinated multi-workspace migrations, including dependencies between workspaces.
- Incremental/low-downtime copying and cutover.
- Broader live cross-tenant qualification and additional currently unsupported item scenarios.

Single-workspace paired migration, configurable transfer concurrency, saved-run controls,
connection advisories and Azure Files-backed staging are already available. Published
support is subject to the limits in [What gets migrated](#what-gets-migrated), not a promise
that every configuration has been qualified live.
