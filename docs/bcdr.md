# Standby and recovery operations

BCDR is a separate, same-tenant workflow in the existing Fab Shuffle wizard. It prepares
inactive recovery workspaces and records metadata in one central **Warehouse** in the
designated recovery workspace. It is not an automatic Microsoft platform failover,
a continuously running data replica, or a guarantee that a restored application is ready.
The ordinary migration and cross-tenant migration workflows remain separate.

There is no Git connection, repository, branch, PAT or Git synchronization configuration
in this workflow. The metadata store does not require a metadata lakehouse or Spark session.
Recovered Spark workloads can still require Spark to execute.

## Prepare the controller

Run the Linux container outside the source failure domain. Arrange the existing Fabric
service-principal permissions, destination capacity access, Warehouse SQL access, and Azure
ARM permission to resume and suspend each explicitly designated recovery capacity. Azure
management authorization and Fabric workspace/capacity authorization are different checks.

Use dedicated recovery capacities: pausing a capacity affects every workspace assigned to it.
Record the **ARM resource ID**, not just the Fabric capacity GUID. The controller must not
guess an Azure resource from a capacity name or pause capacity that is serving production.
Choose explicit source-capacity scope and designated owner principals before setup.

Deploy exactly one authorized controller for a recovery set. Keep its bootstrap descriptor
and access to its credentials available after source-region loss. A browser session is not
an external scheduler and does not survive a controller restart.

### Bootstrap is not a second metadata archive

The web application reads `FAB_SHUFFLE_BCDR_BOOTSTRAP`, defaulting to
`/app/local/bcdr/bootstrap.json` in the standard image. The browser cannot submit an
arbitrary server filesystem path. Mount durable controller storage at `/app/local`, or
set the bootstrap path to another protected persistent deployment mount.

The bootstrap contains only the non-secret identities and coordinates needed to resume
and locate the control Warehouse: tenant/application identity, explicit recovery capacity
ARM resources, control workspace/Warehouse identity, provisioning ownership/operation
records and SQL endpoint/database coordinates when known. It is not a copy of definitions,
ACL inventories or recovered business data. Do not put passwords, tokens, SAS URLs or
connection strings with credentials in it.

The Warehouse is authoritative for generations, definitions, mappings, desired ACLs and
operation history. The controller resumes the catalog capacity before attempting SQL reads;
it must not need a paused SQL endpoint to discover the capacity to resume.

When capacity is paused, new Warehouse queries fail, in-flight queries are canceled and open
transactions roll back. Resume starts with cold caches and can take time before queries
succeed. **Storage remains billable while compute is paused.** See Microsoft's
[Warehouse pause/resume documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/pause-resume).
The central catalog region must remain healthy and resumable; simultaneous loss of that
region is not covered by a silent local-cache fallback.

## Use the wizard

At sign-in, choose **Operate standby & recovery (same tenant)** to go directly to the
operations panel without migration's source-workspace discovery. Supply the recovery
principal credentials through the existing authenticated session. Existing signed-in
same-tenant sessions can also choose **Standby & recovery** in the header. Credentials
and tokens stay in memory; they are not put into form schemas, configuration records,
URLs or browser storage.

Operation forms use native labeled controls backed by the service's validated request
contracts. Generation and failback plan IDs are pinned from backend results rather than
typed as new IDs. Groups must be selected explicitly. Advanced evidence records are
identity-bound observations, not an instruction to trust a general "ready" checkbox.

### Set up one central control Warehouse

1. Choose whether to create a new restricted control workspace or use an existing designated
   workspace on a dedicated recovery capacity. Existing workspace access must already match
   the recovery SPN and designated-owner allowlist. Setup does not delete unrelated grants
   to force a match. Owners of a newly created control workspace currently use the Admin role.
2. If the source is healthy, optionally expand **Healthy-source preparation** and choose
   **Discover setup choices**. This explicitly reads source/recovery workspaces and capacities;
   it is never automatically invoked for outage recovery. Progress and resource counts appear
   beside the button. Empty results and partial failures include an action to take; discovery
   never saves setup or starts a capacity operation. Refreshing preserves still-valid resource
   selections and unrelated form inputs.
3. Open **Set up the control Warehouse**. Enter a new control workspace name or choose the existing
   control workspace by name from the recovery principal's inventory, then enter the new Warehouse
   name. Select the source capacities that define the recovery scope.
   Workspace and source-capacity selection has no manual-ID fallback. Resolve ambiguous names
   or missing access before selecting; a disappeared selection is not silently replaced.
4. Add each dedicated recovery capacity with its Fabric GUID and explicit Azure ARM resource ID.
   Confirm dedicated use and suspend authorization for each, and specify which ARM capacity
   hosts the control Warehouse. Fabric GUIDs and ARM resource IDs are not interchangeable.
5. Review the authenticated recovery principal's tenant and **object ID**, derived from the
   session rather than its application ID. Add the designated owners' object IDs, kinds and
   workspace roles. Only these restricted workspace grants are allowed before enablement.
6. Confirm **Set up the control Warehouse**. The backend creates the new Warehouse and persists
   its bootstrap using the deployment-configured path. Keep that storage durable and restricted.
   Then configure source-to-recovery capacity routes and exact workspace selection for
   **Sync standby**. Read the captured inventory, preview dependencies and explicitly approve
   any required additions before attempting their dependent groups.

Do not repeatedly submit setup after an ambiguous create. Read the service error and reconcile
the recorded provisioning operation; the backend must not adopt an unrelated Warehouse by name.

### Setup troubleshooting

Discovery logs include a correlation ID, resource names and exact workspace/capacity IDs.
Confirmed setup logs record the selected workspace, source capacities and recovery
Fabric/ARM identifiers. Use restricted server logs for identity troubleshooting, not
screenshots of resource pickers. Discovery failures preserve successful independent results;
selections from a failed refresh cannot substitute for a successful read of that inventory.

ARM monitoring queries are service-issued opaque context: the controller uses the returned
query unchanged after validating the host, subscription, Fabric operation path and operation
identity. The initiating request's API version and a documentation example's query keys
are not an allow-list for the monitoring URL.

If an ARM polling header is rejected after `202 Accepted`, Azure has accepted the operation;
the local error does not mean it failed. The controller retains the intent and any valid
request ID, and does not blindly replay it on the next setup request. Check the server's
`ARM polling header rejected` diagnostic for the header name, operation, region, API version
and query parameter names. Signed/opaque query values and credentials are omitted, including
from HTTP request logs. Do not delete bootstrap or change API versions to force a retry.

Recovery capacity ARM/Fabric pairing is not derived from matching display names. The current
explicit capacity fields are not an automatic identity resolver; name-based workspace
discovery does not verify that pair. A fully automatic capacity picker still requires an
authoritative cross-API identity mapping.

| Action | Meaning |
| --- | --- |
| Set up the control Warehouse | Create or choose the restricted control workspace, create its single new metadata Warehouse, and persist the ownership/bootstrap descriptor. |
| Read recovery status | Read the recorded mode, generations, groups, writer state and pending operations without source metadata reads. It can resume the control Warehouse capacity; opening the panel itself does not. |
| Reconcile interrupted operation | Fence the previous controller and reconcile an exact recorded service receipt or saved completed result; never repeat an uncertain create or adopt by name. |
| Preview standby selection | Preview exact include/exclude workspace IDs, positive name keywords, capacity routes and required dependency additions against captured metadata. |
| Sync standby | Resume dedicated recovery capacity, optionally capture healthy-source metadata, update actual inactive standby items, commit results and pause only when the backend determines it is safe and parking was requested. |
| Configure optional data protection | Register a typed, approved SQL/Cosmos export, KQL standby or independent Lakehouse snapshot for the next capture, not a historical generation or automatic business-data copy. |
| Configure qualified temporary attachments | Record exact retained-source OneLake paths and independently verified incident/access evidence. Enable recovery performs the actual attachments; configuration alone does not make data ready. |
| Enable recovery | Explicitly leave standby mode, stop scheduled synchronization/automatic pause, reconcile selected groups and apply only explicitly approved, eligible deferred ACLs. This is not cutover. |
| Approve cutover | Submit current readiness observations and primary-writer fencing evidence for the selected groups. The backend decides whether cutover is permitted. |
| Plan failback | Establish primary availability with evidence and create a linked return/reconciliation plan, not a reversed migration retry. |
| Reconcile failback | Fence recovery writers and reconcile the approved return targets, including data-change evidence required by each workload. |
| Approve cutback | Validate return targets and writer exclusivity before approving consumer return. |
| Rearm standby | Explicitly restore source authority and restricted standby access after recovery no longer serves production; park only when safe. |

**Sync standby is not Enable recovery.** Their confirmation dialogs name different
consequences. Capacity temporarily running for synchronization does not authorize user
access, activate schedules, or fence production writers.

Empty workspace inclusion means eligible workspaces in the configured **source-capacity
scope**, not every workspace in the tenant. Exact exclusions are not silently overridden
by dependency closure. Review the named prerequisites and approve additions explicitly,
or leave their dependent groups blocked. The control workspace is excluded from business
selection and recovery access replay.

Use **Approved connection routes** when a captured connection needs a validated destination
replacement or explicitly approved external connection reuse. Supply exact connection identities
and evidence; the backend reads the destination connection and rejects retained source-compute
references. This is not a credential field or a general source-reference bypass.
For failback, use the separate **Approved return connection routes**. A forward recovery
connection is not automatically a valid primary-return connection.

### Access remains restricted until enablement

Before explicit recovery enablement, only the recovery service principal and designated
owners receive workspace-level access. Other captured workspace, item, SQL, OneLake,
semantic-model and connection ACLs are deferred. The control workspace stays restricted
even after consumer access is enabled elsewhere.

Review exact deferred ACL IDs before approving replay. Workspace access does not establish
SQL/model/OneLake/connection security equivalence. Supply the relevant runtime identity
and effective-access evidence, and keep secrets in the approved external credential
provider rather than recovery metadata.

### Read partial results honestly

Capture generation and applied standby generation measure different things. A successful
capture does not mean every destination definition was applied; applied metadata does not
mean data, bindings or effective access are ready. Review each dependency group's separate
metadata, data, access, readiness and active states, plus its blockers and warnings.
Capacity state is shown from actual ARM observations with timestamps and exact resource IDs.
`standby` mode alone does not mean capacity is paused; reading catalog status can resume its capacity.

`restored_stopped`, `unverified`, `partial`, and `ready_for_cutover` are not interchangeable.
Creation of a report, model or store does not justify an "all recovered" claim. Independent
groups may recover while a group needing an unavailable prerequisite remains blocked.

Closing a browser view or disconnecting does not cancel an operation. The worker finishes
settling the synchronous service call and closes its clients. Do not terminate the controller
or deploy a new revision during an operation. After a crash or ambiguous service failure,
read status and reconcile recorded operations before retrying; never adopt resources by name.

### Reconcile an interrupted operation

Read recovery status first. **Reconcile interrupted operation** lists recorded pending operations
and pins the observed controller ID and **controller epoch**, which is separate from the writer
epoch used for cutover. Choose the exact operation. If a completed service result exists but its
catalog mapping was interrupted, the advanced exact-ID field accepts that existing operation ID.
It is not a field for inventing an operation or entering an item name.

Stop and fence the previous controller, then provide independently established controller-fencing
and destination-quiescence evidence. The explicit confirmation authorizes the backend to take
over that recorded epoch and finish only work justified by the exact owned receipt. It does not
authorize a second create, enable recovery, admit a business group, or approve cutover.
Read status again after each reconciliation. Stale epochs, running previous controllers and
missing/ambiguous receipts remain actionable blockers requiring inspection; this is not a
general reset or a way to clear errors without evidence.

## Data protection and qualification

All current supported migration types remain in the recovery assessment. This does not
promise automatic data recovery or a qualified inactive-create contract for every type.
Missing protection and unsupported restoration details remain visible with the item name,
operator action and dependent impact; they must not be counted as recovered.

Optional SQL/Cosmos logical exports and KQL export/independent-standby protection belong
outside the source failure region. Artifacts are separate from the metadata Warehouse;
the catalog records references and integrity/recovery-point evidence, not bulk databases.
Provider-specific configuration and verification must be completed before relying on those
inputs during an outage.

Use **Configure optional data protection** for typed descriptors and storage/target approval
references. The protected data root is configured on the server, never accepted as an arbitrary
browser request path. SQL/Cosmos portable artifacts require matching off-region storage
approvals; a prepared KQL descriptor does not use a portable-file storage configuration.
Configuration applies to the **next capture** and cannot patch a historical generation in place.

### Independent Lakehouse materialization

Choose the `lakehouse` provider in **Configure optional data protection**, then the typed
`LakehouseProtection` descriptor. Supply the source identity; the independently qualified
off-region snapshot/access references; capture/completion timestamps and consistency window;
and exact `Tables`/`Files` directory and file pins (length, hash and ETag). Confirm that captured
paths were independently verified as local source data. The form does not manufacture this
qualification or a file inventory. Use the provider's healthy-source capture output.
The provider's `capture_lakehouse` helper can produce the exact pins during an approved
healthy-source preparation step. It is not an automatically scheduled data-capture command:
the product's metadata synchronization does not invoke it or export business data implicitly.
An external preparation workflow must capture and retain the approved snapshot and descriptor
before supplying that descriptor through the configuration form or CLI.

This descriptor uses no portable-file `storage` record and no KQL materialized inputs.
The real provider streams the approved pinned global-OneLake bytes into a fresh owned
destination Lakehouse and records byte-copy results. Copied bytes are **not** proof of Delta
consistency, endpoint availability, consumer bindings or readiness. Metadata and fresh target
data/engine/access evidence are still required before cutover. It is distinct from a retained-source
shortcut: consumers must ultimately use the owned materialized destination, not the original.

**Fabric SQL native backups restore within the same workspace only, and have no
geo-replicated backup copy.** They are not cross-region recovery inputs. See the
[SQL database backup limitations](https://learn.microsoft.com/en-us/fabric/database/sql/backup#limitations).
A portable logical export is not a transaction-log backup, arbitrary point-in-time restore,
or proof of consistency without the required preparation. Cosmos recovery must account for
deletes and TTL expiration; KQL needs schema/policy and ingestion-offset/duplicate handling.

A continuously updated data standby needs available compute. It cannot keep ingesting
on a paused recovery capacity. A periodic export instead has the recovery point of that
export, not the time the standby definition was last synchronized.

Temporary no-copy continuity is conditional on exact typed data bindings and qualification.
Retain the original source identities and backing data for as long as those bindings exist.
**Do not retire or delete the source while using temporary continuity.** A shortcut that
works against a healthy primary is not evidence of native replica routing during a real
regional outage. Independent writable recovery must use validated destination-owned data
or an approved independent standby and self-contained compute bindings.

### Qualified temporary Lakehouse attachments

**Configure qualified temporary attachments** requires a pinned captured generation, exact
source Lakehouse and paths, existing target consumer identity, destination shortcut path/name,
and an independent incident qualification with a validity window. Each attachment also needs
the exact binding hash and independently verified caller access evidence. The enforcement
reference must agree with the binding evidence; neither a checked attestation nor an
automatically computed hash proves read-only access.

The backend supports the qualified caller access mode here, not an inferred owner/delegated
SQL or Direct Lake access mode. It checks exact path coverage and known source-write access.
Loose root files and unqualified paths must use independent copying or remain missing;
do not replace them with an invented `Files/recovered` attachment.

Configure first, then explicitly **Enable recovery** for the selected groups. The backend
creates the exact qualified attachments and reports `temporary_attached`, not ready.
Capture fresh post-attachment engine, data, reference and effective-access observations.
Keep original data and identities until all consumers have independent replacements.
To renew expired evidence, supply the existing configuration hash; renewal cannot change
the approved source/target/path/principal scope. This remains incident-qualified temporary
continuity, never a general claim that native replica outage behavior has been demonstrated.

## Noninteractive operations

Use the same production service from an external scheduler, not a notebook on the capacity
it must resume. Execute only inside the Linux image:

```bash
docker build --platform linux/amd64 --target production -t fab-shuffle:bcdr .
docker run --rm --platform linux/amd64 \
  --env FAB_SHUFFLE_BCDR_TENANT_ID \
  --env FAB_SHUFFLE_BCDR_CLIENT_ID \
  --env FAB_SHUFFLE_BCDR_CLIENT_SECRET \
  --mount type=volume,src=fab-shuffle-controller,dst=/controller \
  fab-shuffle:bcdr python -m fabshuffle.bcdr status \
  --bootstrap /controller/bootstrap.json
```

Have the deployment inject the three environment variables from its secret manager.
`--env NAME` forwards an existing value; do not put literal credentials in shell history.
Alternatively inject `FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE` pointing to an external mounted
secret file instead of `FAB_SHUFFLE_BCDR_CLIENT_SECRET`. Supplying both is rejected.
There are no secret command-line options and no stored password configuration.

In Azure Container Apps, the scheduled job instead sets
`FAB_SHUFFLE_BCDR_AUTH_MODE=managed_identity` and the selected UAMI's
`FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID` / `FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID`.
Omit **all** `FAB_SHUFFLE_BCDR_TENANT_ID`, `FAB_SHUFFLE_BCDR_CLIENT_ID`,
`FAB_SHUFFLE_BCDR_CLIENT_SECRET` and `FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE` variables in
that mode. The Container Apps identity endpoint must be available; there is no developer
credential, implicit identity selection or service-principal fallback. The default
noninteractive mode remains `service_principal`. An existing bootstrap cannot be edited
to adopt a different tenant/application/recovery principal.

Commands are `setup`, `status`, `reconcile-operation`, `plan`, `synchronize`, `scheduled-sync`,
`configure-protection`, `configure-replica`, `enable-recovery`,
`cutover`, `plan-failback`, `execute-failback`, `cutback`, and `rearm`. Supply typed JSON through standard input or
`--request /controller/request.json`. Generate the actual request schema without
credentials using, for example:

```bash
docker run --rm --platform linux/amd64 fab-shuffle:bcdr \
  python -m fabshuffle.bcdr synchronize --schema
```

The noninteractive `setup` command uses the same typed setup request and
`--confirm setup`; it creates the bootstrap before opening a catalog. The wizard is the
primary intent-based setup interface, so an operator does not need to construct Python
records manually.

For an unattended schedule, use **`scheduled-sync`**, not the general operator
`synchronize` command. Persist an explicitly approved, non-secret `SyncRequest` with exact
source-to-target capacity routes, workspace selection and approved dependency additions.
`capture` and `park` must both be `true`. The scheduled command requires a request **file**
of at most **128 KiB**; unknown fields, duplicate JSON keys and nonfinite values are rejected.
It cannot be configured to enable recovery, activate items, reconcile an abandoned owner,
cut over, fail back or rearm. In addition to the shared bootstrap/storage and credentials,
configure the **same remote lease URL on every controller process and the job**:
`FAB_SHUFFLE_BCDR_LEASE_BLOB_URL`. Then run inside the container:

```bash
python -m fabshuffle.bcdr scheduled-sync --bootstrap /app/local/bcdr/bootstrap.json \
  --request /app/local/bcdr/scheduled-sync-request.json --confirm scheduled-sync
```

Every consequential command requires `--confirm` followed by that exact action name.
`--confirm scheduled-sync` cannot confirm `enable-recovery`. Persisting the request is the
operator's approval for the exact scheduled scope, not permission to perform other actions.
Use `python -m fabshuffle.bcdr scheduled-sync --schema` **inside the production image** to
obtain the current request schema without credentials.

Use `reconcile-operation --confirm reconcile-operation` with the recorded operation/controller
IDs and fencing evidence. Use `configure-replica --confirm configure-replica` for independently
qualified temporary attachments. Both accept `--request` or standard input, use the same real
service as the wizard, and do not supply a source metadata provider. Neither command starts
production or grants readiness.

Results are JSON on standard output. The service's aggregate `exit_code` is returned:
`0` for success and `2` for partial/blocked results. Validation also exits `2`; credential,
filesystem and service failures exit nonzero with an actionable error on standard error.
Recovery/status commands do not supply a source metadata provider. Synchronization supplies
source credentials only when capture is requested. **Plan failback** separately supplies
source credentials for the explicit primary-availability check after the operator confirms
that the primary is available; this is not an outage-recovery fallback read.

### Azure scheduled metadata sync job

Deploy the [web controller first](../README.md#in-azure), in an Azure region/environment
independent of the source region and of the Fabric capacity it must resume. The controller
and its durable storage/identity/lease endpoint must remain reachable during the planned
incident. The simple web template uses `Standard_LRS`; that alone does not protect the
controller from loss of its own region. A Fabric notebook scheduled on the paused/source
capacity is not the controller scheduler.

Before using the [separate job deployment](../deploy/azuredeploy-sync-job.json):

1. Attach the existing UAMI to the web controller and grant its Fabric tenant, source/control
   workspace, destination capacity, connection, SQL and OneLake access as required. Grant
   [pause/resume permissions](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume#prerequisites)
   only on the explicit dedicated recovery capacity ARM resources. Set up the single control
   Warehouse **using this UAMI**, then complete an operator-reviewed standby sync. Compare
   the web deployment's client/tenant/principal outputs with the bootstrap's recorded
   recovery identity; a different identity must not silently take over.
2. Keep the existing descriptor at `/app/local/bcdr/bootstrap.json` on the web template's
   `fab-shuffle-storage` Azure Files mount. Save the reviewed request as
   `/app/local/bcdr/scheduled-sync-request.json` on **that same share** with restricted write
   access. Do not replace the bootstrap, copy results over it or include credentials in the
   request. Stop active operations before changing a request. The template has no arbitrary
   mount, file-path or shell-command parameter and never writes either file.
3. Verify that both web and job use the **exact** `bcdrLeaseBlobUrl` from the web deployment.
   The private `fab-shuffle-locks` container holds one empty `deployment.lock` coordination
   blob, not metadata or data exports. The runtime conditionally creates it. OAuth authorizes
   lease operations; no storage key or SAS is passed to the application. The template grants
   the UAMI **Storage Blob Data Contributor at that container only**. Do not broaden this to
   subscription Contributor. An SP-mode controller needs the same container data grant
   explicitly; browser EasyAuth does not grant it.
4. Deploy the job in the web controller's **existing resource group and environment
   location**, supplying its `containerAppsEnvironmentName`, the same `managedIdentityResourceId`,
   and `bcdrLeaseBlobUrl` outputs. Use the same published production image **digest** on both.
   This is an existing environment/storage deployment: it does not create a second catalog,
   environment, file share or controller bootstrap. The Azure Files environment mount still
   uses its deployment-held account key; the runtime identity is for service access and the
   Blob lease, not an automatic conversion of SMB authentication to OAuth.
5. Choose `scheduleUtc` (five cron fields, **UTC**, not local time), allowing more than the
   worst-case sync duration between executions. The default is `0 2 * * *` (daily 02:00 UTC).
   `replicaTimeoutSeconds` defaults to four hours and is bounded by the template to one day;
   set it from measured work, not a promised duration. The schedule becomes active on
   deployment, so prepare files, grants and storage before deploying it.

The job runs the exact `scheduled-sync` argument list above without a shell, has no ingress
or EasyAuth configuration, and injects no client secret. It uses the same single controller's
ownership record and catalog, not a new independent controller.

**Overlap protection is not `parallelism=1`.**
[Container Apps jobs](https://learn.microsoft.com/en-us/azure/container-apps/jobs#advanced-job-configuration)
define parallelism and completion count **per execution**; separate cron or manually started
executions can overlap. Both are fixed to one and replica retries to zero. The shared
Azure Blob lease is acquired **before bootstrap/capacity effects**, with a finite 60-second
lease and a 20-second renewal heartbeat. Catalog controller/epoch checks are an additional
fence, not a substitute for that pre-capacity guard.
[Azure Files on Linux](https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-use-files-linux#mount-options)
does not provide the advisory-lock guarantee required between containers. Do not rely on
`flock` over SMB, add `nobrl` as a lock fix, disable the remote guard, or give the job a
different lock URL to get around a busy controller. This implementation supports public
Azure `https://<account>.blob.core.windows.net/...` endpoints only, without query strings,
SAS credentials or custom domains. Blocked lease access must be fixed, not bypassed.

**Read the result, not just the schedule.** A non-standby recovery mode, any persisted
controller owner or pending operation produces a visible blocked result with
`details.scheduled_status="skipped"` and exit code `2`. It does not capture source metadata,
resume business capacities or pause active recovery capacity. Reading authoritative mode
can require resuming the **control Warehouse** capacity; that is not source access and is
not a promise of zero capacity effects during an active incident. Eligible runs return
`scheduled_status="completed"` with the normal result/partial exit code. Invalid requests
exit `2`; authentication, bootstrap, lease and service failures exit nonzero with an
actionable error. Missing persisted configuration is not a successful no-op.

**Timeouts and lease loss require reconciliation.** A killed job or failed heartbeat can
leave a service request in flight, a retained controller owner/pending receipt, or capacity
still running and billing. Lease loss blocks new effects, but cannot undo an already sent
request. The next run must not steal the catalog lease, guess whether a timed-out operation
succeeded or automatically retry uncertain creates. Inspect Container Apps execution logs
and the Warehouse's recorded operation; have an operator reconcile the exact receipt through
the normal guarded controller before retrying. Disable the schedule while investigating.
Do not delete or replace the lock, bootstrap or catalog to make the next run appear clean.
Pausing a Fabric capacity also settles outstanding smoothed usage/overages; see
[capacity billing effects](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume).

Exercise sign-in, identity grants, cross-container lease exclusion/renewal, actual cron
execution and interrupted-run reconciliation in a disposable environment before production.
Offline template/runtime tests do not establish live Azure/Fabric or regional-outage
qualification.

## API surface

All routes require the existing `X-Fab-Shuffle-Session` header. Never send session IDs in
URLs. `GET /api/bcdr/forms` describes the typed operator forms without starting an operation.
`GET /api/bcdr/status` reads backend state. POST actions use the command names in the table
above as `/api/bcdr/<command>`, for example `/api/bcdr/enable-recovery`.
`GET /api/bcdr/discovery` is a separate explicit healthy-source preparation read, not part
of status or any recovery action.

The POST paths are `/api/bcdr/setup`, `/api/bcdr/plan`, `/api/bcdr/synchronize`,
`/api/bcdr/reconcile-operation`, `/api/bcdr/configure-protection`, `/api/bcdr/configure-replica`,
`/api/bcdr/enable-recovery`, `/api/bcdr/cutover`,
`/api/bcdr/plan-failback`, `/api/bcdr/execute-failback`, `/api/bcdr/cutback`, and
`/api/bcdr/rearm`.

Read/preview POST actions accept their typed request directly. Consequential actions use
`{"confirmation": "<exact-command>", "request": {...}}`. Unknown fields are rejected, errors
retain service code/message information, and invalid request values are not echoed. UI
confirmation supplements server-side validation; it is not a replacement for it.

Long service calls run off the FastAPI event loop. There is no fabricated success result
when a provider or catalog is unavailable and no in-memory browser-only recovery store.

## Failback and operator responsibilities

Fence every external writer and event producer on the appropriate side. A Fabric timeout,
an application lock or a checked box is not proof that a writer has stopped. Record who
confirmed fencing, the current writer epoch, the evidence and its validity interval.
Keep readiness observations distinct from operator attestations.
Readiness records bind the generation, current writer epoch, target content hash, authenticated
recovery-principal issuer and the actual intended runtime principal set. The UI pins the issuer,
generation and observed writer epoch from the backend **readiness context**; it never fills in
effective-access checks on their behalf. During failback this evidence generation can differ from
the original failover generation shown for lineage. Missing context blocks the form instead of
falling back to the original generation or guessing an epoch.
Evidence from another generation, writer epoch, issuer or stale target must be collected again.

Prefer newly created return targets and review DR-side data, schema, security, expiration
and ingestion changes. Conflicting writes on both sides require explicit reconciliation,
not an automatic bidirectional merge. Retain the recovery estate until business sign-off;
rearm is an explicit transition, not a side effect of successful cutback.

Exercise disposable non-production recovery groups and failback before relying on them.
Keep simulated source-blocked exercises separate from genuine Microsoft-managed regional
failover evidence. The product does not claim that a credential-free unit test or an
ordinary primary-region drill qualifies native replica/engine outage behavior.

For repository checks, rebuild and run the existing Linux validation target without
credentials, a Docker daemon socket or live recovery volumes:

```bash
docker build --platform linux/amd64 --target test -t fab-shuffle:product-test .
docker run --rm --platform linux/amd64 --network none fab-shuffle:product-test \
  tests/test_bcdr_product.py tests/test_bcdr_ui.py tests/test_cross_tenant_ui.py tests/test_web_api.py
```
