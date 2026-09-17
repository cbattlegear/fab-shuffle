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
   it is never automatically invoked for outage recovery. Exact-ID input remains available.
3. Open **Set up the control Warehouse**. Enter a new control workspace name or choose the existing
   control workspace, then enter the new Warehouse
   name. Select the source capacities that define the recovery scope.
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

| Action | Meaning |
| --- | --- |
| Set up the control Warehouse | Create or choose the restricted control workspace, create its single new metadata Warehouse, and persist the ownership/bootstrap descriptor. |
| Read recovery status | Read the recorded mode, generations, groups, writer state and pending operations without source metadata reads. It can resume the control Warehouse capacity; opening the panel itself does not. |
| Preview standby selection | Preview exact include/exclude workspace IDs, positive name keywords, capacity routes and required dependency additions against captured metadata. |
| Sync standby | Resume dedicated recovery capacity, optionally capture healthy-source metadata, update actual inactive standby items, commit results and pause only when the backend determines it is safe and parking was requested. |
| Configure optional data protection | Register a typed, approved SQL/Cosmos export or KQL standby descriptor for the next capture, not a historical generation or automatic business-data copy. |
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

Commands are `setup`, `status`, `plan`, `synchronize`, `configure-protection`, `enable-recovery`,
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

For a scheduled synchronization, persist only the non-secret request configuration in
`/controller/sync.json`, including explicit source-to-target capacity routes and workspace
selection. Then use the same environment/mount configuration as above with:

```bash
python -m fabshuffle.bcdr synchronize --bootstrap /controller/bootstrap.json \
  --request /controller/sync.json --confirm synchronize
```

That last command is the command **inside the container**, not a host Python invocation.
Every consequential command requires `--confirm` followed by that exact action name.
`--confirm synchronize` cannot confirm `enable-recovery`. Backend mode checks still refuse
normal synchronization during enabled recovery; a scheduler cannot override them.

Results are JSON on standard output. The service's aggregate `exit_code` is returned:
`0` for success and `2` for partial/blocked results. Validation also exits `2`; credential,
filesystem and service failures exit nonzero with an actionable error on standard error.
Recovery/status commands do not supply a source metadata provider. Synchronization supplies
source credentials only when capture is requested. **Plan failback** separately supplies
source credentials for the explicit primary-availability check after the operator confirms
that the primary is available; this is not an outage-recovery fallback read.

## API surface

All routes require the existing `X-Fab-Shuffle-Session` header. Never send session IDs in
URLs. `GET /api/bcdr/forms` describes the typed operator forms without starting an operation.
`GET /api/bcdr/status` reads backend state. POST actions use the command names in the table
above as `/api/bcdr/<command>`, for example `/api/bcdr/enable-recovery`.
`GET /api/bcdr/discovery` is a separate explicit healthy-source preparation read, not part
of status or any recovery action.

The POST paths are `/api/bcdr/setup`, `/api/bcdr/plan`, `/api/bcdr/synchronize`,
`/api/bcdr/configure-protection`, `/api/bcdr/enable-recovery`, `/api/bcdr/cutover`,
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
