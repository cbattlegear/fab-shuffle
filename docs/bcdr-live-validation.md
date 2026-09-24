# Live end-to-end qualification runbook

## Goal and scope

Qualify the actual Azure/Fabric deployment, not just its mocked APIs. Use an isolated,
disposable, same-tenant environment with primary and recovery capacities in different
regions. Start with normal operation, independent Lakehouse recovery, then temporary
attachments, and controlled interruption last.

No production data, shared capacities, real alert recipients or production ingestion
endpoints belong in this exercise. This is a manual plan, not a record of completed live
tests. See [the operator guide](bcdr.md) and [the research rationale](bcdr-viability.md).

| Outcome | What it establishes |
| --- | --- |
| Application qualification | EasyAuth, MI, synchronization, permissions, scheduling, data restoration, reconciliation and failback work for the tested fixtures. |
| Source-compute outage simulation | Recovery with primary compute paused; the stronger test also refuses source-specific control/compute requests. |
| Microsoft-managed regional failover qualification | Actual global OneLake routing and engine behavior against failed-over storage. Pausing a capacity or reading healthy primary storage does not establish this. |

Use the first two as engineering acceptance. Leave the third unverified unless exercised
in coordination with Microsoft during an applicable service failover. Native OneLake
replication is asynchronous, uses a fixed supported regional pair and is not a
customer-triggerable failover API. [OneLake DR], [experience guidance]

## Gate 0: test the right build

The implementation baseline for this runbook is
`77efd709a0f17700d8dc1e7128e1951d9e34581e`. At preparation time, the latest published
GitHub release was v2.2.0. **Do not assume `latest` or a Deploy button targeting `main`
contains these changes.**

Build/publish the reviewed PR head or approved successor to your registry, record its
immutable digest and use the same digest for web and job. Use both ARM templates from
that same source revision. Configure private-registry image pulls explicitly when needed;
attaching the Fabric UAMI alone does not configure them.

```powershell
docker build --platform linux/amd64 --target test -t fab-shuffle:test .
docker run --rm --platform linux/amd64 --network none fab-shuffle:test
docker build --platform linux/amd64 --target production -t fab-shuffle:live-candidate .
```

Do not inject Fabric credentials or mount live recovery storage into the test image.
Live exercises use the production image with required network connectivity, not
credentialed pytest runs. Record commit, template revision, image digest, running
revision IDs and application version; the version string alone is insufficient.

**Pass:** both deployments reference the intended image, which exposes `scheduled-sync`,
`configure-replica` and `reconcile-operation`.

## Environment and permissions

| Resource | Purpose |
| --- | --- |
| Dedicated primary Fabric capacity P in A | Disposable source; safe to pause. |
| Dedicated recovery capacity R in B | Initially hosts the control Warehouse and recovery workspaces. |
| Controller Container Apps environment in B | Web and job, independent of P and of Fabric compute. |
| One user-assigned managed identity | Same tenant/client/object identity on web and job and in bootstrap. |
| Separate Entra registration | Browser EasyAuth, not the Fabric runtime identity. |
| Azure Files share | Shared bootstrap and approved request. |
| Private Blob lease container | Same exact lock blob for both workers; not metadata storage. |
| Test users | Allowed operators A/B, designated recovery owner, ordinary reader and denied user. Reader and owner must be distinct. |

Choose supported regions and workload-appropriate capacity sizes. A later native
replication exercise requires the documented regional pair, not merely two different
regions. Use disposable controller resources, record ownership of resources outside that
resource group, set budget alerts and identify who can stop the drill. The simple
template's locally redundant controller storage does not cover loss of its own region.

Record and grant only the applicable permissions:

- Fabric API/workspace-creation tenant settings, source inventory/definition access and
  destination capacity/workspace creation rights for the UAMI.
- Source and destination SQL/OneLake permissions and explicit connection access.
- Scoped Azure capacity permissions for the exact resources to inspect/resume/suspend;
  failback needs primary-availability visibility.
- The admin inventory APIs used to prove that recovery capacity has no unowned workspaces.
  An accessible-workspace list is not enough for parking.
- Blob data access on the dedicated lease container, not subscription-wide Contributor.
- Reviewed manual actions for any required SQL/model/OneLake ACLs not automatically replayed.

Keep service error codes, messages and request IDs. Do not broaden permissions or
disable guards merely to remove an error.

## Fixtures

First qualify setup/synchronization using a harmless standalone Notebook. Then add:

| Source workspace | Fixture |
| --- | --- |
| `qa-bcdr-prod-data` | Schema-enabled Lakehouse `Sales`, `dbo.Orders`, `audit.Orders`, nested Files directory. |
| `qa-bcdr-prod-compute` | Notebook with explicit default Lakehouse and literal mapped references; no automatic execution. |
| `qa-bcdr-prod-bi` | Working Direct Lake model and report over `Sales`; select one explicit Direct Lake mode first. |
| `qa-bcdr-prod-independent` | Independent harmless Notebook for partial-group tests. |
| `qa-bcdr-excluded` | Selection/exclusion fixture; no initial happy-path dependency on it. |
| Recovery control workspace | New metadata Warehouse only; excluded from business selection and broad ACL replay. |

Make the source fixture work before using it to validate recovery. Use deterministic data:

- `dbo.Orders`: IDs 1-10, amounts 10 through 100 in increments of 10; 10 rows, total 550.
- `audit.Orders`: IDs 1-3, amounts 1000, 2000, 3000; 3 rows, total 6000.
- `Files/input/reference.txt`: known UTF-8 bytes, recorded length and SHA-256.
- Report visuals show both distinct totals, not just an empty report shell.

Record complete ordered rows or a repeatable per-key comparison as well as totals.
Synthetic source loading may use Spark; the requirement is no Spark for metadata
management, not no Spark anywhere in the fixture.

Add classic, empty, Files-only and other required workloads later. Keep deliberately
unsupported items in separate workspaces because current readiness grouping can couple
items in the same workspace.

## Gate 1: EasyAuth authorization

Follow deployment outputs in order:

1. Deploy the matching template with the existing UAMI, separate single-tenant registration
   and explicit operator object IDs.
2. Verify internal ingress and `FAB_SHUFFLE_EASYAUTH_READY=false`. An authorized internal
   test caller with invented principal headers must not reach application operations.
   The exact health probe remains available.
3. Inspect deployed platform/AAD enablement, tenant issuer, audiences, operator allowlist,
   secret reference and only `/api/health` excluded.
4. Enable external ingress while readiness is false.
5. Read the actual public FQDN and register its exact `/.auth/login/aad/callback`.
6. Set readiness true last and wait for the new revision.

| Probe | Required result |
| --- | --- |
| Anonymous/incognito | Entra redirect; no console operation. |
| Allowed operator A | Console opens. |
| Unlisted user in the same tenant | Denied; tenant membership is insufficient. |
| Wrong-tenant user | Denied. |
| Invented external `X-MS-CLIENT-PRINCIPAL` | No bypass of the sidecar. |
| B using A's disposable Fabric session ID | Denied, including SSE and logout of A. |
| Expired sign-in | Actionable reauthentication, not JSON errors or downloaded login HTML. |
| Cross-origin write without application request header | Refused. |

Use separate browser profiles. Do not retain cookies, tokens or secrets in screenshots
or distribute administrator sessions.

**Pass:** positive and negative authorization before connecting the runtime identity.

## Gate 2: managed identity and fresh catalog

Use **Settings > Environment** for one-time infrastructure preparation. Normal **Set up standby**
must only ask for individual workspaces or a name-contains rule, show the matches and saved
destination, and run the initial sync after review. No Warehouse, owner, ARM-ID or mapping
fields belong in that normal flow. Test a rule with no matches, a removed workspace, and
changed routing between review and sync; none may silently broaden scope or guess a target.
Complete the metadata baseline before preparing scheduled-sync instructions. A configuration
download must not claim a scheduler has been deployed, and optional data gaps must remain visible.

1. Choose Azure managed identity. No Fabric client secret should be requested.
2. Compare actual tenant/client/object IDs with the UAMI.
3. Demonstrate source discovery and target access; test a denial using a disposable
   unshared workspace, not tenant-wide permission changes.
   **Discover setup choices** must show local progress and source/recovery counts next
   to the button, including empty/partial failures and the next action. Select source
   capacities and an existing control workspace by name, refresh discovery, and verify
   the selections and typed Warehouse name survive. Confirm that unavailable or ambiguous
   choices cannot be silently submitted. Check exact identities in restricted server logs.
   Select recovery capacities by name and region; review the shown Azure subscription/resource
   group, then choose the catalog capacity from the selected rows. Verify a duplicate or
   unmatched name/region is disabled and that a changed Azure match clears prior suspension
   approvals. The matching rule is a best-effort comparison, not proof of cross-API identity.
4. Create a new restricted control workspace/Warehouse on R. Fresh setup currently expects
   designated owners with Admin roles.
   Also exercise **Use an existing Warehouse** with an empty disposable Warehouse: the
   exact selected resource should become the catalog without another REST creation.
   An unrelated nonempty Warehouse or another recovery set's catalog must be refused
   without changing its contents or the saved Warehouse binding.
   Reopen an interrupted setup and confirm saved names appear. If its operation is unavailable,
   list Warehouses in that metadata workspace and explicitly select the intended item.
   Confirm a workspace switch/sign-out discards late list responses and that listing alone
   does not resume capacity, create a Warehouse or initialize SQL.
5. Inspect durable non-secret bootstrap coordinates and Warehouse-held definitions,
   generations and history.
6. Verify the UAMI retains Admin access. Add a disposable named reader to the control
   workspace and confirm setup/sync shows a warning beside its action rather than blocking
   or removing the grant. A different designated-owner role is also advisory. Missing
   controller Admin remains blocking; business standby workspace access remains restricted.
7. Restart the idle controller, reconnect and load status. Same catalog IDs, no duplicate.

Observe Fabric activity: metadata preparation must not submit a metadata Notebook/Spark
job. Treat live API or SQL permission failures as prerequisites/defects, not a reason to
disable operator authentication.

**Pass:** fresh-share setup, stable identity, verified permissions and durable idle restart.

## Gate 3: dependency-aware standby synchronization

1. Pass the harmless Notebook sync, then capture the connected fixture with P healthy.
2. Preview exact IDs, routes, `prod` keyword and exclusions; control/excluded workspaces
   must not be selected.
3. Select BI without approved prerequisites. Inspect proposed additions/blockers, then
   approve the necessary dependencies and synchronize.
4. Inspect actual definitions: Notebook -> recovery Lakehouse, model -> recovery
   store/endpoint, report -> recovery model. Check real GUID, name and path bindings.
5. Verify generated SQL endpoints are owner-derived resources, not unsupported standalone
   recovery items. Include independent former default models.
6. Verify no business job, ingestion, mirror/rule or schedule starts from metadata sync.
7. Inspect workspace/item access; general users remain deferred, control remains private.
8. Verify R is suspended through Azure state before calling application status, which may
   resume the control capacity.

| Evolution test | Acceptance |
| --- | --- |
| Unchanged resync | Stable owned IDs; no duplicate items/workspaces. A new capture generation is allowed. |
| Change one Notebook | Corresponding applied update; no execution or stale source reference. |
| Independent target edit | Named drift block, no silent overwrite. |
| Delete a disposable source item | Tombstone/retention, not automatic target destruction. |
| Exclude a prerequisite | Named affected consumers; independent group can progress. |
| Lose metadata permission | No empty-inventory success/false deletion; prior complete generation retained. |

**Pass:** correct bindings, safe repeatability, restricted ACLs and observed parking.
Repair through reviewed operations, not direct catalog edits.

## Gate 4: scheduled sync and overlap

Deploy a schedule only after manual sync passes.

1. Save the approved non-secret request at `/app/local/bcdr/scheduled-sync-request.json`
   on the same share; exact scope/routes, `capture=true`, `park=true`, existing bootstrap.
2. Deploy matching job image/template with the same UAMI, environment storage and lease URL.
3. Start one execution manually, then observe an actual cron execution. Record execution
   IDs; cron is UTC. [jobs]
4. Confirm generation/application changes and final capacity state; no second catalog.
5. Start a second worker while the first demonstrably holds the lease. Use harmless
   additional metadata if necessary to observe overlap; sequential successes are not proof.
6. The loser must not touch bootstrap or issue extra capacity effects before ownership.
   A named lease-busy failure is expected.
7. Repeat with recovery enabled/active: visible skip, no source capture, business activation
   or pause of serving recovery capacity.

Distinguish completion, partial result, `scheduled_status="skipped"`/exit 2 and genuine
errors. A job execution marked failed due to an expected safety skip is not evidence of
an unsafe controller; retain its JSON result.

Mode checks may wake the control capacity. Do not claim a skipped execution has zero
capacity effects. `parallelism=1` is per execution, not overlap protection; the shared
Blob lease is the guard. Disable scheduling before fault injection or request-file changes.

## Gate 5: independent data recovery first

### Non-production DR Test first

With production still running, use **DR Test**, not **I'm currently down**. Select the
existing inactive standby groups and start an owners-only test. Verify that scheduled and
manual source synchronization are refused/held during `testing` and `ending_test`.
No primary pause, production ACL replay, writer-side change, consumer routing or workload
activation should occur. Submit real owner data/reference/access evidence and retain
passed/failed/blocked/not-tested outcomes.

End the test without deleting the standby estate or invoking production failback. Interrupt
one disposable test and confirm it resumes with the same test ID and generation; change a
disposable target and confirm ending remains blocked until its state is reconciled. Restore
safe standby before any scheduled run can capture again. A healthy-primary owners-only
exercise does not qualify actual regional storage failover or production-user readiness.

### Incident recovery

Use **I'm currently down** for the following outage exercises. Its navigation and status
loading must not issue healthy-source discovery or source metadata capture. Production
cutover remains a separate approval from recovery preparation, and **Return to primary**
is separate from ending a test.

### Prepare

1. Stop all fixture writers, maintenance/VACUUM, ingestion and applicable TTL. Record
   how fencing was established.
2. Produce a real `LakehouseProtection` descriptor with the healthy-source capture helper
   in the production Linux runtime: exact paths, hashes, ETags, directories and qualification.
3. Configure it and capture a new metadata generation; old generations are immutable.
4. Use an appropriate owned empty destination with only verified expected schema
   scaffolding, no unqualified shortcuts. Do not remove the mandatory `dbo` schema.

**Usability gate:** there is no one-click data-export/pin-capture command in this branch.
Use an approved preparation workflow around the helper. If its descriptor cannot be
generated/verified without inventing evidence, record an operability gap and stop.
Metadata synchronization is not data protection.

Keep the chosen input frozen. Changed files/ETags must be rejected, not repaired by
manually changing the descriptor.

### Recover

1. Save an evidence snapshot and disable the scheduled job.
2. Pause P after fencing writers. Keep approved source OneLake data access available.
3. Use a fresh UAMI session for recovery; no healthy-source discovery or `capture=true`.
4. Enable the pinned generation/group; copying/metadata may complete before readiness.
5. Inspect destination storage and actual SQL/Spark visibility. Compare complete rows in
   both schemas, totals and file hashes; run the model/report and verify data sources.
6. Collect fresh post-copy/post-metadata observations bound to generation, writer epoch,
   target identity/hash, issuer and intended runtime principals.
7. Apply only eligible approved deferred ACLs after enablement. Verify actual reader access
   separately. Record any required manual permission step as manual.
8. Approve cutover only after data, binding, security and source-writer checks. Verify
   the reader uses the recovered report with correct totals and P paused.

The tool records admission; it does not automatically execute every job or switch every
external consumer. Redirect the disposable consumer explicitly and record the action.

**Independence proof:** after materialization, deny source data to the disposable runtime
reader or use a source-specific denial harness, then repeat target queries. Do not
disable the shared UAMI or change tenant-wide settings. No independent consumer may
still depend on original storage.

A pause does not necessarily make every source metadata API unavailable. A stronger
source-control-plane-independent exercise refuses requests for the original workspace/
item IDs to Fabric, Power BI, source SQL and KQL while allowing destination calls and
approved global OneLake reads. Do not block the shared Fabric hostname wholesale.
Without this instrumentation, label the result capacity-pause simulation only.

**Pass:** verified destination data, actual reader access, correct bindings and writer
exclusivity. Keep copy, metadata, ACL and cutover evidence separate.

## Gate 6: failback with DR changes

Start with no new writes, then repeat on a fresh generation with known DR writes.

1. Resume/prove P available; original writers remain stopped.
2. In the writable variation, insert one known ID, update an amount and delete another ID
   in DR. Record expected final rows. Add a reviewed schema change in a later variation.
3. Stop the DR writer and collect reconciliation evidence.
4. Plan with explicit approved return connection routes, not automatically reversed ones.
5. Omit a required route once: expect a blocker without an unrecoverable mode transition.
   Correct it and retry.
6. Inspect fresh return workspaces on P. Restore/reconcile actual data, including deletions,
   using the approved provider/manual procedure; returned metadata is not reconciled data.
7. Compare final rows, report results, files, security and connections before cutback.
8. Switch the disposable consumer to returned IDs; preserve DR as rollback state.
9. Rearm only after recovery no longer serves production; verify restricted owned grants
   and safe parking.
10. Sync again and perform a second cycle. Confirm fresh return IDs and correct authority.

Also fail back only one independent group: unaffected source authority and protection
must remain, with no false tombstones.

If a workload lacks automatic insert/update/delete/TTL/offset reconciliation, use its
documented manual procedure or mark that functional test blocked. Evidence cannot
substitute for missing data or make a conflicting two-way merge safe.

**Pass:** exact reconciliation, correct consumer return, one writer, restricted rearm
and a working second cycle.

## Gate 7: temporary no-copy continuity separately

Use fresh targets/generations, not paths already occupied by materialized data.

1. Retain source IDs/data and independently qualify the caller-mode read-only principal.
   Do not use a source Admin/Contributor as the supposed read-only reader.
2. Select complete exact native Delta paths and supported Files directories. Record
   qualification, validity, binding hash, caller and enforcement evidence.
3. Configure and Enable recovery against owned target IDs.
4. Inspect exact shortcut targets; no unrelated overwrite. Consumers bind to recovery
   items; only intentional attachments retain source references.
5. Verify reads and actual write denial with the test principal, using a safe canary
   location for a denied write, never a destructive source operation.
6. Collect fresh engine/data/security evidence and verify queries in the supported mode.
7. Exercise expired evidence, missing coverage and conflicting shortcuts; refuse safely.

The adapter qualifies caller mode, not all delegated-owner SQL/Direct Lake paths.
A Spark/OneLake read is not Direct Lake-over-SQL qualification. Test identity modes
separately and retain unsupported-mode limitations. [direct-lake]

Healthy-primary or paused-compute shortcuts are a continuity rehearsal, not proof of
geo-replica routing. Do not fabricate an incident qualification or delete source items
while shortcuts remain.

## Gate 8: safety failures and controlled interruption

Run one fault at a time after happy paths pass; disable scheduling first.

| Fault | Required result |
| --- | --- |
| Wrong callback/operator/UAMI | Actionable denial; no Fabric mutation or credential fallback. |
| Missing lease permission | No bootstrap/capacity effects before the guard; restore scoped rights, allowing for RBAC caching. |
| Lease owned by another worker | Visible rejection, no extra effects. |
| Unrelated canary workspace on R | Parking refused; move it through an approved manual action, not a wider ownership allowlist. |
| Target drift or stale/wrong readiness | Refused; no silent overwrite/admission. |
| Unsupported item/missing data protection | Named partial/manual outcome; independent groups may recover. |
| Stop specific worker after persisted intent | Exact pending state; no guessed duplicate create/takeover. |
| Timeout/failed lease renewal | New effects stop; in-flight work may be ambiguous and capacity may remain running. |
| Retry with abandoned catalog owner | Refused until explicit reconciliation, even after Blob lease expiry. |

Before interruption, record operation/controller/writer IDs and epochs, bootstrap revision,
owned targets and service operation IDs. Stop only the owned test job/container.

Verify the old worker is stopped, inspect exact service receipts, then use Reconcile
interrupted operation with current fencing/quiescence evidence. Missing identification
evidence means manual reconciliation, not same-name adoption.

Never delete the lease, clear catalog ownership, overwrite bootstrap, edit successful
records, remove reference guards or turn EasyAuth off to make a test pass. Demonstrate a
clean successful operation after repair, not just error detection.

## Expanded coverage and release decision

After the core path, test classic/schema-enabled/empty/Files-only Lakehouses, duplicate
names, both Direct Lake modes, Import refresh, explicit connection routes, CI/CD Dataflows
and every adapter family required by the intended customer estate.

Current limitations to classify honestly:

- MirroredDatabase, populated Eventstream, Apache Airflow, SnowflakeDatabase and
  MountedDataFactory paths may be blocked by unqualified inactive creation.
- Dashboard/PaginatedReport reassignment is not source-unavailable reconstruction.
- Complex/follower KQL, prepared-target adoption and some ACL replay remain constrained/manual.
- Fabric SQL native backups cannot restore across regions; use separately protected exports.
- Cosmos needs consistency, partition-key and TTL/deletion handling, not analytical-copy assumptions.
- Writable Warehouse recovery is not established by schema creation or Lakehouse shortcuts.

Safe refusal can pass a **safety test**, not **functional recovery acceptance**. If
automatic recovery of a blocked workload is required, that workload is no-go.

An optional third catalog-only capacity can later exercise catalog-first resume,
catalog-last park and business-only failback scope.

## Evidence and cleanup

For each case retain build/template/image/revision IDs; exact scope and principal IDs;
generation, mappings and epochs; expected/observed rows, hashes, ACLs, schedules and
capacity states; and sanitized logs, screenshots, query results and service request IDs.
Classify pass/fail/blocked/not-tested and automatic/manual/simulated/actual-failover.

Measure actual durations and recovered-data age against business-defined objectives.
Do not derive RPO/RTO guarantees from a tiny fixture or metadata freshness.

Stop immediately for source mutation by standby sync, unexpected activation, catalog
exposure, unowned overwrite, source-bound independent consumers, overlapping effects or
lost recovery history.

Acceptance requires authorization denials as well as success, stable MI/bootstrap,
repeatable sync, real cron/exclusion, verified data/reader experience, two-cycle return
and safe reconciliation, with no critical unresolved defect for the intended workload set.

Cleanup: disable/delete the test job first, verify no active/ambiguous work, export
sanitized evidence, confirm no remaining temporary consumers, then delete only recorded
owned disposable resources after sign-off. Never delete shared identities, registrations,
unowned capacities or data behind live shortcuts.

## Official references

[OneLake DR]: https://learn.microsoft.com/en-us/fabric/onelake/onelake-disaster-recovery
[experience guidance]: https://learn.microsoft.com/en-us/fabric/security/experience-specific-guidance
[direct-lake]: https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview
[jobs]: https://learn.microsoft.com/en-us/azure/container-apps/jobs
