# Migration implementation reference

For prerequisites, deployment, supported content and recovery controls, start with the
[README](../README.md). This reference explains the implementation, not a guarantee that
every service operation or target workload will succeed.

## Credential modes and data movement

**Single-principal** means one identity in one tenant. **Paired** means independent source
and destination identities, including two applications in the same tenant. Paired runs
always rebuild; single-principal runs can reassign Power BI-only workspaces or rebuild.

| Work | Single-principal rebuild | Paired rebuild |
| --- | --- | --- |
| Lakehouse tables | Native Fabric Copy Jobs | OneLake file streaming with Delta metadata preflight |
| Lakehouse Files | AzCopy download to disk, then upload | OneLake streaming, excluding shortcuts; Delta checks for unmanaged tables too |
| Warehouse schema | SqlPackage extract, UnpackDacPac script, TDS execution | Destination-aware DacFx script with security exclusions |
| Warehouse rows | Native Fabric Copy Jobs | Independently authenticated SQL row streaming |
| Fabric SQL database schema | Export/apply item definition | Destination-aware SQL schema transfer with security exclusions |
| Fabric SQL database rows | One native bcp table export/import at a time | Independently authenticated SQL row streaming |
| KQL rows | Cross-cluster `.set-or-replace` | Source query streaming and destination ingestion; target update policies disabled |
| Cosmos DB documents | SDK reads and upserts | SDK reads/upserts with separate source and destination credentials |

Single-principal rebuilds create a temporary Fabric workspace for Copy Jobs. Paired runs
do not use that workspace or native cross-tenant Copy Jobs as transport. Both modes use
local staging where needed, independently of whether that filesystem is a Docker volume or
an Azure Files mount.

The bcp path writes a SQL access token to an owner-only temporary file (mode `0600`,
UTF-16LE without a BOM) and removes it in `finally`. This is not a saved session credential
or recovery-journal entry. A hard process termination can bypass normal cleanup; protect
the container filesystem and do not reuse temporary token files.

## Dependency order

The module docstring and `_REBUILD_PHASES` in [orchestrator.py](../fabshuffle/orchestrator.py)
are the implementation authority. [Ordering regressions](../tests/test_rebuild_ordering.py)
pin the sequence. Later phases may only rely on mappings established earlier.

| Phase | Work and ordering constraint |
| --- | --- |
| `assessment` | Inventory supported/unsupported items and establish lifecycle requirements. |
| `dependencies` | Read relations, item definitions and shortcut bindings; collect source references. Supplied connection replacements are not validated against not-yet-created stores. |
| `workspaces` | Create or reuse the destination, folders, custom Spark pools and workspace Spark settings. Single-principal runs also preserve source admins and create/reuse Copy Job scratch space. Paired runs do neither. |
| `eventhouses` | Create eventhouses before their KQL databases. Apply database definitions and move eligible KQL data. |
| `lakehouses` | Create lakehouses before stores and definitions that reference their data or SQL endpoints. Move eligible tables and files. |
| `warehouses` | Transfer schema before rows so later consumers have destinations to bind. |
| `sqldatabases` | Recreate Fabric SQL databases and Cosmos DB databases, then copy eligible data using the mode-specific paths above. |
| `connections` | Validate only explicit operator mappings against the destination. With no mappings, there is no validation work. No connection is automatically created or adopted by name. |
| `mirrored` | Recreate mirrored stores and map returned SQL endpoint identities. Database mirrors arrive stopped; an explicit start option runs here before shortcuts. Databricks catalog sync remains disabled. |
| `shortcuts` | Reconcile lakehouse and KQL shortcuts after their target stores exist. Refresh lakehouse SQL endpoint metadata, then transfer endpoint schema. |
| `realtime` | Recreate KQL querysets/dashboards and eligible eventstreams. Paired eventstreams are withheld because inactive creation is not established for every node. |
| `engineering` | Environments before notebooks, then CI/CD dataflows and other definition-backed consumers such as Spark job definitions, GraphQL, graphs, maps and variable libraries. |
| `analytics` | Semantic models before reports; model dependencies determine ordering among models. |
| `orchestration` | Data pipelines, Copy Jobs and Apache Airflow jobs, after the stores and consumers they reference. Airflow files follow its configuration definition. |
| `reflexes` | Activator rules last among content phases because they can reference streams, stores, pipelines and notebooks. Rules arrive disabled. |
| `connectionadvisory` | Persist a read-only snapshot of visible connections targeting the source; do not mutate connections or item mappings. |
| `permissions` | Replay remaining workspace roles only when enabled for a single-principal run. Paired runs never copy access assignments. |
| Cleanup | After content phases, reconcile tracked jobs and remove owned scratch resources when requested. Retain journals for recovery. |

In single-principal rebuilds, source workspace **Admin** assignments are attempted during
workspace creation even when final permission copying is disabled. This preserves human
access if the run stops early. Errors are reported; successful access replay is not assumed.
Paired runs instead tell destination administrators to arrange access separately.

## Reference and connection safety

Definitions are rewritten through recorded source-to-destination mappings. Known source
dependencies without a destination are refused rather than left pointing at a workspace
the operator intends to retire. An explicit external-item mapping provides both the source
and destination workspace/item IDs; its metadata is read with the corresponding identity.

Connection detection and connection cutover advisories serve different purposes:

- Dependency checks inspect connections actually referenced by selected items or shortcuts.
  Explicit mappings are validated after the relevant stores exist.
- Cutover advisories inventory all connections visible to the source credential. SQL paths
  require a server/database pair, not a database GUID or name in isolation. Other paths use
  recognised literal source references. This does not prove which consumers still use a
  connection.

Neither path creates, adopts by name, deletes, or copies credentials/sharing for a connection.
An earlier automatic-recreation path was deliberately removed: matching a friendly name is
not proof of identity, and existing connection secrets cannot be faithfully exported.
Old identifier-only advisory snapshots are withheld as stale rather than silently treated
as new exact-pair results.

Removed mappings produce durable reference-block records. A retry cannot revive a removed
mapping from an older journal and silently keep a consumer bound to it. Changed target
incarnations invalidate affected bindings and copy checkpoints.

## Delta and SQL staging

OneLake Delta preflight reads JSON logs, classic/multipart/V2 Parquet checkpoints, sidecars
and relevant metadata references. It refuses unsafe absolute/escaping paths and unsupported
features rather than rewriting Delta logs to conceal dependencies. This applies to managed
tables and unmanaged Delta tables under Files; ordinary non-Delta files do not receive
Delta-specific inspection.

Checkpoint files are staged individually under the disk budget. Retained inventories,
JSON records and decoded Arrow data use the separate memory budget. A successful file
transfer does not establish destination SQL catalog visibility; endpoint refresh and
readiness evidence remain separate.

SQL schema jobs use private staging for DACPACs, generated scripts, tool temporary files
and logs, then remove it on exit. Schema disk usage is monitored; an external process can
overshoot between checks. Script reading, SQLCMD expansion, reference rewriting and batch
materialization have separate memory checks. Neither budget is an OS-level aggregate quota.
Legacy single-principal AzCopy Files staging and bcp native exports remain outside this disk
budget; see [storage sizing](../README.md#choosing-the-azure-files-size).

## Item-specific history and constraints

### Semantic models

Models formerly called *default semantic models* are included in migration attempts, even
when named after a lakehouse or warehouse. Fabric
[stopped creating them alongside those stores on 5 September 2025](https://learn.microsoft.com/fabric/data-warehouse/semantic-models)
and decoupled existing defaults by 30 November 2025. Assuming a destination store creates
a replacement model would omit an independent model people may still use.

Rebuilds inspect source large-storage settings and confirm or restore them on the destination.
Missing source-format metadata and failed restoration remain visible as lifecycle evidence.
Imported model definitions do not copy cached data or establish query readiness.

For reassignment, large models are converted to small storage first and restored afterward.
If conversion or assignment fails, restoration is attempted for models already converted;
restoration failures require operator action rather than being treated as success.

### KQL follower databases

Follower definitions do not expose the full leader identity. The implementation reads
`.show follower database` on the follower cluster. For a Fabric leader,
`OriginalDatabaseName` identifies its KQL database item. A migrated leader is remapped;
external dependencies require the applicable mode's reference checks and operator mappings.
Azure Data Explorer followers whose leader cluster URI cannot be resolved are reported,
not guessed from a database name.

### Dataflows and Spark job definitions

The tool attempts Dataflow Gen2 with CI/CD support. It probes definition availability rather
than trusting the item-list type filter to identify the dataflow generation. Unsupported
dataflows are reported with upgrade/Save As guidance.

Spark job definitions are exported as `SparkJobDefinitionV2` so the `Main/` and `Libs/`
files are included. The supported inline definition path cannot carry JAR payloads;
JVM jobs are reported rather than treated as complete file copies.

### Apache Airflow

Configuration and supported UTF-8 files are checked for known literal source references.
Binary files are preserved byte-for-byte; unreadable, oversized and unsafe definitions or
files require operator correction. This is not arbitrary Python code analysis. Inspect
dynamically constructed references, pause settings and runtime dependencies before enabling
the destination scheduler.

## API integration

The interactive schema is served at `/api/docs`. For paired sign-in, `POST /api/login`
accepts source credentials plus a destination object:

```json
{
  "tenant_id": "<source-tenant-id>",
  "client_id": "<source-application-id>",
  "client_secret": "<source-secret>",
  "destination": {
    "tenant_id": "<destination-tenant-id>",
    "client_id": "<destination-application-id>",
    "client_secret": "<destination-secret>"
  }
}
```

Use the returned `sessionId` in `X-Fab-Shuffle-Session`. `POST /api/preview/dependencies`
and `POST /api/runs` accept options including `write_freeze_confirmed`,
`start_database_mirrors`, `connection_mappings` and `reference_mappings`.
External references contain `source_workspace_id`, `source_item_id`,
`target_workspace_id` and `target_item_id`.

Resume can update mappings and explicitly opt into destination mirror starts for that
attempt; it does not change the tenant/application pair, source workspace or destination.
Readiness is a recorded snapshot at `GET /api/runs/{run_id}/readiness`, with
`?download=true` for a JSON attachment. Reading or exporting it performs no activation.

This implementation reference does not imply live two-tenant qualification, Azure
deployment qualification, or permission to delete a source after a successful run.
