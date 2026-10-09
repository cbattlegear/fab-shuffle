# Fabric BCDR research and design rationale

This report preserves the research behind [issue #24][issue] and the implementation
in this branch. The original migration-code assessment used commit
`9d879dc969553027cc3331a6dfa7b2627814225f`, before the BCDR implementation.
It is historical design evidence, not a statement that the new recovery code is absent.

Use [the operator guide](bcdr.md) for implemented behavior and
[the live qualification runbook](bcdr-live-validation.md) for release acceptance.
No live regional failover, Azure deployment or customer recovery was performed during
this research. Documented component capabilities do not establish that their composition
works in a real outage.

## Verdict and chosen design

**Conditional go:** Fab Shuffle's adapters provide a foundation for a BCDR controller,
but a successful migration or definition import does not establish application recovery.
Multi-workspace recovery needs pre-disaster capture, an item-level dependency plan,
workload-specific data inputs and explicit cutover/failback controls.

The selected design is:

- One central metadata **Warehouse** in a designated recovery workspace, accessed through
  REST and TDS, without Spark for metadata management.
- Pre-disaster synchronization of actual standby items across destination workspaces.
  Dedicated recovery capacity is resumed for synchronization and paused only when safe.
- Workspace access restricted to the recovery service principal or managed identity and
  designated owners before recovery. Other ACLs are captured and deferred.
- Separate temporary no-copy attachments and independent materialization paths. Only an
  explicit, qualified temporary attachment can retain original source identities.
- Optional SQL/Cosmos/KQL data protection. Missing protection produces named partial or
  blocked outcomes, not success and not an automatic abort of unrelated groups.
- No Git integration. Git findings below explain the accelerator's limitations, not a
  feature in Fab Shuffle.

No-copy continuity is not a universal replacement for copying. OneLake documents access
through its global endpoint after Microsoft-managed failover; shortcuts support
cross-workspace references, and Direct Lake supports local lakehouses containing
shortcuts. Those facts do not prove that a newly created shortcut and every consuming
engine work against unavailable source items during a real regional incident.
Microsoft explicitly says shortcuts are not themselves a DR/failover solution.
[OneLake DR], [shortcuts], [move-shortcuts], [direct-lake]

## Shared responsibility and workload limits

| Area | Documented behavior | Design consequence |
| --- | --- | --- |
| Regional failover | Microsoft initiates platform failover. Recovery depends on tenant home-region availability or its Microsoft-managed recovery. | The controller cannot bypass an unavailable Fabric control plane or trigger platform failover. [reliability], [experiences] |
| OneLake | Capacity opt-in; fixed supported regional pair; asynchronous replication; global-endpoint reads/writes after failover. Unreplicated data can be lost. | Verify protection before an incident. Do not invent secondary storage URLs or promise zero loss. [OneLake DR] |
| Power BI | Managed read-only continuity in supported paired regions. Import data is only as fresh as the last refresh; DirectQuery/Direct Lake require available sources. | Visible reports are not proof of a restored application. Recreated models/reports need consumer cutover. [powerbi-dr] |
| Lakehouse | Recover items and Delta data separately. The recovery example uses a support-provided failover timestamp; non-Delta tables need saved DDL. | Accessible folders and copied logs are not evidence of a consistent recovery point. Never repair source logs. [experiences] |
| Warehouse | Recover Delta data through interim Lakehouse storage, rebuild schema and load a real Warehouse. | A shortcut over Warehouse data is not a writable Warehouse or its SQL objects/endpoint. [experiences] |
| Fabric SQL database | Native backups are regional, not geo-replicated, with restore limited to the same workspace. | Cross-region recovery needs a separately protected portable export. A BACPAC is not transaction-log backup or PITR. [sql-backup], [sqlpackage] |
| KQL | Guidance calls for independent regional databases, replicated schema/policy changes and parallel ingestion. | OneLake DR alone is not full KQL recovery. A paused capacity cannot continuously ingest. [experiences] |
| Fabric Cosmos DB | No customer-configurable transactional backup/PITR is documented. | Do not import Azure Cosmos DB account failover assumptions or treat analytical Delta as a faithful transactional restore. [cosmos-security] |
| Definitions/configuration | Many non-Power BI definitions and runtime settings are not restored by platform failover. | Capture code, properties, bindings, security and activation requirements before losing source access. [experiences], [get-definition] |
| External systems/gateways | Their regional availability requires separate preparation. | Include external source, gateway, network, secret and identity dependencies. [gateways] |

The general reliability page describes both read-only failover operation and replacement
workspace creation during recovery. Experience-specific guidance clarifies the home-region
prerequisite. Qualify actual destination operations rather than assuming every API is
writable throughout every outage.

Some general shortcut pages retain flat-`Tables` wording alongside schema-shortcut support.
The schema-specific documentation explicitly describes schemas and schema shortcuts.
Use the applicable endpoint contract and exercise actual layouts. Every schema-enabled
lakehouse has a default `dbo` schema that cannot be removed. [schemas]

General architecture summaries suggesting SQL recovery through OneLake do not override
the SQL-specific backup limitations. Likewise, old references to default semantic models
must not be used to omit independent models formerly called defaults.

## Why the metadata store is a Warehouse

| Capability or constraint | Implementation requirement |
| --- | --- |
| Create Warehouse supports service principals and asynchronous provisioning; creation with definition is not supported. | Create through REST, observe returned identity/endpoint, then create metadata tables through TDS. [create-warehouse] |
| TDS supports Entra service principals and explicit catalogs. | Reuse Linux ODBC/token infrastructure; this is not the Fabric SQL-database connector. [warehouse-connectivity] |
| Persisted `varchar`/`varbinary` are supported; their `max` values have a documented 16 MB limit. `nvarchar` and native `json` are unsupported for persisted tables. | Store bounded ordered chunks with lengths, encodings and hashes. Do not truncate definitions. [warehouse-types] |
| ACID snapshot transactions, table-level write conflicts; no distributed transactions or savepoints. | Publish complete-generation pointers in short transactions. REST deployment is a separate resumable operation. [warehouse-transactions] |
| Primary/unique/foreign-key constraints are not enforced. | Validate duplicates and identities in code; do not use declared unique keys as locks. [warehouse-constraints] |
| Pause rejects SQL requests and rolls back open transactions; resume is cold; storage remains billable. | Commit/drain before parking, resume the catalog first, and keep minimal non-secret bootstrap coordinates outside it. [warehouse-pause] |

The catalog holds captured definitions/properties/runtime payloads, dependency edges,
desired ACLs, target mappings, applied hashes and operation history. Business-data exports
remain separately protected inputs. Capture completion and actual application to standby
are distinct states.

The controller never replays business ACLs into the control workspace. Additional
control-workspace members and owner-role differences are advisory warnings, not an
exact-membership gate; their grants remain unchanged. Controller Admin access is still
required. Business standby workspaces retain strict access checks until enablement.
Loss of the catalog's own region is not solved by silently using an old local cache.
ARM resume/suspend acts on explicitly authorized dedicated capacities, not capacities
discovered by name. [capacity-resume], [capacity-suspend]

For Azure web/job concurrency, an empty Blob lease is an additional deployment guard
before bootstrap/capacity effects. It is not a metadata archive. Azure Files advisory
locks and job per-execution parallelism are not a cross-container ownership guarantee.
The operator guide documents this later deployment integration.

## Investigation of the linked accelerator

Research pin: `microsoft/fabric-toolbox` commit
`c38ea357b804b335d1ed3b558dda38cda778cf70`. Its latest BCDR-directory change at inspection
was `af8590bd712fdfb84ac83b17a78c7b3dd72dde49`.
Notebook JSON is minified, so citations use zero-based cell numbers rather than misleading
file line anchors. The notebooks were read, not executed.

Notebook 01 records capacities, workspaces, generic items, Git connection details and
workspace roles in an attached Lakehouse. It does not produce a complete definition
archive or application-consistent data checkpoint. Tables are overwritten separately
rather than published as a complete generation. [primary] (cells 10, 12, 17),
[utilities] (cell 1).

Notebook 02 requires an active capacity and working Fabric notebook environment. It
copies metadata from original OneLake paths, creates suffixed workspaces, synchronizes
Git items, copies Lakehouse data, stages Warehouse data through shortcuts and repairs
selected references. [dr] (cells 7, 9-25, 29-35).

| Concern | Code-observed finding at the pinned revision |
| --- | --- |
| Runtime | Unpinned `semantic-link-labs`, NotebookUtils, Spark, pandas, JSONPath and private library helpers. The guide says SPN/managed-identity execution was untested. [utilities] (cells 0-2), [guide] (p9). |
| Capacity | Inactive capacity raises an error; resume is not implemented. [dr] (cell 7). |
| Selection | Exact include/exclude and negative SQL LIKE exist, but no positive keyword selector. A helper replaces nonempty includes with a hardcoded `%_DR` match. [primary] (cell 6), [dr] (cells 3, 12), [utilities] (cell 1). |
| Git | Recreation depends on connected Git workspaces and the then-current remote commit, not necessarily the captured recovery version. [dr] (cell 14). |
| GitHub credentials | The main request omits required configured GitHub credentials. The per-endpoint contract requires `myGitCredentials` for GitHub. [dr] (cell 14), [git-connect]. |
| Schemas | Partial schema handling exists, but recovery still queries source Lakehouse properties. Metadata bootstrap assumes immediate `Tables` children are tables. Files-only schema Lakehouses can be skipped. [dr] (cells 9-12, 20), [utilities] (cell 1). |
| Warehouse | Temporary source-ID shortcuts feed a copy pipeline; the embedded sink fixes schema to `dbo` and permits truncation. This is not general schema-preserving/no-copy recovery. [dr] (cells 21-25), [utilities] (embedded pipeline). |
| Direct Lake | Reads original models/workspaces during recovery, assumes same-workspace sources and skips models classified as defaults. [dr] (cell 31), [utilities] (cell 2). |
| Cross-workspace bindings | Name/type/suffix joins and literal substitution, not a complete graph. A Notebook update can use the Lakehouse workspace rather than the Notebook workspace. [dr] (cells 29, 33, 35). |
| Reports | Report capture is commented out while repair reads the expected captured report tables. [primary] (cell 10), [dr] (cells 16, 35). |
| Security | Workspace role replay exists despite README wording; complete data security, gateways and connection credentials are not restored. [dr] (cells 27, 33), [tool-readme]. |
| Reliability | Print-and-continue errors, limited explicit pagination, fixed sleeps, no durable ownership journal; some copies delete destination trees first. [utilities] (cell 1), [dr] (cells 12, 14, 23, 25). |
| Failback | Explicitly out of scope. Frozen suffix-based deletion is cleanup, not failback. [guide] (p7), [dr] (cells 36-37). |

At inspection, [microsoft/fabric-toolbox#482][schema-pr], "make logic schema agnostic",
was open and unmerged. The researched head
`616e11892ede466a49a5604297db44f9948cf15b` adds recursive Delta discovery, schema-aware
metadata paths, report capture and existing GitHub-connection lookup. It retains source
Lakehouse/model reads. Inspected execution issues include a missing new required helper
argument, two-value unpacking of a four-value result, unreachable update code and storage
errors converted to empty inventories. [pr-primary], [pr-dr], [pr-utilities].

**Reuse decision:** use the prepare/recover concept and binding examples as references,
not a vendored execution engine. The MIT-licensed accelerator labels itself demonstration
material, not a final supported DR solution. Preserve notices if code is reused later.
[tool-readme], [guide], [license]

## Why an estate-wide item graph is necessary

The earlier migration baseline had one source/target workspace per `MigrationPlan`,
live source-reading phases, local ID maps and journal-based migration resume.
External mapping resolution validated the source live. Schema-enabled Lakehouse
enumeration used its source SQL endpoint. Those paths were not source-unavailable BCDR.

A report in A can read a model in B, which reads a Lakehouse in C, while a pipeline in C
invokes a Notebook in A. The workspace graph appears cyclic even when the actual item
operations can be scheduled. Fully finishing A before B/C either fails or leaves old
references.

Create workspace containers first, then order item operations across workspaces.
Distinguish create, bind, data-ready and activate dependencies. Use
`(tenant, workspace, item)` identities and separately scoped connections/endpoints.
Detect real strongly connected components; use shell-then-bind only where the specific
API safely supports it. Never create a definition against original compute with a promise
to repoint it later.

Definitions, shortcuts, connection descriptors, model/report bindings and optional
scanner data all contribute evidence. Scanner lineage is not arbitrary Python/SQL
dependency analysis, and workspace lineage omits some cross-workspace shortcuts.
Record provenance and uncertainty; missing evidence does not prove independence.
[scanner], [shortcuts]

Preserve existing single-workspace migration phase order. BCDR gets a separate coordinator
and lifecycle, reusing adapters at explicit boundaries.

## Practices and live qualification still required

Define business-service RPO/RTO and read-only tolerances; protect metadata before failure;
keep the controller/identity/runbook outside the primary failure domain; prepare
capacity, gateway and external-source dependencies; fence writers rather than treating
timeouts as proof they stopped; and exercise failback with DR changes. [operations]

| Gate | Evidence required beyond offline tests |
| --- | --- |
| Native attachments | Actual failed-over storage access, newly created shortcut resolution and relevant runtime identity modes. |
| Delta consistency | Recoverable versions, complete required files/checkpoints and agreement with support-provided failover information. |
| Schema behavior | Classic/schema-enabled, `dbo`, custom schemas, duplicate names, empty tables, Files-only and shortcut cases. |
| Direct Lake | OneLake/SQL modes, target-region stores, views/fallback, multiple source workspaces and effective identity permissions. |
| Workload coverage | Per-type definition, data, inactive-create, security and failback qualification; no blanket claim from an API matrix. |
| Optional data protection | SQL export, Cosmos consistency/TTL/deletions and independent KQL input/ingestion behavior. |
| Azure integration | Real EasyAuth denial, UAMI grants, Blob lease exclusion, scheduled execution and timeout reconciliation. |
| Failback | Exact DR-write reconciliation and consumer cutback without split-brain or lost changes. |

Expected safe refusal is not functional recovery acceptance for that workload. The live
runbook keeps automatic, manual, blocked, simulated and genuine failover results distinct.

## References

[issue]: https://github.com/cbattlegear/fab-shuffle/issues/24
[reliability]: https://learn.microsoft.com/en-us/fabric/security/reliability-fabric
[OneLake DR]: https://learn.microsoft.com/en-us/fabric/onelake/onelake-disaster-recovery
[experiences]: https://learn.microsoft.com/en-us/fabric/security/experience-specific-guidance
[shortcuts]: https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts
[move-shortcuts]: https://learn.microsoft.com/en-us/fabric/onelake/shortcuts/move-shortcuts-region
[schemas]: https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas
[powerbi-dr]: https://learn.microsoft.com/en-us/power-bi/enterprise/service-admin-failover
[direct-lake]: https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview
[operations]: https://learn.microsoft.com/en-us/azure/well-architected/microsoft-fabric/operational-excellence
[capacity-resume]: https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/resume?view=rest-microsoftfabric-2023-11-01
[capacity-suspend]: https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/suspend?view=rest-microsoftfabric-2023-11-01
[gateways]: https://learn.microsoft.com/en-us/data-integration/gateways-business-continuity-disaster-recovery
[get-definition]: https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition
[git-connect]: https://learn.microsoft.com/en-us/rest/api/fabric/core/git/connect
[sql-backup]: https://learn.microsoft.com/en-us/fabric/database/sql/backup
[sqlpackage]: https://learn.microsoft.com/en-us/fabric/database/sql/sqlpackage
[cosmos-security]: https://learn.microsoft.com/en-us/fabric/database/cosmos-db/security
[scanner]: https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info
[create-warehouse]: https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/items/create-warehouse
[warehouse-connectivity]: https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity
[warehouse-types]: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
[warehouse-transactions]: https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions
[warehouse-constraints]: https://learn.microsoft.com/en-us/fabric/data-warehouse/table-constraints
[warehouse-pause]: https://learn.microsoft.com/en-us/fabric/data-warehouse/pause-resume
[primary]: https://github.com/microsoft/fabric-toolbox/blob/c38ea357b804b335d1ed3b558dda38cda778cf70/accelerators/BCDR/01%20-%20Run%20In%20Primary.ipynb
[dr]: https://github.com/microsoft/fabric-toolbox/blob/c38ea357b804b335d1ed3b558dda38cda778cf70/accelerators/BCDR/02%20-%20Run%20In%20DR.ipynb
[utilities]: https://github.com/microsoft/fabric-toolbox/blob/c38ea357b804b335d1ed3b558dda38cda778cf70/accelerators/BCDR/workspaceutils.ipynb
[tool-readme]: https://github.com/microsoft/fabric-toolbox/blob/c38ea357b804b335d1ed3b558dda38cda778cf70/accelerators/BCDR/README.md
[guide]: https://github.com/microsoft/fabric-toolbox/blob/c38ea357b804b335d1ed3b558dda38cda778cf70/accelerators/BCDR/Fabric%20BCDR%20Accelerator%20User%20Guide.pdf
[schema-pr]: https://github.com/microsoft/fabric-toolbox/pull/482
[pr-primary]: https://github.com/iamjenetzler/fabric-toolbox/blob/616e11892ede466a49a5604297db44f9948cf15b/accelerators/BCDR/01%20-%20Run%20In%20Primary.ipynb
[pr-dr]: https://github.com/iamjenetzler/fabric-toolbox/blob/616e11892ede466a49a5604297db44f9948cf15b/accelerators/BCDR/02%20-%20Run%20In%20DR.ipynb
[pr-utilities]: https://github.com/iamjenetzler/fabric-toolbox/blob/616e11892ede466a49a5604297db44f9948cf15b/accelerators/BCDR/workspaceutils.ipynb
[license]: https://github.com/microsoft/fabric-toolbox/blob/c38ea357b804b335d1ed3b558dda38cda778cf70/accelerators/BCDR/LICENSE.md
