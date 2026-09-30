"""Recovery qualification requirements, not claims of source-outage support."""

from __future__ import annotations

from types import MappingProxyType

from fabshuffle.bcdr.contracts import Nonempty, Qualification, Record
from fabshuffle.fabric.support import POWER_BI_TYPES, REBUILT_TYPES


class TypeContract(Record):
    item_type: Nonempty
    migration_rebuild: bool
    migration_reassign: bool = False
    capture_requirements: tuple[str, ...]
    data_requirements: tuple[str, ...] = ()
    restrictions: tuple[str, ...] = ()
    restore_qualification: Qualification = Qualification.UNVERIFIED
    inactive_create_qualification: Qualification = Qualification.UNVERIFIED
    failback_qualification: Qualification = Qualification.UNVERIFIED


def _contract(
    item_type: str, capture: str, data: str = "", restrictions: tuple[str, ...] = (),
) -> TypeContract:
    return TypeContract(
        item_type=item_type, migration_rebuild=True, migration_reassign=item_type in POWER_BI_TYPES,
        capture_requirements=(
            "properties", "dependencies", "desired_acls", "activation", *capture.split(","),
        ),
        data_requirements=tuple(data.split(",")) if data else (), restrictions=restrictions,
    )


_CONTRACTS = (
    # Per-type getDefinition endpoints now include Lakehouse/Eventhouse. A Lakehouse
    # definition still is not a complete security or physical-data inventory.
    # rest/api/fabric/{lakehouse,eventhouse}/items/get-{lakehouse,eventhouse}-definition
    _contract(
        "Lakehouse", "definition,data_access_roles,schema_mode,schemas,tables,non_delta_ddl,"
        "files_inventory,shortcuts", "consistent_delta_or_qualified_attachment,non_delta_data,files",
    ),
    _contract("Warehouse", "sql_schema,security_objects,table_storage_paths", "independent_warehouse_load"),
    _contract("SQLDatabase", "sql_schema,security_objects", "off_region_portable_export",
              ("Native backups are same-workspace and not geo-replicated; supply an off-region export.",)),
    _contract(
        "CosmosDBDatabase", "containers,partition_keys,ttl,consistency_evidence",
        "protected_logical_export", ("Analytical OneLake data is not a qualified transactional restore.",),
    ),
    _contract("Eventhouse", "definition,topology,kql_schema,policies", "independent_kql_standby_or_export"),
    _contract("KQLDatabase", "kql_schema,policies,mappings,follower_relationships",
              "independent_kql_standby_or_export"),
    _contract("MirroredDatabase", "definition,connection_requirements,mirroring_settings", "external_source"),
    _contract("MirroredAzureDatabricksCatalog", "definition,connection_requirements,autosync",
              "external_catalog"),
    _contract("SnowflakeDatabase", "creation_payload,connection_requirements", "external_source"),
    _contract("Environment", "definition,spark_settings,libraries"),
    _contract("Notebook", "definition,default_bindings"),
    _contract("SparkJobDefinition", "definition,main_files,libs_files",
              restrictions=("Only existing supported job languages and runtime file types are in scope.",)),
    _contract("Dataflow", "definition,connections,staging_dependencies,destinations",
              restrictions=("Only supported CI/CD Gen2 subtypes; not classic Dataflow Gen2.",)),
    _contract("Eventstream", "definition,topology,ingestion_state",
              restrictions=("Demonstrate inactive creation for each node before restoring.",)),
    _contract("KQLQueryset", "definition,kql_bindings"),
    _contract("KQLDashboard", "definition,kql_bindings"),
    _contract("GraphQLApi", "definition,source_bindings"),
    _contract("GraphModel", "definition,source_bindings", "graph_data_or_rebuild"),
    _contract("GraphQuerySet", "definition,model_bindings"),
    _contract("Map", "definition,backing_resources"),
    _contract("VariableLibrary", "definition,active_value_set"),
    _contract("MountedDataFactory", "definition,backing_resources"),
    _contract("SemanticModel", "definition,storage_mode,bindings,security_definitions",
              "import_reload_or_available_query_sources",
              ("Include independent former default models. Definitions do not preserve Import cache.",)),
    _contract("Report", "definition,model_bindings"),
    _contract("DataPipeline", "definition,connections,schedules"),
    _contract("CopyJob", "definition,connections,schedules"),
    _contract("ApacheAirflowJob", "definition,runtime_files,connections,schedules"),
    _contract("Reflex", "definition,triggers,actions,rule_state"),
    *(
        TypeContract(
            item_type=name, migration_rebuild=False, migration_reassign=True,
            capture_requirements=("properties", "desired_acls", "platform_continuity"),
            restrictions=("Migration reassignment is not source-unavailable content reconstruction.",),
            restore_qualification=Qualification.MANUAL,
        )
        for name in ("PaginatedReport", "Dashboard")
    ),
)
TYPE_REGISTRY = MappingProxyType({contract.item_type: contract for contract in _CONTRACTS})


def validate_registry() -> None:
    if {name for name, contract in TYPE_REGISTRY.items() if contract.migration_rebuild} != REBUILT_TYPES:
        raise ValueError("Update the BCDR registry for every supported migration rebuild type")
    if {name for name, contract in TYPE_REGISTRY.items() if contract.migration_reassign} != POWER_BI_TYPES:
        raise ValueError("Update the BCDR registry for the Power BI reassignment footprint")
