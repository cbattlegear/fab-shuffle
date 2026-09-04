"""What Fab Shuffle can and cannot move, and which migration strategy a workspace needs.

Keeping the support matrix in one place means the review screen, the run warnings, and the
strategy choice can never drift apart.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

# Item types Fab Shuffle rebuilds in the target workspace, with data where applicable.
# Semantic models and reports appear here *and* in POWER_BI_TYPES: they do not force a
# rebuild, but they are recreated and rebound when one happens for another reason.
REBUILT_TYPES = frozenset(
    {
        "Lakehouse",
        "Warehouse",
        "Eventhouse",
        "KQLDatabase",
        "MirroredDatabase",
        "SemanticModel",
        "Report",
        "DataPipeline",
        "CopyJob",
        "Notebook",
        "Environment",
        "Dataflow",
        "Eventstream",
        "KQLDashboard",
        "KQLQueryset",
        "GraphQLApi",
        "Map",
        "Reflex",
        "SparkJobDefinition",
        "VariableLibrary",
        "MountedDataFactory",
        "GraphModel",
        "GraphQuerySet",
        "MirroredAzureDatabricksCatalog",
        "SnowflakeDatabase",
        "SQLDatabase",
        "CosmosDBDatabase",
        "ApacheAirflowJob",
    }
)

# Types Fabric derives from another item. They appear in the item list but are never
# created directly, so they are neither migrated nor worth warning about.
#
# The relations APIs report several types under different, internal names than the item APIs
# do, so both spellings are listed. Everything here is compared case insensitively.
DERIVED_TYPES = frozenset(
    {
        "SQLEndpoint",
        "SqlAnalyticsEndpoint",
        "MirroredWarehouse",
    }
)

# Type names the relations APIs use, mapped to the item API names they correspond to. Only
# used to make dependency messages read the way the portal does.
RELATION_TYPE_ALIASES = {
    "model": "SemanticModel",
    "sqlanalyticsendpoint": "SQLEndpoint",
    "kustoeventhouse": "Eventhouse",
    "kustodatabase": "KQLDatabase",
    "datamart": "Datamart",
    # The relations APIs call a Fabric SQL database this, matching the GraphQL source type
    # enum rather than the item type.
    "sqldbnative": "SQLDatabase",
}


def normalise_type(item_type: str) -> str:
    """Map a relations API type name onto the item API name for the same thing."""
    return RELATION_TYPE_ALIASES.get((item_type or "").casefold(), item_type)


def is_derived_type(item_type: str) -> bool:
    """Whether Fabric creates this type as a side effect of creating something else.

    A lakehouse, warehouse, and mirrored database each come with a SQL analytics endpoint, so
    the endpoint arrives on its own once its parent is migrated.
    """
    folded = (item_type or "").casefold()
    return any(folded == derived.casefold() for derived in DERIVED_TYPES)


# Pure Power BI content. A workspace holding only these can be moved by reassigning it to a
# capacity in the target region, because the cross-region restriction on assignToCapacity
# applies to Fabric (non-Power BI) items only.
POWER_BI_TYPES = frozenset(
    {
        "Report",
        "PaginatedReport",
        "SemanticModel",
        "Dashboard",
    }
)

# Regions that support Azure Premium Files, and therefore large semantic model storage.
# A workspace with large models can only land in one of these.
# https://learn.microsoft.com/power-bi/enterprise/service-premium-large-models
LARGE_MODEL_REGIONS = frozenset(
    {
        "australiaeast", "australiasoutheast", "austriaeast", "brazilsouth", "brazilsouthb",
        "canadacentral", "canadaeast", "centralindia", "centralus", "chilecentral",
        "eastasia", "eastus", "eastus2", "francecentral", "francesouth",
        "germanynorth", "germanywestcentral", "indonesiacentral", "israelcentral", "italynorth",
        "japaneast", "japanwest", "koreacentral", "koreasouth", "malaysiawest",
        "mexicocentral", "newzealandnorth", "northcentralus", "northeurope", "norwayeast",
        "norwaywest", "polandcentral", "qatarcentral", "singapore", "southafricanorth",
        "southafricawest", "southcentralus", "southeastasia", "southindia", "spaincentral",
        "swedencentral", "switzerlandnorth", "switzerlandwest", "taiwannorth", "taiwannorthwest",
        "uaecentral", "uaenorth", "uksouth", "ukwest", "westeurope",
        "westindia", "westus", "westus2", "westus3",
    }
)


# The order the migration creates things in, used to present a count of what will move.
# Data stores first, then what reads them, then what orchestrates that. Keeping it here next
# to REBUILT_TYPES means a newly supported type is noticed when it is added rather than
# quietly dropped off the end of the review screen.
DISPLAY_ORDER = (
    "Lakehouse",
    "Warehouse",
    "SQLDatabase",
    "CosmosDBDatabase",
    "Eventhouse",
    "KQLDatabase",
    "MirroredDatabase",
    "MirroredAzureDatabricksCatalog",
    "SnowflakeDatabase",
    "Eventstream",
    "KQLQueryset",
    "KQLDashboard",
    "Environment",
    "Notebook",
    "Dataflow",
    "SparkJobDefinition",
    "GraphQLApi",
    "GraphModel",
    "GraphQuerySet",
    "Map",
    "VariableLibrary",
    "MountedDataFactory",
    "SemanticModel",
    "Report",
    "DataPipeline",
    "CopyJob",
    "ApacheAirflowJob",
    "Reflex",
)

# Plurals a machine would get wrong, and casing Fabric's own type names lose.
TYPE_LABELS = {
    "ApacheAirflowJob": "Apache Airflow jobs",
    "CopyJob": "Copy Jobs",
    "CosmosDBDatabase": "Cosmos DB databases",
    "DataPipeline": "Data pipelines",
    "GraphQLApi": "GraphQL APIs",
    "GraphQuerySet": "Graph query sets",
    "KQLDashboard": "KQL dashboards",
    "KQLDatabase": "KQL databases",
    "KQLQueryset": "KQL querysets",
    "MirroredAzureDatabricksCatalog": "Mirrored Azure Databricks catalogs",
    "Reflex": "Activator items",
    "SQLDatabase": "SQL databases",
    "SparkJobDefinition": "Spark job definitions",
}


def type_label(item_type: str) -> str:
    """A human readable, plural name for an item type."""
    if item_type in TYPE_LABELS:
        return TYPE_LABELS[item_type]
    # Split the CamelCase Fabric uses into a sentence: leading word capitalised, the rest
    # not, so "SemanticModel" reads as "Semantic models" rather than "Semantic Models".
    words = re.findall(r"[A-Z]+(?![a-z])|[A-Z][a-z]*|[a-z]+", item_type) or [item_type]
    spaced = " ".join([words[0], *(word.lower() for word in words[1:])])
    return f"{spaced[:-1]}ies" if spaced.endswith("y") else f"{spaced}s"


class Strategy(StrEnum):
    """How a given workspace can be moved."""

    REASSIGN = "reassign"
    """Power BI only: reassign the existing workspace to a capacity in the target region."""

    REBUILD = "rebuild"
    """Fabric items present: recreate everything in a new workspace and copy the data."""


@dataclass(frozen=True, slots=True)
class UnsupportedItem:
    name: str
    type: str
    reason: str

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "type": self.type, "reason": self.reason}

    def message(self) -> str:
        return f"{self.type} '{self.name}' was not migrated: {self.reason}"


@dataclass(frozen=True, slots=True)
class WorkspaceAssessment:
    strategy: Strategy
    migrated: list[dict[str, Any]] = field(default_factory=list)
    unsupported: list[UnsupportedItem] = field(default_factory=list)

    @property
    def unsupported_types(self) -> list[str]:
        return sorted({item.type for item in self.unsupported})

    def migrated_counts(self) -> list[dict[str, Any]]:
        """What will move, counted by type, in the order the migration creates it.

        Built from the assessment rather than by asking Fabric again, so it costs nothing and
        can never disagree with what the run then does. Types that are not in
        ``DISPLAY_ORDER`` still appear, at the end, so a newly supported one is visible even
        before anyone decides where it belongs.
        """
        counts: dict[str, int] = {}
        for item in self.migrated:
            item_type = item.get("type") or "Unknown"
            counts[item_type] = counts.get(item_type, 0) + 1

        ranked = {item_type: rank for rank, item_type in enumerate(DISPLAY_ORDER)}
        ordered = sorted(counts, key=lambda t: (ranked.get(t, len(ranked)), t))
        return [{"type": t, "label": type_label(t), "count": counts[t]} for t in ordered]

    @property
    def migrated_total(self) -> int:
        return len(self.migrated)

    def grouped_messages(self) -> list[str]:
        """One message per item type rather than per item.

        A workspace can easily hold dozens of unmigrated items that share a single reason,
        and repeating that reason for each one buries the information.
        """
        groups: dict[str, list[UnsupportedItem]] = {}
        for item in self.unsupported:
            groups.setdefault(item.type, []).append(item)

        messages: list[str] = []
        for item_type in sorted(groups):
            items = groups[item_type]
            names = ", ".join(sorted(f"'{item.name}'" for item in items))
            messages.append(f"{item_type} ({len(items)}) not migrated: {names}. {items[0].reason}.")
        return messages

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "unsupported": [item.as_dict() for item in self.unsupported],
            "unsupportedTypes": self.unsupported_types,
            "unsupportedSummary": self.grouped_messages(),
        }


# Types whose reason for not migrating is worth stating precisely, because "not supported
# yet" would imply the work is simply outstanding when in fact the API refuses.
SPECIFIC_REASONS = {
    "DigitalTwinBuilder": (
        "its APIs refuse service principals, and Fab Shuffle signs in as one. Recreate it by "
        "hand, or run the migration as a user if that becomes possible"
    ),
    "DigitalTwinBuilderFlow": (
        "a flow only means anything alongside its Digital Twin Builder, whose APIs refuse "
        "service principals, so migrating the flow on its own would leave it pointing at a "
        "twin that does not exist. Recreate both by hand"
    ),
    "MLModel": (
        "its APIs refuse service principals, and the trained model itself lives outside the "
        "item definition. Re-register the model in the new workspace"
    ),
    "MLExperiment": (
        "its APIs refuse service principals, and run history cannot be exported. Recreate the "
        "experiment in the new workspace"
    ),
    "UserDataFunction": "its APIs refuse service principals, and Fab Shuffle signs in as one",
    "AnomalyDetector": "its APIs refuse service principals, and Fab Shuffle signs in as one",
    "OperationsAgent": "its APIs refuse service principals, and Fab Shuffle signs in as one",
    "Dashboard": (
        "Fabric exposes no way to read a dashboard's definition, so it cannot be recreated. "
        "Rebuild it in the new workspace and re-pin its tiles"
    ),
    "Datamart": "Fabric exposes no API to read or create one, so it cannot be recreated",
    "WarehouseSnapshot": (
        "a snapshot is a point in time view of a warehouse, and that history does not exist "
        "in a newly created one. Take a fresh snapshot after the warehouse has migrated"
    ),
}


def _reason_for(item_type: str) -> str:
    if item_type in SPECIFIC_REASONS:
        return SPECIFIC_REASONS[item_type]
    if item_type in POWER_BI_TYPES:
        return (
            "Fab Shuffle cannot recreate this Power BI item type yet. Recreate it in the new "
            "workspace and point it at the migrated semantic model"
        )
    return "Fab Shuffle does not migrate this item type yet"


def assess_workspace(items: Iterable[Mapping[str, Any]]) -> WorkspaceAssessment:
    """Classify a workspace's items and decide which migration strategy applies."""
    migrated: list[dict[str, Any]] = []
    unsupported: list[UnsupportedItem] = []
    has_fabric_item = False

    for item in items:
        item_type = item.get("type") or "Unknown"
        name = item.get("displayName") or item.get("id") or "(unnamed)"

        if is_derived_type(item_type):
            continue
        # Anything that is not Power BI content is a Fabric item, and a single one of those
        # pins the workspace to the rebuild strategy. Unknown future types count as Fabric.
        if item_type not in POWER_BI_TYPES:
            has_fabric_item = True

        if item_type in REBUILT_TYPES:
            migrated.append(dict(item))
        else:
            unsupported.append(UnsupportedItem(name=name, type=item_type, reason=_reason_for(item_type)))

    if has_fabric_item:
        return WorkspaceAssessment(Strategy.REBUILD, migrated, unsupported)

    # Power BI only: reassignment moves everything, so nothing is left behind.
    return WorkspaceAssessment(Strategy.REASSIGN, [], [])


def supports_large_semantic_models(region: str) -> bool:
    return region.replace(" ", "").lower() in LARGE_MODEL_REGIONS


__all__ = [
    "DERIVED_TYPES",
    "DISPLAY_ORDER",
    "LARGE_MODEL_REGIONS",
    "POWER_BI_TYPES",
    "REBUILT_TYPES",
    "RELATION_TYPE_ALIASES",
    "SPECIFIC_REASONS",
    "TYPE_LABELS",
    "Strategy",
    "UnsupportedItem",
    "WorkspaceAssessment",
    "assess_workspace",
    "is_derived_type",
    "normalise_type",
    "supports_large_semantic_models",
    "type_label",
]
