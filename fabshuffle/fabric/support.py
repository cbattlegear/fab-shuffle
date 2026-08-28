"""What Fab Shuffle can and cannot move, and which migration strategy a workspace needs.

Keeping the support matrix in one place means the review screen, the run warnings, and the
strategy choice can never drift apart.
"""

from __future__ import annotations

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
        "SemanticModel",
        "Report",
    }
)

# Types Fabric derives from another item. They appear in the item list but are never
# created directly, so they are neither migrated nor worth warning about.
DERIVED_TYPES = frozenset({"SQLEndpoint", "MirroredWarehouse"})

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

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "unsupported": [item.as_dict() for item in self.unsupported],
            "unsupportedTypes": self.unsupported_types,
        }


def _reason_for(item_type: str) -> str:
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

        if item_type in DERIVED_TYPES:
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
    "LARGE_MODEL_REGIONS",
    "POWER_BI_TYPES",
    "REBUILT_TYPES",
    "Strategy",
    "UnsupportedItem",
    "WorkspaceAssessment",
    "assess_workspace",
    "supports_large_semantic_models",
]
