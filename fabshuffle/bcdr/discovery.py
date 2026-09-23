"""Best-effort name/region matching, never an authoritative cross-API identity."""

from __future__ import annotations

import re
from collections import Counter, defaultdict

from fabshuffle.fabric.workspaces import capacity_region


def match_recovery_capacities(fabric: list[dict], azure: list[dict]) -> list[dict]:
    def key(entry: dict, field: str) -> tuple[str, str]:
        return str(entry.get(field) or "").strip().casefold(), capacity_region(entry)

    fabric_counts = Counter(key(entry, "displayName") for entry in fabric)
    resources: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    for entry in azure:
        resources[key(entry, "name")][entry["id"]] = entry

    result = []
    for capacity in fabric:
        identity = key(capacity, "displayName")
        matches = list(resources[identity].values())
        choice = {
            **capacity, "matchMethod": "name_region", "arm_resource_id": None,
            "matchStatus": "unmatched",
            "matchMessage": (
                "No Azure capacity has the same name and region. Check Azure read access and retry discovery."
            ),
        }
        if not re.fullmatch(r"F[1-9][0-9]*", str(capacity.get("sku") or "").upper()):
            choice.update(matchStatus="unsupported",
                          matchMessage="Choose an Azure Fabric F capacity for dedicated recovery.")
        elif not all(identity):
            choice["matchMessage"] = (
                "Capacity name or region is missing. Refresh discovery before selecting it."
            )
        elif fabric_counts[identity] > 1 or len(matches) > 1:
            choice.update(
                matchStatus="ambiguous",
                matchMessage=(
                    "Multiple capacities share this name and region. "
                    "Resolve the duplicate scope before selecting."
                ),
            )
        elif len(matches) == 1:
            match = matches[0]
            choice.update(
                arm_resource_id=match["id"], subscriptionName=match["subscriptionName"],
                resourceGroup=match["resourceGroup"], matchStatus="matched",
                matchMessage=(
                    "Matched by name and region. Review the subscription and resource group "
                    "before authorizing suspension."
                ),
            )
        result.append(choice)
    return result
