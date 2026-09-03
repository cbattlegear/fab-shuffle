"""Item dependency graph, from the beta relations APIs.

Fab Shuffle rewrites references using an id map built from the items it creates. Anything
outside that map keeps pointing at the original region, and anything a migrated item depends
on but that is not itself migrated leaves the copy broken. Neither is visible by inspecting
item definitions alone, but the relations APIs report both.

The APIs are beta and must be called with ``?beta=true``. Everything here degrades to "no
information" rather than failing a migration.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.items import is_monitoring_item
from fabshuffle.fabric.support import is_derived_type, normalise_type

logger = logging.getLogger(__name__)

# A relation that says nothing about whether one item needs another to work.
IGNORED_RELATION_TYPES = frozenset({"HiddenInWorkspace"})

# Status codes that mean the beta API is unavailable to this caller, rather than that one
# particular item could not be read.
_UNAVAILABLE_STATUS = frozenset({400, 401, 403, 404, 501})


@dataclass
class DependencyGraph:
    """What each item depends on, plus enough metadata to describe it to a human."""

    dependencies: dict[str, set[str]] = field(default_factory=dict)
    items: dict[str, dict[str, Any]] = field(default_factory=dict)
    workspaces: dict[str, str] = field(default_factory=dict)
    available: bool = True

    def dependents_of(self, item_id: str) -> set[str]:
        """What reads from, or is written to by, this item.

        The reverse of ``dependencies``. Needed because an item can be broken by something it
        *feeds* as easily as by something it reads: a Copy Job whose destination is a
        lakehouse in another workspace is a copy that leaves the region it was moved out of.
        """
        return {
            other for other, upstream in self.dependencies.items() if item_id in upstream
        }

    def name_of(self, item_id: str) -> str:
        return (self.items.get(item_id) or {}).get("displayName") or item_id

    def type_of(self, item_id: str) -> str:
        raw = (self.items.get(item_id) or {}).get("type") or "Unknown"
        return normalise_type(raw)

    def raw_type_of(self, item_id: str) -> str:
        return (self.items.get(item_id) or {}).get("type") or "Unknown"

    def is_monitoring(self, item_id: str) -> bool:
        return is_monitoring_item(self.items.get(item_id) or {})

    def workspace_of(self, item_id: str) -> str | None:
        return (self.items.get(item_id) or {}).get("workspaceId")

    def workspace_name(self, workspace_id: str) -> str:
        return self.workspaces.get(workspace_id) or workspace_id


@dataclass(frozen=True, slots=True)
class DependencyIssue:
    item: str
    item_type: str
    dependency: str
    dependency_type: str
    reason: str
    # Whether the item reads the other one, or writes to it. Only changes the wording.
    reading: bool = True

    def message(self) -> str:
        verb = "depends on" if self.reading else "writes to"
        return (
            f"{self.item_type} '{self.item}' {verb} {self.dependency_type} "
            f"'{self.dependency}', which {self.reason}."
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "item": self.item,
            "itemType": self.item_type,
            "dependency": self.dependency,
            "dependencyType": self.dependency_type,
            "reason": self.reason,
        }


def get_upstream(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
) -> dict[str, Any] | None:
    """Fetch what an item depends on. Returns ``None`` when the beta API is unavailable."""
    return _relations(client, workspace_id, item_id, "upstream")


def get_downstream(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
) -> dict[str, Any] | None:
    """Fetch what depends on an item. Returns ``None`` when the beta API is unavailable.

    Upstream alone is not enough. It reports what an item *reads*, so the destination of a
    Copy Job or a pipeline, which it writes to, never appears. That destination decides
    whether the migrated copy still works just as much as its source does, and a destination
    in another workspace means the copy keeps writing into the region we moved out of.
    """
    return _relations(client, workspace_id, item_id, "downstream")


def _relations(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    direction: str,
) -> dict[str, Any] | None:
    try:
        return client.get(
            f"workspaces/{workspace_id}/items/{item_id}/relations/{direction}",
            params={"beta": "true"},
        )
    except FabricApiError as error:
        if error.status_code in _UNAVAILABLE_STATUS:
            return None
        raise


def build_graph(
    client: FabricClient,
    workspace_id: str,
    items: Iterable[Mapping[str, Any]],
) -> DependencyGraph:
    """Build a dependency graph for the given items.

    Both directions are read for every item. Upstream says what it reads; downstream says
    what reads from it, which is the only way a write destination shows up. Both report the
    same kind of edge, ``itemId depends on dependentOnItemId``, just discovered from opposite
    ends, so they merge into one set of edges.

    If the very first call shows the beta API is unavailable, the rest are skipped rather
    than spending the caller's API quota on certain failures.
    """
    graph = DependencyGraph()
    first = True

    for item in items:
        item_id = item.get("id")
        if not item_id:
            continue

        graph.items.setdefault(
            item_id,
            {
                "id": item_id,
                "type": item.get("type"),
                "displayName": item.get("displayName"),
                "workspaceId": workspace_id,
            },
        )

        response = get_upstream(client, workspace_id, item_id)
        if response is None:
            if first:
                logger.info("Relations API unavailable; skipping dependency analysis")
                graph.available = False
                return graph
            continue
        first = False

        # Downstream is best effort on top: losing it costs us write destinations, not the
        # whole analysis, so it must not take the run down with it.
        downstream = get_downstream(client, workspace_id, item_id)
        for payload in (response, downstream):
            if payload:
                _absorb(graph, payload)

    return graph


def _absorb(graph: DependencyGraph, response: Mapping[str, Any]) -> None:
    for related in response.get("items") or []:
        if related.get("id"):
            graph.items.setdefault(related["id"], dict(related))
    for workspace in response.get("workspaces") or []:
        if workspace.get("id"):
            graph.workspaces[workspace["id"]] = workspace.get("displayName") or workspace["id"]

    for edge in response.get("relations") or []:
        if edge.get("relationType") in IGNORED_RELATION_TYPES:
            continue
        source = edge.get("itemId")
        dependency = edge.get("dependentOnItemId")
        if not source or not dependency or source == dependency:
            continue
        graph.dependencies.setdefault(source, set()).add(dependency)


def analyse(
    graph: DependencyGraph,
    *,
    migrated_ids: set[str],
    source_workspace_id: str,
) -> list[DependencyIssue]:
    """Report dependencies that will not survive the move."""
    if not graph.available:
        return []

    issues: list[DependencyIssue] = []

    for item_id in sorted(migrated_ids):
        # Workspace monitoring items are turned on as a feature rather than created, so
        # nothing about them is actionable here; they get their own single warning.
        if graph.is_monitoring(item_id):
            continue

        for dependency_id in sorted(graph.dependencies.get(item_id, set())):
            issue = _issue(
                graph,
                item_id=item_id,
                other_id=dependency_id,
                migrated_ids=migrated_ids,
                source_workspace_id=source_workspace_id,
                reading=True,
            )
            if issue:
                issues.append(issue)

        # And what it feeds. A Copy Job or pipeline writing into a lakehouse that is not
        # coming across, or that lives in another workspace, is just as broken as one whose
        # source is missing, and it never shows up as something the item depends on.
        for dependent_id in sorted(graph.dependents_of(item_id)):
            if dependent_id in migrated_ids:
                continue
            issue = _issue(
                graph,
                item_id=item_id,
                other_id=dependent_id,
                migrated_ids=migrated_ids,
                source_workspace_id=source_workspace_id,
                reading=False,
            )
            if issue:
                issues.append(issue)

    return issues


def _issue(
    graph: DependencyGraph,
    *,
    item_id: str,
    other_id: str,
    migrated_ids: set[str],
    source_workspace_id: str,
    reading: bool,
) -> DependencyIssue | None:
    # A SQL analytics endpoint is created with its lakehouse, warehouse, or mirrored
    # database, so it arrives on its own and never needs reporting.
    if is_derived_type(graph.raw_type_of(other_id)) or graph.is_monitoring(other_id):
        return None

    other_workspace = graph.workspace_of(other_id)
    if other_workspace and other_workspace != source_workspace_id:
        where = "keep reading from" if reading else "keep writing into"
        reason = (
            f"lives in workspace '{graph.workspace_name(other_workspace)}'. That workspace is "
            f"not part of this migration, so the copy will {where} the original region"
        )
    elif other_id not in migrated_ids:
        reason = (
            "is not migrated, so the copy will be missing its source"
            if reading
            else "is not migrated, so the copy has nowhere to write"
        )
    else:
        return None

    return DependencyIssue(
        item=graph.name_of(item_id),
        item_type=graph.type_of(item_id),
        dependency=graph.name_of(other_id),
        dependency_type=graph.type_of(other_id),
        reason=reason,
        reading=reading,
    )


def topological_order(
    item_ids: list[str],
    graph: DependencyGraph,
) -> list[str]:
    """Order items so each one follows anything it depends on.

    Only dependencies within ``item_ids`` constrain the result. Cycles cannot be ordered, so
    the members are emitted in their original order rather than dropped.
    """
    remaining = list(item_ids)
    within = set(item_ids)
    ordered: list[str] = []
    placed: set[str] = set()

    while remaining:
        ready = [
            item_id
            for item_id in remaining
            if (graph.dependencies.get(item_id, set()) & within) <= placed
        ]
        if not ready:
            # A cycle, or a dependency that cannot be satisfied. Keep the caller's order.
            ordered.extend(remaining)
            break
        ordered.extend(ready)
        placed.update(ready)
        remaining = [item_id for item_id in remaining if item_id not in placed]

    return ordered


__all__ = [
    "IGNORED_RELATION_TYPES",
    "DependencyGraph",
    "DependencyIssue",
    "analyse",
    "build_graph",
    "get_downstream",
    "get_upstream",
    "topological_order",
]
