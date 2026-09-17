"""Authenticated binding for materialized Fabric KQL data, not ingestion health.

Endpoint/display name and region come from destination-side typed Fabric reads:
https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/get-kql-database
https://learn.microsoft.com/rest/api/fabric/core/workspaces/get-workspace
The official map Python tutorial resolves the Kusto database name from displayName:
https://learn.microsoft.com/fabric/real-time-intelligence/map/tutorial-create-real-time-map-python
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Annotated, Self

from pydantic import AfterValidator, Field, model_validator

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.contracts import ItemIdentity, Record
from fabshuffle.bcdr.protection import (
    Cancel,
    DataEndpoint,
    DataIdentity,
    ProtectionAssessment,
    ProtectionError,
    ProtectionLimits,
    evidence_ref,
    operation_cancel,
    require_independent,
)
from fabshuffle.bcdr.protection_kql import KqlProtection, KqlTable, _probe_tables, restore_kql
from fabshuffle.fabric.client import FabricClient
from fabshuffle.fabric.eventhouses import get_kql_database
from fabshuffle.fabric.workspaces import capacity_region, get_workspace
from fabshuffle.transfer.common import StagingBudgetError, check_cancelled


class KqlMaterializedInput(Record):
    """A regional physical-data input identity. Never a URL, function or event subscription."""

    resource_ref: Annotated[str, AfterValidator(evidence_ref)]
    item: ItemIdentity
    tables: Annotated[tuple[KqlTable, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def unique_tables(self) -> Self:
        if len({table.name for table in self.tables}) != len(self.tables):
            raise ValueError("Materialized KQL input table names must be unique.")
        if any(table.minimum_rows <= 0 for table in self.tables):
            raise ValueError("Materialized KQL inputs require positive row evidence, not an empty schema.")
        return self


def validate_materialized_bindings(
    protection: KqlProtection,
    bindings: tuple[KqlMaterializedInput, ...],
) -> None:
    if not protection.tables or any(table.minimum_rows <= 0 for table in protection.tables):
        raise ValueError(
            "Prepared KQL targets need positive row evidence; empty schema alone is not recovered data."
        )
    references = [value.resource_ref for value in bindings]
    expected = [value.resource_ref for value in protection.inputs]
    if (
        len(set(references)) != len(references)
        or len(set(expected)) != len(expected)
        or set(references) != set(expected)
    ):
        raise ValueError(
            "Bind every KQL input reference exactly once to its explicit materialized data identity."
        )
    for binding in bindings:
        source = protection.source.identity
        if (
            binding.item.tenant_id != source.tenant_id
            or binding.item.workspace_id == source.workspace_id
            or binding.item.item_id == source.item_id
        ):
            raise ValueError(
                "KQL materialized inputs must be same-tenant items outside the failed source workspace."
            )


def _resolve_database(
    client: FabricClient,
    identity: DataIdentity,
    protection: KqlProtection,
    cancel: Cancel,
) -> DataEndpoint:
    if identity.tenant_id != protection.source.identity.tenant_id:
        raise ProtectionError("KQL materialized recovery supports same-tenant inputs only.")
    if (
        identity.workspace_id == protection.source.identity.workspace_id
        or identity.item_id == protection.source.identity.item_id
    ):
        raise ProtectionError("Do not resolve a KQL input in the failed source workspace.")
    check_cancelled(cancel)
    workspace = get_workspace(client, identity.workspace_id)
    if str(workspace.get("id", "")).casefold() != identity.workspace_id:
        raise ProtectionError("Fabric returned the wrong workspace for a prepared KQL input.")
    region_value = workspace.get("capacityRegion")
    if not isinstance(region_value, str) or not region_value:
        raise ProtectionError("The prepared KQL workspace has no verifiable capacity region.")
    region = capacity_region({"region": region_value})
    if region == capacity_region({"region": protection.source_region}) or region != capacity_region(
        {"region": protection.target_region}
    ):
        raise ProtectionError(
            "Prepared KQL data must be in its approved recovery region, outside the source region."
        )
    if workspace.get("capacityAssignmentProgress") != "Completed":
        raise ProtectionError("Complete the prepared KQL workspace capacity assignment before recovery.")
    check_cancelled(cancel)
    item = get_kql_database(client, identity.workspace_id, identity.item_id)
    if (
        str(item.get("id", "")).casefold() != identity.item_id
        or str(item.get("workspaceId", "")).casefold() != identity.workspace_id
        or item.get("type") != "KQLDatabase"
    ):
        raise ProtectionError("Fabric returned the wrong prepared KQL database identity/type.")
    properties = item.get("properties") or {}
    if properties.get("databaseType") != "ReadWrite":
        raise ProtectionError(
            "Prepared KQL inputs must be independent databases, not shortcut/follower databases."
        )
    query_uri, name = properties.get("queryServiceUri"), item.get("displayName")
    if not isinstance(query_uri, str) or not isinstance(name, str) or not query_uri or not name:
        raise ProtectionError("The prepared KQL database endpoint/name is not yet available.")
    endpoint = DataEndpoint(identity, query_uri, name)
    require_independent(protection.source, endpoint)
    if endpoint.endpoint == protection.source.endpoint:
        raise ProtectionError("Prepared KQL data must not share the failed primary cluster endpoint.")
    check_cancelled(cancel)
    # Do not use list_shortcuts(), which treats 404 as an empty list. An inaccessible
    # inventory is not proof that a KQL table is independent of source OneLake data.
    shortcuts = client.list_all(f"workspaces/{identity.workspace_id}/items/{identity.item_id}/shortcuts")
    if shortcuts:
        raise ProtectionError(
            "Prepared KQL database contains shortcuts; materialize independent local tables "
            "before using this data-only recovery binding."
        )
    check_cancelled(cancel)
    return endpoint


def restore_materialized_kql(
    *,
    client: FabricClient,
    tokens: TokenProvider,
    protection: KqlProtection,
    bindings: tuple[KqlMaterializedInput, ...],
    target: DataIdentity,
    max_age: timedelta,
    limits: ProtectionLimits,
    cancel: Cancel,
) -> ProtectionAssessment:
    """Authenticate current local data availability without starting any ingestion."""
    validate_materialized_bindings(protection, bindings)
    if protection.continuous:
        return ProtectionAssessment(
            "deferred",
            warnings=(
                "Materialized KQL table checks cannot verify continuous parallel ingestion or external event "
                "sources. Use a workload-specific live-input qualification "
                "before enabling continuous recovery.",
            ),
        )
    if len(protection.tables) + sum(len(value.tables) for value in bindings) > limits.max_manifest_entries:
        raise StagingBudgetError("Materialized KQL input inventory exceeds max_manifest_entries.")
    cancel = operation_cancel(limits, cancel)
    check_cancelled(cancel)
    if client.tenant_id() != protection.source.identity.tenant_id:
        raise ProtectionError("Authenticate the KQL recovery client in the configured recovery tenant.")
    if target != protection.target.identity:
        raise ProtectionError("The prepared KQL data belongs to a different approved target.")
    endpoint = _resolve_database(client, target, protection, cancel)
    if endpoint != protection.target:
        raise ProtectionError(
            "The live KQL target endpoint/database name differs from its pinned descriptor; "
            "capture a reviewed replacement configuration."
        )

    def probe_inputs(_inputs) -> bool:
        for binding in bindings:
            identity = DataIdentity(binding.item.tenant_id, binding.item.workspace_id, binding.item.item_id)
            resolved = _resolve_database(client, identity, protection, cancel)
            _probe_tables(replace(protection, target=resolved, tables=binding.tables), tokens, limits, cancel)
        return True

    assessment = restore_kql(
        protection,
        source=protection.source.identity,
        target=endpoint,
        tokens=tokens,
        probe_inputs=probe_inputs,
        max_age=max_age,
        recovery_capacity_paused=False,
        limits=limits,
        cancel=cancel,
    )
    return replace(
        assessment,
        warnings=(
            *assessment.warnings,
            "Verified current access and minimum row counts for materialized KQL inputs and target. "
            "This does not certify stream offsets, continuous ingestion, "
            "full data equivalence or business cutover.",
        ),
    )
