"""Tenant-wide advisory scan for connections that still point at the source workspace.

A migrated item's own connection references are handled elsewhere (see
:mod:`fabshuffle.fabric.connections`): only a mapping the operator supplied is ever rewritten,
and nothing is created, adopted by name, or deleted. This module answers a different, narrower
question - reporting only, never a gate on migration: of every connection this credential can
see anywhere in the tenant, which ones still name the source workspace, one of its items, or one
of its data-store endpoints? Those are exactly the connections something *outside* this
migration - a pipeline in another workspace, a Power Automate flow, a report nobody remembered -
might still be reading through once the source workspace is gone.

SQL paths require an exact server AND database pair from source data-store metadata. Even a
GUID-shaped database name is not globally unique across SQL servers. Other paths use literal
workspace/item identifiers and complete OneLake or non-SQL endpoint references.

Nothing here talks the caller into believing more than it can measure. Fabric connections
cannot have their target changed through the API, so this never promises to repoint one; it
names the connection and lets the operator recreate or repoint it by hand, using
:func:`fabshuffle.orchestrator.connections_lookup_script` to learn what each id actually is.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from fabshuffle.auth import AuthError
from fabshuffle.fabric import connections as connections_module
from fabshuffle.fabric import cosmosdb, data_stores, eventhouses, migration_refs, sqldatabases
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError
from fabshuffle.fabric.definitions import build_rewriter
from fabshuffle.fabric.items import list_items
from fabshuffle.lifecycle import safe_text

# Scan-level states. ``stale`` and ``not_applicable`` are only ever assigned when a persisted
# result is read back for a report; a fresh scan only ever produces ``complete`` or
# ``incomplete``.
SCAN_COMPLETE = "complete"
SCAN_INCOMPLETE = "incomplete"
SCAN_NOT_APPLICABLE = "not_applicable"
SCAN_UNKNOWN = "unknown"
SCAN_STALE = "stale"
MATCHER_VERSION = 2

_SCAN_ACTION = (
    "Review the reported service error and the source principal's connection access in "
    "Manage connections and gateways. Run the lookup script with your user account to inspect "
    "connections you can access before retiring the source."
)

#: Attached to every cutover report section this module produces, regardless of scan state.
#: Read alongside the section's own state and message, not instead of them: a state of
#: ``unknown`` or ``incomplete`` already says a scan was not done or not finished, but an
#: operator skimming straight to "reviewed" connections could still mistake a *complete* scan
#: for an exhaustive one, which it is not.
LIMITS = [
    "Scope is limited by what this credential could see when the scan ran, and by literal "
    "metadata matching: SQL requires the complete server/database pair; other paths use "
    "workspace/item GUIDs, complete OneLake roots or non-SQL endpoints. It does not read what "
    "an item's definition "
    "does with a connection, only whether the connection's own path names something from the "
    "source. A connection that reaches the source workspace some other way is not detected.",
    "Not proven exhaustive. A connection this credential cannot list at all is not in the "
    "result either way; see the scan state before treating an empty match list as clean.",
    "A missing, unread, or stale scan (state unknown, incomplete, or stale) is not evidence "
    "that no connection needs review - only that this was not checked, or not checked since "
    "the mapping last changed. Treat it the same as a report with no evidence at all.",
    "PersonalCloud is not proof of default-semantic-model ownership. Automatic denotes an "
    "implicit/SSO binding. A matching target does not establish which consumers still use "
    "the connection, or whether it needs manual recreation.",
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _boundary_pattern(token: str) -> re.Pattern[str]:
    """Case-insensitive match for ``token`` that is not itself part of a longer run of
    identifier characters, so a GUID or endpoint cannot be "found" as a coincidental
    substring of something unrelated."""
    return re.compile(rf"(?<![0-9A-Za-z_.-]){re.escape(token)}(?![0-9A-Za-z_.-])", re.IGNORECASE)


def _error_detail(error: FabricError | AuthError) -> str:
    if isinstance(error, FabricApiError) and (error.error_code or error.detail):
        return safe_text(f"{error.error_code}: {error.detail}")
    return safe_text(str(error))


def redact_path(path: str) -> str:
    """Strip anything in a connection path that reads as a credential or a short-lived grant.

    A connection's ``path`` is meant to hold only an endpoint, container, or catalog name; a
    query string is never needed to recognise one of those, and it is exactly where a SAS
    token or an API key would ride along, so it is dropped outright rather than parsed field
    by field. Basic-auth-style ``user:pass@`` userinfo, seen occasionally on Web connections,
    is dropped the same way. ``safe_text`` catches anything else that reads as a credential.
    """
    if not path:
        return path
    path = str(path).split("?", 1)[0]
    path = re.sub(r"(?i)://[^/@\s]+@", "://", path)
    return safe_text(path)


@dataclass(frozen=True, slots=True)
class _Signature:
    """One known-source identifier, ready to be searched for in a connection's path.

    ``owner_item_id`` is empty only for the bare source workspace GUID itself; every other
    signature (an item id, a OneLake root, a SQL/Kusto endpoint) is attributed to the specific
    item it was built from, so a match can say which source item a connection appears to read.
    """

    token: str
    owner_item_id: str
    pattern: re.Pattern[str] = field(compare=False)


def _signature(token: str, owner_item_id: str) -> _Signature:
    return _Signature(token, owner_item_id, _boundary_pattern(token))


def _item_signatures(
    client: FabricClient, *, source_workspace_id: str, source_workspace_name: str,
    check_cancel: Callable[[], None] | None = None,
) -> tuple[list[_Signature], dict[connections_module.SqlTarget, set[str]], list[str]]:
    """Every boundary-matchable identifier the source workspace and its items are known by.

    Read independently of the migration's own bookkeeping (``ctx.id_map``/``ctx.source_items``)
    so this scan reflects the source workspace as it stands *now*, including items that were
    never migrated at all - a connection can still point at one of those. A read that fails is
    recorded as a limitation and skipped, rather than aborting the whole scan: this is an
    advisory report, not a precondition for anything, so a partial answer is better than none.
    """
    signatures = [_signature(source_workspace_id, "")]
    sql_targets: dict[connections_module.SqlTarget, set[str]] = {}
    warnings: list[str] = []
    try:
        source_items = list_items(client, source_workspace_id)
    except (FabricError, AuthError) as error:
        warnings.append(f"the workspace's item list ({_error_detail(error)})")
        source_items = []
    for item in source_items:
        item_id = str(item.get("id") or "")
        if not item_id:
            continue
        signatures.append(_signature(item_id, item_id))
        for root in migration_refs.onelake_aliases(source_workspace_id, source_workspace_name, item):
            signatures.append(_signature(root, item_id))

    for label, item_type, lister in (
        (
            "lakehouses", "Lakehouse", data_stores.list_lakehouses,
        ),
        ("warehouses", "Warehouse", data_stores.list_warehouses),
        ("SQL databases", "SQLDatabase", sqldatabases.list_sql_databases),
        (
            "mirrored databases", "MirroredDatabase", data_stores.list_mirrored_databases,
        ),
        ("Cosmos DB databases", "CosmosDBDatabase", cosmosdb.list_cosmos_databases),
    ):
        if check_cancel:
            check_cancel()
        try:
            items = lister(client, source_workspace_id)
        except (FabricError, AuthError) as error:
            warnings.append(f"{label} ({_error_detail(error)})")
            continue
        for entry in items:
            owner = str(entry.get("id") or "")
            pairs = connections_module.item_sql_targets({**entry, "type": item_type})
            for pair in pairs:
                sql_targets.setdefault(pair, set()).add(owner)
            if item_type != "CosmosDBDatabase" and not pairs:
                warnings.append(
                    f"{label} '{safe_text(str(entry.get('displayName') or owner))}' "
                    "(SQL coordinates not returned)"
                )
            if item_type == "CosmosDBDatabase" and (endpoint := cosmosdb.endpoint_url(entry)):
                signatures.append(_signature(str(endpoint), owner))

    if check_cancel:
        check_cancel()
    try:
        for eventhouse in eventhouses.list_eventhouses(client, source_workspace_id):
            properties = eventhouse.get("properties") or {}
            owner = str(eventhouse.get("id") or "")
            for key in ("queryServiceUri", "ingestionServiceUri"):
                if properties.get(key):
                    signatures.append(_signature(str(properties[key]), owner))
    except (FabricError, AuthError) as error:
        warnings.append(f"eventhouses ({_error_detail(error)})")

    return signatures, sql_targets, warnings


@dataclass(frozen=True, slots=True)
class ConnectionAdvisory:
    """One tenant-visible connection whose path appears to reference the source workspace."""

    connection_id: str
    connection_name: str = ""
    connectivity_type: str = ""
    connection_type: str = ""
    path: str = ""
    matched_source_items: tuple[str, ...] = ()
    expected_new_path: str = ""
    match_basis: str = "literal_reference"

    def as_dict(self) -> dict[str, Any]:
        if self.connectivity_type == "Automatic":
            action = (
                "Implicit/SSO connection. Check the consuming semantic model's data source "
                "and SSO settings against the destination; do not treat this as a shared "
                "connection that must be recreated."
            )
        elif self.connectivity_type == "PersonalCloud":
            action = (
                "Personal cloud connection; it may be used by semantic models. Review their "
                "Gateway and cloud connections settings and destination data sources. This "
                "does not prove default-model ownership or that manual recreation is needed."
            )
        else:
            action = (
                "Review current consumers before cutover. If they must move, configure a "
                "destination connection and update their bindings manually. No connection "
                "is changed by this report."
            )
        result = {
            "connectionId": self.connection_id,
            "connectionName": self.connection_name,
            "connectivityType": self.connectivity_type,
            "type": self.connection_type,
            "path": self.path,
            "matchedSourceItems": list(self.matched_source_items),
            "matchBasis": self.match_basis,
            "usageState": "not_checked",
            "action": action,
        }
        if self.expected_new_path:
            result["expectedNewPath"] = self.expected_new_path
        return result


@dataclass(frozen=True, slots=True)
class ConnectionAdvisoryScan:
    """The result of one scan attempt, ready to persist and to render."""

    source_workspace_id: str
    target_workspace_id: str = ""
    generated_at: str = ""
    attempt_id: str = ""
    scan_state: str = SCAN_INCOMPLETE
    connections: tuple[ConnectionAdvisory, ...] = ()
    message: str = ""
    action: str = ""
    error_code: str = ""

    def as_dict(self) -> dict[str, Any]:
        result = {
            "matcherVersion": MATCHER_VERSION,
            "sourceWorkspaceId": self.source_workspace_id,
            "targetWorkspaceId": self.target_workspace_id,
            "generatedAt": self.generated_at,
            "attemptId": self.attempt_id,
            "scanState": self.scan_state,
            "connections": [entry.as_dict() for entry in self.connections],
            "message": self.message,
        }
        if self.action:
            result["action"] = self.action
        if self.error_code:
            result["errorCode"] = self.error_code
        return result


def scan_source_connections(
    client: FabricClient,
    *,
    source_workspace_id: str,
    source_workspace_name: str = "",
    target_workspace_id: str = "",
    attempt_id: str = "",
    id_map: Mapping[str, str] | None = None,
    check_cancel: Callable[[], None] | None = None,
) -> ConnectionAdvisoryScan:
    """Scan every tenant-visible connection for a path that still names the source workspace.

    Reporting only: nothing is created, adopted by name, deleted, or granted, and the result
    never blocks or gates item migration. ``id_map`` is used only to preview what a matched
    connection's path would become if it were repointed by hand - Fabric does not let a
    connection's target be changed through the API - and only when the map already resolves
    something in that path; otherwise the preview is left empty rather than guessed at.
    """
    generated_at = _now()
    if check_cancel:
        check_cancel()
    try:
        raw_connections = client.list_all("connections")
    except (FabricError, AuthError) as error:
        message = "The tenant's connections could not be listed, so this scan cannot say " \
            "whether any of them still point at the source workspace."
        detail = _error_detail(error)
        if detail:
            message = f"{message} {detail}"
        return ConnectionAdvisoryScan(
            source_workspace_id=source_workspace_id, target_workspace_id=target_workspace_id,
            generated_at=generated_at, attempt_id=attempt_id, scan_state=SCAN_INCOMPLETE,
            message=message, action=_SCAN_ACTION,
            error_code=error.error_code if isinstance(error, FabricApiError) else "",
        )

    if check_cancel:
        check_cancel()
    signatures, sql_targets, signature_warnings = _item_signatures(
        client, source_workspace_id=source_workspace_id, source_workspace_name=source_workspace_name,
        check_cancel=check_cancel,
    )
    rewrite = build_rewriter(dict(id_map or {}))
    sql_servers = {target.server for target in sql_targets}

    advisories: list[ConnectionAdvisory] = []
    for candidate in raw_connections:
        if check_cancel:
            check_cancel()
        connection_id = str(candidate.get("id") or "")
        if not connection_id:
            continue
        details = candidate.get("connectionDetails") or {}
        original_path = str(details.get("path") or "")
        raw_path = redact_path(original_path)
        sql = str(details.get("type") or "").casefold() == "sql"
        # Also prevent an unrecognised connector carrying a SQL-style path from bypassing
        # the pair check through the generic GUID matcher.
        sql = sql or (
            ";" in original_path and any(
                domain in original_path.split(";", 1)[0].casefold()
                for domain in (".datawarehouse.fabric.microsoft.com", ".database.windows.net")
            )
        )
        expected_new_path = ""
        if sql:
            target = connections_module.sql_path_target(original_path)
            if target is None:
                signature_warnings.append(
                    f"connection {connection_id} (SQL server/database path could not be verified)"
                )
                continue
            matched_items = sorted(sql_targets.get(target, set()))
            if not matched_items:
                if target.server in sql_servers:
                    signature_warnings.append(
                        f"connection {connection_id} (SQL server matches, but the database "
                        "could not be matched to a source item)"
                    )
                continue
            destination = connections_module.mapped_sql_target(target, id_map or {})
            if destination is not None and destination.server not in sql_servers:
                expected_new_path = redact_path(f"{destination.server};{destination.database}")
        else:
            if not raw_path:
                continue
            matched_items = sorted({
                signature.owner_item_id for signature in signatures
                if signature.owner_item_id and signature.pattern.search(raw_path)
            })
            workspace_matched = any(
                not signature.owner_item_id and signature.pattern.search(raw_path)
                for signature in signatures
            )
            if not matched_items and not workspace_matched:
                continue

        if not sql and rewrite is not None:
            candidate_new_path = rewrite(raw_path)
            # A partial rewrite is not a usable destination. Do not suggest a path that
            # still names any known source resource, even if the workspace ID changed.
            if candidate_new_path != raw_path and not any(
                signature.pattern.search(candidate_new_path) for signature in signatures
            ):
                expected_new_path = candidate_new_path

        advisories.append(ConnectionAdvisory(
            connection_id=connection_id,
            connection_name=safe_text(connections_module.display_name(candidate)),
            connectivity_type=safe_text(str(candidate.get("connectivityType") or "")),
            connection_type=safe_text(str(details.get("type") or "")),
            path=raw_path,
            matched_source_items=tuple(matched_items),
            expected_new_path=expected_new_path,
            match_basis="sql_server_database" if sql else "literal_reference",
        ))

    advisories.sort(key=lambda entry: (entry.connection_name.casefold(), entry.connection_id))

    scan_state = SCAN_INCOMPLETE if signature_warnings else SCAN_COMPLETE
    if signature_warnings:
        message = (
            "This scan could not read everything it uses to recognise a source reference, so "
            "it may miss some connections: " + "; ".join(signature_warnings) + "."
        )
    elif advisories:
        message = (
            f"{len(advisories)} tenant-visible connection(s) still reference the source "
            "workspace, one of its items, or one of its endpoints."
        )
    else:
        message = "No tenant-visible connection's path was found to reference the source workspace."

    return ConnectionAdvisoryScan(
        source_workspace_id=source_workspace_id, target_workspace_id=target_workspace_id,
        generated_at=generated_at, attempt_id=attempt_id, scan_state=scan_state,
        connections=tuple(advisories), message=message,
        action=_SCAN_ACTION if signature_warnings else "",
    )


def section_for_report(
    payload: Mapping[str, Any] | None, *, run_id: str, strategy: str,
) -> dict[str, Any]:
    """Shape a persisted (or missing) scan for the cutover report.

    Kept independent of the item-lifecycle counts in the rest of the report: this section has
    its own state, and an empty or unread scan is reported as ``unknown`` rather than folded
    into a falsely reassuring "ready". By the same reasoning, a nonempty match list here does
    not get mixed into the report's overall ``state``/``counts`` either: those describe
    recorded migration work, and this describes something the migration explicitly did not
    do anything about. Conflating the two would make a run that migrated everything correctly
    look unfinished because of a connection nothing in this migration ever touched.
    """
    if strategy == "reassign":
        return {
            "scanState": SCAN_NOT_APPLICABLE, "connections": [],
            "sourceWorkspaceId": "", "targetWorkspaceId": "", "generatedAt": "",
            "message": (
                "Reassignment moves the existing workspace to the new capacity; its connections "
                "keep the same ids and paths, so nothing needs to be repointed."
            ),
            "limits": list(LIMITS),
        }
    if not payload:
        return {
            "scanState": SCAN_UNKNOWN, "connections": [],
            "sourceWorkspaceId": "", "targetWorkspaceId": "", "generatedAt": "",
            "message": (
                "No connection scan has been recorded for this run yet. It runs late in the "
                "migration, once every item that could be repointed has a destination. Unknown "
                "is not a clean result - it means this has not been checked at all."
            ),
            "limits": list(LIMITS),
        }
    if payload.get("matcherVersion") != MATCHER_VERSION:
        return {
            "scanState": SCAN_STALE, "connections": [],
            "sourceWorkspaceId": payload.get("sourceWorkspaceId", ""),
            "targetWorkspaceId": payload.get("targetWorkspaceId", ""),
            "generatedAt": payload.get("generatedAt", ""),
            "message": (
                "This snapshot used the retired identifier-only matcher. Its connection "
                "matches and suggested destination paths have been withheld, not revalidated."
            ),
            "action": (
                "Use the lookup script to identify connections, then inspect their server/database "
                "pairs in Fabric before cutover. A new migration attempt records a new scan; "
                "there is no need "
                "to restart a migration just to look up connection names."
            ),
            "limits": list(LIMITS),
        }
    section = {**payload, "connections": [dict(entry) for entry in payload.get("connections") or []]}
    attempt_id = section.pop("attemptId", "")
    if attempt_id and attempt_id != run_id:
        section["scanState"] = SCAN_STALE
        note = "This scan is from an earlier attempt and has not been refreshed by this one."
        section["message"] = f"{note} {section.get('message', '')}".strip()
    section["limits"] = list(LIMITS)
    return section


__all__ = [
    "LIMITS",
    "SCAN_COMPLETE",
    "SCAN_INCOMPLETE",
    "SCAN_NOT_APPLICABLE",
    "SCAN_STALE",
    "SCAN_UNKNOWN",
    "ConnectionAdvisory",
    "ConnectionAdvisoryScan",
    "redact_path",
    "scan_source_connections",
    "section_for_report",
]
