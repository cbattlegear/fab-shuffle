"""Connection references inside item definitions.

Connections are tenant scoped, so the same connection id resolves from the migrated
workspace; the API never returns an existing connection's credentials, so a faithful copy is
never possible. Fab Shuffle never creates, adopts by name, or deletes a connection on the
operator's behalf: it only rewrites an explicit ``connection_mappings`` entry the operator
supplied, and reports the connections that still need one.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.definitions import GUID_PATTERN, decode_payload, is_text_part

logger = logging.getLogger(__name__)

_GUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)

# The keys different item types use to bind a connection.
CONNECTION_KEYS = ("connection", "connectionId", "dataConnectionId")

# Connectivity types that are bound to a gateway rather than reachable from anywhere. A
# virtual network gateway in particular is provisioned into one Azure region.
GATEWAY_TYPES = frozenset(
    {
        "OnPremisesGateway",
        "OnPremisesGatewayPersonal",
        "VirtualNetworkGateway",
        "StreamingVirtualNetworkGateway",
    }
)
REGIONAL_GATEWAY_TYPES = frozenset({"VirtualNetworkGateway", "StreamingVirtualNetworkGateway"})


@dataclass(frozen=True, slots=True)
class ConnectionIssue:
    item: str
    connection_id: str
    connection_name: str
    reason: str

    def message(self) -> str:
        name = f"'{self.connection_name}'" if self.connection_name else self.connection_id
        return f"{self.item} uses connection {name}, which {self.reason}."

    def as_dict(self) -> dict[str, str]:
        return {
            "item": self.item,
            "connectionId": self.connection_id,
            "connectionName": self.connection_name,
            "reason": self.reason,
        }


def list_connections(client: FabricClient) -> list[dict[str, Any]]:
    try:
        return client.list_all("connections")
    except FabricApiError as error:
        if error.status_code in (401, 403):
            logger.info("Service principal cannot list connections: %s", error)
            return []
        raise


def connections_by_id(client: FabricClient) -> dict[str, dict[str, Any]]:
    return {c["id"]: c for c in list_connections(client) if c.get("id")}


def _walk(node: Any, found: set[str]) -> None:
    """Collect connection ids from a parsed definition.

    Item types spell the binding differently: pipelines and Copy Jobs use
    ``externalReferences.connection``, eventstreams use ``dataConnectionId``, mirrored
    databases use a bare ``connection`` under ``typeProperties``, and shortcut-style payloads
    use ``connectionId``. Only values that actually look like a connection id are taken,
    because a dataflow stores ``connectionId`` as an embedded JSON document instead.
    """
    if isinstance(node, Mapping):
        for key in CONNECTION_KEYS:
            value = node.get(key)
            if isinstance(value, str) and _GUID.match(value.strip()):
                found.add(value.strip())
        for value in node.values():
            _walk(value, found)
    elif isinstance(node, list):
        for value in node:
            _walk(value, found)


def referenced_connection_ids(parts: Iterable[Mapping[str, Any]]) -> set[str]:
    """Every connection id an item's definition binds."""
    import json

    found: set[str] = set()
    for part in parts:
        path = part.get("path", "")
        payload = part.get("payload")
        if not payload or not is_text_part(path):
            continue
        try:
            document = json.loads(decode_payload(payload).decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            continue
        _walk(document, found)
    return found


def check(
    item_name: str,
    connection_ids: Iterable[str],
    known: Mapping[str, Mapping[str, Any]],
    *,
    target_region: str = "",
) -> list[ConnectionIssue]:
    """Report connections that will not work from the migrated item."""
    issues: list[ConnectionIssue] = []

    for connection_id in sorted(connection_ids):
        connection = known.get(connection_id)

        if connection is None:
            issues.append(
                ConnectionIssue(
                    item=item_name,
                    connection_id=connection_id,
                    connection_name="",
                    reason=(
                        "this service principal cannot see. Grant it access to the connection, "
                        "or the migrated item will fail to run"
                    ),
                )
            )
            continue

        name = connection.get("displayName") or ""
        connectivity = connection.get("connectivityType") or ""

        if connectivity == "PersonalCloud":
            issues.append(
                ConnectionIssue(
                    item=item_name,
                    connection_id=connection_id,
                    connection_name=name,
                    reason=(
                        "is a personal cloud connection and cannot be shared, so it will only "
                        "work for the person who created it"
                    ),
                )
            )
        elif connectivity in REGIONAL_GATEWAY_TYPES:
            where = f" in {target_region}" if target_region else ""
            issues.append(
                ConnectionIssue(
                    item=item_name,
                    connection_id=connection_id,
                    connection_name=name,
                    reason=(
                        f"routes through a {connectivity}. That gateway stays in its original "
                        f"region, so check it can still reach the workspace{where}"
                    ),
                )
            )
        elif connectivity in GATEWAY_TYPES:
            issues.append(
                ConnectionIssue(
                    item=item_name,
                    connection_id=connection_id,
                    connection_name=name,
                    reason=(
                        "routes through an on-premises data gateway. Confirm the gateway can "
                        "still reach both the data source and the new workspace"
                    ),
                )
            )

    return issues


# ------------------------------------------------- workspace bound connections


def list_role_assignments(client: FabricClient, connection_id: str) -> list[dict[str, Any]] | None:
    """Role assignments on a connection, or ``None`` when the caller may not read them."""
    try:
        return client.list_all(f"connections/{connection_id}/roleAssignments")
    except FabricApiError as error:
        if error.status_code in (401, 403, 404):
            return None
        raise


def is_owned_by(assignments: Iterable[Mapping[str, Any]] | None, client_id: str) -> bool:
    """Whether the service principal holds Owner on the connection.

    Owner is the role that allows a connection to be managed; the other two, User and
    UserWithReshare, only allow it to be used.
    """
    if not assignments:
        return False
    for assignment in assignments:
        if assignment.get("role") != "Owner":
            continue
        principal = assignment.get("principal") or {}
        details = principal.get("servicePrincipalDetails") or {}
        if details.get("aadAppId") == client_id or principal.get("id") == client_id:
            return True
    return False


def add_role_assignment(
    client: FabricClient,
    connection_id: str,
    principal: Mapping[str, Any],
    role: str,
) -> dict[str, Any]:
    """Grant one principal a role on a connection."""
    body = {"principal": {"id": principal.get("id"), "type": principal.get("type")}, "role": role}
    return client.post(f"connections/{connection_id}/roleAssignments", json=body)


def copy_role_assignments(
    client: FabricClient,
    *,
    source_connection_id: str,
    target_connection_id: str,
    client_id: str,
) -> tuple[int, list[str]]:
    """Give a replacement connection the same people the original had.

    A replacement is a new connection, so it starts out visible only to whoever created it.
    Everyone who could use the original would otherwise have to be added again by hand, which
    is exactly the manual work the replacement was meant to avoid.

    The service principal's own Owner assignment is not replayed, because creating the
    connection already granted it.
    """
    assignments = list_role_assignments(client, source_connection_id)
    if not assignments:
        return 0, [
            f"The role assignments on connection {source_connection_id} could not be read, so "
            f"the replacement is visible only to this service principal. Share it with whoever "
            "used the original."
        ]

    copied = 0
    failed: list[str] = []
    for assignment in assignments:
        principal = assignment.get("principal") or {}
        principal_id = principal.get("id")
        role = assignment.get("role")
        if not principal_id or not role:
            continue

        details = principal.get("servicePrincipalDetails") or {}
        if details.get("aadAppId") == client_id or principal_id == client_id:
            # Creating the connection already made us its owner.
            continue

        try:
            add_role_assignment(client, target_connection_id, principal, role)
            copied += 1
        except FabricApiError as error:
            name = principal.get("displayName") or principal_id
            failed.append(f"{name} ({role}, {error})")

    warnings: list[str] = []
    if failed:
        warnings.append(
            "These principals could not be given the same access to the replacement connection "
            "as they had on the original: " + ", ".join(failed) + ". Share it with them by hand."
        )
    return copied, warnings


def display_name(connection: Mapping[str, Any]) -> str:
    """A connection's name for messages, falling back to its id when unnamed.

    An operator-created connection can be left unnamed, and Fabric does not always
    populate ``displayName`` on every connection it returns. Quoting the empty result
    verbatim reads as ``Connection 'None'`` and gives the operator nothing to act on;
    the id at least identifies which connection needs attention.
    """
    return str(connection.get("displayName") or connection.get("id") or "")


def same_path(left: str, right: str) -> bool:
    """Only GUID casing is immaterial; paths may contain case-sensitive components."""
    return GUID_PATTERN.sub(lambda m: m.group().lower(), left) == GUID_PATTERN.sub(
        lambda m: m.group().lower(), right
    )


def matches_replacement(
    candidate: Mapping[str, Any],
    source: Mapping[str, Any],
    new_path: str,
) -> bool:
    details = candidate.get("connectionDetails") or {}
    return bool(
        candidate.get("id")
        and candidate["id"].casefold() != str(source.get("id") or "").casefold()
        and candidate.get("connectivityType") == source.get("connectivityType")
        and details.get("type") == (source.get("connectionDetails") or {}).get("type")
        and same_path(details.get("path") or "", new_path)
    )


__all__ = [
    "GATEWAY_TYPES",
    "REGIONAL_GATEWAY_TYPES",
    "ConnectionIssue",
    "check",
    "connections_by_id",
    "display_name",
    "is_owned_by",
    "list_connections",
    "list_role_assignments",
    "referenced_connection_ids",
]
