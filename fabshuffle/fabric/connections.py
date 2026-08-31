"""Connection references inside item definitions.

Connections are tenant scoped: the same connection id resolves from any workspace, in any
region, so a migrated pipeline or Copy Job keeps working without being repointed. They also
cannot be recreated faithfully, because the API never returns credentials, only the
credential *type*.

So Fab Shuffle does not copy connections. It checks the ones a migrated item references and
reports the cases that will not work in the new workspace.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.definitions import decode_payload, is_text_part

logger = logging.getLogger(__name__)

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
            logger.info("Service principal cannot list connections: HTTP %s", error.status_code)
            return []
        raise


def connections_by_id(client: FabricClient) -> dict[str, dict[str, Any]]:
    return {c["id"]: c for c in list_connections(client) if c.get("id")}


def _walk(node: Any, found: set[str]) -> None:
    """Collect connection ids from a parsed definition.

    Pipelines and Copy Jobs both bind a connection through
    ``"externalReferences": {"connection": "<guid>"}``; shortcut-style payloads use a
    ``connectionId`` key instead.
    """
    if isinstance(node, Mapping):
        external = node.get("externalReferences")
        if isinstance(external, Mapping) and isinstance(external.get("connection"), str):
            found.add(external["connection"])
        if isinstance(node.get("connectionId"), str):
            found.add(node["connectionId"])
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


__all__ = [
    "GATEWAY_TYPES",
    "REGIONAL_GATEWAY_TYPES",
    "ConnectionIssue",
    "check",
    "connections_by_id",
    "list_connections",
    "referenced_connection_ids",
]
