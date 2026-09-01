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
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.definitions import decode_payload, is_text_part

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


@dataclass(frozen=True, slots=True)
class ConnectionPrerequisite:
    """A connection that points into the workspace being migrated.

    Its target cannot be repointed: no Update Connection request accepts
    ``connectionDetails``, so the path is fixed for the life of the connection. It has to be
    replaced by a new connection aimed at the migrated item, and doing that needs the
    credentials, which the API never returns.
    """

    connection_id: str
    connection_name: str
    path: str
    matched: str
    credential_type: str
    connectivity_type: str
    manageable: bool

    def message(self) -> str:
        access = (
            ""
            if self.manageable
            else (
                " The service principal also has no Owner role on it, so grant that first if "
                "you want it managed programmatically."
            )
        )
        return (
            f"Connection '{self.connection_name}' points at '{self.matched}' in the source "
            f"workspace (path '{self.path}'). Fabric does not allow a connection's target to "
            f"be changed, so create a replacement {self.credential_type} connection against "
            f"the migrated item and repoint the items that use it.{access}"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "connectionId": self.connection_id,
            "connectionName": self.connection_name,
            "path": self.path,
            "matched": self.matched,
            "credentialType": self.credential_type,
            "connectivityType": self.connectivity_type,
            "manageable": self.manageable,
            "message": self.message(),
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


def source_identifiers(
    workspace_id: str,
    items: Iterable[Mapping[str, Any]],
    endpoints: Iterable[str] = (),
) -> dict[str, str]:
    """Strings that, if they appear in a connection path, mean it points into this workspace.

    Maps the identifier to a human readable name for it.
    """
    identifiers: dict[str, str] = {workspace_id: "this workspace"}
    for item in items:
        item_id = item.get("id")
        if item_id:
            identifiers[item_id] = f"{item.get('type') or 'item'} '{item.get('displayName')}'"
    for endpoint in endpoints:
        if endpoint:
            identifiers[endpoint] = endpoint
    return identifiers


def scan_prerequisites(
    client: FabricClient,
    *,
    identifiers: Mapping[str, str],
    client_id: str,
    known: Iterable[Mapping[str, Any]] | None = None,
) -> list[ConnectionPrerequisite]:
    """Find connections that point into the workspace being migrated.

    These are the ones a migration cannot fix by itself, so they are surfaced before anything
    is created, together with what has to change.
    """
    candidates = list(known) if known is not None else list_connections(client)
    found: list[ConnectionPrerequisite] = []

    for connection in candidates:
        details = connection.get("connectionDetails") or {}
        path = details.get("path") or ""
        if not path:
            continue

        lowered = path.casefold()
        matched = next(
            (label for key, label in identifiers.items() if key and key.casefold() in lowered),
            None,
        )
        if matched is None:
            continue

        connection_id = connection.get("id") or ""
        credentials = connection.get("credentialDetails") or {}
        found.append(
            ConnectionPrerequisite(
                connection_id=connection_id,
                connection_name=connection.get("displayName") or connection_id,
                path=path,
                matched=matched,
                credential_type=credentials.get("credentialType") or "unknown",
                connectivity_type=connection.get("connectivityType") or "",
                manageable=is_owned_by(list_role_assignments(client, connection_id), client_id),
            )
        )

    return found


# Credential types Fab Shuffle can set on a new connection without being handed a secret.
# Everything else (Basic, Key, ServicePrincipal, OAuth2, ...) needs input from the operator,
# because the API never returns an existing connection's credentials.
NO_SECRET_CREDENTIALS = frozenset({"WorkspaceIdentity", "Anonymous"})

# Only cloud connections can be recreated unattended. Gateway-bound ones need a gateway id
# and stay tied to their gateway's region anyway.
RECREATABLE_CONNECTIVITY = frozenset({"ShareableCloud"})


@dataclass(frozen=True, slots=True)
class Replacement:
    """A connection that was recreated against the migrated items."""

    old_id: str
    new_id: str
    name: str
    old_path: str
    new_path: str


def supported_types(client: FabricClient) -> dict[str, dict[str, Any]]:
    try:
        types = client.list_all("connections/supportedConnectionTypes")
    except FabricApiError as error:
        if error.status_code in (401, 403):
            return {}
        raise
    return {entry["type"]: entry for entry in types if entry.get("type")}


def _creation_method(metadata: Mapping[str, Any], connection_type: str) -> dict[str, Any] | None:
    """Pick the creation method to rebuild a connection with.

    Prefer the one named after the type, which is the recommended method; fall back to the
    only method when there is exactly one. Anything ambiguous is refused rather than guessed.
    """
    methods = metadata.get("creationMethods") or []
    for method in methods:
        if method.get("name") == connection_type:
            return dict(method)
    return dict(methods[0]) if len(methods) == 1 else None


def build_parameters(path: str, method: Mapping[str, Any]) -> list[dict[str, str]] | None:
    """Turn a rendered connection path back into typed creation parameters.

    ``path`` is the creation method's parameter values joined with ``;`` in declaration
    order, so ``contoso.database.windows.net;sales`` maps onto ``server`` then ``database``.
    Returns ``None`` when the two cannot be lined up, rather than sending a malformed payload.
    """
    declared = method.get("parameters") or []
    values = path.split(";")
    if not declared or len(values) > len(declared):
        return None

    parameters: list[dict[str, str]] = []
    for index, parameter in enumerate(declared):
        name = parameter.get("name")
        if not name:
            return None
        value = values[index] if index < len(values) else ""
        if not value:
            if parameter.get("required"):
                return None
            continue
        parameters.append({"name": name, "dataType": parameter.get("dataType") or "Text", "value": value})

    return parameters or None


def can_recreate(
    connection: Mapping[str, Any],
    metadata: Mapping[str, Any] | None,
) -> str | None:
    """Why a connection cannot be recreated unattended, or ``None`` when it can be."""
    credential_type = (connection.get("credentialDetails") or {}).get("credentialType") or ""
    connectivity = connection.get("connectivityType") or ""

    if connectivity not in RECREATABLE_CONNECTIVITY:
        return (
            f"is a {connectivity or 'gateway'} connection, which has to be recreated by hand "
            "against its gateway"
        )
    if credential_type not in NO_SECRET_CREDENTIALS:
        return (
            f"uses {credential_type or 'unknown'} credentials, which Fabric never returns, so "
            "a replacement has to be created with the credentials supplied by hand"
        )
    if metadata is None:
        return "has a connection type this tenant does not report as supported"
    if credential_type not in (metadata.get("supportedCredentialTypes") or []):
        return f"cannot use {credential_type} credentials for its connection type"
    return None


def build_creation_payload(
    connection: Mapping[str, Any],
    new_path: str,
    metadata: Mapping[str, Any],
    *,
    display_name: str,
) -> dict[str, Any] | None:
    connection_type = (connection.get("connectionDetails") or {}).get("type") or ""
    method = _creation_method(metadata, connection_type)
    if not method:
        return None

    parameters = build_parameters(new_path, method)
    if not parameters:
        return None

    credentials = connection.get("credentialDetails") or {}
    return {
        "connectivityType": connection.get("connectivityType"),
        "displayName": display_name,
        "privacyLevel": connection.get("privacyLevel") or "Organizational",
        "connectionDetails": {
            "type": connection_type,
            "creationMethod": method["name"],
            "parameters": parameters,
        },
        "credentialDetails": {
            "singleSignOnType": credentials.get("singleSignOnType") or "None",
            "connectionEncryption": credentials.get("connectionEncryption") or "NotEncrypted",
            "skipTestConnection": bool(credentials.get("skipTestConnection", False)),
            "credentials": {"credentialType": credentials.get("credentialType")},
        },
    }


def create_connection(client: FabricClient, payload: Mapping[str, Any]) -> dict[str, Any]:
    return client.post("connections", json=dict(payload))


__all__ = [
    "GATEWAY_TYPES",
    "NO_SECRET_CREDENTIALS",
    "RECREATABLE_CONNECTIVITY",
    "REGIONAL_GATEWAY_TYPES",
    "ConnectionIssue",
    "ConnectionPrerequisite",
    "Replacement",
    "build_creation_payload",
    "build_parameters",
    "can_recreate",
    "check",
    "connections_by_id",
    "create_connection",
    "is_owned_by",
    "list_connections",
    "list_role_assignments",
    "referenced_connection_ids",
    "scan_prerequisites",
    "source_identifiers",
    "supported_types",
]
