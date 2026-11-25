"""Provision migration-owned SQL Server cloud connections for Copy Jobs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote, quote_plus
from uuid import UUID

from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric.client import FabricApiError, FabricClient, FabricError


def migration_connection_name(lineage: str, source_id: str, target_id: str, role: str) -> str:
    scope = "|".join((lineage, source_id, target_id, role))
    digest = hashlib.sha256(scope.encode()).hexdigest()[:32]
    return f"FabShuffle-SQL-{role}-{digest}"


def matches_endpoint(connection: Mapping[str, Any], server: str, database: str) -> bool:
    details = connection.get("connectionDetails") or {}
    path = details.get("path")
    parts = path.split(";") if isinstance(path, str) else []
    return (
        str(details.get("type") or "").casefold() == "sql" and len(parts) == 2
        and parts[0].strip().casefold().removesuffix(",1433") == server.casefold().removesuffix(",1433")
        and parts[1].strip() == database
    )


def ensure_connection(
    client: FabricClient, principal: ServicePrincipal, *, name: str, server: str, database: str,
    recorded_id: str | None, record: Callable[[str | None], None],
) -> str:
    """Reuse only a journaled connection, or discover it after a lost create response."""
    def verified(connection: Mapping[str, Any]) -> str:
        try:
            connection_id = str(UUID(connection.get("id")))
        except (ValueError, TypeError, AttributeError):
            connection_id = ""
        if (
            not connection_id or (recorded_id and connection_id != recorded_id)
            or connection.get("displayName") != name
            or connection.get("connectivityType") != "ShareableCloud"
            or connection.get("myRole") != "Owner"
            or (connection.get("credentialDetails") or {}).get("credentialType") != "ServicePrincipal"
            or not matches_endpoint(connection, server, database)
        ):
            raise FabricError(
                f"Fabric connection '{name}' does not match the recorded owner, type or SQL endpoint."
            )
        return connection_id

    if recorded_id:
        return verified(client.get(f"connections/{recorded_id}"))
    if recorded_id == "":
        matches = [item for item in client.list_all("connections") if item.get("displayName") == name]
        if len(matches) != 1:
            raise FabricError(
                f"Creation of '{name}' is unresolved. Check Manage connections and gateways before retrying; "
                "Fab Shuffle will not create a duplicate."
            )
        connection_id = verified(matches[0])
        record(connection_id)
        return connection_id
    if any(item.get("displayName") == name for item in client.list_all("connections")):
        raise FabricError(f"Connection '{name}' already exists without a recorded creation intent.")
    record("")
    failure = None
    try:
        created = client.post("connections", json={
            "connectivityType": "ShareableCloud",
            "displayName": name,
            "connectionDetails": {
                "type": "SQL",
                "creationMethod": "SQL",
                "parameters": [
                    {"dataType": "Text", "name": "server", "value": server},
                    {"dataType": "Text", "name": "database", "value": database},
                ],
            },
            "privacyLevel": "Organizational",
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "Encrypted",
                "skipTestConnection": False,
                "credentials": {
                    "credentialType": "ServicePrincipal",
                    "tenantId": principal.tenant_id,
                    "servicePrincipalClientId": principal.client_id,
                    "servicePrincipalSecret": principal.client_secret,
                },
            },
        })
    except FabricApiError as error:
        if error.status_code in (400, 401, 403, 429) and not any(
            word in error.error_code.casefold() for word in ("duplicate", "alreadyexists", "conflict")
        ):
            record(None)
        failure = f"HTTP {error.status_code} {error.error_code}: {error.detail}"
    except FabricError:
        failure = "Fabric did not confirm the create request. Resume to look for the recorded connection."
    if failure is not None:
        if secret := principal.client_secret:
            variants = {secret, quote(secret, safe=""), quote_plus(secret)}
            variants.update(json.dumps(secret, ensure_ascii=ascii_only)[1:-1] for ascii_only in (True, False))
            for value in sorted(variants, key=len, reverse=True):
                failure = failure.replace(value, "[redacted]")
        raise FabricError(f"Could not create SQL Server connection '{name}': {failure}")
    connection_id = verified(created)
    record(connection_id)
    return connection_id
