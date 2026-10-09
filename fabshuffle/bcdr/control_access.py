"""Control workspace membership is advisory; controller access remains required."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from fabshuffle.bcdr.bootstrap import BootstrapError
from fabshuffle.bcdr.contracts import Principal, WorkspaceGrant
from fabshuffle.lifecycle import safe_text


def control_workspace_warnings(
    assignments: Sequence[Mapping[str, Any]], recovery_spn: Principal, owners: Sequence[WorkspaceGrant],
) -> tuple[str, ...]:
    expected = {(grant.principal.object_id, grant.principal.kind): grant.role for grant in owners}
    controller = (recovery_spn.object_id, recovery_spn.kind)
    expected[controller] = "Admin"
    observed: dict[tuple[str, str], list[str]] = {}
    warnings = []
    for assignment in assignments:
        raw = assignment.get("principal")
        role = assignment.get("role")
        if not isinstance(raw, Mapping) or not isinstance(role, str) or not raw.get("type"):
            raise BootstrapError("Read a complete control workspace access list before continuing.")
        try:
            key = (str(UUID(str(raw.get("id", "")))), str(raw["type"]))
        except ValueError as error:
            raise BootstrapError("Control workspace access has an invalid principal identity.") from error
        observed.setdefault(key, []).append(role)
        if key == controller:
            continue
        label = safe_text(str(raw.get("displayName") or f"{key[1]} {key[0]}"))
        if key not in expected:
            warnings.append(
                f"Control workspace: {label} has {role} access and is not a designated owner. "
                "Review this additional access in Fabric Manage access; the grant was left unchanged."
            )
        elif role != expected[key]:
            warnings.append(
                f"Control workspace: {label} has {role} access, not the configured {expected[key]} role. "
                "Review the role in Fabric Manage access; the grant was left unchanged."
            )
    if observed.get(controller) != ["Admin"]:
        raise BootstrapError(
            "Grant the recovery service principal Admin access on the control workspace, then retry. "
            "Its required Admin role is missing or ambiguous; no access was changed."
        )
    for key, role in expected.items():
        if key != controller and key not in observed:
            warnings.append(
                f"Control workspace: designated {key[1]} {key[0]} has no recorded {role} grant. "
                "Review the owner list or grant access in Fabric Manage access if intended; "
                "no access was added."
            )
    return tuple(dict.fromkeys(safe_text(warning) for warning in warnings))
