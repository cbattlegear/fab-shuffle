"""OneLake shortcut operations."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote

from fabshuffle.fabric import analytics
from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.fabric.definitions import identity_key, part
from fabshuffle.lifecycle import EvidenceState, ItemLifecycle

_IDENTITY_TARGET_FIELDS = frozenset({"connectionId", "itemId", "workspaceId"})
logger = logging.getLogger(__name__)


def _shortcut_collection_path(workspace_id: str, item_id: str) -> str:
    return f"workspaces/{workspace_id}/items/{item_id}/shortcuts"


def list_shortcuts(client: FabricClient, workspace_id: str, item_id: str) -> list[dict[str, Any]]:
    try:
        return _list_shortcuts_strict(client, workspace_id, item_id)
    except FabricApiError as error:
        if error.status_code == 404:
            return []
        raise


def _list_shortcuts_strict(client: FabricClient, workspace_id: str, item_id: str) -> list[dict[str, Any]]:
    return client.list_all(_shortcut_collection_path(workspace_id, item_id))


def _get_shortcut_strict(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    parent_path: str,
    name: str,
) -> dict[str, Any]:
    encoded_path = quote(parent_path.strip("/"), safe="/")
    encoded_name = quote(name, safe="")
    suffix = f"{encoded_path}/{encoded_name}" if encoded_path else encoded_name
    return client.get(f"{_shortcut_collection_path(workspace_id, item_id)}/{suffix}")


def table_shortcut_keys(shortcuts: Iterable[Mapping[str, Any]]) -> set[tuple[str | None, str]]:
    """Identify which of a lakehouse's tables are really shortcuts.

    Nothing in the lakehouse tables API distinguishes them: ``TableType`` is only ever
    ``Managed`` or ``External``, so a shortcut to a delta table is reported as ``Managed``
    exactly like a table that lives there. The only way to tell them apart is to ask the
    shortcuts API and match on name, which is what this returns keys for.

    That matters because a shortcut's data belongs to whatever it points at. Copying it would
    duplicate the data into the target and then leave a real table sitting on the name the
    shortcut phase needs, so recreating the shortcut fails on a collision with our own copy.

    Keys are ``(schema, name)`` case-folded, with ``schema`` ``None`` for a lakehouse without
    schemas, matching how :class:`~fabshuffle.fabric.data_stores.TableRef` describes a table.
    Shortcuts under ``Files/`` are ignored: they are not tables, and one sharing a name with a
    table must not exclude it.
    """
    keys: set[tuple[str | None, str]] = set()
    for shortcut in shortcuts:
        name = (shortcut.get("name") or "").strip()
        if not name:
            continue
        segments = [
            segment
            for segment in str(shortcut.get("path") or "").replace("\\", "/").split("/")
            if segment
        ]
        if not segments or segments[0].casefold() != "tables":
            continue
        schema = segments[1].casefold() if len(segments) > 1 else None
        keys.add((schema, name.casefold()))
    return keys


def create_shortcut(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    shortcut: Mapping[str, Any],
    *,
    conflict_policy: str = "Abort",
) -> dict[str, Any]:
    body = {
        "path": shortcut["path"],
        "name": shortcut["name"],
        "target": shortcut["target"],
    }
    return client.post(
        _shortcut_collection_path(workspace_id, item_id),
        json=body,
        params={"shortcutConflictPolicy": conflict_policy},
    )


def remap_shortcut_target(
    shortcut: Mapping[str, Any],
    id_map: Mapping[str, str],
) -> dict[str, Any]:
    """Rewrite a shortcut so internal OneLake targets point at the migrated items.

    OneLake identities and proven replacement connection IDs are rewritten. Paths and
    genuinely external connections are preserved.
    """
    remapped = {
        "path": shortcut.get("path"),
        "name": shortcut.get("name"),
        "target": _remap_target(shortcut.get("target") or {}, id_map),
    }
    return remapped


def _remap_target(target: Mapping[str, Any], id_map: Mapping[str, str]) -> dict[str, Any]:
    identities = {identity_key(key): value for key, value in id_map.items()}
    result: dict[str, Any] = {}
    for target_type, settings in target.items():
        # ``type`` is a discriminator string the create API does not accept back.
        if target_type == "type" or not isinstance(settings, Mapping):
            continue
        if target_type == "oneLake":
            workspace_id = settings.get("workspaceId") or ""
            if identity_key(workspace_id) not in identities:
                remapped = dict(settings)
            else:
                remapped = {
                    **settings,
                    "workspaceId": identities.get(
                        identity_key(settings.get("workspaceId") or ""), settings.get("workspaceId")
                    ),
                    "itemId": identities.get(
                        identity_key(settings.get("itemId") or ""), settings.get("itemId")
                    ),
                }
            # A delegated cross-tenant OneLake shortcut carries its own connectionId (the
            # producer-tenant connection used for the cross-tenant auth), independent of
            # whether workspaceId/itemId needed remapping. Leaving it pointed at the source
            # connection makes the create call a legitimate 400 against the wrong tenant.
            connection_id = settings.get("connectionId")
            if isinstance(connection_id, str):
                remapped["connectionId"] = identities.get(identity_key(connection_id), connection_id)
            result[target_type] = remapped
        else:
            result[target_type] = dict(settings)
            connection_id = settings.get("connectionId")
            if isinstance(connection_id, str):
                result[target_type]["connectionId"] = identities.get(
                    identity_key(connection_id), connection_id
                )
    return result


def _target_kind(target: Mapping[str, Any]) -> str:
    """The single target key a shortcut carries, such as ``oneLake`` or ``adlsGen2``."""
    return next((key for key in target if key != "type"), "")


def _shortcut_location(shortcut: Mapping[str, Any], *, table: bool = False) -> str:
    name = "" if shortcut.get("name") is None else str(shortcut.get("name"))
    if table:
        return name
    path = "" if shortcut.get("path") is None else str(shortcut.get("path"))
    return f"{path}/{name}" if path else name


def _shortcut_key(shortcut: Mapping[str, Any], *, table: bool = False) -> tuple[str, str]:
    name = "" if shortcut.get("name") is None else str(shortcut.get("name"))
    if table:
        return ("", name)
    path = "" if shortcut.get("path") is None else str(shortcut.get("path"))
    return (path, name)


def _normalise_target_value(key: str, value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            nested_key: _normalise_target_value(str(nested_key), nested)
            for nested_key, nested in value.items()
        }
    if isinstance(value, list):
        return [_normalise_target_value("", entry) for entry in value]
    if isinstance(value, str) and key in _IDENTITY_TARGET_FIELDS:
        return identity_key(value)
    return value


def _normalise_shortcut_target(target: Mapping[str, Any]) -> dict[str, Any]:
    # The REST Get/List responses include target.type as a discriminator, while Create
    # examples omit it from the payload. Do not ignore any other metadata when comparing.
    return {
        key: _normalise_target_value(str(key), value)
        for key, value in target.items()
        if key != "type"
    }


def _same_shortcut_definition(
    existing: Mapping[str, Any],
    desired: Mapping[str, Any],
    *,
    table: bool = False,
) -> bool:
    if _shortcut_key(existing, table=table) != _shortcut_key(desired, table=table):
        return False
    if _normalise_shortcut_target(existing.get("target") or {}) != _normalise_shortcut_target(
        desired.get("target") or {}
    ):
        return False
    if table:
        return bool(existing.get("enableQueryAcceleration", False)) == bool(
            desired.get("enableQueryAcceleration", False)
        )
    return True


def _destination_match(
    inventory: Iterable[Mapping[str, Any]],
    desired: Mapping[str, Any],
    *,
    table: bool = False,
) -> tuple[str, Mapping[str, Any] | None]:
    """Classify the destination shortcut occupying the desired identity, if any."""
    desired_key = _shortcut_key(desired, table=table)
    first_same_identity: Mapping[str, Any] | None = None
    for existing in inventory:
        if _shortcut_key(existing, table=table) != desired_key:
            continue
        if _same_shortcut_definition(existing, desired, table=table):
            return "match", existing
        if first_same_identity is None:
            first_same_identity = existing
    return ("mismatch", first_same_identity) if first_same_identity is not None else ("missing", None)


def _existing_mismatch_message(
    shortcut: Mapping[str, Any],
    existing: Mapping[str, Any],
    *,
    label: str,
    table: bool = False,
) -> str:
    location = _shortcut_location(shortcut, table=table)
    detail = "it points at a different target"
    if table and _normalise_shortcut_target(existing.get("target") or {}) == _normalise_shortcut_target(
        shortcut.get("target") or {}
    ):
        detail = "its query acceleration setting differs"
    return (
        f"{label} '{shortcut.get('name')}' was not created: the destination already has a "
        f"shortcut at '{location}', but {detail}. Resolve that destination shortcut or choose "
        "a clean destination path, then retry."
    )


def _record_cached_destination(
    inventory: list[Mapping[str, Any]] | None,
    observed: Mapping[str, Any],
) -> None:
    if inventory is not None:
        inventory.append(observed)


def _adopt_lakehouse_shortcut_after_conflict(
    client: FabricClient,
    workspace_id: str,
    item_id: str,
    desired: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    try:
        existing = _get_shortcut_strict(
            client,
            workspace_id,
            item_id,
            "" if desired.get("path") is None else str(desired.get("path")),
            "" if desired.get("name") is None else str(desired.get("name")),
        )
    except FabricApiError as error:
        logger.info("Existing shortcut could not be verified after a create conflict: %s", error)
        return None
    return existing if _same_shortcut_definition(existing, desired) else None


def _adopt_table_shortcut_after_conflict(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
    desired: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    try:
        state, existing = _destination_match(
            _list_table_shortcuts_strict(client, workspace_id, database_id),
            desired,
            table=True,
        )
    except FabricApiError as error:
        logger.info("Existing KQL shortcut could not be verified after a create conflict: %s", error)
        return None
    return existing if state == "match" else None


def _unmigrated_connection(
    shortcut: Mapping[str, Any],
    id_map: Mapping[str, str],
    source_items: Mapping[str, Mapping[str, Any]] | None,
    *,
    strict_references: bool = False,
) -> str | None:
    identities = {identity_key(key): value for key, value in id_map.items()}
    items = {identity_key(key): value for key, value in (source_items or {}).items()}
    for settings in (shortcut.get("target") or {}).values():
        if not isinstance(settings, Mapping):
            continue
        connection_id = settings.get("connectionId") or ""
        item = items.get(identity_key(connection_id)) or {}
        replacement = identities.get(identity_key(connection_id))
        if strict_references and connection_id and (
            not replacement or identity_key(replacement) == identity_key(connection_id)
        ):
            return (
                f"connection '{item.get('displayName') or connection_id}' has no destination mapping. "
                "Create or select a connection in the destination tenant and supply its mapping, then retry"
            )
        if item.get("type") == "Connection" and (
            not replacement or identity_key(replacement) == identity_key(connection_id)
        ):
            return (
                f"connection '{item.get('displayName') or connection_id}' still targets the "
                "source workspace. Create its replacement against the migrated store and "
                "retry the migration"
            )
    return None


def onelake_item_id(shortcut: Mapping[str, Any]) -> str:
    """The source item a OneLake shortcut points at, before any remapping."""
    settings = (shortcut.get("target") or {}).get("oneLake")
    return str(settings.get("itemId") or "") if isinstance(settings, Mapping) else ""


def unmigrated_target(
    shortcut: Mapping[str, Any],
    id_map: Mapping[str, str],
    source_workspace_id: str,
) -> str | None:
    """The source item a shortcut points at that did not make it to the new workspace.

    Read from the shortcut as it was exported, before remapping, because that is the only
    place the original ids survive.

    Only a target *inside the workspace being migrated* counts. A shortcut into a different
    workspace is correct to leave exactly as it is: that workspace is not moving, and the
    reference still resolves.

    Worth checking before the call rather than after, because the failure it produces is
    unreadable. The workspace id remaps and the item id does not, so Fabric is asked for an
    item that was never in that workspace and answers with a bare 400.
    """
    target = shortcut.get("target") or {}
    settings = target.get("oneLake")
    if not isinstance(settings, Mapping):
        return None

    workspace_id = settings.get("workspaceId") or ""
    item_id = settings.get("itemId") or ""
    if identity_key(workspace_id) != identity_key(source_workspace_id) or not item_id:
        return None
    identities = {identity_key(key): value for key, value in id_map.items()}
    replacement = identities.get(identity_key(item_id))
    workspace = identities.get(identity_key(workspace_id))
    return (
        None
        if replacement and identity_key(replacement) != identity_key(item_id)
        and workspace and identity_key(workspace) != identity_key(workspace_id)
        else str(item_id)
    )


def describe_unmigrated(
    name: str,
    item_id: str,
    source_items: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    label: str = "Shortcut",
) -> str:
    """Say which item a shortcut needed, in the words the operator will recognise."""
    return (
        f"{label} '{name}' was not created: it points at {_describe_item(item_id, source_items)}, "
        "which is not in the new workspace because it did not migrate. Migrate that item, then "
        "recreate the shortcut."
    )


def _describe_item(
    item_id: str,
    source_items: Mapping[str, Mapping[str, Any]] | None,
) -> str:
    items = {identity_key(key): value for key, value in (source_items or {}).items()}
    item = items.get(identity_key(item_id)) or {}
    if item.get("displayName"):
        return f"{item.get('type') or 'item'} '{item['displayName']}'"
    return f"the item {item_id}"


def describe_dormant(
    name: str,
    item_id: str,
    reason: str,
    status_code: int,
    source_items: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    label: str = "Shortcut",
    said: str = "",
) -> str:
    """Explain a failure caused by the target having arrived without its data.

    Some items are migrated deliberately switched off, so that a copy does not start doing
    the original's work in a second region the moment it exists. Nothing has written to them
    yet, so a shortcut into one has nothing to point at.

    This is only used once a create has actually failed. Whether Fabric refuses such a
    shortcut is its business, and guessing at that in advance is how the last wrong message
    got written.
    """
    message = (
        f"{label} '{name}' could not be created (HTTP {status_code}): it points at "
        f"{_describe_item(item_id, source_items)}, which migrated but {reason} "
        "Do that, then recreate this shortcut."
    )
    return f"{message} The service said: {said}." if said else message


def describe_failure(
    name: str,
    target: Mapping[str, Any],
    status_code: int,
    *,
    label: str = "Shortcut",
    said: str = "",
    location: str = "",
) -> str:
    """Explain why a shortcut could not be created.

    The status says which of three quite different things went wrong, and the fix differs for
    an internal OneLake target and an external one, so the message is built from both rather
    than blaming the connection for everything.

    ``said`` is whatever the service itself reported. Where our own reading of the status is
    a guess, that is the only thing that says what actually happened, so it is repeated back
    rather than being flattened into "the request was rejected".
    """
    internal = _target_kind(target) == "oneLake"

    if status_code == 409:
        reason = (
            "something at that destination path/name is already there. Resolve the collision "
            "or choose a clean target, then retry"
        )
    elif status_code == 404 and internal:
        reason = (
            "the item it points at does not exist in the new workspace, so it was probably "
            "not migrated. Recreate the shortcut once that item is there"
        )
    elif status_code == 404:
        reason = "the path it points at was not found. Check the target still exists"
    elif status_code in (401, 403):
        reason = (
            said
            if said
            else "the service denied access. Check the caller, target item, and connection permissions"
        )
    elif said:
        # Nothing useful can be inferred from the status alone, but the service said why.
        reason = said
    else:
        reason = "the request was rejected. Recreate it by hand"

    subject = f"{label} '{name}'"
    if location and location != name:
        subject = f"{subject} at '{location}'"
    message = f"{subject} could not be created (HTTP {status_code}): {reason}."
    # A recognised status still benefits from the detail, in case our reading of it is wrong.
    if said and said not in reason:
        message = f"{message} The service said: {said}."
    return message


def failure_detail(error: FabricApiError) -> str:
    return " ".join(part for part in (error.error_code, error.detail) if part).strip()


class _ShortcutEvidence:
    def __init__(self, lifecycle: ItemLifecycle | None) -> None:
        self.lifecycle = lifecycle
        self.dependencies: set[str] = set()
        self.unresolved: list[str] = []
        self.failures: dict[str, str] = {}
        self.last_error: FabricApiError | None = None
        self.previous_failures: set[str] = set()
        self.total = 0
        self.created = 0
        self.reused = 0

    @contextmanager
    def operation(self) -> Iterator[_ShortcutEvidence]:
        if self.lifecycle:
            item = self.lifecycle.owner.get(self.lifecycle.source_id)
            self.previous_failures = {step for step in item.steps if step.startswith("shortcut:")}
            self.lifecycle.step(
                "shortcuts", EvidenceState.UNKNOWN,
                "Shortcut inventory and copy started; completion not recorded.",
            )
        try:
            yield self
        except Exception as error:
            if self.lifecycle:
                self.lifecycle.references(self.dependencies, self.unresolved, scope="shortcuts")
                cancelled = type(error).__name__ == "CancelledError"
                self.lifecycle.step(
                    "shortcuts", EvidenceState.UNKNOWN if cancelled else EvidenceState.FAILED,
                    "Shortcut copy interrupted." if cancelled else "Shortcut inventory or copy failed.",
                    action="Review the reported error, then retry this item's shortcuts.",
                    error=error,
                )
            raise
        else:
            self.finish()

    def observe(
        self,
        shortcut: Mapping[str, Any],
        source_workspace_id: str,
        source_items: Mapping[str, Mapping[str, Any]] | None,
    ) -> None:
        if not self.lifecycle:
            return
        items = {identity_key(key): key for key in (source_items or {})}
        target = shortcut.get("target") or {}
        settings = target.get("oneLake")
        if (
            isinstance(settings, Mapping)
            and identity_key(settings.get("workspaceId") or "") == identity_key(source_workspace_id)
            and (item_id := onelake_item_id(shortcut))
        ):
            self.dependencies.add(items.get(identity_key(item_id), item_id))
        self.dependencies.update(
            items.get(identity_key(connection_id), connection_id)
            for connection_id in _source_connection_ids(shortcut, source_items)
        )

    def missing(
        self,
        shortcut: Mapping[str, Any],
        item_id: str,
        source_items: Mapping[str, Mapping[str, Any]] | None,
    ) -> None:
        self.unresolved.append(
            f"Shortcut target: '{shortcut.get('name')}' needs "
            f"{_describe_item(item_id, source_items)} ({item_id})."
        )

    def missing_connections(
        self,
        shortcut: Mapping[str, Any],
        id_map: Mapping[str, str],
        source_items: Mapping[str, Mapping[str, Any]] | None,
    ) -> None:
        identities = {identity_key(key): value for key, value in id_map.items()}
        for connection_id in _source_connection_ids(shortcut, source_items):
            replacement = identities.get(identity_key(connection_id))
            if not replacement or identity_key(replacement) == identity_key(connection_id):
                self.missing(shortcut, connection_id, source_items)

    def failed(
        self, shortcut: Mapping[str, Any], reason: str, action: str,
        error: FabricApiError | None = None,
    ) -> None:
        name = str(shortcut.get("name"))
        key = f"shortcut:{shortcut.get('path') or ''}/{name}"
        self.failures[key] = name
        if error is not None:
            self.last_error = error
        if self.lifecycle:
            self.lifecycle.step(key, EvidenceState.FAILED, reason, action=action, error=error)
            self.lifecycle.step(
                "shortcuts", EvidenceState.FAILED,
                "At least one enumerated shortcut was not created.",
                action=f"Resolve the failure for shortcut '{name}', then recreate it.",
                error=self.last_error,
            )

    def finish(self) -> None:
        if not self.lifecycle:
            return
        self.lifecycle.references(self.dependencies, self.unresolved, scope="shortcuts")
        for key in self.previous_failures - self.failures.keys():
            self.lifecycle.step(
                key, EvidenceState.SUCCEEDED,
                "This previous shortcut failure was not observed in the completed retry.",
            )
        completed = self.created + self.reused
        if completed == self.total:
            reason = (
                "Shortcut inventory was empty; no shortcuts were enumerated."
                if not self.total
                else f"Created all {self.total} enumerated shortcuts."
                if not self.reused
                else f"Reused all {self.total} enumerated shortcuts already present in the destination."
                if not self.created
                else f"Created {self.created} and reused {self.reused} of {self.total} enumerated shortcuts."
            )
            self.lifecycle.step("shortcuts", EvidenceState.SUCCEEDED, reason)
        else:
            names = ", ".join(f"'{name}'" for name in self.failures.values())
            reason = (
                f"Created {self.created} of {self.total} enumerated shortcuts."
                if not self.reused
                else f"Completed {completed} of {self.total} enumerated shortcuts."
            )
            self.lifecycle.step(
                "shortcuts", EvidenceState.FAILED,
                reason,
                action=f"Resolve the reported failures, then recreate these shortcuts: {names}.",
                error=self.last_error,
            )


def referenced_connection_ids(source: Iterable[Mapping[str, Any]]) -> set[str]:
    """Connection ids a shortcut's target names directly.

    A shortcut can bind an external connection (an ADLS Gen2 account, an S3 endpoint, a
    delegated cross-tenant OneLake target) that never appears anywhere in its item's own
    definition, since the connection lives on the shortcut target, not in the item's
    definition parts. Anything that needs to know which connections an item actually depends
    on - before deciding whether one of them still needs an explicit destination mapping -
    has to look at its shortcuts separately from ``connections.referenced_connection_ids``.
    """
    found: set[str] = set()
    for shortcut in source:
        for settings in (shortcut.get("target") or {}).values():
            if not isinstance(settings, Mapping):
                continue
            connection_id = settings.get("connectionId")
            if isinstance(connection_id, str) and connection_id:
                found.add(connection_id)
    return found


def _source_connection_ids(
    shortcut: Mapping[str, Any],
    source_items: Mapping[str, Mapping[str, Any]] | None,
) -> list[str]:
    items = {identity_key(key): value for key, value in (source_items or {}).items()}
    connections = []
    for settings in (shortcut.get("target") or {}).values():
        if not isinstance(settings, Mapping):
            continue
        connection_id = settings.get("connectionId") or ""
        if (items.get(identity_key(connection_id)) or {}).get("type") == "Connection":
            connections.append(connection_id)
    return connections


def _with_connection_references(
    source: Iterable[Mapping[str, Any]],
    source_items: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, Mapping[str, Any]]:
    references = dict(source_items or {})
    known = {identity_key(key) for key in references}
    for shortcut in source:
        for settings in (shortcut.get("target") or {}).values():
            if not isinstance(settings, Mapping):
                continue
            connection_id = settings.get("connectionId")
            if isinstance(connection_id, str) and connection_id and identity_key(connection_id) not in known:
                references[connection_id] = {
                    "id": connection_id, "type": "Connection", "displayName": connection_id,
                }
    return references


def copy_shortcuts(
    client: FabricClient,
    source_workspace_id: str,
    source_item_id: str,
    target_workspace_id: str,
    target_item_id: str,
    id_map: Mapping[str, str],
    *,
    source_items: Mapping[str, Mapping[str, Any]] | None = None,
    dormant: Mapping[str, str] | None = None,
    lifecycle: ItemLifecycle | None = None,
    target_client: FabricClient | None = None,
    cross_tenant: bool = False,
    strict_references: bool | None = None,
) -> tuple[int, list[str]]:
    """Create missing shortcuts and reuse proven identical destination shortcuts.

    The count is completed shortcuts (created plus reused), not new create requests.

    ``dormant`` maps a source item id to why its copy has no data yet, for the items that are
    migrated switched off. Used only to explain a failure, never to predict one.

    Source inventory uses ``client``; creates use ``target_client`` when supplied.
    ``cross_tenant`` requires destination connection mappings and workspace read evidence.
    """
    destination = client if target_client is None else target_client
    strict = cross_tenant or bool(strict_references)
    created = 0
    reused = 0
    warnings: list[str] = []
    target_inventory: list[Mapping[str, Any]] | None = None

    def destination_shortcuts() -> list[Mapping[str, Any]]:
        nonlocal target_inventory
        if target_inventory is None:
            target_inventory = _list_shortcuts_strict(destination, target_workspace_id, target_item_id)
        return target_inventory

    with _ShortcutEvidence(lifecycle).operation() as evidence:
        source = list_shortcuts(client, source_workspace_id, source_item_id)
        if strict:
            source_items = _with_connection_references(source, source_items)
        evidence.total = len(source)
        for shortcut in source:
            evidence.observe(shortcut, source_workspace_id, source_items)
            name = str(shortcut.get("name"))
            connection_problem = _unmigrated_connection(
                shortcut, id_map, source_items, strict_references=strict,
            )
            if connection_problem:
                message = f"Shortcut '{name}' was not created: {connection_problem}."
                warnings.append(message)
                evidence.missing_connections(shortcut, id_map, source_items)
                evidence.failed(shortcut, message, connection_problem)
                continue
            missing = unmigrated_target(shortcut, id_map, source_workspace_id)
            if missing:
                message = describe_unmigrated(name, missing, source_items)
                warnings.append(message)
                evidence.missing(shortcut, missing, source_items)
                evidence.failed(
                    shortcut, message,
                    f"Migrate {_describe_item(missing, source_items)}, then recreate shortcut '{name}'.",
                )
                continue

            remapped = remap_shortcut_target(shortcut, id_map)
            if not remapped["target"]:
                message = f"Shortcut '{name}' has no recognised target, skipped"
                warnings.append(message)
                evidence.failed(
                    shortcut, message,
                    f"Set a recognised target for shortcut '{name}', then recreate it.",
                )
                continue
            try:
                if strict:
                    analytics.validate_cross_tenant_identities([part("shortcut.json", remapped)])
                    analytics.validate_target_workspaces(
                        destination, [part("shortcut.json", remapped)],
                        source_workspace_id=source_workspace_id,
                    )
            except FabricApiError as error:
                source_id = onelake_item_id(shortcut)
                reason = {identity_key(k): v for k, v in (dormant or {}).items()}.get(
                    identity_key(source_id)
                )
                message = (
                    describe_dormant(
                        name,
                        source_id,
                        reason,
                        error.status_code,
                        source_items,
                        said=failure_detail(error),
                    )
                    if reason
                    else describe_failure(
                        name,
                        remapped["target"],
                        error.status_code,
                        said=failure_detail(error),
                        location=_shortcut_location(remapped),
                    )
                )
                warnings.append(message)
                evidence.failed(
                    shortcut, message,
                    f"{message} Resolve the reported error, then recreate this shortcut.", error,
                )
                continue

            state, existing = _destination_match(destination_shortcuts(), remapped)
            if state == "match":
                reused += 1
                continue
            if state == "mismatch":
                message = _existing_mismatch_message(
                    remapped, existing or {}, label="Shortcut",
                )
                warnings.append(message)
                evidence.failed(
                    shortcut,
                    message,
                    "Resolve the conflicting destination shortcut, then retry this shortcut.",
                )
                continue
            try:
                response = create_shortcut(destination, target_workspace_id, target_item_id, remapped)
                created += 1
                _record_cached_destination(target_inventory, response or remapped)
            except FabricApiError as error:
                if error.status_code == 409:
                    existing = _adopt_lakehouse_shortcut_after_conflict(
                        destination, target_workspace_id, target_item_id, remapped,
                    )
                    if existing is not None:
                        reused += 1
                        _record_cached_destination(target_inventory, existing)
                        continue
                source_id = onelake_item_id(shortcut)
                reason = {identity_key(k): v for k, v in (dormant or {}).items()}.get(
                    identity_key(source_id)
                )
                message = (
                    describe_dormant(
                        name,
                        source_id,
                        reason,
                        error.status_code,
                        source_items,
                        said=failure_detail(error),
                    )
                    if reason
                    else describe_failure(
                        name,
                        remapped["target"],
                        error.status_code,
                        said=failure_detail(error),
                        location=_shortcut_location(remapped),
                    )
                )
                warnings.append(message)
                evidence.failed(
                    shortcut, message,
                    f"{message} Resolve the reported error, then recreate this shortcut.", error,
                )
        evidence.created = created
        evidence.reused = reused
    return created + reused, warnings


# ------------------------------------------------------- KQL table shortcuts

# A KQL database exposes its table shortcuts on its own endpoint. They use the same target
# shape as OneLake shortcuts, but are named without a path and carry a query acceleration
# flag instead.


def _table_shortcut_collection_path(workspace_id: str, database_id: str) -> str:
    return f"workspaces/{workspace_id}/kqlDatabases/{database_id}/shortcuts"


def list_table_shortcuts(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
) -> list[dict[str, Any]]:
    try:
        return _list_table_shortcuts_strict(client, workspace_id, database_id)
    except FabricApiError as error:
        if error.status_code in (400, 404):
            return []
        raise


def _list_table_shortcuts_strict(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
) -> list[dict[str, Any]]:
    return client.list_all(_table_shortcut_collection_path(workspace_id, database_id))


def table_shortcut_names(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
) -> set[str]:
    """Names of tables that are shortcuts rather than real tables.

    Their data lives at the target of the shortcut, so copying them would either fail or
    duplicate someone else's data into the migrated database.
    """
    return {
        shortcut["name"]
        for shortcut in list_table_shortcuts(client, workspace_id, database_id)
        if shortcut.get("name")
    }


def create_table_shortcut(
    client: FabricClient,
    workspace_id: str,
    database_id: str,
    shortcut: Mapping[str, Any],
) -> dict[str, Any]:
    return client.post(
        _table_shortcut_collection_path(workspace_id, database_id),
        json={
            "name": shortcut["name"],
            "enableQueryAcceleration": bool(shortcut.get("enableQueryAcceleration", False)),
            "target": shortcut["target"],
        },
    )


def copy_table_shortcuts(
    client: FabricClient,
    source_workspace_id: str,
    source_database_id: str,
    target_workspace_id: str,
    target_database_id: str,
    id_map: Mapping[str, str],
    *,
    shortcuts: Iterable[Mapping[str, Any]] | None = None,
    source_items: Mapping[str, Mapping[str, Any]] | None = None,
    dormant: Mapping[str, str] | None = None,
    lifecycle: ItemLifecycle | None = None,
    target_client: FabricClient | None = None,
    cross_tenant: bool = False,
    strict_references: bool | None = None,
) -> tuple[int, list[str]]:
    """Reconcile KQL shortcuts; return the number created or proven identical and reused."""
    destination = client if target_client is None else target_client
    strict = cross_tenant or bool(strict_references)
    created = 0
    reused = 0
    warnings: list[str] = []
    target_inventory: list[Mapping[str, Any]] | None = None

    def destination_shortcuts() -> list[Mapping[str, Any]]:
        nonlocal target_inventory
        if target_inventory is None:
            target_inventory = _list_table_shortcuts_strict(
                destination, target_workspace_id, target_database_id,
            )
        return target_inventory

    with _ShortcutEvidence(lifecycle).operation() as evidence:
        source = (
            list(shortcuts)
            if shortcuts is not None
            else list_table_shortcuts(client, source_workspace_id, source_database_id)
        )
        if strict:
            source_items = _with_connection_references(source, source_items)
        evidence.total = len(source)
        for shortcut in source:
            evidence.observe(shortcut, source_workspace_id, source_items)
            name = str(shortcut.get("name"))
            connection_problem = _unmigrated_connection(
                shortcut, id_map, source_items, strict_references=strict,
            )
            if connection_problem:
                message = f"KQL table shortcut '{name}' was not created: {connection_problem}."
                warnings.append(message)
                evidence.missing_connections(shortcut, id_map, source_items)
                evidence.failed(shortcut, message, connection_problem)
                continue
            missing = unmigrated_target(shortcut, id_map, source_workspace_id)
            if missing:
                message = describe_unmigrated(name, missing, source_items, label="KQL table shortcut")
                warnings.append(message)
                evidence.missing(shortcut, missing, source_items)
                evidence.failed(
                    shortcut, message,
                    f"Migrate {_describe_item(missing, source_items)}, then recreate shortcut '{name}'.",
                )
                continue

            target = _remap_target(shortcut.get("target") or {}, id_map)
            if not target:
                message = f"KQL table shortcut '{name}' has no recognised target, skipped"
                warnings.append(message)
                evidence.failed(
                    shortcut, message,
                    f"Set a recognised target for shortcut '{name}', then recreate it.",
                )
                continue
            desired = {
                "name": shortcut["name"],
                "enableQueryAcceleration": bool(shortcut.get("enableQueryAcceleration", False)),
                "target": target,
            }
            try:
                if strict:
                    analytics.validate_cross_tenant_identities([part("shortcut.json", {"target": target})])
                    analytics.validate_target_workspaces(
                        destination, [part("shortcut.json", {"target": target})],
                        source_workspace_id=source_workspace_id,
                    )
            except FabricApiError as error:
                source_id = onelake_item_id(shortcut)
                reason = {identity_key(k): v for k, v in (dormant or {}).items()}.get(
                    identity_key(source_id)
                )
                message = (
                    describe_dormant(
                        name,
                        source_id,
                        reason,
                        error.status_code,
                        source_items,
                        label="KQL table shortcut",
                        said=failure_detail(error),
                    )
                    if reason
                    else describe_failure(
                        name,
                        target,
                        error.status_code,
                        label="KQL table shortcut",
                        said=failure_detail(error),
                        location=_shortcut_location(desired, table=True),
                    )
                )
                warnings.append(message)
                evidence.failed(
                    shortcut, message,
                    f"{message} Resolve the reported error, then recreate this shortcut.", error,
                )
                continue

            state, existing = _destination_match(destination_shortcuts(), desired, table=True)
            if state == "match":
                reused += 1
                continue
            if state == "mismatch":
                message = _existing_mismatch_message(
                    desired, existing or {}, label="KQL table shortcut", table=True,
                )
                warnings.append(message)
                evidence.failed(
                    shortcut,
                    message,
                    "Resolve the conflicting destination shortcut, then retry this shortcut.",
                )
                continue
            try:
                response = create_table_shortcut(
                    destination, target_workspace_id, target_database_id, desired,
                )
                created += 1
                _record_cached_destination(target_inventory, response or desired)
            except FabricApiError as error:
                if error.status_code == 409:
                    existing = _adopt_table_shortcut_after_conflict(
                        destination, target_workspace_id, target_database_id, desired,
                    )
                    if existing is not None:
                        reused += 1
                        _record_cached_destination(target_inventory, existing)
                        continue
                source_id = onelake_item_id(shortcut)
                reason = {identity_key(k): v for k, v in (dormant or {}).items()}.get(
                    identity_key(source_id)
                )
                message = (
                    describe_dormant(
                        name,
                        source_id,
                        reason,
                        error.status_code,
                        source_items,
                        label="KQL table shortcut",
                        said=failure_detail(error),
                    )
                    if reason
                    else describe_failure(
                        name,
                        target,
                        error.status_code,
                        label="KQL table shortcut",
                        said=failure_detail(error),
                        location=_shortcut_location(desired, table=True),
                    )
                )
                warnings.append(message)
                evidence.failed(
                    shortcut, message,
                    f"{message} Resolve the reported error, then recreate this shortcut.", error,
                )
        evidence.created = created
        evidence.reused = reused
    return created + reused, warnings


__all__ = [
    "copy_shortcuts",
    "copy_table_shortcuts",
    "create_shortcut",
    "create_table_shortcut",
    "describe_dormant",
    "describe_failure",
    "describe_unmigrated",
    "failure_detail",
    "list_shortcuts",
    "list_table_shortcuts",
    "onelake_item_id",
    "referenced_connection_ids",
    "remap_shortcut_target",
    "table_shortcut_keys",
    "table_shortcut_names",
    "unmigrated_target",
]
