"""Retry-safe shortcut reconciliation against destination inventory."""

from __future__ import annotations

import json
from typing import Any

import pytest

from fabshuffle.fabric import shortcuts
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.lifecycle import Disposition, EvidenceState, Lifecycle

SOURCE_WS = "11111111-1111-1111-1111-111111111111"
TARGET_WS = "22222222-2222-2222-2222-222222222222"
SOURCE_ITEM = "33333333-3333-3333-3333-333333333333"
TARGET_ITEM = "44444444-4444-4444-4444-444444444444"
SOURCE_TARGET = "55555555-5555-5555-5555-555555555555"
TARGET_TARGET = "66666666-6666-6666-6666-666666666666"
SOURCE_CONNECTION = "77777777-7777-7777-7777-777777777777"
TARGET_CONNECTION = "88888888-8888-8888-8888-888888888888"

ID_MAP = {SOURCE_WS: TARGET_WS, SOURCE_TARGET: TARGET_TARGET}


def api_error(code: str = "EntityConflict", message: str = "The shortcut already exists.") -> FabricApiError:
    return FabricApiError("POST", "shortcuts", 409, json.dumps({"errorCode": code, "message": message}))


def onelake(
    name: str = "random_table",
    *,
    parent_path: str = "Tables/dbo",
    target_path: str = "Tables/CaseSensitive/Stock",
    workspace_id: str = SOURCE_WS,
    item_id: str = SOURCE_TARGET,
    accelerated: bool = False,
) -> dict[str, Any]:
    return {
        "path": parent_path,
        "name": name,
        "enableQueryAcceleration": accelerated,
        "target": {
            "type": "OneLake",
            "oneLake": {"workspaceId": workspace_id, "itemId": item_id, "path": target_path},
        },
    }


def external(
    name: str = "External",
    *,
    parent_path: str = "Files",
    connection_id: str = SOURCE_CONNECTION,
) -> dict[str, Any]:
    return {
        "path": parent_path,
        "name": name,
        "target": {
            "type": "AdlsGen2",
            "adlsGen2": {
                "connectionId": connection_id,
                "location": "https://storage.dfs.core.windows.net",
                "subpath": "/raw",
            },
        },
    }


def remapped(shortcut: dict[str, Any], id_map: dict[str, str] | None = None) -> dict[str, Any]:
    return shortcuts.remap_shortcut_target(shortcut, ID_MAP if id_map is None else id_map)


def target_shortcut(
    shortcut: dict[str, Any],
    *,
    copy_fn,
    id_map: dict[str, str] | None = None,
    accelerated: bool | None = None,
) -> dict[str, Any]:
    mapped = remapped(shortcut, id_map)
    target_type = "OneLake" if "oneLake" in mapped["target"] else "AdlsGen2"
    if copy_fn is shortcuts.copy_table_shortcuts:
        return {
            "name": shortcut["name"],
            "enableQueryAcceleration": (
                bool(shortcut.get("enableQueryAcceleration", False))
                if accelerated is None
                else accelerated
            ),
            "target": {"type": target_type, **mapped["target"]},
        }
    return {
        "path": mapped["path"],
        "name": mapped["name"],
        "target": {"type": target_type, **mapped["target"]},
    }


class FakeShortcutClient:
    def __init__(
        self,
        source: list[dict[str, Any]] | None = None,
        *,
        target: list[dict[str, Any]] | None = None,
        source_workspace: str = SOURCE_WS,
        target_list_error: FabricApiError | None = None,
        post_errors: dict[str, FabricApiError] | None = None,
        materialize_on_conflict: bool = False,
        crash_after_post: bool = False,
    ) -> None:
        self.source = list(source or [])
        self.target = list(target or [])
        self.source_workspace = source_workspace
        self.target_list_error = target_list_error
        self.post_errors = post_errors or {}
        self.materialize_on_conflict = materialize_on_conflict
        self.crash_after_post = crash_after_post
        self.listed: list[str] = []
        self.got: list[str] = []
        self.posted: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []

    def list_all(self, path, params=None, value_key="value"):
        self.listed.append(path)
        if path.startswith(f"workspaces/{self.source_workspace}/"):
            return list(self.source)
        if self.target_list_error:
            raise self.target_list_error
        return list(self.target)

    def get(self, path, params=None):
        self.got.append(path)
        for shortcut in self.target:
            parent = shortcut.get("path", "")
            if parent and path.endswith(f"{parent}/{shortcut.get('name')}"):
                return shortcut
        raise FabricApiError(
            "GET", path, 404, json.dumps({"errorCode": "ItemNotFound", "message": "missing"}),
        )

    def post(self, path, json=None, params=None, wait=True):
        body = dict(json or {})
        self.posted.append((path, body, params))
        error = self.post_errors.get(str(body.get("name")))
        if error:
            if self.materialize_on_conflict:
                self.target.append(body)
            raise error
        self.target.append(body)
        if self.crash_after_post:
            self.crash_after_post = False
            raise RuntimeError("process stopped before shortcut evidence was recorded")
        return body


@pytest.fixture(params=[shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
def copy_fn(request):
    return request.param


def copy(copy_fn, client, *, source=None, id_map=None, lifecycle=None, target_client=None):
    kwargs = {
        "source_items": {
            SOURCE_TARGET: {"type": "Lakehouse", "displayName": "Bronze"},
            SOURCE_CONNECTION: {"type": "Connection", "displayName": "Landing connection"},
        },
        "lifecycle": lifecycle,
        "target_client": target_client,
    }
    if copy_fn is shortcuts.copy_table_shortcuts:
        if source is not None:
            kwargs["shortcuts"] = source
        return copy_fn(
            client, SOURCE_WS, SOURCE_ITEM, TARGET_WS, TARGET_ITEM,
            ID_MAP if id_map is None else id_map, **kwargs,
        )
    return copy_fn(
        client, SOURCE_WS, SOURCE_ITEM, TARGET_WS, TARGET_ITEM,
        ID_MAP if id_map is None else id_map, **kwargs,
    )


def lifecycle_for(copy_fn):
    owner = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS)
    item = owner.item(
        SOURCE_ITEM,
        "Store",
        "KQLDatabase" if copy_fn is shortcuts.copy_table_shortcuts else "Lakehouse",
    )
    item.resolve(TARGET_ITEM, TARGET_WS, Disposition.CREATED)
    return item


def test_retry_reuses_exact_destination_shortcut_without_posting(copy_fn):
    source = [onelake("random_table")]
    client = FakeShortcutClient(source)

    assert copy(copy_fn, client, source=source) == (1, [])
    assert len(client.posted) == 1
    client.posted.clear()

    assert copy(copy_fn, client, source=source) == (1, [])
    assert client.posted == []


def test_partial_retry_only_attempts_the_missing_shortcuts_at_distinct_paths():
    source = [
        onelake("random_table", parent_path="/Tables"),
        onelake("random_table", parent_path="/Files/ShortTest"),
        onelake("Forza", parent_path="/Files/TestOne/TestTwo/TestThree"),
        onelake("dbo_movies", parent_path="/Tables"),
        onelake("adventureworks", parent_path="/Files/ShortTest"),
    ]
    existing = [target_shortcut(s, copy_fn=shortcuts.copy_shortcuts) for s in source[:3]]
    client = FakeShortcutClient(source, target=existing, post_errors={
        "dbo_movies": FabricApiError(
            "POST", "shortcuts", 400, '{"errorCode":"BadRequest","message":"Missing table"}',
        ),
        "adventureworks": FabricApiError(
            "POST", "shortcuts", 403, '{"errorCode":"InsufficientPrivileges","message":"Access denied"}',
        ),
    })
    lifecycle = lifecycle_for(shortcuts.copy_shortcuts)
    count, warnings = copy(shortcuts.copy_shortcuts, client, lifecycle=lifecycle)
    assert count == 3
    assert [body["name"] for _, body, _ in client.posted] == ["dbo_movies", "adventureworks"]
    assert len(warnings) == 2
    assert "BadRequest Missing table" in warnings[0]
    assert "InsufficientPrivileges Access denied" in warnings[1]
    assert all("409" not in warning for warning in warnings)
    assert lifecycle.owner.get(SOURCE_ITEM).steps["shortcuts"].reason == \
        "Completed 3 of 5 enumerated shortcuts."


def test_retry_after_post_succeeded_but_evidence_was_not_recorded_reuses_target(copy_fn):
    source = [onelake("Forza")]
    lifecycle = lifecycle_for(copy_fn)
    client = FakeShortcutClient(source, crash_after_post=True)

    with pytest.raises(RuntimeError):
        copy(copy_fn, client, source=source, lifecycle=lifecycle)
    client.posted.clear()

    assert copy(copy_fn, client, source=source, lifecycle=lifecycle) == (1, [])
    assert client.posted == []
    outcome = lifecycle.owner.get(SOURCE_ITEM)
    assert outcome.steps["shortcuts"].state == EvidenceState.SUCCEEDED
    assert "Reused all 1" in outcome.steps["shortcuts"].reason


def test_same_name_under_a_different_lakehouse_path_is_not_reused():
    source = [onelake("random_table", parent_path="Tables/dbo")]
    wrong_path = target_shortcut(
        onelake("random_table", parent_path="Tables/other"),
        copy_fn=shortcuts.copy_shortcuts,
    )
    client = FakeShortcutClient(source, target=[wrong_path])

    assert copy(shortcuts.copy_shortcuts, client) == (1, [])
    assert len(client.posted) == 1
    assert client.posted[0][1]["path"] == "Tables/dbo"


def test_same_path_and_name_with_different_target_fails_without_mutation(copy_fn):
    source = [onelake("year_2021")]
    existing = target_shortcut(onelake("year_2021", target_path="Tables/Other"), copy_fn=copy_fn)
    client = FakeShortcutClient(source, target=[existing])

    count, warnings = copy(copy_fn, client, source=source)

    assert count == 0
    assert "different target" in warnings[0]
    assert client.posted == []


def test_retargeted_connection_id_must_match_existing_destination(copy_fn):
    source = [external("Landing")]
    id_map = {SOURCE_CONNECTION: TARGET_CONNECTION}
    existing = target_shortcut(
        external("Landing", connection_id=SOURCE_CONNECTION), copy_fn=copy_fn, id_map={},
    )
    client = FakeShortcutClient(source, target=[existing])

    count, warnings = copy(copy_fn, client, source=source, id_map=id_map)

    assert count == 0
    assert "different target" in warnings[0]
    assert client.posted == []


def test_guid_identity_case_is_ignored_but_target_path_case_is_not(copy_fn):
    source = [
        onelake(
            "CaseShortcut",
            workspace_id=SOURCE_WS.upper(),
            item_id=SOURCE_TARGET.upper(),
            target_path="Tables/CaseSensitive/Stock",
        )
    ]
    exact = target_shortcut(source[0], copy_fn=copy_fn)
    exact["target"]["oneLake"]["workspaceId"] = TARGET_WS.upper()
    exact["target"]["oneLake"]["itemId"] = TARGET_TARGET.upper()
    client = FakeShortcutClient(source, target=[exact])

    assert copy(copy_fn, client, source=source) == (1, [])
    assert client.posted == []

    different_path = target_shortcut(source[0], copy_fn=copy_fn)
    different_path["target"]["oneLake"]["path"] = "Tables/casesensitive/Stock"
    client = FakeShortcutClient(source, target=[different_path])

    count, warnings = copy(copy_fn, client, source=source)

    assert count == 0
    assert "different target" in warnings[0]
    assert client.posted == []


def test_unreadable_destination_inventory_fails_instead_of_becoming_empty_success(copy_fn):
    source = [onelake("Unreadable")]
    error = FabricApiError(
        "GET", "shortcuts", 403,
        json.dumps({"errorCode": "TargetDenied", "message": "Cannot list destination shortcuts"}),
    )
    lifecycle = lifecycle_for(copy_fn)
    client = FakeShortcutClient(source, target_list_error=error)

    with pytest.raises(FabricApiError) as caught:
        copy(copy_fn, client, source=source, lifecycle=lifecycle)

    assert caught.value is error
    assert client.posted == []
    evidence = lifecycle.owner.get(SOURCE_ITEM).steps["shortcuts"]
    assert evidence.state == EvidenceState.FAILED
    assert evidence.errorCode == "TargetDenied"
    assert evidence.message == "Cannot list destination shortcuts"


def test_conflict_after_empty_inventory_is_adopted_only_when_reread_proves_exact_match(copy_fn):
    source = [onelake("Race")]
    client = FakeShortcutClient(
        source,
        post_errors={"Race": api_error("EntityConflict", "Shortcut appeared during retry.")},
        materialize_on_conflict=True,
    )

    assert copy(copy_fn, client, source=source) == (1, [])
    assert len(client.posted) == 1


def test_conflict_from_unlisted_real_table_is_not_ignored(copy_fn):
    source = [onelake("RealTable")]
    error = api_error("EntityConflict", "A table or folder already exists at that path.")
    lifecycle = lifecycle_for(copy_fn)
    client = FakeShortcutClient(source, post_errors={"RealTable": error})

    count, warnings = copy(copy_fn, client, source=source, lifecycle=lifecycle)

    assert count == 0
    assert "EntityConflict A table or folder already exists at that path" in warnings[0]
    assert lifecycle.owner.get(SOURCE_ITEM).steps["shortcuts"].errorCode == "EntityConflict"


def test_lifecycle_retry_clears_previous_shortcut_failure_when_destination_matches(copy_fn):
    source = [onelake("Recovered")]
    error = api_error("EntityConflict", "The name is already taken.")
    lifecycle = lifecycle_for(copy_fn)
    client = FakeShortcutClient(source, post_errors={"Recovered": error})
    copy(copy_fn, client, source=source, lifecycle=lifecycle)
    failure = lifecycle.owner.get(SOURCE_ITEM).steps["shortcut:Tables/dbo/Recovered"]
    assert failure.state == EvidenceState.FAILED

    client.post_errors = {}
    client.target = [target_shortcut(source[0], copy_fn=copy_fn)]
    client.posted.clear()

    assert copy(copy_fn, client, source=source, lifecycle=lifecycle) == (1, [])
    assert client.posted == []
    outcome = lifecycle.owner.get(SOURCE_ITEM)
    assert outcome.steps["shortcut:Tables/dbo/Recovered"].state == EvidenceState.SUCCEEDED
    assert outcome.steps["shortcuts"].state == EvidenceState.SUCCEEDED


def test_destination_inventory_and_create_use_target_client(copy_fn):
    source = [onelake("TwoClient")]
    source_client = FakeShortcutClient(source)
    target_client = FakeShortcutClient([], source_workspace="not-the-source")

    assert copy(copy_fn, source_client, source=source, target_client=target_client) == (1, [])

    assert source_client.listed == ([] if copy_fn is shortcuts.copy_table_shortcuts else [
        f"workspaces/{SOURCE_WS}/items/{SOURCE_ITEM}/shortcuts"
    ])
    assert source_client.posted == []
    assert len(target_client.posted) == 1
    assert target_client.listed == [
        (
            f"workspaces/{TARGET_WS}/kqlDatabases/{TARGET_ITEM}/shortcuts"
            if copy_fn is shortcuts.copy_table_shortcuts
            else f"workspaces/{TARGET_WS}/items/{TARGET_ITEM}/shortcuts"
        )
    ]


def test_kql_query_acceleration_difference_is_not_reused():
    source = [onelake("Accelerated", accelerated=True)]
    existing = target_shortcut(source[0], copy_fn=shortcuts.copy_table_shortcuts, accelerated=False)
    client = FakeShortcutClient(source, target=[existing])

    count, warnings = copy(shortcuts.copy_table_shortcuts, client, source=source)

    assert count == 0
    assert "query acceleration setting differs" in warnings[0]
    assert client.posted == []
