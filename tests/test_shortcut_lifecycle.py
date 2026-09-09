"""Shortcut evidence follows actual inventory, refusal and create results."""

from __future__ import annotations

import json

import pytest

from fabshuffle.fabric import shortcuts
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.lifecycle import Disposition, EvidenceState, Lifecycle

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
SOURCE_ITEM = "store-source"
TARGET_ITEM = "store-target"
DEPENDENCY = "warehouse-source"
CONNECTION = "connection-source"
ID_MAP = {SOURCE_WS: TARGET_WS, DEPENDENCY: "warehouse-target"}
SOURCE_ITEMS = {
    DEPENDENCY: {"type": "Warehouse", "displayName": "Inventory"},
    CONNECTION: {"type": "Connection", "displayName": "Inventory connection"},
}


def onelake(name="Stock", workspace=SOURCE_WS, item=DEPENDENCY, path="Tables"):
    return {
        "name": name,
        "path": path,
        "target": {"oneLake": {"workspaceId": workspace, "itemId": item, "path": "Tables/Stock"}},
    }


def external(name="External", connection=CONNECTION):
    return {
        "name": name,
        "path": "Files",
        "target": {"adlsGen2": {"connectionId": connection, "location": "https://storage"}},
    }


def api_error(code="InvalidPath", message="The shortcut path was not found.", status=400):
    return FabricApiError(
        "POST", "shortcuts", status, json.dumps({"errorCode": code, "message": message}),
    )


class Client:
    def __init__(self, source=(), failures=None, list_error=None, target=(), target_list_error=None):
        self.source = list(source)
        self.target = list(target)
        self.failures = failures or {}
        self.list_error = list_error
        self.target_list_error = target_list_error
        self.listed = []
        self.posted = []

    def list_all(self, path):
        self.listed.append(path)
        if f"workspaces/{TARGET_WS}/" in path:
            if self.target_list_error:
                raise self.target_list_error
            return list(self.target)
        if self.list_error:
            raise self.list_error
        return list(self.source)

    def get(self, path):
        for shortcut in self.target:
            if path.endswith(f"{shortcut.get('path', '')}/{shortcut.get('name')}"):
                return shortcut
        raise api_error("ItemNotFound", "No shortcut found.", status=404)

    def post(self, path, json=None, params=None):
        self.posted.append((path, json, params))
        if error := self.failures.get(json["name"]):
            raise error
        self.target.append(dict(json or {}))
        return {}


@pytest.fixture(params=[shortcuts.copy_shortcuts, shortcuts.copy_table_shortcuts])
def copy_fn(request):
    return request.param


@pytest.fixture
def lifecycle(copy_fn):
    owner = Lifecycle(attempt_id="attempt", source_workspace=SOURCE_WS)
    item_type = "Lakehouse" if copy_fn is shortcuts.copy_shortcuts else "KQLDatabase"
    item = owner.item(SOURCE_ITEM, "Target store", item_type)
    item.resolve(TARGET_ITEM, TARGET_WS, Disposition.CREATED)
    return item


def outcome(lifecycle):
    return lifecycle.owner.get(SOURCE_ITEM)


def copy(copy_fn, client, lifecycle, id_map=None, **kwargs):
    return copy_fn(
        client, SOURCE_WS, SOURCE_ITEM, TARGET_WS, TARGET_ITEM,
        ID_MAP if id_map is None else id_map,
        source_items=kwargs.pop("source_items", SOURCE_ITEMS), lifecycle=lifecycle, **kwargs,
    )


def test_empty_inventory_is_successful_but_not_a_copy(copy_fn, lifecycle):
    client = Client()

    assert copy(copy_fn, client, lifecycle) == (0, [])

    summary = outcome(lifecycle).steps["shortcuts"]
    assert summary.state == EvidenceState.SUCCEEDED
    assert summary.reason == "Shortcut inventory was empty; no shortcuts were enumerated."
    assert summary.errorCode == summary.message == summary.action == ""
    assert len(client.listed) == 1
    assert client.posted == []
    assert not any(key.startswith("shortcut:") for key in outcome(lifecycle).steps)


def test_only_reads_the_current_item_outcome(copy_fn, lifecycle, monkeypatch):
    owner = lifecycle.owner
    owner.item("unrelated-source", "Other store", "Lakehouse")
    original_get = owner.get
    read_sources = []

    def get(source_id):
        read_sources.append(source_id)
        return original_get(source_id)

    def snapshot():
        pytest.fail("Shortcut evidence must not copy all run outcomes")

    monkeypatch.setattr(owner, "get", get)
    monkeypatch.setattr(owner, "snapshot", snapshot)

    assert copy(copy_fn, Client([onelake()]), lifecycle) == (1, [])

    assert read_sources == [SOURCE_ITEM]
    assert outcome(lifecycle).steps["shortcuts"].state == EvidenceState.SUCCEEDED


def test_all_creates_succeed_without_overwriting_definition_evidence(copy_fn, lifecycle):
    client = Client([onelake(), external(connection="external-connection")])
    lifecycle.references(["definition-source"], ["Definition dependency: missing model"])
    lifecycle.step("definition", EvidenceState.FAILED, "Definition failed.", error=api_error("BadModel"))

    assert copy(copy_fn, client, lifecycle) == (2, [])

    item = outcome(lifecycle)
    assert item.steps["shortcuts"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcuts"].reason == "Created all 2 enumerated shortcuts."
    assert set(item.dependencies) == {DEPENDENCY, "definition-source"}
    assert item.unresolvedReferences == ["Definition dependency: missing model"]
    assert item.steps["definition"].state == EvidenceState.FAILED
    assert item.steps["definition"].errorCode == "BadModel"
    assert not any(key.startswith("shortcut:") for key in item.steps)
    assert len(client.listed) == 2
    assert len(client.posted) == 2
    assert client.posted[0][1]["target"]["oneLake"]["workspaceId"] == TARGET_WS
    assert client.posted[0][1]["target"]["oneLake"]["itemId"] == "warehouse-target"
    assert client.posted[1][1]["target"]["adlsGen2"]["connectionId"] == "external-connection"


def test_inventory_failure_preserves_service_error_and_exception(copy_fn, lifecycle):
    error = api_error("InsufficientPrivileges", "The caller cannot list shortcuts.", status=403)
    client = Client(list_error=error)

    with pytest.raises(FabricApiError) as caught:
        copy(copy_fn, client, lifecycle)

    assert caught.value is error
    summary = outcome(lifecycle).steps["shortcuts"]
    assert summary.state == EvidenceState.FAILED
    assert summary.errorCode == "InsufficientPrivileges"
    assert summary.message == "The caller cannot list shortcuts."
    assert "retry" in summary.action
    assert client.posted == []


def test_existing_missing_endpoint_result_still_counts_as_empty(copy_fn, lifecycle):
    client = Client(list_error=api_error("NotFound", "No shortcut endpoint.", status=404))

    assert copy(copy_fn, client, lifecycle) == (0, [])
    assert outcome(lifecycle).steps["shortcuts"].state == EvidenceState.SUCCEEDED
    assert "empty" in outcome(lifecycle).steps["shortcuts"].reason


@pytest.mark.parametrize(
    "id_map",
    [{SOURCE_WS: TARGET_WS}, {DEPENDENCY: "warehouse-target"},
     {SOURCE_WS: TARGET_WS, DEPENDENCY: DEPENDENCY}],
)
def test_unmigrated_target_records_named_reference_and_dependency(copy_fn, lifecycle, id_map):
    client = Client([onelake()])

    created, warnings = copy(copy_fn, client, lifecycle, id_map)

    item = outcome(lifecycle)
    assert created == 0
    assert client.posted == []
    assert "Warehouse 'Inventory'" in warnings[0]
    assert item.dependencies == [DEPENDENCY]
    assert item.unresolvedReferences == [
        f"Shortcut target: 'Stock' needs Warehouse 'Inventory' ({DEPENDENCY}).",
    ]
    assert item.steps["shortcuts"].state == EvidenceState.FAILED
    failure = item.steps["shortcut:Tables/Stock"]
    assert failure.state == EvidenceState.FAILED
    assert "Migrate Warehouse 'Inventory'" in failure.action
    assert failure.errorCode == failure.message == ""


def test_unnamed_missing_target_uses_its_id(copy_fn, lifecycle):
    client = Client([onelake(item="unknown-source")])

    created, warnings = copy(copy_fn, client, lifecycle)

    assert created == 0
    assert "the item unknown-source" in warnings[0]
    assert outcome(lifecycle).dependencies == ["unknown-source"]
    assert outcome(lifecycle).unresolvedReferences == [
        "Shortcut target: 'Stock' needs the item unknown-source (unknown-source).",
    ]


@pytest.mark.parametrize("replacement", [None, CONNECTION])
def test_unmigrated_connection_is_a_named_unresolved_dependency(copy_fn, lifecycle, replacement):
    client = Client([external()])
    mapping = dict(ID_MAP)
    if replacement:
        mapping[CONNECTION] = replacement

    created, warnings = copy(copy_fn, client, lifecycle, mapping)

    item = outcome(lifecycle)
    assert created == 0 and client.posted == []
    assert "connection 'Inventory connection' still targets the source workspace" in warnings[0]
    assert item.dependencies == [CONNECTION]
    assert item.unresolvedReferences == [
        f"Shortcut target: 'External' needs Connection 'Inventory connection' ({CONNECTION}).",
    ]
    assert item.steps["shortcuts"].state == EvidenceState.FAILED
    assert "Create its replacement against the migrated store" in item.steps["shortcut:Files/External"].action


def test_migrated_connection_is_a_resolved_dependency(copy_fn, lifecycle):
    client = Client([external()])

    assert copy(copy_fn, client, lifecycle, {**ID_MAP, CONNECTION: "connection-target"}) == (1, [])

    assert outcome(lifecycle).dependencies == [CONNECTION]
    assert outcome(lifecycle).unresolvedReferences == []
    assert client.posted[0][1]["target"]["adlsGen2"]["connectionId"] == "connection-target"


def test_other_workspace_target_does_not_become_a_migration_dependency(copy_fn, lifecycle):
    original = onelake(workspace="another-workspace")
    client = Client([original])

    assert copy(copy_fn, client, lifecycle) == (1, [])

    assert outcome(lifecycle).dependencies == []
    assert outcome(lifecycle).unresolvedReferences == []
    assert client.posted[0][1]["target"] == original["target"]


def test_dependency_identity_matches_source_inventory_casing(copy_fn, lifecycle):
    source_id = "aaaabbbb-1234-5678-90ab-cdef12345678"
    client = Client([onelake(item=source_id.upper())])

    assert copy(
        copy_fn, client, lifecycle, {SOURCE_WS: TARGET_WS, source_id: "new-warehouse"},
        source_items={source_id: {"type": "Warehouse", "displayName": "Inventory"}},
    ) == (1, [])

    assert outcome(lifecycle).dependencies == [source_id]


def test_failed_creates_keep_each_service_error_after_later_success(copy_fn, lifecycle):
    client = Client(
        [onelake("First"), onelake("Second"), onelake("Success")],
        failures={
            "First": api_error("InvalidPath", "The first path was not found."),
            "Second": api_error("NameConflict", "The second name is reserved.", status=409),
        },
    )

    created, warnings = copy(copy_fn, client, lifecycle)

    assert created == 1 and len(warnings) == 2
    assert len(client.posted) == 3
    item = outcome(lifecycle)
    first = item.steps["shortcut:Tables/First"]
    second = item.steps["shortcut:Tables/Second"]
    assert first.state == second.state == EvidenceState.FAILED
    assert (first.errorCode, first.message) == ("InvalidPath", "The first path was not found.")
    assert (second.errorCode, second.message) == ("NameConflict", "The second name is reserved.")
    assert "recreate" in first.action and "'First'" in first.action
    assert "recreate" in second.action and "'Second'" in second.action
    summary = item.steps["shortcuts"]
    assert summary.state == EvidenceState.FAILED
    assert summary.reason == "Created 1 of 3 enumerated shortcuts."
    assert summary.errorCode == "NameConflict"
    assert summary.message == "The second name is reserved."
    assert "'First'" in summary.action and "'Second'" in summary.action
    assert "shortcut:Tables/Success" not in item.steps


def test_unrecognised_target_is_not_reported_as_success(copy_fn, lifecycle):
    client = Client([{"name": "Mystery", "target": {"type": "Unknown"}}])

    created, warnings = copy(copy_fn, client, lifecycle)

    assert created == 0
    assert warnings[0].endswith("'Mystery' has no recognised target, skipped")
    assert client.posted == []
    item = outcome(lifecycle)
    assert item.steps["shortcuts"].state == EvidenceState.FAILED
    assert item.steps["shortcut:/Mystery"].state == EvidenceState.FAILED
    assert "Set a recognised target" in item.steps["shortcut:/Mystery"].action


def test_dormant_target_is_attempted_and_diagnosed_only_after_failure(copy_fn, lifecycle):
    error = api_error()
    client = Client([onelake()], failures={"Stock": error})
    dormant = {DEPENDENCY: "arrives stopped. Start replication in the new workspace."}

    created, warnings = copy(copy_fn, client, lifecycle, dormant=dormant)

    assert created == 0 and len(client.posted) == 1
    assert "Start replication in the new workspace." in warnings[0]
    item = outcome(lifecycle)
    assert item.unresolvedReferences == []
    failure = item.steps["shortcut:Tables/Stock"]
    assert failure.errorCode == error.error_code and failure.message == error.detail
    assert "Start replication in the new workspace." in failure.action


@pytest.mark.parametrize("removed", [False, True])
def test_successful_retry_clears_previous_shortcut_errors_only(copy_fn, lifecycle, removed):
    lifecycle.step("definition", EvidenceState.FAILED, "Definition failed.", error=api_error("BadModel"))
    client = Client([onelake()], failures={"Stock": api_error()})
    copy(copy_fn, client, lifecycle)
    client.failures.clear()
    if removed:
        client.source.clear()

    assert copy(copy_fn, client, lifecycle) == (0 if removed else 1, [])

    item = outcome(lifecycle)
    assert item.steps["shortcuts"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcut:Tables/Stock"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcut:Tables/Stock"].errorCode == ""
    assert item.steps["shortcut:Tables/Stock"].message == ""
    assert item.steps["definition"].state == EvidenceState.FAILED
    assert item.steps["definition"].errorCode == "BadModel"


def test_retry_replaces_shortcut_unresolved_names_without_dropping_definition_refs(copy_fn, lifecycle):
    lifecycle.references(["definition-source"], ["Definition dependency: missing model"])
    client = Client([onelake()])
    copy(copy_fn, client, lifecycle, {SOURCE_WS: TARGET_WS})

    assert copy(copy_fn, client, lifecycle) == (1, [])

    item = outcome(lifecycle)
    assert item.unresolvedReferences == ["Definition dependency: missing model"]
    assert set(item.dependencies) == {DEPENDENCY, "definition-source"}
    assert item.steps["shortcuts"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcut:Tables/Stock"].state == EvidenceState.SUCCEEDED


def test_empty_retry_removes_shortcut_dependencies_but_preserves_definition_scope(copy_fn, lifecycle):
    definition_reference = "Shortcut target: a definition-owned reference with the same prefix"
    lifecycle.references(["definition-source", DEPENDENCY], [definition_reference])
    client = Client([onelake(), external()])
    copy(copy_fn, client, lifecycle, {SOURCE_WS: TARGET_WS})
    assert set(outcome(lifecycle).dependencies) == {"definition-source", DEPENDENCY, CONNECTION}
    assert len(outcome(lifecycle).unresolvedReferences) == 3
    client.source.clear()

    assert copy(copy_fn, client, lifecycle) == (0, [])

    item = outcome(lifecycle)
    assert set(item.dependencies) == {"definition-source", DEPENDENCY}
    assert item.unresolvedReferences == [definition_reference]
    assert item.steps["shortcuts"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcut:Tables/Stock"].state == EvidenceState.SUCCEEDED
    assert item.steps["shortcut:Files/External"].state == EvidenceState.SUCCEEDED


def test_fresh_inventory_replaces_removed_shortcut_dependencies(copy_fn, lifecycle):
    lifecycle.references(["definition-source"])
    client = Client([onelake()])
    assert copy(copy_fn, client, lifecycle) == (1, [])
    client.source = [onelake("New shortcut", item="new-source")]

    assert copy(
        copy_fn, client, lifecycle, {SOURCE_WS: TARGET_WS, "new-source": "new-target"},
    ) == (1, [])

    assert set(outcome(lifecycle).dependencies) == {"definition-source", "new-source"}


def test_exception_replaces_shortcut_scope_without_losing_definition_refs(copy_fn, lifecycle):
    lifecycle.references(["definition-source"], ["Definition dependency: missing model"])
    client = Client([onelake()])
    created, warnings = copy(copy_fn, client, lifecycle, {SOURCE_WS: TARGET_WS})
    assert created == 0 and warnings
    assert len(outcome(lifecycle).unresolvedReferences) == 2
    client.source = [onelake("New shortcut", item="new-source")]
    error = RuntimeError("The new shortcut could not be created.")
    client.failures = {"New shortcut": error}

    with pytest.raises(RuntimeError) as caught:
        copy(copy_fn, client, lifecycle, {SOURCE_WS: TARGET_WS, "new-source": "new-target"})

    assert caught.value is error
    item = outcome(lifecycle)
    assert set(item.dependencies) == {"definition-source", "new-source"}
    assert item.unresolvedReferences == ["Definition dependency: missing model"]
    assert item.steps["shortcuts"].state == EvidenceState.FAILED
    assert item.steps["shortcuts"].message == str(error)


def test_evidence_does_not_change_warnings_results_or_service_calls(copy_fn, lifecycle):
    source = [
        onelake("Missing", item="missing-item"),
        external("Connection"),
        {"name": "Unknown", "target": {"type": "Future"}},
        onelake("Rejected"),
        onelake("Created"),
    ]
    plain = Client(source, failures={"Rejected": api_error()})
    observed = Client(source, failures={"Rejected": api_error()})

    expected = copy(copy_fn, plain, None)

    assert copy(copy_fn, observed, lifecycle) == expected
    assert expected[0] == 1 and len(expected[1]) == 4
    assert observed.listed == plain.listed
    assert observed.posted == plain.posted


def test_unexpected_create_error_is_recorded_and_still_propagates(copy_fn, lifecycle):
    error = RuntimeError("The create operation could not be completed.")
    client = Client([onelake()], failures={"Stock": error})

    with pytest.raises(RuntimeError) as caught:
        copy(copy_fn, client, lifecycle)

    assert caught.value is error
    summary = outcome(lifecycle).steps["shortcuts"]
    assert summary.state == EvidenceState.FAILED
    assert summary.message == str(error)
    assert outcome(lifecycle).dependencies == [DEPENDENCY]


def test_kql_uses_supplied_inventory_without_an_extra_service_call():
    owner = Lifecycle()
    lifecycle = owner.item(SOURCE_ITEM, "KQL store", "KQLDatabase")
    client = Client(list_error=AssertionError("Inventory already supplied"))

    assert copy(
        shortcuts.copy_table_shortcuts, client, lifecycle,
        shortcuts=(shortcut for shortcut in [onelake()]),
    ) == (1, [])

    assert client.listed == [f"workspaces/{TARGET_WS}/kqlDatabases/{TARGET_ITEM}/shortcuts"]
    assert len(client.posted) == 1
    assert outcome(lifecycle).steps["shortcuts"].reason == "Created all 1 enumerated shortcuts."
