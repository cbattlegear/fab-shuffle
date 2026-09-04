"""The three failures from a real SQL endpoint and shortcut run.

1. ``unpackdacpac`` died during argument binding for every lakehouse endpoint, because
   ``Schemas`` is not a member of DacFx's ``ObjectType`` enum. v1 passed it too and never
   checked the exit code, so the same bug was there and silent.
2. ``sqlpackage extract`` timed out against a cold *source* endpoint; only the target was
   ever waited for.
3. Shortcut failures were all reported as "its connection may not exist in the target
   region", including 409 and 404, where that is not the problem.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from fabshuffle.fabric import shortcuts
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.transfer import sqlschema

ONELAKE = {"type": "OneLake", "oneLake": {"workspaceId": "ws", "itemId": "it", "path": "Tables/x"}}
EXTERNAL = {"type": "AdlsGen2", "adlsGen2": {"connectionId": "conn", "location": "https://x"}}


# ------------------------------------------------------------------ unpackdacpac


def test_only_tables_is_excluded(monkeypatch, tmp_path: Path) -> None:
    """`Schemas` is not an ObjectType, and sending it fails the whole command."""
    captured: list[list[str]] = []
    monkeypatch.setattr(sqlschema, "_run", lambda command, what: captured.append(command))
    monkeypatch.setattr(Path, "exists", lambda self: True)

    sqlschema.unpack_dacpac(tmp_path / "a.dacpac", tmp_path / "out", exclude_tables=True)

    excluded = [
        captured[0][i + 1]
        for i, arg in enumerate(captured[0])
        if arg == "--deploy-script-exclude-object-type"
    ]
    assert excluded == ["Tables"]


def test_nothing_is_excluded_for_a_warehouse(monkeypatch, tmp_path: Path) -> None:
    captured: list[list[str]] = []
    monkeypatch.setattr(sqlschema, "_run", lambda command, what: captured.append(command))
    monkeypatch.setattr(Path, "exists", lambda self: True)

    sqlschema.unpack_dacpac(tmp_path / "a.dacpac", tmp_path / "out", exclude_tables=False)

    assert "--deploy-script-exclude-object-type" not in captured[0]


# --------------------------------------------------------------- endpoint waiting


def test_both_endpoints_are_waited_for(monkeypatch, tmp_path: Path) -> None:
    """A cold source endpoint times out sqlpackage's own connection attempt."""
    waited: list[str] = []
    monkeypatch.setattr(
        sqlschema, "wait_for_database", lambda server, db, tokens, on_progress=None: waited.append(server)
    )
    monkeypatch.setattr(sqlschema, "extract_dacpac", lambda **kwargs: tmp_path / "a.dacpac")
    monkeypatch.setattr(sqlschema, "unpack_dacpac", lambda *a, **k: tmp_path / "Deploy.sql")
    monkeypatch.setattr(sqlschema, "apply_script", lambda *a, **k: [])

    sqlschema.transfer_schema(
        source_server="source.sql",
        target_server="target.sql",
        database="CloneTest",
        principal=object(),
        tokens=object(),
        scratch_dir=tmp_path,
        source_type="Lakehouse",
    )

    assert waited == ["source.sql", "target.sql"]


def test_the_source_is_waited_for_before_extracting(monkeypatch, tmp_path: Path) -> None:
    order: list[str] = []
    monkeypatch.setattr(
        sqlschema,
        "wait_for_database",
        lambda server, db, tokens, on_progress=None: order.append(f"wait:{server}"),
    )

    def fake_extract(**kwargs):
        order.append("extract")
        return tmp_path / "a.dacpac"

    monkeypatch.setattr(sqlschema, "extract_dacpac", fake_extract)
    monkeypatch.setattr(sqlschema, "unpack_dacpac", lambda *a, **k: tmp_path / "Deploy.sql")
    monkeypatch.setattr(sqlschema, "apply_script", lambda *a, **k: [])

    sqlschema.transfer_schema(
        source_server="source.sql",
        target_server="target.sql",
        database="X",
        principal=object(),
        tokens=object(),
        scratch_dir=tmp_path,
        source_type="Warehouse",
    )

    assert order.index("wait:source.sql") < order.index("extract")


# ------------------------------------------------------------- shortcut messages


def test_a_conflict_says_the_name_is_taken() -> None:
    message = shortcuts.describe_failure("random_table", ONELAKE, 409)

    # Deliberately not "a shortcut of that name": whatever holds the name need not be a
    # shortcut, and saying so sent us hunting for a shortcut that was never there.
    assert "already there" in message
    assert "shortcut of that name" not in message
    assert "connection" not in message


def test_an_unreadable_status_repeats_what_the_service_said() -> None:
    """A 400 tells us nothing on its own, so guessing is worse than quoting."""
    message = shortcuts.describe_failure(
        "dbo_movies", ONELAKE, 400, said="InvalidPath The specified path is not supported"
    )

    assert "The specified path is not supported" in message
    assert "the request was rejected" not in message


def test_a_recognised_status_still_carries_the_detail() -> None:
    message = shortcuts.describe_failure("dbo_movies", ONELAKE, 404, said="ItemNotFound no such item")

    # Our reading of the status is a guess, so the service's own words go alongside it.
    assert "does not exist in the new workspace" in message
    assert "ItemNotFound no such item" in message


def test_a_status_with_nothing_said_still_reads_as_a_sentence() -> None:
    message = shortcuts.describe_failure("dbo_movies", ONELAKE, 400)

    assert message.endswith("the request was rejected. Recreate it by hand.")


def test_the_detail_joins_the_code_and_the_message() -> None:
    error = FabricApiError(
        "POST", "url", 400, '{"errorCode":"InvalidPath","message":"not supported"}'
    )

    assert shortcuts.failure_detail(error) == "InvalidPath not supported"


def test_a_missing_internal_target_blames_the_item_not_the_connection() -> None:
    message = shortcuts.describe_failure("dbo_movies", ONELAKE, 404)

    assert "does not exist in the new workspace" in message
    assert "connection" not in message


def test_a_forbidden_internal_target_asks_for_workspace_access() -> None:
    message = shortcuts.describe_failure("adventureworks", ONELAKE, 403)

    assert "service principal" in message
    assert "Grant it access" in message


def test_a_forbidden_external_target_does_blame_the_connection() -> None:
    # The one case where the original message was right.
    message = shortcuts.describe_failure("adventureworks", EXTERNAL, 403)

    assert "connection denied access" in message


def test_a_missing_external_target_talks_about_the_path() -> None:
    assert "path it points at" in shortcuts.describe_failure("x", EXTERNAL, 404)


def test_an_unexpected_status_still_produces_something_useful() -> None:
    message = shortcuts.describe_failure("x", ONELAKE, 500)

    assert "HTTP 500" in message
    assert "Recreate it by hand" in message


def test_the_label_distinguishes_kql_table_shortcuts() -> None:
    message = shortcuts.describe_failure("t", ONELAKE, 409, label="KQL table shortcut")

    assert message.startswith("KQL table shortcut 't'")


@pytest.mark.parametrize("status", [409, 404, 403, 500])
def test_every_message_names_the_shortcut_and_status(status: int) -> None:
    message = shortcuts.describe_failure("my_shortcut", ONELAKE, status)

    assert "'my_shortcut'" in message
    assert f"HTTP {status}" in message


class FailingClient:
    def __init__(self, status: int) -> None:
        self.status = status

    def list_all(self, path, params=None, value_key="value"):
        return [{"name": "dbo_movies", "path": "Tables", "target": ONELAKE}]

    def post(self, path, json=None, params=None, wait=True):
        raise FabricApiError("POST", path, self.status, "denied")


def test_copy_shortcuts_reports_the_specific_reason() -> None:
    created, warnings = shortcuts.copy_shortcuts(
        FailingClient(404), "ws-source", "item-source", "ws-target", "item-target", {}
    )

    assert created == 0
    assert "does not exist in the new workspace" in warnings[0]
