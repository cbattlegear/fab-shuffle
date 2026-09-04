"""Moving SQL database rows with bcp.

A Copy Job cannot do this. The SQL database in Fabric connector accepts only an
organizational account, so there is no connection a service principal can create that a job
could use; the one we tried failed with an invalid token. bcp supports SQL database in
Fabric directly and takes an access token from a file, which is the token we already hold.
"""

from __future__ import annotations

import subprocess

import pytest

from fabshuffle.fabric.data_stores import TableRef
from fabshuffle.transfer import bulkcopy


class FakeTokens:
    def __init__(self, token="header.payload.signature") -> None:
        self.token = token
        self.asked = 0

    def sql_token(self):
        self.asked += 1
        return self.token


def record_runs(monkeypatch, *, fail_on=None, stdout="10 rows copied."):
    runs: list[list[str]] = []

    def run(command, capture_output=True, text=True, check=False):
        runs.append(list(command))
        failed = fail_on is not None and fail_on in " ".join(command)

        class Result:
            returncode = 1 if failed else 0
            stderr = "the table went away" if failed else ""

        Result.stdout = "" if failed else stdout
        return Result()

    monkeypatch.setattr(subprocess, "run", run)
    return runs


def copy(monkeypatch, tmp_path, tables=(TableRef(name="Orders", schema="dbo"),), **kwargs):
    runs = record_runs(monkeypatch, **kwargs)
    warnings = bulkcopy.copy_tables(
        source_server="src.database.fabric.microsoft.com,1433",
        source_database="Sales-1111",
        target_server="dst.database.fabric.microsoft.com,1433",
        target_database="Sales-2222",
        tables=list(tables),
        tokens=FakeTokens(),
        scratch_dir=tmp_path / "bcp",
    )
    return runs, warnings


# ------------------------------------------------------------- the token file


def test_the_token_is_written_as_utf16le_with_no_bom():
    tokens = FakeTokens("abc")
    with bulkcopy.token_file(tokens) as path:
        raw = path.read_bytes()

    # bcp reads UTF-16LE and rejects a byte order mark.
    assert raw == b"a\x00b\x00c\x00"
    assert not raw.startswith(b"\xff\xfe")


def test_the_token_file_is_owner_only_and_removed_afterwards():
    import os
    import stat

    tokens = FakeTokens()
    with bulkcopy.token_file(tokens) as path:
        assert path.exists()
        mode = stat.S_IMODE(os.stat(path).st_mode)
        if os.name != "nt":
            # It holds a live credential for as long as it exists.
            assert mode == 0o600
    assert not path.exists()


def test_the_token_file_goes_away_even_when_the_copy_raises():
    tokens = FakeTokens()
    captured = {}
    with pytest.raises(RuntimeError):
        with bulkcopy.token_file(tokens) as path:
            captured["path"] = path
            raise RuntimeError("boom")

    assert not captured["path"].exists()


def test_one_token_serves_the_whole_batch(monkeypatch, tmp_path):
    tokens = FakeTokens()
    record_runs(monkeypatch)
    bulkcopy.copy_tables(
        source_server="a",
        source_database="b",
        target_server="c",
        target_database="d",
        tables=[TableRef(name=f"T{n}") for n in range(4)],
        tokens=tokens,
        scratch_dir=tmp_path,
    )

    assert tokens.asked == 1


# --------------------------------------------------------------- the commands


def test_each_table_goes_out_then_in(monkeypatch, tmp_path):
    runs, warnings = copy(monkeypatch, tmp_path)

    assert warnings == []
    assert [command[2] for command in runs] == ["out", "in"]
    assert runs[0][1] == "dbo.Orders"


def test_the_source_is_read_and_the_target_written(monkeypatch, tmp_path):
    runs, _ = copy(monkeypatch, tmp_path)

    out, into = runs
    assert out[out.index("-S") + 1] == "src.database.fabric.microsoft.com,1433"
    assert out[out.index("-d") + 1] == "Sales-1111"
    assert into[into.index("-S") + 1] == "dst.database.fabric.microsoft.com,1433"
    assert into[into.index("-d") + 1] == "Sales-2222"


def test_authentication_is_the_access_token_file(monkeypatch, tmp_path):
    runs, _ = copy(monkeypatch, tmp_path)

    for command in runs:
        # -G with -P pointing at a file is token auth; without -U it is not a password.
        assert "-G" in command
        assert "-U" not in command
        assert command[command.index("-P") + 1].endswith(".tok")


def test_native_format_is_used_so_types_survive(monkeypatch, tmp_path):
    runs, _ = copy(monkeypatch, tmp_path)

    # Both ends are Fabric SQL databases, so there is no reason to go through text.
    assert all("-n" in command for command in runs)
    assert all("-c" not in command for command in runs)


def test_identity_values_are_preserved_on_the_way_in(monkeypatch, tmp_path):
    runs, _ = copy(monkeypatch, tmp_path)

    # A copy whose keys differ from the original is not a copy.
    assert "-E" in runs[1]
    assert "-E" not in runs[0]


def test_the_name_is_not_bracketed(monkeypatch, tmp_path):
    """Brackets reach the server as one literal identifier: "Invalid object name '[a].[b]'".

    ``-q`` sets QUOTED_IDENTIFIER ON and expects quotation marks, not square brackets, and
    the name is a single argv entry so a space in it needs no quoting from us.
    """
    runs, _ = copy(monkeypatch, tmp_path, tables=[TableRef(name="Order Detail", schema="user")])

    assert runs[0][1] == "user.Order Detail"
    assert all("-q" in command for command in runs)


def test_a_table_without_a_schema_defaults_to_dbo(monkeypatch, tmp_path):
    runs, _ = copy(monkeypatch, tmp_path, tables=[TableRef(name="Orders")])

    assert runs[0][1] == "dbo.Orders"


# ------------------------------------------------------------------ failures


def test_one_table_failing_does_not_stop_the_rest(monkeypatch, tmp_path):
    tables = [TableRef(name="Good"), TableRef(name="Bad"), TableRef(name="AlsoGood")]
    runs, warnings = copy(monkeypatch, tmp_path, tables=tables, fail_on="dbo.Bad")

    assert len(warnings) == 1
    assert "dbo.Bad" in warnings[0]
    assert "the table went away" in warnings[0]
    # The other two still went out and in.
    assert len(runs) == 5


def test_a_missing_bcp_says_so_rather_than_failing_obscurely(monkeypatch, tmp_path):
    def missing(*_args, **_kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(subprocess, "run", missing)

    warnings = bulkcopy.copy_tables(
        source_server="a",
        source_database="b",
        target_server="c",
        target_database="d",
        tables=[TableRef(name="Orders")],
        tokens=FakeTokens(),
        scratch_dir=tmp_path,
    )

    assert "is not installed in this image" in warnings[0]


def test_the_data_file_is_removed_after_each_table(monkeypatch, tmp_path):
    scratch = tmp_path / "bcp"
    record_runs(monkeypatch)
    bulkcopy.copy_tables(
        source_server="a",
        source_database="b",
        target_server="c",
        target_database="d",
        tables=[TableRef(name="Orders")],
        tokens=FakeTokens(),
        scratch_dir=scratch,
    )

    assert list(scratch.iterdir()) == []


def test_nothing_to_copy_does_nothing(monkeypatch, tmp_path):
    runs = record_runs(monkeypatch)
    assert (
        bulkcopy.copy_tables(
            source_server="a",
            source_database="b",
            target_server="c",
            target_database="d",
            tables=[],
            tokens=FakeTokens(),
            scratch_dir=tmp_path / "unused",
        )
        == []
    )
    assert runs == []


# ------------------------------------------------------------------ progress


def test_progress_names_the_table_and_counts_them(monkeypatch, tmp_path):
    seen: list[str] = []
    record_runs(monkeypatch)
    bulkcopy.copy_tables(
        source_server="a",
        source_database="b",
        target_server="c",
        target_database="d",
        tables=[TableRef(name="A"), TableRef(name="B")],
        tokens=FakeTokens(),
        scratch_dir=tmp_path,
        on_progress=seen.append,
    )

    assert seen == ["Copying dbo.A (1 of 2)", "Copying dbo.B (2 of 2)"]
