"""Running a sqlpackage deployment script over a plain TDS connection.

sqlpackage writes Deploy.sql for the *sqlcmd utility*, not for a server. It opens with
``:setvar`` directives and refers to them as ``$(Name)`` throughout. v1 sidestepped this with
``Invoke-Sqlcmd -DisableCommands -DisableVariables`` plus skipping the first 44 lines; we go
over ODBC, which has no SQLCMD support at all, so the script has to be resolved first.

The reported failure was five warnings per warehouse, but the real damage was invisible: the
batch *between* them succeeded and ran ``SET NOEXEC ON``, which is connection scoped, so every
later batch silently became a no-op and the warehouse arrived with no schema.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from fabshuffle.transfer import sqlschema

# The preamble sqlpackage emits, reproduced from the batches that failed in a real run.
DEPLOY_SQL = '''/*
Deployment script for DacTest-7cbd3c3d
*/

GO
SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON;

SET NUMERIC_ROUNDABORT OFF;


GO
:setvar DatabaseName "DacTest-7cbd3c3d"
:setvar DefaultFilePrefix "DacTest-7cbd3c3d"
:setvar DefaultDataPath ""
:setvar DefaultLogPath ""

GO
:on error exit
GO
/*
Detect SQLCMD mode and disable script execution if SQLCMD mode is not supported.
To re-enable the script after enabling SQLCMD mode, execute the following:
SET NOEXEC OFF;
*/
:setvar __IsSqlCmdEnabled "True"
GO
IF N'$(__IsSqlCmdEnabled)' NOT IN (N'True',N'False')
    BEGIN
        PRINT N'SQLCMD mode must be enabled to successfully execute this script.';
        SET NOEXEC ON;
    END


GO
USE [$(DatabaseName)];

GO
CREATE SCHEMA [sales];


GO
CREATE TABLE [sales].[Orders] (
    [Id] INT NOT NULL
);


GO
'''


def resolved(script: str = DEPLOY_SQL) -> list[str]:
    trimmed = sqlschema._strip_sqlcmd_header(script)
    return sqlschema._batches(sqlschema.resolve_sqlcmd(trimmed))


def test_no_batch_still_contains_a_sqlcmd_directive() -> None:
    """These are utility directives; the server reports them as syntax errors (42000)."""
    for batch in resolved():
        for line in batch.splitlines():
            assert not line.lstrip().startswith(":"), batch


def test_no_batch_still_contains_an_unsubstituted_variable() -> None:
    assert not any("$(" in batch for batch in resolved())


def test_the_noexec_guard_never_reaches_the_server() -> None:
    """The real bug. NOEXEC is connection scoped, so one stray batch voids the whole script."""
    assert not any(sqlschema._NOEXEC.search(batch) for batch in resolved())


def test_comment_only_batches_are_dropped() -> None:
    # Removing the directives can leave a batch holding only the comment that introduced them.
    assert not any(sqlschema._is_noise(batch) for batch in resolved())


def test_use_statements_are_dropped() -> None:
    # We are already connected to the target database, and Fabric rejects USE (08004).
    assert not any("USE " in batch.upper() for batch in resolved())


def test_the_actual_schema_survives() -> None:
    batches = resolved()

    assert any("CREATE SCHEMA [sales]" in batch for batch in batches)
    assert any("CREATE TABLE [sales].[Orders]" in batch for batch in batches)


def test_the_leading_set_options_batch_is_kept() -> None:
    # It is real T-SQL and it succeeded against Fabric in the failing run, so dropping it
    # would be a silent behaviour change.
    assert any("SET ANSI_NULLS" in batch for batch in resolved())


def test_setvar_values_are_substituted_where_they_are_used() -> None:
    script = ':setvar DatabaseName "Target-123"\nGO\nPRINT N\'$(DatabaseName)\';\n'

    assert sqlschema.resolve_sqlcmd(script).strip().endswith("PRINT N'Target-123';")


def test_an_unquoted_setvar_value_is_read() -> None:
    script = ":setvar Edition Premium\nGO\nPRINT N'$(Edition)';\n"

    assert "Premium" in sqlschema.resolve_sqlcmd(script)


def test_an_empty_setvar_value_is_read() -> None:
    script = ':setvar DefaultDataPath ""\nGO\nPRINT N\'$(DefaultDataPath)x\';\n'

    resolved_script = sqlschema.resolve_sqlcmd(script)
    assert "$(" not in resolved_script
    assert "PRINT N'x';" in resolved_script


def test_an_unknown_variable_is_left_alone_rather_than_blanked() -> None:
    # Blanking it would silently corrupt a statement; leaving it fails loudly instead.
    assert "$(NotDeclared)" in sqlschema.resolve_sqlcmd("PRINT N'$(NotDeclared)';")


def test_a_script_with_no_sqlcmd_at_all_is_untouched() -> None:
    script = "CREATE TABLE [dbo].[T] ([Id] INT);\n"

    assert sqlschema.resolve_sqlcmd(script) == script


def test_a_colon_inside_a_statement_is_not_mistaken_for_a_directive() -> None:
    # Only a directive at the start of a line is a directive.
    script = "SELECT CAST('12:30' AS TIME);\n"

    assert sqlschema.resolve_sqlcmd(script) == script


def test_the_whole_preamble_costs_no_warnings(tmp_path: Path, monkeypatch) -> None:
    """End to end: the five reported failures should simply not happen any more."""
    executed: list[str] = []

    class FakeCursor:
        def execute(self, batch: str) -> None:
            executed.append(batch)

    class FakeConnection:
        autocommit = False

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def __enter__(self) -> FakeConnection:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(sqlschema, "connect", lambda *a, **k: FakeConnection())

    script_path = tmp_path / "Deploy.sql"
    script_path.write_text(DEPLOY_SQL, encoding="utf-8")

    warnings = sqlschema.apply_script(
        script_path, server="srv", database="DacTest", tokens=object()
    )

    assert warnings == []
    assert any("CREATE TABLE" in batch for batch in executed)
    assert not any(sqlschema._NOEXEC.search(batch) for batch in executed)
    # Every batch sent must be executable, not leftover commentary.
    assert all(batch.strip() for batch in executed)


@pytest.mark.parametrize(
    "statement",
    [
        "USE [$(DatabaseName)];",
        "USE [DacTest-7cbd3c3d];",
        "  use MyDb  ",
        'USE "Quoted";',
        "USE MyDb",
        "\tUSE\t[Tabbed]\t;\t",
    ],
)
def test_use_statement_shapes_are_all_removed(statement: str) -> None:
    assert sqlschema.resolve_sqlcmd(statement).strip() == ""


def test_a_use_sharing_a_batch_with_real_schema_is_still_removed() -> None:
    """The whole point: a USE that is not alone in its batch must not survive."""
    script = "USE [Target];\nCREATE TABLE [dbo].[T] ([Id] INT);\n"

    resolved_script = sqlschema.resolve_sqlcmd(script)

    assert "USE" not in resolved_script.upper()
    assert "CREATE TABLE [dbo].[T]" in resolved_script


def test_every_use_in_a_script_is_removed_not_just_the_first() -> None:
    script = "USE [A];\nGO\nCREATE TABLE [dbo].[T] ([Id] INT);\nGO\nUSE [B];\nGO\n"

    assert "USE" not in sqlschema.resolve_sqlcmd(script).upper()


def test_a_table_named_like_a_keyword_is_not_mangled() -> None:
    assert not sqlschema._is_noise("CREATE TABLE [dbo].[Uses] ([Id] INT);")


def test_use_inside_a_string_literal_is_left_alone() -> None:
    # Removal is line anchored, so only a statement on its own line is taken.
    script = "INSERT INTO [dbo].[T] ([Note]) VALUES (N'USE [Something];');\n"

    assert sqlschema.resolve_sqlcmd(script) == script


def test_a_column_default_mentioning_use_survives() -> None:
    script = "ALTER TABLE [dbo].[T] ADD CONSTRAINT [D] DEFAULT N'use me' FOR [Note];\n"

    assert sqlschema.resolve_sqlcmd(script) == script
