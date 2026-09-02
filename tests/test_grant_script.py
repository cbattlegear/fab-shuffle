"""The PowerShell script that shares connections with the service principal.

Finding a connection in the portal means matching a bare GUID by eye against a list that does
not show ids. The same grant is one API call, so the ids that make it unreasonable by hand
are exactly what make it easy in a script.

These tests check the script's shape rather than run it. A companion test parses it with the
PowerShell parser when one is available, so a syntax error cannot ship silently.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from fabshuffle.orchestrator import ConnectionAccess, grant_script

APP_ID = "9b57d15c-f03f-4112-adb8-b480df80bd02"
ENTRIES = [
    ConnectionAccess(
        connection_id="081da81e-f477-4715-8b66-c2a1debf8909",
        used_by=("MirroredDatabase 'BattleCabbageReplTest'",),
    ),
    ConnectionAccess(
        connection_id="aa89a365-f638-49a8-81d7-ded77940ce84",
        used_by=("CopyJob 'copyjob1'", "DataPipeline 'CopyRunFromDB'"),
    ),
]


def test_every_connection_is_included() -> None:
    script = grant_script(APP_ID, ENTRIES)

    for entry in ENTRIES:
        assert f"'{entry.connection_id}'" in script


def test_each_id_is_commented_with_what_needs_it() -> None:
    """The ids are unreadable, so the comment is the only way to tell them apart."""
    script = grant_script(APP_ID, ENTRIES)

    assert "# MirroredDatabase 'BattleCabbageReplTest'" in script
    assert "# CopyJob 'copyjob1', DataPipeline 'CopyRunFromDB'" in script


def test_the_object_id_is_resolved_rather_than_assumed() -> None:
    """The role assignment API wants the object id; an operator has the application id."""
    script = grant_script(APP_ID, ENTRIES)

    assert "Get-AzADServicePrincipal -ApplicationId $applicationId" in script
    assert "id = $principal.Id" in script
    assert f"'{APP_ID}'" in script


def test_it_asks_for_user_not_owner() -> None:
    assert "role      = 'User'" in grant_script(APP_ID, ENTRIES)


def test_it_posts_to_the_role_assignment_endpoint() -> None:
    script = grant_script(APP_ID, ENTRIES)

    assert "/v1/connections/$id/roleAssignments" in script
    assert "-Method POST" in script


def test_a_secure_string_token_is_handled() -> None:
    # Newer Az versions return a SecureString, older ones a plain string.
    script = grant_script(APP_ID, ENTRIES)

    assert "$token -isnot [string]" in script
    assert "NetworkCredential" in script


def test_one_failure_does_not_stop_the_others() -> None:
    """An operator may own some of these connections and not others."""
    script = grant_script(APP_ID, ENTRIES)

    assert "try {" in script
    assert "Write-Warning" in script


def test_a_hostile_item_name_cannot_break_out_of_its_comment() -> None:
    entry = ConnectionAccess(
        connection_id="c-1",
        used_by=("Pipeline 'evil\nRemove-Item C:\\ -Recurse'",),
    )

    script = grant_script(APP_ID, [entry])
    listed = [line for line in script.splitlines() if "c-1" in line]

    assert len(listed) == 1
    # The newline is gone, so the injected command stays inside the comment.
    assert listed[0].lstrip().startswith("'c-1'")
    assert "\n" not in listed[0]


def test_a_very_long_name_is_trimmed() -> None:
    entry = ConnectionAccess(connection_id="c-1", used_by=("x" * 500,))

    line = next(line for line in grant_script(APP_ID, [entry]).splitlines() if "c-1" in line)

    assert len(line) < 140


def test_an_empty_list_still_produces_a_runnable_script() -> None:
    script = grant_script(APP_ID, [])

    assert "$connectionIds = @(" in script
    assert "foreach ($id in $connectionIds)" in script


@pytest.mark.skipif(
    not (shutil.which("pwsh") or shutil.which("powershell")),
    reason="no PowerShell available to parse with",
)
def test_the_generated_script_is_valid_powershell() -> None:
    """Generated code that does not parse is worse than no code at all."""
    shell = shutil.which("pwsh") or shutil.which("powershell")

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "grant.ps1"
        path.write_text(grant_script(APP_ID, ENTRIES), encoding="utf-8")

        check = (
            "$e=$null; $t=$null; "
            f"[System.Management.Automation.Language.Parser]::ParseFile('{path}', "
            "[ref]$t, [ref]$e) | Out-Null; "
            "if ($e) { $e | ForEach-Object { $_.Message }; exit 1 } else { exit 0 }"
        )
        result = subprocess.run(
            [shell, "-NoProfile", "-NonInteractive", "-Command", check],
            capture_output=True,
            text=True,
            timeout=120,
        )

    assert result.returncode == 0, result.stdout + result.stderr
