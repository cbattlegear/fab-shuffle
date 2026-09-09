"""The supported validation path cannot succeed by skipping absent runtime tools."""

import subprocess

import pytest

from scripts import run_container_tests as runner


def test_validation_refuses_native_host_python(monkeypatch):
    monkeypatch.setattr(runner.sys, "platform", "win32")
    with pytest.raises(RuntimeError, match="not host Python"):
        runner.require_runtime()


def test_validation_refuses_linux_host_without_docker_marker(monkeypatch):
    monkeypatch.setattr(runner.sys, "platform", "linux")
    monkeypatch.setattr(runner.Path, "is_file", lambda _: False)
    with pytest.raises(RuntimeError, match="Dockerfile test target"):
        runner.require_runtime()


@pytest.mark.parametrize("missing", ["node", "pwsh", "sqlpackage", "unpackdacpac", "azcopy", "bcp"])
def test_required_tool_absence_fails_before_pytest_can_skip(monkeypatch, missing):
    monkeypatch.setattr(runner.sys, "platform", "linux")
    monkeypatch.setattr(runner.Path, "is_file", lambda _: True)
    monkeypatch.setattr(runner.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(runner.shutil, "which", lambda name: None if name == missing else f"/tools/{name}")
    with pytest.raises(RuntimeError, match=f"missing required tools: {missing}"):
        runner.require_runtime()


def test_validation_runs_smoke_lock_lint_and_selected_tests_in_container(monkeypatch):
    monkeypatch.setattr(runner, "require_runtime", lambda: None)
    commands = []
    monkeypatch.setattr(runner.subprocess, "run", lambda command, **kw: commands.append((command, kw)))
    runner.validate(["tests/test_mirror_activation.py"])
    assert commands[0][0] == ["node", "--version"]
    assert commands[1][0][0] == "pwsh"
    assert [entry[0][1:] for entry in commands[2:]] == [
        ["-m", "uv", "pip", "check", "--system"],
        ["scripts/lock_dependencies.py", "--check"],
        ["scripts/smoke_image.py"],
        ["-m", "ruff", "check", "."],
        ["-m", "pytest", "-q", "-rs", "tests/test_mirror_activation.py"],
    ]
    assert all(options == {"cwd": runner.ROOT, "check": True} for _, options in commands)


def test_failed_validation_step_stops_the_remaining_commands(monkeypatch):
    monkeypatch.setattr(runner, "require_runtime", lambda: None)
    commands = []
    def fail(command, **kwargs):
        commands.append(command)
        raise subprocess.CalledProcessError(3, command)
    monkeypatch.setattr(runner.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError) as error:
        runner.validate([])
    assert error.value.returncode == 3
    assert commands == [["node", "--version"]]
