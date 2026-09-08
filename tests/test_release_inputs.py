"""Guard the release path against silently falling back to fresh dependency resolution."""

import hashlib
import io
import json
import re
import subprocess
import sys
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from packaging.markers import default_environment
from packaging.requirements import Requirement

from scripts import install_tools, lock_dependencies, smoke_image

ROOT = Path(__file__).resolve().parents[1]


def requirements(profile):
    text = (ROOT / "requirements" / f"{profile}.txt").read_text(encoding="utf-8")
    result = []
    for line in text.replace("\\\n", "").splitlines():
        if line and not line.startswith("#"):
            requirement = Requirement(line.split("--hash=", 1)[0].strip())
            assert re.search(r"--hash=sha256:[a-f0-9]{64}", line), requirement.name
            assert len(requirement.specifier) == 1
            assert next(iter(requirement.specifier)).operator == "=="
            assert requirement.url is None
            result.append(requirement)
    return result


def test_all_exported_dependencies_are_hashed_exact_versions():
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked = {(p["name"], p["version"]) for p in lock["package"]}
    for profile in lock_dependencies.EXPORTS:
        for requirement in requirements(profile):
            assert (requirement.name, next(iter(requirement.specifier)).version) in locked


def test_build_and_bootstrap_tools_are_part_of_the_lock():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["requires-python"] == ">=3.11"
    assert project["build-system"]["requires"] == project["dependency-groups"]["build"]
    build = {r.name: str(r.specifier) for r in requirements("build")}
    for dependency in project["build-system"]["requires"]:
        requirement = Requirement(dependency)
        assert build[requirement.name] == str(requirement.specifier)
    bootstrap = {r.name: str(r.specifier) for r in requirements("bootstrap")}
    assert bootstrap["uv"] == project["tool"]["uv"]["required-version"]
    assert "packaging" in build  # wheel's transitive build dependency must not float.
    assert "pip" in bootstrap


@pytest.mark.parametrize("platform", ["linux", "win32"])
def test_universal_exports_preserve_platform_specific_dependencies(platform):
    environment = {
        **default_environment(), "sys_platform": platform, "platform_python_implementation": "CPython",
    }
    for profile in ("runtime", "dev"):
        active = {
            r.name for r in requirements(profile)
            if r.marker is None or r.marker.evaluate(environment)
        }
        assert ("uvloop" in active) == (platform == "linux")
        assert {"pyodbc", "uvicorn", "azure-kusto-data"} <= active
        assert ("pytest" in active) == (profile == "dev")


def test_docker_consumes_locked_inputs_and_runs_smoke():
    docker = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"FROM python:3\.12\.\d+-bookworm@sha256:[a-f0-9]{64}", docker)
    assert "aka.ms/" not in docker
    for profile in ("bootstrap", "build", "runtime"):
        assert f"requirements/{profile}.txt" in docker
    assert "--require-hashes" in docker
    assert "--only-binary=:all:" in docker
    assert "--no-build-isolation --no-deps" in docker
    assert "scripts/install_tools.py" in docker
    assert "scripts/smoke_image.py" in docker
    assert "NUGET_SOURCE" in docker and "PIP_INDEX_URL" in docker
    for command in docker.replace("\\\n", "").split("&&"):
        if "pip install " in command and "--require-hashes" not in command:
            assert "--no-build-isolation --no-deps" in command


def test_ci_keeps_release_semantics_and_gates_builds_on_locked_tests():
    workflows = ROOT / ".github" / "workflows"
    release = yaml.safe_load((workflows / "docker-publish.yml").read_text(encoding="utf-8"))
    tests = yaml.safe_load((workflows / "tests.yml").read_text(encoding="utf-8"))
    assert release["jobs"]["validate"]["uses"] == "./.github/workflows/tests.yml"
    assert release["jobs"]["build"]["needs"] == "validate"
    steps = release["jobs"]["build"]["steps"]
    metadata = next(s for s in steps if s.get("id") == "meta")["with"]
    assert "type=semver,pattern={{version}}" in metadata["tags"]
    assert metadata["flavor"].strip() == "latest=auto"
    build = next(s for s in steps if s.get("id") == "build-and-push")["with"]
    assert build["push"] == "${{ github.event_name != 'pull_request' }}"
    assert "linux/arm64" in build["platforms"]
    assert any("cosign sign --yes" in s.get("run", "") for s in steps)
    for workflow in (release, tests):
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                if "uses" in step:
                    assert re.search(r"@[a-f0-9]{40}$", step["uses"])
    commands = "\n".join(s.get("run", "") for s in tests["jobs"]["test"]["steps"])
    for profile in ("bootstrap", "build", "dev"):
        assert f"requirements/{profile}.txt" in commands
    assert "--require-hashes" in commands and "--only-binary=:all:" in commands
    assert "--no-build-isolation --no-deps" in commands
    assert "scripts/lock_dependencies.py --check" in commands
    platforms = tests["jobs"]["test"]["strategy"]["matrix"]["include"]
    assert any(row["python"].startswith("3.11.") for row in platforms)
    assert any(row["os"].startswith("windows-") for row in platforms)


def test_export_check_refuses_stale_files(tmp_path, monkeypatch):
    monkeypatch.setattr(lock_dependencies, "ROOT", tmp_path)
    monkeypatch.setattr(lock_dependencies, "EXPORTS", {"runtime": []})
    monkeypatch.setattr(sys, "argv", ["lock_dependencies.py", "--check"])

    def export(command, **kwargs):
        assert "--locked" in command
        assert "--no-emit-project" in command
        assert kwargs["check"]
        return SimpleNamespace(stdout="example==1.0 --hash=sha256:abc\n")

    monkeypatch.setattr(lock_dependencies.subprocess, "run", export)
    with pytest.raises(SystemExit, match=r"runtime\.txt is stale"):
        lock_dependencies.main()
    monkeypatch.setattr(sys, "argv", ["lock_dependencies.py"])
    lock_dependencies.main()
    monkeypatch.setattr(sys, "argv", ["lock_dependencies.py", "--check"])
    lock_dependencies.main()


def test_export_resolver_failure_is_not_hidden(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["lock_dependencies.py"])

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0])

    monkeypatch.setattr(lock_dependencies.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        lock_dependencies.main()


def test_python_smoke_refuses_an_installed_version_mismatch(tmp_path, monkeypatch):
    (tmp_path / "requirements").mkdir()
    (tmp_path / "requirements" / "bootstrap.txt").write_text("uv==0.12.9\n", encoding="utf-8")
    monkeypatch.setattr(smoke_image, "ROOT", tmp_path)
    monkeypatch.setattr(smoke_image.importlib.metadata, "version", lambda _: "0.0.0")
    with pytest.raises(RuntimeError, match=r"installed 0\.0\.0"):
        smoke_image.verify_python()


def test_tool_smoke_refuses_an_installed_version_mismatch(monkeypatch):
    pins = json.loads((ROOT / "tools.lock.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(smoke_image, "output", lambda _: "0.0.0")
    with pytest.raises(RuntimeError, match="SDK does not match"):
        smoke_image.verify_tools(pins)


def test_image_health_smoke_refuses_non_container_platform_before_starting(monkeypatch):
    monkeypatch.setattr(smoke_image.sys, "platform", "win32")
    with pytest.raises(RuntimeError, match="inside the Linux container"):
        smoke_image.verify_health()


def test_both_architectures_have_hashed_external_artifacts():
    pins = json.loads((ROOT / "tools.lock.json").read_text(encoding="utf-8"))
    for architecture in ("amd64", "arm64"):
        artifacts = pins["apt_artifacts"][architecture]
        assert set(artifacts) == set(pins["apt_packages"])
        for name, artifact in artifacts.items():
            assert pins["apt_packages"][name] in artifact["url"]
        for artifact in [*artifacts.values(), pins["azcopy"]["artifacts"][architecture]]:
            assert re.fullmatch(r"[a-f0-9]{64}", artifact["sha256"])
            assert artifact["url"].startswith("https://")
            assert "aka.ms" not in artifact["url"]
            assert "latest" not in artifact["url"]
    for pin in pins["nuget_tools"].values():
        assert re.fullmatch(r"[a-f0-9]{64}", pin["sha256"])
        assert re.fullmatch(r"\d+(\.\d+)+", pin["version"])


def test_download_rejects_changed_artifact_before_install(tmp_path, monkeypatch):
    monkeypatch.setattr(install_tools.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"changed"))
    artifact = {"url": "https://example.invalid/tool", "sha256": hashlib.sha256(b"reviewed").hexdigest()}
    with pytest.raises(RuntimeError, match="sha256 mismatch"):
        install_tools.download(artifact, tmp_path / "tool")


@pytest.mark.parametrize("changed", [False, True])
def test_nuget_uses_only_selected_feed_exact_version_and_verified_bytes(tmp_path, monkeypatch, changed):
    monkeypatch.setattr(install_tools.Path, "home", lambda: tmp_path)
    source = "https://mirror.invalid/v3/index.json?name=tools&region=test"
    pins = {"nuget_tools": {
        "example.tool": {"version": "1.2.3", "sha256": hashlib.sha256(b"ok").hexdigest()},
    }}

    def install(command, **kwargs):
        assert command[:7] == ["dotnet", "tool", "install", "--global", "example.tool", "--version", "1.2.3"]
        assert kwargs["check"]
        config = ET.parse(command[-1]).getroot().find("packageSources")
        assert [element.tag for element in config] == ["clear", "add"]
        assert config.find("add").attrib == {"key": "primary", "value": source}
        store = tmp_path / ".dotnet" / "tools" / ".store" / "example.tool" / "1.2.3"
        store.mkdir(parents=True)
        (store / "example.tool.1.2.3.nupkg").write_bytes(b"bad" if changed else b"ok")

    monkeypatch.setattr(install_tools.subprocess, "run", install)
    if changed:
        with pytest.raises(RuntimeError, match="sha256 mismatch"):
            install_tools.install_nuget(pins, source)
    else:
        install_tools.install_nuget(pins, source)
