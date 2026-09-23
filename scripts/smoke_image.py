"""Check pinned tools, packaged Python dependencies and a credential-free HTTP startup."""

import importlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]


def output(command: list[str]) -> str:
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"{command} exited {result.returncode}:\n{result.stdout}")
    return result.stdout.strip()


def verify_python() -> None:
    for profile in ("bootstrap", "build", "runtime"):
        text = (ROOT / "requirements" / f"{profile}.txt").read_text(encoding="utf-8")
        for line in text.replace("\\\n", "").splitlines():
            if not line or line.startswith("#"):
                continue
            requirement = Requirement(line.split("--hash=", 1)[0].strip())
            if requirement.marker is None or requirement.marker.evaluate():
                actual = importlib.metadata.version(requirement.name)
                if not requirement.specifier.contains(actual):
                    raise RuntimeError(
                        f"{requirement.name}: installed {actual}, expected {requirement.specifier}"
                    )
    for module in ("pyodbc", "azure.kusto.data", "azure.cosmos", "msal", "fabshuffle.web.app"):
        importlib.import_module(module)
    print("Pinned Python dependencies and application imports OK.", flush=True)


def verify_tools(pins: dict) -> None:
    if output(["dotnet", "--version"]) != pins["dotnet_sdk_version"]:
        raise RuntimeError("Installed .NET SDK does not match tools.lock.json")
    for package, version in pins["apt_packages"].items():
        actual = output(["dpkg-query", "-W", "-f=${Version}", package])
        if actual != version:
            raise RuntimeError(f"{package}: installed {actual}, expected {version}")
    tools = output(["dotnet", "tool", "list", "--global"]).lower().splitlines()
    for package, pin in pins["nuget_tools"].items():
        if not any(line.split()[:2] == [package.lower(), pin["version"]] for line in tools):
            raise RuntimeError(f"{package} {pin['version']} missing from dotnet tool list")
    for command, expected in pins["version_commands"]:
        actual = output(command)
        if expected not in actual:
            raise RuntimeError(f"{command}: expected {expected!r}, got {actual!r}")
        print(actual, flush=True)
    import pyodbc

    if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
        raise RuntimeError("Microsoft ODBC Driver 18 is not registered")


def verify_health() -> None:
    if sys.platform != "linux":
        raise RuntimeError("Run the image smoke check inside the Linux container.")
    # The build container is isolated; no host port is published and no tenant is contacted.
    with tempfile.TemporaryDirectory(prefix="fab-shuffle-smoke-") as scratch, tempfile.TemporaryFile(
        mode="w+", encoding="utf-8",
    ) as log:
        env = {
            **os.environ,
            "FAB_SHUFFLE_HOST": "127.0.0.1",
            "FAB_SHUFFLE_PORT": "18080",
            "FAB_SHUFFLE_SCRATCH": scratch,
        }
        process = subprocess.Popen(
            [sys.executable, "-m", "fabshuffle"], env=env, cwd=scratch,
            stdout=log, stderr=subprocess.STDOUT,
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Application exited during startup: {process.returncode}")
                try:
                    with opener.open("http://127.0.0.1:18080/api/health", timeout=2) as response:
                        health = json.load(response)
                    if health != {"status": "ok", "version": importlib.metadata.version("fab-shuffle")}:
                        raise RuntimeError(f"Unexpected health response: {health}")
                    break
                except urllib.error.URLError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)
            for path in ("/", "/static/app.js", "/static/auth.js", "/static/styles.css",
                         "/static/bcdr.js", "/static/bcdr-workflows.js"):
                with opener.open(f"http://127.0.0.1:18080{path}", timeout=2) as response:
                    if response.status != 200 or not response.read():
                        raise RuntimeError(f"Packaged asset unavailable: {path}")
            print("Credential-free HTTP health and packaged assets OK.", flush=True)
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            log.seek(0)
            print(log.read(), end="", flush=True)


def main() -> None:
    pins = json.loads((ROOT / "tools.lock.json").read_text(encoding="utf-8"))
    verify_python()
    verify_tools(pins)
    verify_health()


if __name__ == "__main__":
    main()
