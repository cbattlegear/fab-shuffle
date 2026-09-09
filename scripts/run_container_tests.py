"""One Docker-only entrypoint for local and CI validation of the production runtime."""

from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require_runtime() -> None:
    if sys.platform != "linux" or not Path("/.dockerenv").is_file():
        raise RuntimeError("Run validation with the Dockerfile test target, not host Python.")
    if platform.machine().lower() not in ("x86_64", "amd64"):
        raise RuntimeError("Use --platform linux/amd64 for production-runtime validation.")
    missing = [tool for tool in ("node", "pwsh", "sqlpackage", "unpackdacpac", "azcopy", "bcp")
               if shutil.which(tool) is None]
    if missing:
        raise RuntimeError(
            f"Test image is missing required tools: {', '.join(missing)}. "
            "Rebuild the Dockerfile test target; do not skip these checks."
        )


def validate(selectors: list[str]) -> None:
    require_runtime()
    commands = [
        ["node", "--version"],
        ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
         "$PSVersionTable.PSVersion.ToString()"],
        [sys.executable, "-m", "uv", "pip", "check", "--system"],
        [sys.executable, "scripts/lock_dependencies.py", "--check"],
        [sys.executable, "scripts/smoke_image.py"],
        [sys.executable, "-m", "ruff", "check", "."],
        [sys.executable, "-m", "pytest", "-q", "-rs", *selectors],
    ]
    for command in commands:
        print(f"\n> {' '.join(command)}", flush=True)
        subprocess.run(command, cwd=ROOT, check=True)


if __name__ == "__main__":
    try:
        validate(sys.argv[1:])
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from error
