"""OneLake file transfer.

Copy Jobs handle table data, but they do not distinguish real files from shortcuts under
``Files/``. OneLake also has no server-side copy API, so file payloads go through azcopy
with a local staging hop (a direct OneLake-to-OneLake azcopy transfer fails).
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

from fabshuffle.auth import ServicePrincipal
from fabshuffle.config import SETTINGS

logger = logging.getLogger(__name__)

TRUSTED_SUFFIXES = "onelake.dfs.fabric.microsoft.com"


class FileTransferError(RuntimeError):
    """azcopy could not move the OneLake files."""


def _azcopy_env(principal: ServicePrincipal) -> dict[str, str]:
    import os

    return {
        **os.environ,
        "AZCOPY_AUTO_LOGIN_TYPE": "SPN",
        "AZCOPY_SPA_APPLICATION_ID": principal.client_id,
        "AZCOPY_SPA_CLIENT_SECRET": principal.client_secret,
        "AZCOPY_TENANT_ID": principal.tenant_id,
    }


def copy_files(
    *,
    source_files_path: str,
    target_files_path: str,
    principal: ServicePrincipal,
    scratch_dir: Path,
    on_progress: Callable[[str], None] | None = None,
) -> None:
    """Stage ``Files/`` from the source lakehouse locally, then upload to the target."""
    staging = scratch_dir / "files"
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    try:
        if on_progress:
            on_progress("Downloading OneLake files")
        _azcopy(["copy", f"{source_files_path.rstrip('/')}/*", str(staging), "--recursive"], principal)

        if not any(staging.iterdir()):
            if on_progress:
                on_progress("No files to transfer")
            return

        if on_progress:
            on_progress("Uploading OneLake files")
        _azcopy(["copy", f"{staging}/*", target_files_path, "--recursive"], principal)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _azcopy(args: list[str], principal: ServicePrincipal) -> None:
    command = [SETTINGS.azcopy_path, *args, f"--trusted-microsoft-suffixes={TRUSTED_SUFFIXES}"]
    result = subprocess.run(command, capture_output=True, text=True, env=_azcopy_env(principal))
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-1500:]
        raise FileTransferError(f"azcopy {args[0]} failed with exit code {result.returncode}: {detail}")


__all__ = ["FileTransferError", "copy_files"]
