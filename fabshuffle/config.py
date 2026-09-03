"""Runtime configuration for Fab Shuffle."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"

# Token audiences. Client-credentials flow requires the ``/.default`` suffix.
SCOPE_FABRIC = "https://api.fabric.microsoft.com/.default"
SCOPE_STORAGE = "https://storage.azure.com/.default"
SCOPE_KUSTO = "https://kusto.kusto.windows.net/.default"
SCOPE_SQL = "https://database.windows.net/.default"
# Power BI rejects tokens issued for any other audience, so its endpoints cannot reuse
# the Fabric token even though the two services overlap.
SCOPE_POWERBI = "https://analysis.windows.net/powerbi/api/.default"

AUTHORITY_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


@dataclass(slots=True)
class Settings:
    """Process level settings, all overridable through environment variables."""

    scratch_root: Path = field(
        default_factory=lambda: Path(os.environ.get("FAB_SHUFFLE_SCRATCH", "./local")).resolve()
    )
    host: str = os.environ.get("FAB_SHUFFLE_HOST", "0.0.0.0")
    port: int = _env_int("FAB_SHUFFLE_PORT", 8080)

    # Fabric REST client behaviour.
    request_timeout_seconds: int = _env_int("FAB_SHUFFLE_REQUEST_TIMEOUT", 120)
    max_retries: int = _env_int("FAB_SHUFFLE_MAX_RETRIES", 6)
    lro_poll_seconds: int = _env_int("FAB_SHUFFLE_LRO_POLL_SECONDS", 5)
    lro_timeout_seconds: int = _env_int("FAB_SHUFFLE_LRO_TIMEOUT_SECONDS", 3600)

    # Copy job / data movement behaviour.
    copy_job_poll_seconds: int = _env_int("FAB_SHUFFLE_COPY_JOB_POLL_SECONDS", 10)
    copy_job_timeout_seconds: int = _env_int("FAB_SHUFFLE_COPY_JOB_TIMEOUT_SECONDS", 43200)
    # How many Copy Jobs to have running at once. Zero means work it out from the target
    # capacity's SKU, which is the sensible default because a Copy Job runs on that capacity:
    # see ``workspaces.copy_job_concurrency``. Any other value overrides that outright.
    copy_job_concurrency: int = _env_int("FAB_SHUFFLE_COPY_JOB_CONCURRENCY", 0)
    sql_endpoint_timeout_seconds: int = _env_int("FAB_SHUFFLE_SQL_ENDPOINT_TIMEOUT_SECONDS", 1800)

    # External tooling that has no REST equivalent yet.
    # Both of these are bounded low on purpose. sqlpackage is a .NET process of a few hundred
    # megabytes; azcopy tunes its own concurrency and stages whole directories through local
    # disk. Several at once compete for the same memory and disk rather than going faster.
    schema_transfer_concurrency: int = _env_int("FAB_SHUFFLE_SCHEMA_CONCURRENCY", 2)
    file_transfer_concurrency: int = _env_int("FAB_SHUFFLE_FILE_CONCURRENCY", 2)
    sqlpackage_path: str = os.environ.get("FAB_SHUFFLE_SQLPACKAGE", "sqlpackage")
    unpackdacpac_path: str = os.environ.get("FAB_SHUFFLE_UNPACKDACPAC", "unpackdacpac")
    azcopy_path: str = os.environ.get("FAB_SHUFFLE_AZCOPY", "azcopy")
    bcp_path: str = os.environ.get("FAB_SHUFFLE_BCP", "bcp")

    def scratch_dir_for(self, run_id: str) -> Path:
        path = self.scratch_root / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path


SETTINGS = Settings()
