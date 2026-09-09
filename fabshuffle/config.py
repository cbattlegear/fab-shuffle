"""Runtime configuration for Fab Shuffle."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

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
    max_staging_bytes: int = field(
        default_factory=lambda: _env_int("FAB_SHUFFLE_MAX_STAGING_BYTES", 1024 ** 3)
    )
    sqlpackage_path: str = os.environ.get("FAB_SHUFFLE_SQLPACKAGE", "sqlpackage")
    unpackdacpac_path: str = os.environ.get("FAB_SHUFFLE_UNPACKDACPAC", "unpackdacpac")
    azcopy_path: str = os.environ.get("FAB_SHUFFLE_AZCOPY", "azcopy")
    bcp_path: str = os.environ.get("FAB_SHUFFLE_BCP", "bcp")

    def scratch_dir_for(self, run_id: str) -> Path:
        path = self.scratch_root / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def journal_dir(self) -> Path:
        """Where run journals live.

        Deliberately a sibling of the per-run scratch directories rather than a child of one:
        ``cleanup_run`` deletes those when a run succeeds, and a finished run's journal is what
        makes retrying the items it left behind possible.
        """
        return self.scratch_root / "journal"

    def journal_dir_for(
        self, *, source_tenant_id: str = "", target_tenant_id: str = "",
    ) -> Path:
        """Keep paired runs outside the directory consumed by legacy single-client builds."""
        source = source_tenant_id.strip().casefold()
        target = target_tenant_id.strip().casefold()
        if bool(source) != bool(target):
            raise ValueError("Journal access requires both source and target tenant IDs.")
        if not source:
            return self.journal_dir
        pair = sha256(f"{source}\0{target}".encode()).hexdigest()
        return self.scratch_root / "journal-paired-v1" / pair

    def journal_for(
        self, run_id: str, *, source_tenant_id: str = "", target_tenant_id: str = "",
    ) -> Path:
        return self.journal_dir_for(
            source_tenant_id=source_tenant_id, target_tenant_id=target_tenant_id,
        ) / f"{run_id}.jsonl"

    def journal_dir_for_plan(self, plan: Mapping[str, Any]) -> Path:
        """Select recovery storage from the same serialized plan used for admission."""
        return self.journal_dir_for(
            source_tenant_id=plan.get("source_tenant_id", ""),
            target_tenant_id=plan.get("target_tenant_id", ""),
        )

    def journal_for_plan(self, run_id: str, plan: Mapping[str, Any]) -> Path:
        return self.journal_dir_for_plan(plan) / f"{run_id}.jsonl"


SETTINGS = Settings()
