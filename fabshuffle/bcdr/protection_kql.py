"""Independent prepared KQL data only; no export from a failed primary.

The existing transfer helper streams live primary queries. That is NOT an outage
recovery input. Learn instead documents independent regional databases, synchronized
schema/policies and parallel ingestion:
https://learn.microsoft.com/fabric/security/experience-specific-guidance#real-time-intelligence
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from azure.kusto.data import ClientRequestProperties

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.protection import (
    Cancel,
    DataEndpoint,
    DataIdentity,
    ProtectionAssessment,
    ProtectionError,
    ProtectionLimits,
    evidence_ref,
    operation_cancel,
    require_independent,
    utc,
)
from fabshuffle.transfer.common import StagingBudgetError, check_cancelled
from fabshuffle.transfer.kql import _client as kql_client
from fabshuffle.transfer.kql import _ident as kql_identifier


@dataclass(frozen=True, slots=True)
class KqlTable:
    name: str
    minimum_rows: int

    def __post_init__(self) -> None:
        if not self.name or type(self.minimum_rows) is not int or self.minimum_rows < 0:
            raise ValueError("Prepared KQL tables need an explicit name and nonnegative minimum row count.")


@dataclass(frozen=True, slots=True)
class KqlInput:
    """An opaque input identity, not a SAS URL or an ingestion command."""

    resource_ref: str
    access_evidence_ref: str

    def __post_init__(self) -> None:
        evidence_ref(self.resource_ref)
        evidence_ref(self.access_evidence_ref)


@dataclass(frozen=True, slots=True)
class KqlProtection:
    source: DataEndpoint
    target: DataEndpoint
    source_region: str
    target_region: str
    captured_at: datetime
    data_as_of: datetime
    dataset_evidence_ref: str
    schema_policy_evidence_ref: str
    target_approval_ref: str
    tables: tuple[KqlTable, ...]
    inputs: tuple[KqlInput, ...]
    continuous: bool = False
    independent_active_compute: bool = False


# The integration must resolve approved input identities and perform their authenticated
# availability checks. No dynamically imported callback or code is stored in the record.
InputProbe = Callable[[tuple[KqlInput, ...]], bool]


def _probe_tables(
    protection: KqlProtection,
    tokens: TokenProvider,
    limits: ProtectionLimits,
    cancel: Cancel,
) -> None:
    properties = ClientRequestProperties()
    properties.set_option("servertimeout", timedelta(seconds=limits.timeout_seconds))
    # A named entity can resolve to a function. Do not let a readiness check call the
    # failed primary, external tables, a plugin or an RLS function on the caller's behalf.
    # https://learn.microsoft.com/kusto/api/rest/request-properties?view=microsoft-fabric
    for option in (
        "request_readonly",
        "request_readonly_hardline",
        "request_remote_entities_disabled",
        "request_external_data_disabled",
        "request_external_table_disabled",
        "request_impersonation_disabled",
        "request_callout_disabled",
        "request_sandboxed_execution_disabled",
        "request_block_row_level_security",
    ):
        properties.set_option(option, True)
    properties.set_option("query_datascope", "all")
    properties.set_option("query_results_cache_max_age", timedelta(0))
    properties.set_option("truncationmaxrecords", 2)
    properties.set_option("truncationmaxsize", limits.max_record_bytes)
    tokens.assert_active()
    with kql_client(protection.target.endpoint, tokens.principal, tokens=tokens) as client:
        for table in protection.tables:
            check_cancelled(cancel)
            tokens.assert_active()
            details = client.execute_mgmt(
                protection.target.database,
                f".show table {kql_identifier(table.name)} details | project TableName",
                properties=properties,
            ).primary_results[0]
            tokens.assert_active()
            if len(details) != 1 or details[0]["TableName"] != table.name:
                raise ProtectionError(
                    f"Prepared KQL input '{table.name}' is not an exact physical table; "
                    "materialize its data independently before recovery."
                )
            check_cancelled(cancel)
            tokens.assert_active()
            response = client.execute_query(
                protection.target.database,
                f"{kql_identifier(table.name)} | count",
                properties=properties,
            )
            tokens.assert_active()
            rows = response.primary_results[0]
            if len(rows) != 1 or int(rows[0][0]) < table.minimum_rows:
                raise ProtectionError(
                    f"Prepared KQL table '{table.name}' does not meet its approved row-count evidence; "
                    "complete independent ingestion before declaring its data recovered."
                )


def validate_kql(
    protection: KqlProtection,
    *,
    source: DataIdentity,
    target: DataEndpoint,
    max_age: timedelta,
    recovery_capacity_paused: bool,
    limits: ProtectionLimits = ProtectionLimits(),
    now: datetime | None = None,
) -> ProtectionAssessment:
    require_independent(protection.source, target)
    if protection.source.endpoint == target.endpoint:
        raise ProtectionError("Prepare KQL data on an independent regional cluster, not the source cluster.")
    if protection.source.identity != source or target != protection.target:
        raise ProtectionError("The prepared KQL input is not approved for this exact source/target mapping.")
    for reference in (
        protection.dataset_evidence_ref,
        protection.schema_policy_evidence_ref,
        protection.target_approval_ref,
    ):
        evidence_ref(reference)
    if (
        not protection.source_region
        or not protection.target_region
        or protection.source_region.casefold() == protection.target_region.casefold()
    ):
        raise ProtectionError("Prepare the independent KQL database in another approved region.")
    if not protection.tables or not protection.inputs:
        raise ProtectionError(
            "Supply independently verified KQL data, schema/policy and input access evidence."
        )
    if len(protection.tables) + len(protection.inputs) > limits.max_manifest_entries:
        raise StagingBudgetError("KQL protection inventory exceeds max_manifest_entries.")
    if len({t.name for t in protection.tables}) != len(protection.tables):
        raise ProtectionError("Duplicate table in the prepared KQL data inventory.")
    current = utc(now or datetime.now(UTC))
    if not utc(protection.data_as_of) <= utc(protection.captured_at) <= current or max_age <= timedelta(0):
        raise ProtectionError("KQL data/capture timestamps or max_age are invalid.")
    if current - utc(protection.data_as_of) > max_age:
        return ProtectionAssessment(
            "protection_stale",
            warnings=(
                "The prepared KQL data exceeds max_age. Supply fresher independent data "
                "or approve a new recovery point.",
            ),
        )
    if protection.continuous and not protection.independent_active_compute:
        return ProtectionAssessment(
            "deferred",
            warnings=(
                "Continuous KQL protection requires independent active compute. "
                "A paused recovery capacity cannot continuously ingest; supply a separate "
                "active regional standby or accept periodic data protection.",
            ),
        )
    if recovery_capacity_paused and not protection.independent_active_compute:
        return ProtectionAssessment(
            "deferred",
            warnings=(
                "Resume the prepared KQL target capacity before checking protected data "
                "and input accessibility.",
            ),
        )
    return ProtectionAssessment(
        "protected",
        warnings=(
            "KQL protection uses explicitly prepared independent data only; live-primary transfer/export "
            "is not supported during recovery. Preserve ingestion offsets and reconcile them "
            "before cutover or failback.",
        ),
    )


def capture_kql(
    protection: KqlProtection,
    *,
    tokens: TokenProvider,
    probe_inputs: InputProbe,
    max_age: timedelta,
    recovery_capacity_paused: bool,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> KqlProtection:
    """Validate the prepared target and its inputs, then return evidence for the Warehouse."""
    status = restore_kql(
        protection,
        source=protection.source.identity,
        target=protection.target,
        tokens=tokens,
        probe_inputs=probe_inputs,
        max_age=max_age,
        recovery_capacity_paused=recovery_capacity_paused,
        limits=limits,
        cancel=cancel,
    )
    if not status.data_ready:
        raise ProtectionError(" ".join(status.warnings))
    return protection


def restore_kql(
    protection: KqlProtection,
    *,
    source: DataIdentity,
    target: DataEndpoint,
    tokens: TokenProvider,
    probe_inputs: InputProbe,
    max_age: timedelta,
    recovery_capacity_paused: bool,
    limits: ProtectionLimits = ProtectionLimits(),
    cancel: Cancel = None,
) -> ProtectionAssessment:
    """Qualify an already prepared mapping; never start ingestion, create data or query primary."""
    cancel = operation_cancel(limits, cancel)
    check_cancelled(cancel)
    status = validate_kql(
        protection,
        source=source,
        target=target,
        max_age=max_age,
        recovery_capacity_paused=recovery_capacity_paused,
        limits=limits,
    )
    if status.state != "protected":
        return status
    if probe_inputs(protection.inputs) is not True:
        return ProtectionAssessment(
            "deferred",
            warnings=(
                "Grant the recovery identity access to the prepared KQL data inputs "
                "and verify their availability "
                "independently of the failed primary before continuing.",
            ),
        )
    _probe_tables(protection, tokens, limits, cancel)
    check_cancelled(cancel)
    return ProtectionAssessment(
        "restored_stopped",
        data_ready=True,
        target=target.identity,
        warnings=(
            *status.warnings,
            "Keep recovery-side ingestion/jobs and deferred ACLs stopped until Enable recovery.",
        ),
    )
