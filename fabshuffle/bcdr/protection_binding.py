"""Production binding of optional providers to pinned Warehouse configurations.

HTTP/CLI requests contain typed descriptors, never host paths or Python callbacks.
The protected-data root is server configuration. Configuration documents are immutable,
content-addressed records; generations pin their digest rather than a mutable selection.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Self
from uuid import UUID, uuid4, uuid5
from xml.etree.ElementTree import ParseError
from zipfile import BadZipFile

from azure.core.exceptions import AzureError
from azure.kusto.data.exceptions import KustoServiceError
from pydantic import AfterValidator, ConfigDict, Field, JsonValue, model_validator

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.catalog import CapturedGeneration, CatalogConflict, RecoveryCatalog
from fabshuffle.bcdr.contracts import (
    Digest,
    ItemIdentity,
    ItemRecord,
    OperationState,
    ProtectionKind,
    ProtectionRecord,
    Qualification,
    Record,
    RecoveryMode,
    RecoveryOutcome,
    canonical_json,
    digest,
    reject_embedded_secrets,
)
from fabshuffle.bcdr.protection import (
    DataEndpoint,
    DataIdentity,
    ProtectedLocation,
    ProtectionError,
    ProtectionLimits,
    evidence_ref,
    missing_protection,
    require_independent,
    validate_manifest,
)
from fabshuffle.bcdr.protection_cosmos import CosmosProtection, restore_cosmos
from fabshuffle.bcdr.protection_kql import KqlProtection, validate_kql
from fabshuffle.bcdr.protection_kql_binding import (
    KqlMaterializedInput,
    restore_materialized_kql,
    validate_materialized_bindings,
)
from fabshuffle.bcdr.protection_sql import SqlProtection, restore_sql
from fabshuffle.fabric import cosmosdb, sqldatabases
from fabshuffle.fabric.client import FabricApiError, FabricClient
from fabshuffle.lifecycle import safe_text
from fabshuffle.transfer.common import StagingBudgetError
from fabshuffle.transfer.sqlschema import SchemaTransferError

if TYPE_CHECKING:
    from fabshuffle.bcdr.backend import DurableRuntime

EvidenceRef = Annotated[str, AfterValidator(evidence_ref)]
_ITEM_TYPES = {"sql": "SQLDatabase", "cosmos": "CosmosDBDatabase", "kql": "KQLDatabase"}
_PROVIDER_ERRORS = (
    ProtectionError,
    SchemaTransferError,
    StagingBudgetError,
    AzureError,
    KustoServiceError,
    FabricApiError,
    ValueError,
    OSError,
    BadZipFile,
    ParseError,
)


class ProtectionStorage(Record):
    source_region: Annotated[str, Field(pattern=r"^[a-z0-9-]{1,64}$")]
    storage_region: Annotated[str, Field(pattern=r"^[a-z0-9-]{1,64}$")]
    approval_ref: EvidenceRef

    @model_validator(mode="after")
    def independent_region(self) -> Self:
        if self.source_region == self.storage_region:
            raise ValueError("Protected artifacts must be stored outside the source region.")
        return self


class ProtectionConfiguration(Record):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True, hide_input_in_errors=True)
    provider: Literal["sql", "cosmos", "kql"]
    descriptor: SqlProtection | CosmosProtection | KqlProtection
    storage: ProtectionStorage | None = None
    max_age_seconds: Annotated[int, Field(strict=True, gt=0, le=86399999913600)]
    target_approval_ref: EvidenceRef
    kql_materialized_inputs: tuple[KqlMaterializedInput, ...] = ()

    @model_validator(mode="after")
    def provider_matches(self) -> Self:
        expected = {"sql": SqlProtection, "cosmos": CosmosProtection, "kql": KqlProtection}[self.provider]
        if not isinstance(self.descriptor, expected):
            raise ValueError("Protection provider and typed descriptor do not match.")
        if isinstance(self.descriptor, KqlProtection):
            if self.storage is not None:
                raise ValueError("Prepared KQL data does not use a portable-file storage configuration.")
            if self.target_approval_ref != self.descriptor.target_approval_ref:
                raise ValueError("KQL target approval must match the prepared mapping.")
            if self.kql_materialized_inputs:
                validate_materialized_bindings(self.descriptor, self.kql_materialized_inputs)
        else:
            if self.kql_materialized_inputs:
                raise ValueError("Materialized KQL inputs are only valid for the KQL provider.")
            manifest = self.descriptor.manifest
            if self.storage is None or (
                self.storage.storage_region != manifest.storage_region
                or self.storage.approval_ref != manifest.storage_approval_ref
            ):
                raise ValueError("Portable protection needs storage metadata matching its descriptor.")
            expected_provider = (
                ("sql-dacpac", "sql-bacpac") if self.provider == "sql" else ("cosmos-logical",)
            )
            if (
                manifest.provider not in expected_provider
                or not manifest.complete
                or manifest.schema_version != 1
            ):
                raise ValueError(
                    "Portable protection descriptor is incomplete or belongs to another provider."
                )
        reject_embedded_secrets(canonical_json(self))
        return self

    @property
    def source(self) -> DataIdentity:
        if isinstance(self.descriptor, KqlProtection):
            return self.descriptor.source.identity
        return self.descriptor.manifest.source.identity

    @property
    def recovery_point(self) -> datetime:
        if isinstance(self.descriptor, KqlProtection):
            return self.descriptor.data_as_of
        return self.descriptor.manifest.captured_at


class ConfigureProtectionRequest(Record):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True, hide_input_in_errors=True)
    source: ItemIdentity
    configuration: ProtectionConfiguration
    expected_revision: Annotated[int, Field(strict=True, ge=1)] | None = None

    @model_validator(mode="after")
    def source_matches(self) -> Self:
        if _data_identity(self.source) != self.configuration.source:
            raise ValueError("Protection configuration belongs to a different source item.")
        return self


class _Selection(Record):
    protection: ProtectionRecord


class _RestoreResult(Record):
    configuration_digest: Digest
    source: ItemIdentity
    target: ItemIdentity
    state: Literal["restored_stopped", "protection_missing", "protection_stale", "deferred", "protected"]
    data_ready: Annotated[bool, Field(strict=True)]
    ready_for_cutover: Literal[False] = False
    warnings: tuple[str, ...]


class ProviderRestoreFailed(ProtectionError):
    """A provider failure was recorded without declaring a partially written target ready."""


def _data_identity(value: ItemIdentity) -> DataIdentity:
    return DataIdentity(value.tenant_id, value.workspace_id, value.item_id)


def _load_config(record: ProtectionRecord, catalog: RecoveryCatalog) -> ProtectionConfiguration:
    reference = record.artifact_reference
    if not reference or reference != record.sha256 or len(reference) != 64:
        raise ProtectionError(
            "Reconfigure protection and capture a generation with a pinned configuration digest."
        )
    row = catalog.get_record("protection-config", reference)
    if row is None:
        raise ProtectionError("The pinned protection configuration is missing; restore its Warehouse record.")
    if digest(canonical_json(row.document)) != reference:
        raise ProtectionError(
            "Pinned protection configuration integrity failed; do not replace its captured input."
        )
    configuration = ProtectionConfiguration.model_validate(row.document)
    expected_kind = (
        ProtectionKind.PREPARED_STANDBY if configuration.provider == "kql" else ProtectionKind.PORTABLE_EXPORT
    )
    if record.kind != expected_kind:
        raise ProtectionError("The captured protection kind does not match its configured provider.")
    if (
        configuration.source != _data_identity(record.item)
        or configuration.recovery_point != record.recovery_point
    ):
        raise ProtectionError(
            "Pinned protection identity or recovery point differs from the captured generation."
        )
    return configuration


def _selected_record(generation: CapturedGeneration, item: ItemRecord) -> ProtectionRecord | None:
    if item not in generation.snapshot.items or item.tombstone:
        raise ProtectionError("Restore only an item from this exact captured generation.")
    records = [p for p in generation.snapshot.protections if p.item == item.identity]
    if len(records) > 1:
        raise ProtectionError(
            "Choose one explicit protection input per item before capturing this generation."
        )
    return records[0] if records else None


def owns_schema(generation: CapturedGeneration, item: ItemRecord, catalog: RecoveryCatalog) -> bool:
    """Reserve an empty SQL shell even when a configured data export later proves stale."""
    record = _selected_record(generation, item)
    if item.item_type != "SQLDatabase" or record is None or not record.artifact_reference:
        return False
    configuration = _load_config(record, catalog)
    return (
        configuration.provider == "sql"
        and isinstance(configuration.descriptor, SqlProtection)
        and configuration.descriptor.manifest.provider == "sql-dacpac"
    )


def prepared_target(
    generation: CapturedGeneration,
    item: ItemRecord,
    catalog: RecoveryCatalog,
) -> ItemIdentity | None:
    """Nominate an explicit prepared mapping, never certify metadata or adopt by name.

    The coordinator must independently qualify the prepared target's metadata,
    ownership, placement and restricted ACLs before registering this mapping.
    """
    record = _selected_record(generation, item)
    if item.item_type != "KQLDatabase" or record is None or not record.artifact_reference:
        return None
    configuration = _load_config(record, catalog)
    descriptor = configuration.descriptor
    if (
        not isinstance(descriptor, KqlProtection)
        or not configuration.kql_materialized_inputs
        or descriptor.continuous
    ):
        return None
    identity = descriptor.target.identity
    return ItemIdentity(
        tenant_id=identity.tenant_id,
        workspace_id=identity.workspace_id,
        item_id=identity.item_id,
    )


def _location(configuration: ProtectionConfiguration, protected_root: Path | None) -> ProtectedLocation:
    if protected_root is None:
        raise ProtectionError(
            "Configure the server's approved protected-data root before using portable exports."
        )
    storage = configuration.storage
    if storage is None:
        raise ProtectionError("Portable export storage approval is missing.")
    return ProtectedLocation(
        protected_root,
        storage.source_region,
        storage.storage_region,
        storage.approval_ref,
    )


def configure_protection(
    runtime: DurableRuntime,
    request: ConfigureProtectionRequest,
    *,
    protected_root: Path | None,
    limits: ProtectionLimits = ProtectionLimits(),
) -> ProtectionRecord:
    """Under a standby controller lease, persist an immutable config and CAS selection.

    The caller attaches ``capture_protection_records`` to the NEXT generation. Existing
    published generations are never retroactively switched to a new data export.
    """
    runtime.fence()
    if runtime.mode != RecoveryMode.STANDBY:
        raise ProtectionError(
            "Configure protection only in standby mode, not while recovery is serving users."
        )
    configuration = request.configuration
    point = configuration.recovery_point
    if isinstance(configuration.descriptor, KqlProtection):
        status = validate_kql(
            configuration.descriptor,
            source=configuration.source,
            target=configuration.descriptor.target,
            max_age=timedelta(seconds=configuration.max_age_seconds),
            recovery_capacity_paused=False,
            limits=limits,
        )
        outcome = RecoveryOutcome.MANUAL
        concrete = bool(configuration.kql_materialized_inputs) and not configuration.descriptor.continuous
        qualification = Qualification.UNVERIFIED if concrete else Qualification.NEEDS_PROVIDER
        limitations = (
            *status.warnings,
            (
                "Materialized KQL data bindings are configured but unverified; qualify the exact prepared "
                "target metadata and run authenticated local-table probes before data readiness."
                if concrete
                else "Manual KQL protection only: opaque or continuous ingestion inputs have no executable "
                "default binding. Supply typed materialized KQL inputs "
                "or a qualified workload-specific provider."
            ),
        )
    else:

        def fence() -> bool:
            runtime.fence()
            return False

        result = validate_manifest(
            configuration.descriptor.manifest,
            source=configuration.source,
            location=_location(configuration, protected_root),
            max_age=timedelta(seconds=configuration.max_age_seconds),
            limits=limits,
            cancel=fence,
        )
        outcome = RecoveryOutcome.PROTECTED if result.state == "protected" else RecoveryOutcome.MANUAL
        qualification = Qualification.DOCUMENTED
        limitations = result.warnings
        if configuration.descriptor.manifest.provider == "sql-bacpac":
            outcome = RecoveryOutcome.MANUAL
            limitations += (
                "Supply a data-bearing DACPAC; raw BACPAC import cannot safely exclude source grants.",
            )
    encoded = canonical_json(configuration)
    reference = digest(encoded)
    record = ProtectionRecord(
        protection_id=str(uuid5(UUID(request.source.item_id), reference)),
        item=request.source,
        kind=ProtectionKind.PREPARED_STANDBY
        if configuration.provider == "kql"
        else ProtectionKind.PORTABLE_EXPORT,
        outcome=outcome,
        qualification=qualification,
        artifact_reference=reference,
        recovery_point=point,
        sha256=reference,
        provenance="Immutable Warehouse protection-config descriptor",
        limitations=limitations,
        action="Capture this configuration in a new generation, then restore "
        "to its restricted target and complete workload readiness checks before Enable recovery.",
    )
    runtime.fence()
    existing = runtime.catalog.get_record("protection-config", reference)
    document = configuration.model_dump(mode="json")
    if existing is not None:
        if canonical_json(existing.document) != encoded:
            raise ProtectionError("Immutable protection configuration key has conflicting content.")
    else:
        runtime.catalog.put_record(
            runtime.require_lease(),
            "protection-config",
            reference,
            document,
            expected_revision=None,
        )
    runtime.fence()
    runtime.catalog.put_record(
        runtime.require_lease(),
        "protection-selection",
        request.source.key,
        _Selection(protection=record).model_dump(mode="json"),
        expected_revision=request.expected_revision,
    )
    return record


def capture_protection_records(
    items: Sequence[ItemRecord], catalog: RecoveryCatalog
) -> tuple[ProtectionRecord, ...]:
    """Resolve selected configs without source reads; replace same-item default assessments."""
    records: list[ProtectionRecord] = []
    for item in items:
        row = catalog.get_record("protection-selection", item.identity.key)
        if row is None:
            continue
        record = _Selection.model_validate(row.document).protection
        configuration = _load_config(record, catalog)
        if record.item != item.identity or _ITEM_TYPES[configuration.provider] != item.item_type:
            raise ProtectionError(
                "Configured protection does not match the captured item identity and workload type."
            )
        records.append(record)
    return tuple(records)


class ProviderDataRecovery:
    def __init__(
        self,
        *,
        client: FabricClient,
        tokens: TokenProvider,
        protected_root: Path | None,
        scratch: Path,
        limits: ProtectionLimits = ProtectionLimits(),
    ) -> None:
        if not scratch.is_absolute() or not scratch.is_dir() or scratch.is_symlink():
            raise ValueError("Provider scratch must be an existing absolute server-configured directory.")
        self.client = client
        self.tokens = tokens
        self.protected_root = protected_root
        self.scratch = scratch
        self.limits = limits

    def _target(self, provider: str, target: ItemIdentity) -> DataEndpoint:
        getters = {"sql": sqldatabases.get_sql_database, "cosmos": cosmosdb.get_cosmos_database}
        response = getters[provider](self.client, target.workspace_id, target.item_id)
        returned = ItemIdentity(
            tenant_id=target.tenant_id,
            workspace_id=response.get("workspaceId"),
            item_id=response.get("id"),
        )
        if returned != target or response.get("type") != _ITEM_TYPES[provider]:
            raise ProtectionError(
                "Destination service returned an item outside the approved target identity/type."
            )
        properties = response.get("properties") or {}
        host = properties.get("serverFqdn")
        database = properties.get("databaseName")
        if not isinstance(host, str) or not host or not isinstance(database, str) or not database:
            raise ProtectionError(
                "The destination database endpoint is not ready; retry after provisioning completes."
            )
        if provider == "sql":
            host, separator, port = host.partition(",")
            if separator and port != "1433":
                raise ProtectionError("The SQL target returned an unsupported endpoint port.")
            endpoint = f"https://{host}"
        else:
            endpoint = cosmosdb.endpoint_url(response)
        return DataEndpoint(_data_identity(target), endpoint, database)

    def _materialized_kql(
        self,
        generation: CapturedGeneration,
        item: ItemRecord,
        target: ItemIdentity,
        runtime: DurableRuntime,
        record: ProtectionRecord,
        configuration: ProtectionConfiguration,
    ) -> tuple[bool, tuple[str, ...]]:
        descriptor = configuration.descriptor
        if not isinstance(descriptor, KqlProtection):
            raise ProtectionError("Materialized KQL validation needs a KQL protection descriptor.")
        if runtime.catalog.pending_operations():
            raise CatalogConflict("Reconcile pending operations before validating prepared KQL data.")

        def fence() -> bool:
            runtime.fence()
            return False

        def action() -> dict[str, JsonValue]:
            try:
                assessment = restore_materialized_kql(
                    client=self.client,
                    tokens=self.tokens,
                    protection=descriptor,
                    bindings=configuration.kql_materialized_inputs,
                    target=_data_identity(target),
                    max_age=timedelta(seconds=configuration.max_age_seconds),
                    limits=self.limits,
                    cancel=fence,
                )
                runtime.fence()
                return _RestoreResult(
                    configuration_digest=record.sha256,
                    source=item.identity,
                    target=target,
                    state=assessment.state,
                    data_ready=assessment.data_ready,
                    warnings=tuple(safe_text(warning) for warning in assessment.warnings),
                ).model_dump(mode="json")
            except _PROVIDER_ERRORS as error:
                # Every service operation in this branch is read-only. Record the
                # failed observation without making an unrelated group ambiguous.
                runtime.fence()
                return _RestoreResult(
                    configuration_digest=record.sha256,
                    source=item.identity,
                    target=target,
                    state="deferred",
                    data_ready=False,
                    warnings=(
                        f"{item.display_name}: {safe_text(str(error))}. "
                        "Repair the independent prepared KQL input and repeat its authenticated checks.",
                    ),
                ).model_dump(mode="json")

        # Readiness probes are observations, not imports. Requery every time; never
        # infer current availability from yesterday's successful validation journal.
        key = (
            f"{generation.snapshot.generation_id}:{item.identity.key}:{target.key}:"
            f"{record.artifact_reference}:{uuid4()}"
        )
        result = _RestoreResult.model_validate(
            runtime.effect(
                "data-validate",
                key,
                action,
                generation_id=generation.snapshot.generation_id,
                source=item.identity,
                target=target,
            )
        )
        runtime.fence()
        if (
            result.configuration_digest != record.sha256
            or result.source != item.identity
            or result.target != target
        ):
            raise ProtectionError("KQL validation result does not match the pinned recovery mapping.")
        return result.data_ready and result.state == "restored_stopped", result.warnings

    def restore(
        self,
        generation: CapturedGeneration,
        item: ItemRecord,
        target: ItemIdentity,
        runtime: DurableRuntime,
    ) -> tuple[bool, tuple[str, ...]]:
        runtime.fence()
        record = _selected_record(generation, item)
        if record is None or not record.artifact_reference:
            missing = missing_protection(item.display_name, item.item_type)
            return False, missing.warnings
        configuration = _load_config(record, runtime.catalog)
        if _ITEM_TYPES[configuration.provider] != item.item_type:
            raise ProtectionError("The configured data provider does not match the captured workload type.")
        if isinstance(configuration.descriptor, KqlProtection):
            status = validate_kql(
                configuration.descriptor,
                source=_data_identity(item.identity),
                target=configuration.descriptor.target,
                max_age=timedelta(seconds=configuration.max_age_seconds),
                recovery_capacity_paused=False,
                limits=self.limits,
            )
            if configuration.descriptor.target.identity != _data_identity(target):
                raise ProtectionError("The KQL prepared data belongs to a different target mapping.")
            if status.state != "protected":
                return False, status.warnings
            if configuration.kql_materialized_inputs and not configuration.descriptor.continuous:
                return self._materialized_kql(generation, item, target, runtime, record, configuration)
            unresolved = ", ".join(value.resource_ref for value in configuration.descriptor.inputs)
            return False, (
                *status.warnings,
                f"{item.display_name}: manual KQL protection only for data inputs [{unresolved}]. "
                "Opaque or continuous ingestion inputs have no executable default binding. "
                "Supply typed independent materialized KQL data inputs "
                "or qualify a workload-specific provider.",
            )
        if self.protected_root is None:
            return False, (
                f"{item.display_name}: configure the server's approved protected-data root "
                "and make the pinned export accessible before restoration.",
            )

        def fence() -> bool:
            runtime.fence()
            return False

        try:
            location = _location(configuration, self.protected_root)
            preflight = validate_manifest(
                configuration.descriptor.manifest,
                source=_data_identity(item.identity),
                location=location,
                max_age=timedelta(seconds=configuration.max_age_seconds),
                limits=self.limits,
                cancel=fence,
            )
        except (ProtectionError, ValueError, OSError, StagingBudgetError) as error:
            return False, (
                f"{item.display_name}: {safe_text(str(error))} "
                "Repair the protected input before attempting destination data restoration.",
            )
        if preflight.state != "protected":
            return False, tuple(f"{item.display_name}: {warning}" for warning in preflight.warnings)
        if runtime.catalog.pending_operations():
            raise CatalogConflict("Reconcile pending operations before starting another data restore.")

        # Include the immutable input as well as generation/source/target in idempotency.
        key = (
            f"{generation.snapshot.generation_id}:{item.identity.key}:"
            f"{target.key}:{record.artifact_reference}"
        )
        executed = False

        def action() -> dict[str, JsonValue]:
            nonlocal executed
            executed = True
            try:
                runtime.fence()
                endpoint = self._target(configuration.provider, target)
                require_independent(configuration.descriptor.manifest.source, endpoint)
                kwargs = dict(
                    source=_data_identity(item.identity),
                    target=endpoint,
                    tokens=self.tokens,
                    location=location,
                    scratch=self.scratch,
                    max_age=timedelta(seconds=configuration.max_age_seconds),
                    target_approval_ref=configuration.target_approval_ref,
                    limits=self.limits,
                    cancel=fence,
                )
                descriptor = configuration.descriptor
                if isinstance(descriptor, SqlProtection):
                    assessment = restore_sql(descriptor, **kwargs)
                elif isinstance(descriptor, CosmosProtection):
                    assessment = restore_cosmos(descriptor, **kwargs)
                else:
                    raise ProtectionError(
                        "Only explicit portable exports can enter a data-restore operation."
                    )
                runtime.fence()
                if assessment.data_ready and (
                    assessment.state != "restored_stopped" or assessment.target != _data_identity(target)
                ):
                    raise ProtectionError(
                        "Provider returned readiness for a different target or incomplete state."
                    )
                return _RestoreResult(
                    configuration_digest=record.sha256,
                    source=item.identity,
                    target=target,
                    state=assessment.state,
                    data_ready=assessment.data_ready,
                    warnings=tuple(safe_text(warning) for warning in assessment.warnings),
                ).model_dump(mode="json")
            except _PROVIDER_ERRORS as error:
                message = safe_text(str(error))
                operation = runtime.current_operation
                if operation is None:
                    raise ProtectionError(
                        "Provider mutation requires a committed durable operation intent."
                    ) from error
                # A source-offline import may have written part of the destination.
                # A definitive service rejection is not proof the composite action was atomic.
                runtime.fence()
                runtime.catalog.record_operation(
                    runtime.require_lease(),
                    operation.model_copy(
                        update={
                            "state": OperationState.AMBIGUOUS,
                            "recorded_at": datetime.now(UTC),
                            "error_code": safe_text(str(getattr(error, "error_code", "") or "")) or None,
                            "message": message,
                        }
                    ),
                )
                raise ProviderRestoreFailed(
                    f"{item.display_name}: {message}. Keep the target unready and reconcile the recorded "
                    "partial restore before retrying; do not merge data with blind upserts."
                ) from None

        try:
            result = _RestoreResult.model_validate(
                runtime.effect(
                    "data-restore",
                    key,
                    action,
                    generation_id=generation.snapshot.generation_id,
                    source=item.identity,
                    target=target,
                )
            )
        except ProviderRestoreFailed as error:
            return False, (str(error),)
        if (
            result.configuration_digest != record.sha256
            or result.source != item.identity
            or result.target != target
        ):
            raise ProtectionError(
                "Stored data-restore result does not match this pinned source/target/input."
            )
        runtime.fence()
        if result.data_ready and not executed:
            # A durable successful import must not be repeated into a now nonempty target.
            # Nor does an old journal record establish current business-data availability.
            return False, (
                *result.warnings,
                f"{item.display_name}: data was restored previously; "
                "verify current target data and supply fresh coordinator readiness evidence "
                "rather than replaying the import.",
            )
        return result.data_ready and result.state == "restored_stopped", result.warnings


def build_data_recovery(
    *,
    client: FabricClient,
    tokens: TokenProvider,
    protected_root: Path | None,
    scratch: Path,
    limits: ProtectionLimits = ProtectionLimits(),
) -> ProviderDataRecovery:
    return ProviderDataRecovery(
        client=client,
        tokens=tokens,
        protected_root=protected_root,
        scratch=scratch,
        limits=limits,
    )
