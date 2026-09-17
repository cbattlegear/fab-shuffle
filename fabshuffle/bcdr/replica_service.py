"""Warehouse-pinned orchestration for explicitly qualified, retained-source data paths."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from fabshuffle.bcdr.backend import DurableRuntime, RecoveryBlocked, now
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    ItemRecord,
    RecoveryMode,
    RecoveryOutcome,
    RecoverySet,
    canonical_json,
    digest,
)
from fabshuffle.bcdr.service import ConfigureReplicaRequest, ItemReadiness
from fabshuffle.fabric.client import FabricClient

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


class ReplicaRecovery:
    def __init__(
        self,
        client: FabricClient,
        runtime: DurableRuntime,
        recovery_set: RecoverySet,
        observe: Callable,
    ) -> None:
        self.client = client
        self.runtime = runtime
        self.recovery_set = recovery_set
        self.observe = observe

    @staticmethod
    def key(generation_id: str, source_key: str) -> str:
        return f"{generation_id}/{source_key}"

    def selected(self, generation_id: str, source_key: str) -> ConfigureReplicaRequest | None:
        record = self.runtime.get("replica-config", self.key(generation_id, source_key))
        if record is None:
            return None
        request = ConfigureReplicaRequest.model_validate(record["request"])
        if (
            request.generation_id != generation_id
            or request.source.key != source_key
            or digest(canonical_json(request)) != record["sha256"]
        ):
            raise RecoveryBlocked(
                "The stored temporary attachment configuration has invalid identity/integrity"
            )
        return request

    def validate(
        self,
        request: ConfigureReplicaRequest,
        generation: CapturedGeneration,
        item: ItemRecord,
        applied: AppliedItem,
    ) -> None:
        from fabshuffle.bcdr.replica import (
            validate_attachment_selection,
            validate_recovery_attachment,
        )

        self.runtime.fence()
        if request.generation_id != generation.snapshot.generation_id or request.source != item.identity:
            raise RecoveryBlocked("Pin the attachment configuration to its exact complete captured source")
        if applied.source != item.identity or applied.capture_generation_id != request.generation_id:
            raise RecoveryBlocked("Synchronize this source's exact target before configuring temporary data")
        if not request.qualified_at <= now() < request.valid_until:
            raise RecoveryBlocked("Renew the incident-specific temporary data access qualification")
        if any(row.binding.consumer != applied.target for row in request.attachments):
            raise RecoveryBlocked("Every temporary attachment must name the exact owned recovery target")
        coverage = validate_attachment_selection(
            generation.snapshot,
            item,
            [(row.binding, row.shortcut_path, row.shortcut_name) for row in request.attachments],
        )
        if not coverage.required_paths or not coverage.complete:
            raise RecoveryBlocked(
                f"Qualify the complete captured data scope for '{item.display_name}'; "
                f"missing paths: {list(coverage.missing_paths)}. "
                "An individual shortcut is not complete Lakehouse recovery."
            )
        if item.item_type != "Lakehouse" or item.tombstone:
            raise RecoveryBlocked("Temporary attachments support captured live Lakehouses only")
        if self.observe(self.client, applied.target, item.item_type) != applied.target_observed_sha256:
            raise RecoveryBlocked(
                "The recovery target drifted; reconcile it before attaching retained-source data"
            )
        for entry in request.attachments:
            validate_recovery_attachment(
                generation.snapshot,
                entry.binding,
                item,
                applied,
                entry.shortcut_path,
                entry.shortcut_name,
                recovery_set=self.recovery_set,
                access_evidence=entry.access_evidence,
            )

    def configure(
        self,
        request: ConfigureReplicaRequest,
        generation: CapturedGeneration,
        item: ItemRecord,
        applied: AppliedItem,
    ) -> None:
        self.validate(request, generation, item, applied)
        address = self.key(request.generation_id, request.source.key)
        previous = self.runtime.get("replica-config", address)
        fingerprint = digest(canonical_json(request))
        if previous is not None:
            if previous["sha256"] == fingerprint:
                return
            old = ConfigureReplicaRequest.model_validate(previous["request"])
            if request.expected_configuration_sha256 != previous["sha256"]:
                raise RecoveryBlocked(
                    "Renew qualification only against the displayed current configuration hash"
                )

            def scope(config):
                return [
                    (
                        entry.binding.generation_id,
                        entry.binding.source.key,
                        entry.binding.source_path,
                        entry.binding.consumer.key,
                        entry.shortcut_path,
                        entry.shortcut_name,
                        entry.access_evidence.principal.key,
                        entry.access_evidence.access_mode,
                    )
                    for entry in config.attachments
                ]

            if scope(old) != scope(request):
                raise RecoveryBlocked(
                    "Renewal cannot change pinned source paths, targets, names or runtime principals"
                )
        elif request.expected_configuration_sha256 is not None:
            raise RecoveryBlocked("No prior temporary configuration exists for the supplied renewal hash")
        self.runtime.put(
            "replica-config",
            address,
            {
                "sha256": fingerprint,
                "request": request.model_dump(mode="json"),
            },
        )

    def prepare(
        self,
        generation: CapturedGeneration,
        item: ItemRecord,
        applied: AppliedItem,
    ) -> tuple[AppliedItem, tuple[str, ...]]:
        from fabshuffle.bcdr.replica import attach_recovery_data

        if self.runtime.mode not in {RecoveryMode.ENABLING_RECOVERY, RecoveryMode.ACTIVE_RECOVERY}:
            raise RecoveryBlocked(
                "Retained-source shortcuts may be attached only during explicit recovery enable"
            )
        request = self.selected(generation.snapshot.generation_id, item.identity.key)
        if request is None:
            raise RecoveryBlocked("Configure exact qualified temporary attachments before attempting them")
        self.validate(request, generation, item, applied)

        def attach():
            observations = []
            diagnostics = []
            for entry in request.attachments:
                result = attach_recovery_data(
                    self.client,
                    generation=generation.snapshot,
                    binding=entry.binding,
                    source=item,
                    target_mapping=applied,
                    recovery_set=self.recovery_set,
                    shortcut_path=entry.shortcut_path,
                    shortcut_name=entry.shortcut_name,
                    access_evidence=entry.access_evidence,
                    mutation_guard=self.runtime.fence,
                )
                if not result.attachment_verified:
                    raise RecoveryBlocked(
                        "The destination did not verify the exact requested temporary shortcut"
                    )
                observations.append(
                    {
                        "binding": result.binding.model_dump(mode="json"),
                        "shortcut": result.shortcut,
                        "changed": result.changed,
                        "attachment_verified": True,
                        "data_ready": False,
                    }
                )
                diagnostics.extend(result.diagnostics)
            updated = applied.model_copy(
                update={
                    "applied_at": now(),
                    "outcome": RecoveryOutcome.TEMPORARY_ATTACHED,
                    "target_observed_sha256": self.observe(self.client, applied.target, item.item_type),
                    "operation_id": self.runtime.current_operation.operation_id,
                }
            )
            return {
                "applied": updated.model_dump(mode="json"),
                "attachments": observations,
                "diagnostics": diagnostics,
                "data_ready": False,
                "endpoint_ready": False,
            }

        result = self.runtime.effect(
            "replica-attach",
            f"{self.key(request.generation_id, item.identity.key)}/{self.runtime.require_lease().epoch}",
            attach,
            generation_id=request.generation_id,
            source=item.identity,
            target=applied.target,
        )
        updated = AppliedItem.model_validate(result["applied"])
        self.runtime.catalog.record_applied(self.runtime.require_lease(), updated)
        previous = self.runtime.get("replica-applied", self.key(request.generation_id, item.identity.key))
        changed_at = (
            previous["changed_at"]
            if previous is not None and not any(row["changed"] for row in result["attachments"])
            else now().isoformat()
        )
        self.runtime.put(
            "replica-applied",
            self.key(request.generation_id, item.identity.key),
            {
                "configuration_sha256": digest(canonical_json(request)),
                "attachments": result["attachments"],
                "target": applied.target.model_dump(mode="json"),
                "target_observed_sha256": updated.target_observed_sha256,
                "data_ready": False,
                "endpoint_ready": False,
                "changed_at": changed_at,
            },
        )
        return updated, tuple(result["diagnostics"])

    def verify_runtime_principals(
        self,
        generation_id: str,
        item_keys: Sequence[str],
        readiness: Sequence[ItemReadiness],
    ) -> bool:
        proofs = {row.source.key: row for row in readiness}
        temporary = False
        for source_key in item_keys:
            request = self.selected(generation_id, source_key)
            if request is None:
                continue
            temporary = True
            if not request.qualified_at <= now() < request.valid_until:
                raise RecoveryBlocked("Renew the expired temporary read-only runtime qualification")
            proof = proofs.get(source_key)
            if proof is None:
                raise RecoveryBlocked("Provide current data/read-only runtime evidence after attachment")
            observation = self.runtime.get("replica-applied", self.key(generation_id, source_key))
            if observation is None or proof.observed_at < datetime.fromisoformat(observation["changed_at"]):
                raise RecoveryBlocked(
                    "Verify data and runtime identity after the retained-source shortcuts changed"
                )
            qualified = {entry.access_evidence.principal.key for entry in request.attachments}
            effective = {principal.key for principal in proof.effective_principals}
            if not effective or effective != qualified:
                raise RecoveryBlocked(
                    "Verify exactly the qualified caller principals; delegated item-owner access "
                    "and additional unqualified runtime identities are unsupported for temporary attachments"
                )
            for entry in request.attachments:
                evidence = entry.access_evidence
                if not evidence.verified_at <= now() < evidence.valid_until:
                    raise RecoveryBlocked(
                        "Renew the temporary attachment caller's read-only enforcement evidence"
                    )
        return temporary
