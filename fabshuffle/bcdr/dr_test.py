"""Exercise existing standby under owner access without production admission."""

from __future__ import annotations

from uuid import uuid4

from fabshuffle.bcdr.backend import RecoveryBlocked, now
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.service import ContinueDrTestRequest, EndDrTestRequest, ServiceResult, StartDrTestRequest
from fabshuffle.bcdr.workflow import prepared_metadata_groups


class DrTestController:
    def __init__(self, coordinator):
        self.c = coordinator
        self.runtime = coordinator.runtime

    def _record(self, test_id):
        record = self.runtime.get("lifecycle", "dr-test")
        if record is None or record["test_id"] != test_id:
            raise RecoveryBlocked("Load the current DR Test and continue its recorded test, not another one.")
        writer = self.runtime.get("lifecycle", "writer") or {"epoch": 0, "side": "primary"}
        if writer != record["writer"] or writer["side"] != "primary":
            raise RecoveryBlocked("Writer authority changed during the test; reconcile before proceeding.")
        if self.c.catalog.state().current_generation_id != record["generation_id"]:
            raise RecoveryBlocked("The test recovery point changed; reconcile the recorded test.")
        return record

    def _owner_access(self, groups):
        workspaces = self.c.workspace_mappings()
        selected = {"/".join(item.key.split("/")[:2]) for group in groups for item in group.items}
        for source in selected:
            workspace = workspaces.get(source)
            if workspace is None:
                raise RecoveryBlocked("Synchronize the selected standby workspaces before testing.")
            allowed = self.c.access.owner_acls(workspace)
            assignments = self.c.access.fabric.assignments(allowed[0])
            expected = {(row.principal.object_id, row.principal.kind, row.permission) for row in allowed}
            actual = {
                (row["principal"]["id"], row["principal"]["type"], row["role"]) for row in assignments
            }
            if actual != expected:
                raise RecoveryBlocked(
                    "Standby workspace access must match the configured recovery owners before testing. "
                    "No test access or existing grant was changed."
                )
        allowed_principals = {
            grant.principal.key for grant in self.c.recovery_set.access_policy.workspace_grants()
        }
        generation = self.c.catalog.load_generation(self.c.catalog.state().current_generation_id)
        items = {item.identity.key: item for item in generation.snapshot.items}
        mappings = self.c.item_mappings()
        self.c.access.fabric.inspect_items([
            (mappings[item.key].target, items[item.key].item_type)
            for group in groups for item in group.items if item.key in mappings
        ], allowed_principals)

    def start(self, request: StartDrTestRequest) -> ServiceResult:
        self.c._wake()
        with self.runtime.controller({RecoveryMode.STANDBY}):
            if self.c.catalog.state().current_generation_id != request.generation_id:
                raise RecoveryBlocked("Choose the current captured recovery point for the DR Test.")
            groups = self.c._selected_groups(request.generation_id, request.group_ids)
            generation = self.c.catalog.load_generation(request.generation_id)
            prepared = prepared_metadata_groups(self.c, generation, groups)
            if any(
                group.group_id not in prepared or group.active or group.access_enabled for group in groups
            ):
                raise RecoveryBlocked(
                    "Resolve metadata failures and use inactive, owned standby targets for this test."
                )
            writer = self.runtime.get("lifecycle", "writer") or {"epoch": 0, "side": "primary"}
            if writer["side"] != "primary":
                raise RecoveryBlocked("DR Test cannot run while recovery has production writer authority.")
            self._owner_access(groups)
            record = {
                "test_id": str(uuid4()), "generation_id": request.generation_id,
                "group_ids": list(request.group_ids), "writer": writer,
                "started_at": now().isoformat(), "phase": "testing",
                "results": [], "production_cutover": False,
            }
            self.runtime.put("dr-tests", record["test_id"], record)
            self.runtime.put("lifecycle", "dr-test", record)
            self.runtime.transition(RecoveryMode.TESTING)
            return self._exercise(record, ())

    def continue_test(self, request: ContinueDrTestRequest) -> ServiceResult:
        self.c._wake()
        with self.runtime.controller({RecoveryMode.TESTING}):
            return self._exercise(self._record(request.test_id), request.readiness)

    def _exercise(self, record, readiness):
        groups = self.c._selected_groups(record["generation_id"], record["group_ids"])
        self._owner_access(groups)
        self.c.capacities.resume_business(self.runtime)
        generation = self.c.catalog.load_generation(record["generation_id"])
        applied = self.c.item_mappings()
        items = {item.identity.key: item for item in generation.snapshot.items}
        for group in groups:
            for item in group.items:
                current = applied.get(item.key)
                if current is None or self.c.observe(
                    self.c.destination, current.target, items[item.key].item_type,
                ) != current.target_observed_sha256:
                    raise RecoveryBlocked(
                        "A standby target changed before test preparation. "
                        "Reconcile the target before restoring data."
                    )
        results, warnings, evidence = [], [], []
        for group in groups:
            prepared, messages = self.c.prepare_group_data(generation, group, applied, owners_only=True)
            warnings.extend(messages)
            state = "blocked" if prepared.blockers else "not_tested"
            if not prepared.blockers:
                try:
                    self.c.validate_readiness(
                        generation, (prepared,), readiness, security=True, owners_only=True,
                    )
                    state = "passed"
                    prepared = prepared.model_copy(update={"data_ready": True})
                except RecoveryBlocked as error:
                    state = "not_tested" if not readiness else "failed"
                    prepared = prepared.model_copy(update={
                        "blockers": (str(error),), "data_ready": False,
                    })
            results.append(prepared.model_copy(update={
                "active": False, "access_enabled": False, "ready_for_cutover": False,
            }))
            evidence.append({
                "group_id": group.group_id, "outcome": state, "messages": list(prepared.blockers),
            })
        record = {
            **record, "results": evidence, "observed_at": now().isoformat(),
            "readiness": [row.model_dump(mode="json") for row in readiness],
        }
        self.runtime.put("lifecycle", "dr-test", record)
        return ServiceResult(
            mode=self.runtime.mode, generation_id=record["generation_id"], groups=tuple(results),
            details={"dr_test": record, "readiness_context": {
                "generation_id": record["generation_id"], "writer_epoch": record["writer"]["epoch"],
            }},
            warnings=(
                "DR Test only: production remains primary. Automatic sync is held until End test completes.",
                "Owner-only checks are not production-user admission or proof of regional storage failover.",
                *warnings,
            ),
        )

    def end(self, request: EndDrTestRequest) -> ServiceResult:
        self.c._wake()
        with self.runtime.controller({RecoveryMode.TESTING, RecoveryMode.ENDING_TEST}):
            record = self._record(request.test_id)
            if self.runtime.mode == RecoveryMode.TESTING:
                self.runtime.transition(RecoveryMode.ENDING_TEST)
            groups = self.c._selected_groups(record["generation_id"], record["group_ids"])
            self._owner_access(groups)
            generation = self.c.catalog.load_generation(record["generation_id"])
            items = {item.identity.key: item for item in generation.snapshot.items}
            applied = self.c.item_mappings()
            for group in groups:
                for item in group.items:
                    current = applied.get(item.key)
                    if current is None or self.c.observe(
                        self.c.destination, current.target, items[item.key].item_type,
                    ) != current.target_observed_sha256:
                        raise RecoveryBlocked(
                            "A standby target changed during the test. Reconcile before ending. "
                            "Automatic sync remains held and no resources were deleted."
                        )
            record = {**record, "phase": "ended", "ended_at": now().isoformat()}
            self.runtime.put("dr-tests", record["test_id"], record)
            self.runtime.put("lifecycle", "dr-test", record)
            self.runtime.transition(RecoveryMode.STANDBY)
            if request.park:
                self.c.capacities.park(self.runtime, tuple(self.c.workspace_mappings().values()))
            return ServiceResult(
                mode=self.runtime.mode, generation_id=record["generation_id"],
                details={"dr_test": record},
                warnings=(
                    "Test ended without cutover. Standby resources and restored data were retained.",
                    "Automatic sync may run again once the catalog is safely back in standby.",
                ),
            )
