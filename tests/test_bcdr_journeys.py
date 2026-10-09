from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.catalog import CapturedGeneration
from fabshuffle.bcdr.contracts import RecoveryMode
from fabshuffle.bcdr.deployment_lock import LEASE_ENV
from fabshuffle.bcdr.scheduled import read_request, scheduled_sync
from fabshuffle.bcdr.service import (
    ContinueDrTestRequest,
    CutoverRequest,
    EnableRecoveryRequest,
    EndDrTestRequest,
    ScheduleGuideRequest,
    StartDrTestRequest,
)
from tests.test_bcdr_contracts import guid
from tests.test_bcdr_coordinator import fence, proofs
from tests.test_bcdr_coordinator import system as system
from tests.test_bcdr_deployment_lock import URL


def start(system):
    result = system.service.synchronize(system.request)
    request = StartDrTestRequest(generation_id=result.generation_id,
                                group_ids=tuple(group.group_id for group in result.groups))
    return system.service.start_dr_test(request)


def test_metadata_baseline_and_guided_schedule_are_durable_and_do_not_deploy(system, tmp_path):
    assert not system.service.status().details["workflow"]["metadata_baseline_ready"]
    sync = system.service.synchronize(system.request)
    status = system.service.status().details["workflow"]
    assert status["metadata_baseline_ready"] and status["schedule_eligible"]
    assert status["last_sync"]["generation_id"] == sync.generation_id
    before = list(system.estate.calls)
    guide = system.service.schedule_guide(ScheduleGuideRequest(
        generation_id=sync.generation_id, approve_scope=True,
    ))
    document = guide.details["schedule_guide"]
    assert document["deployment_status"] == "not_verified"
    assert document["request"]["generation_id"] is None
    assert document["request"]["capture"] and document["request"]["park"]
    path = tmp_path / document["request_filename"]
    path.write_text(json.dumps(document["request"]))
    assert read_request(path).capacity_routes == system.request.capacity_routes
    assert system.estate.calls == before
    assert "client_secret" not in json.dumps(document)
    assert "same" in " ".join(document["steps"]).lower()
    with pytest.raises(RecoveryBlocked, match="current generation"):
        system.service.schedule_guide(ScheduleGuideRequest(generation_id=guid(), approve_scope=True))


def test_dr_test_owner_evidence_never_enables_production_and_ends_without_deletion(system):
    started = start(system)
    record = started.details["dr_test"]
    assert started.mode == RecoveryMode.TESTING
    assert record["results"][0]["outcome"] == "not_tested"
    assert not started.groups[0].active and not started.groups[0].access_enabled
    captures = len(system.captures)
    system.c.source = None
    system.c.source_tokens = None
    system.c.capture = Mock(side_effect=AssertionError("No capture during test"))
    before = list(system.estate.calls)
    completed = system.service.continue_dr_test(ContinueDrTestRequest(
        test_id=record["test_id"], readiness=proofs(system, started.generation_id),
    ))
    assert completed.details["dr_test"]["results"][0]["outcome"] == "passed"
    assert completed.groups[0].data_ready and not completed.groups[0].ready_for_cutover
    assert system.c.runtime.get("lifecycle", "writer") is None
    assert not any(method != "GET" for method, _ in system.estate.calls[len(before):])
    with pytest.raises(RecoveryBlocked, match="Operation not allowed"):
        system.service.cutover(CutoverRequest(
            generation_id=started.generation_id, group_ids=(started.groups[0].group_id,),
            readiness=proofs(system, started.generation_id), writer_fence=fence(),
        ))
    with pytest.raises(RecoveryBlocked, match="Operation not allowed"):
        system.service.enable_recovery(EnableRecoveryRequest(
            generation_id=started.generation_id, group_ids=(started.groups[0].group_id,),
        ))
    ended = system.service.end_dr_test(EndDrTestRequest(test_id=record["test_id"], park=False))
    assert ended.mode == RecoveryMode.STANDBY and ended.details["dr_test"]["phase"] == "ended"
    assert system.c.runtime.get("lifecycle", "writer") is None
    assert len(system.captures) == captures
    assert not any(method == "DELETE" for method, _ in system.estate.calls)
    assert system.service.status().details["workflow"]["test_eligible"]
    archived = system.catalog.get_record("dr-tests", record["test_id"])
    assert archived.document["phase"] == "ended"
    assert archived.document["results"][0]["outcome"] == "passed"
    second = system.service.start_dr_test(StartDrTestRequest(
        generation_id=started.generation_id, group_ids=(started.groups[0].group_id,),
    ))
    assert second.details["dr_test"]["test_id"] != record["test_id"]
    assert system.catalog.get_record("dr-tests", record["test_id"]).document == archived.document


def test_active_test_blocks_scheduled_and_manual_capture(system, monkeypatch, tmp_path):
    started = start(system)
    monkeypatch.setenv(LEASE_ENV, URL)
    system.capacities.lock = SimpleNamespace(remote=object(), assert_held=Mock())
    captures = len(system.captures)
    events = len(system.capacities.events)
    result = scheduled_sync(tmp_path / "unused.json",
                            system.request.model_copy(update={"park": True}),
                            target_tokens=system.c.tokens, service_factory=lambda *a, **k: system.service)
    assert result.details["scheduled_status"] == "skipped"
    assert not any(isinstance(event, tuple) or event == "resume_business"
                   for event in system.capacities.events[events:])
    assert len(system.captures) == captures
    with pytest.raises(RecoveryBlocked, match="Operation not allowed"):
        system.service.synchronize(system.request)
    with pytest.raises(RecoveryBlocked, match="Operation not allowed"):
        system.service.schedule_guide(ScheduleGuideRequest(
            generation_id=started.generation_id, approve_scope=True,
        ))


def test_changed_target_leaves_ending_test_fenced_and_can_resume_after_reconciliation(system):
    started = start(system)
    test_id = started.details["dr_test"]["test_id"]
    original = dict(system.observed)
    key = next(iter(system.observed))
    system.observed[key] = "0" * 64
    with pytest.raises(RecoveryBlocked, match="changed during the test"):
        system.service.end_dr_test(EndDrTestRequest(test_id=test_id, park=False))
    assert system.catalog.state().mode == RecoveryMode.ENDING_TEST
    assert system.service.status().details["workflow"]["test"]["phase"] == "ending_test"
    with pytest.raises(RecoveryBlocked):
        system.service.synchronize(system.request)
    system.observed.update(original)
    ended = system.service.end_dr_test(EndDrTestRequest(test_id=test_id, park=False))
    assert ended.mode == RecoveryMode.STANDBY


def test_stale_test_id_and_nonowner_proof_are_not_accepted(system):
    started = start(system)
    with pytest.raises(RecoveryBlocked, match="recorded test"):
        system.service.continue_dr_test(ContinueDrTestRequest(test_id=guid()))
    evidence = proofs(system, started.generation_id)
    other = evidence[0].issuer.model_copy(update={"object_id": guid(), "kind": "User"})
    result = system.service.continue_dr_test(ContinueDrTestRequest(
        test_id=started.details["dr_test"]["test_id"],
        readiness=(evidence[0].model_copy(update={"effective_principals": (other,)}),),
    ))
    assert result.details["dr_test"]["results"][0]["outcome"] == "failed"
    assert not result.groups[0].active


def test_read_only_navigation_summary_does_not_confuse_data_with_metadata(system):
    sync = system.service.synchronize(system.request)
    group = sync.groups[0]
    with system.c.runtime.controller({RecoveryMode.STANDBY}):
        system.c._save_group(sync.generation_id, group.model_copy(update={"data_ready": False}))
    status = system.service.status().details["workflow"]
    assert status["metadata_baseline_ready"] and status["schedule_eligible"]
    assert status["recovery_gap_groups"] == [group.group_id]


def test_healthy_group_can_be_tested_when_an_unrelated_group_is_blocked(system):
    synced = system.service.synchronize(system.request)
    healthy = synced.groups[0]
    blocked = healthy.model_copy(update={
        "group_id": "blocked-group",
        "items": (healthy.items[0].model_copy(update={"item_id": guid()}),),
        "metadata_applied": False, "blockers": ("Unrelated metadata failed to apply",),
    })
    with system.c.runtime.controller({RecoveryMode.STANDBY}):
        system.c._save_group(synced.generation_id, blocked)
    workflow = system.service.status().details["workflow"]
    assert not workflow["schedule_eligible"]
    assert workflow["test_eligible"] and workflow["test_eligible_group_ids"] == [healthy.group_id]
    with pytest.raises(RecoveryBlocked, match="metadata failures"):
        system.service.start_dr_test(StartDrTestRequest(
            generation_id=synced.generation_id, group_ids=(blocked.group_id,),
        ))
    started = system.service.start_dr_test(StartDrTestRequest(
        generation_id=synced.generation_id, group_ids=(healthy.group_id,),
    ))
    assert started.mode == RecoveryMode.TESTING


def test_test_restores_data_only_through_existing_provider_and_requires_owner_evidence(system):
    item = system.captured.items[0].model_copy(update={"item_type": "Lakehouse"})
    generation = system.captured.model_copy(update={"items": (item,)})
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(generation, ())
    synced = system.service.synchronize(system.request)
    calls = []

    class Provider:
        def restore(self, captured, source, target, runtime):
            assert runtime.mode == RecoveryMode.TESTING
            calls.append((source.identity.key, target.key))
            return True, ()

    system.c.data_recovery = Provider()
    started = system.service.start_dr_test(StartDrTestRequest(
        generation_id=synced.generation_id, group_ids=(synced.groups[0].group_id,),
    ))
    assert len(calls) == 1
    assert started.details["dr_test"]["results"][0]["outcome"] == "not_tested"
    completed = system.service.continue_dr_test(ContinueDrTestRequest(
        test_id=started.details["dr_test"]["test_id"], readiness=proofs(system, synced.generation_id),
    ))
    assert len(calls) == 1, "Owned restoration must not be replayed when evidence is submitted"
    assert completed.details["dr_test"]["results"][0]["outcome"] == "passed"
    assert not completed.groups[0].active and system.c.runtime.get("lifecycle", "writer") is None
