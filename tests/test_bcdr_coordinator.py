"""Real Warehouse journal + deterministic destination transport, not regional DR qualification."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from fabshuffle.bcdr.adapters import AdapterResult
from fabshuffle.bcdr.backend import RecoveryBlocked, now
from fabshuffle.bcdr.catalog import CapturedGeneration, CatalogConflict
from fabshuffle.bcdr.contracts import (
    AppliedItem,
    DependencyEdge,
    ItemIdentity,
    OperationState,
    RecoveryMode,
    RecoveryOutcome,
    canonical_json,
    digest,
)
from fabshuffle.bcdr.coordinator import RecoveryCoordinator
from fabshuffle.bcdr.service import (
    BcdrService,
    CapacityRoute,
    CutoverRequest,
    EnableRecoveryRequest,
    ItemReadiness,
    PlanRequest,
    SyncRequest,
    WriterFence,
)
from fabshuffle.bcdr.warehouse_catalog import WarehouseCatalog
from fabshuffle.fabric.client import FabricClient
from tests.test_bcdr_contracts import guid, recovery_set, snapshot
from tests.test_bcdr_warehouse_catalog import SqlHarness


class Capacities:
    def __init__(self):
        self.events = []

    def resume_catalog(self):
        self.events.append("resume_catalog")

    def close(self):
        self.events.append("close")

    def resume_business(self, runtime):
        runtime.fence()
        self.events.append("resume_business")

    def park(self, runtime, workspaces):
        assert not runtime.catalog.pending_operations()
        assert runtime.mode == RecoveryMode.STANDBY
        runtime.transition(RecoveryMode.PARKING)
        self.events.append(("park", tuple(workspaces)))


class Estate:
    def __init__(self, config):
        self.config = config
        self.calls = []
        self.roles = {}
        self.workspaces = {}
        self.items = {}
        self.lost_create = False
        self.unexpected_item_grant = None

    def handler(self, request):
        path = request.url.path.removeprefix("/v1/")
        self.calls.append((request.method, path))
        import json

        body = json.loads(request.content) if request.content else {}
        if path.startswith("capacities/"):
            return httpx.Response(200, json={"id": path.split("/")[1], "state": "Active"})
        if path == "workspaces" and request.method == "POST":
            identifier = guid()
            self.workspaces[identifier] = {"id": identifier, **body}
            self.roles[identifier] = [
                {
                    "id": guid(),
                    "principal": {
                        "id": self.config.access_policy.recovery_spn.object_id,
                        "type": "ServicePrincipal",
                    },
                    "role": "Admin",
                }
            ]
            return httpx.Response(201, json=self.workspaces[identifier])
        if path.endswith("/roleAssignments"):
            workspace = path.split("/")[1]
            if workspace not in self.roles:
                self.roles[workspace] = [
                    {
                        "id": guid(),
                        "principal": {"id": grant.principal.object_id, "type": grant.principal.kind},
                        "role": grant.role,
                    }
                    for grant in self.config.access_policy.workspace_grants()
                ]
            if request.method == "POST":
                row = {"id": guid(), **body}
                self.roles[workspace].append(row)
                return httpx.Response(201, json=row)
            return httpx.Response(200, json={"value": self.roles[workspace]})
        if "/roleAssignments/" in path and request.method == "DELETE":
            workspace, assignment = path.split("/")[1], path.split("/")[-1]
            self.roles[workspace] = [row for row in self.roles[workspace] if row["id"] != assignment]
            return httpx.Response(200)
        if path.startswith("admin/") and path.endswith("/users"):
            values = []
            if self.unexpected_item_grant:
                values = [{"principal": {"id": self.unexpected_item_grant, "type": "User"}}]
            return httpx.Response(200, json={"accessDetails": values})
        if path.endswith("/items") and request.method == "POST":
            identifier = guid()
            self.items[identifier] = {"id": identifier, **body}
            if self.lost_create:
                raise httpx.ReadError("connection lost after destination accepted create", request=request)
            return httpx.Response(201, json=self.items[identifier])
        if "/items/" in path and request.method == "PATCH":
            identifier = path.split("/")[-1]
            self.items[identifier].update(body)
            return httpx.Response(200, json=self.items[identifier])
        if path.startswith("workspaces/") and request.method == "GET":
            identifier = path.split("/")[1]
            return httpx.Response(200, json=self.workspaces.get(identifier, {"id": identifier}))
        raise AssertionError((request.method, path))


@pytest.fixture
def system(tmp_path, monkeypatch):
    config = recovery_set()
    harness = SqlHarness(tmp_path / "catalog.db")
    catalog = WarehouseCatalog(harness.connect, config, sleep=Mock())
    catalog.initialize()
    estate = Estate(config)
    tokens = Mock()
    tokens.token.return_value = "unit-test"
    tokens.tenant_id.return_value = config.tenant_id
    tokens.principal.client_id = guid()
    client = FabricClient(tokens, transport=httpx.MockTransport(estate.handler))
    captured = snapshot(config)
    observed = {}
    captures = []

    def capture(client, recovery_set, **kwargs):
        captures.append(kwargs)
        value = captured.model_copy(
            update={
                "generation_id": guid(),
                "parent_generation_id": kwargs.get("parent_generation_id"),
                "captured_at": now(),
            }
        )
        return CapturedGeneration(value, ())

    def hashes(item, payloads):
        return digest(canonical_json({"name": item.display_name})), digest(canonical_json(item.properties))

    monkeypatch.setattr("fabshuffle.bcdr.coordinator.captured_hashes", hashes)

    def apply(client, item, payloads, **kwargs):
        target_id = kwargs["target_id"]
        if target_id:
            client.patch(
                f"workspaces/{kwargs['target_workspace'].workspace_id}/items/{target_id}",
                json={"displayName": item.display_name},
            )
        else:
            result = client.post(
                f"workspaces/{kwargs['target_workspace'].workspace_id}/items",
                json={"displayName": item.display_name, "type": item.item_type},
            )
            target_id = result["id"]
        target = ItemIdentity(
            **kwargs["target_workspace"].model_dump(exclude={"schema_version"}),
            item_id=target_id,
        )
        observed[target.key] = digest(canonical_json({"name": item.display_name}))
        definition_hash, properties_hash = hashes(item, payloads)
        row = AppliedItem(
            source=item.identity,
            target=target,
            capture_generation_id=kwargs["generation_id"],
            operation_id=kwargs["operation_id"],
            applied_at=now(),
            definition_sha256=definition_hash,
            properties_sha256=properties_hash,
            target_observed_sha256=observed[target.key],
            outcome=RecoveryOutcome.RESTORED_STOPPED,
        )
        return AdapterResult(RecoveryOutcome.RESTORED_STOPPED, applied=row, metadata_applied=True)

    capacities = Capacities()
    coordinator = RecoveryCoordinator(
        config,
        catalog,
        client,
        capacities,
        source=client,
        tokens=tokens,
        source_tokens=tokens,
        capture=capture,
        apply=apply,
        observe=lambda _client, target, _type: observed[target.key],
        capabilities=lambda item: SimpleNamespace(
            inactive_create=True, is_store=False, reason="test contract"
        ),
    )
    request = SyncRequest(
        capacity_routes=(
            CapacityRoute(
                source_capacity_id=config.source_capacity_ids[0],
                target_capacity_id=config.target_capacity_ids[0],
            ),
        ),
        park=False,
    )
    return SimpleNamespace(
        c=coordinator,
        service=BcdrService(coordinator),
        config=config,
        catalog=catalog,
        estate=estate,
        captured=captured,
        capture=capture,
        captures=captures,
        observed=observed,
        request=request,
        capacities=capacities,
        harness=harness,
    )


def proofs(system, generation_id):
    return tuple(
        ItemReadiness(
            source=row.source,
            target=row.target,
            target_observed_sha256=system.observed[row.target.key],
            data_verified=True,
            references_verified=True,
            security_verified=True,
            effective_principals=(system.config.access_policy.recovery_spn,),
            evidence="operator-query-run-42",
            observed_at=now() - timedelta(seconds=1),
            valid_until=now() + timedelta(hours=1),
        )
        for row in system.catalog.applied_items()
        if row.capture_generation_id == generation_id
    )


def fence(side="primary", epoch=0, **updates):
    return WriterFence(
        expected_epoch=epoch,
        fenced_side=side,
        writers_stopped=True,
        evidence="external-writers-stopped-run",
        confirmed_by="incident-commander",
        observed_at=now() - timedelta(seconds=1),
        valid_until=now() + timedelta(hours=1),
        **updates,
    )


def enable(system):
    synced = system.service.synchronize(system.request)
    request = EnableRecoveryRequest(
        generation_id=synced.generation_id,
        group_ids=tuple(row.group_id for row in synced.groups),
        readiness=proofs(system, synced.generation_id),
    )
    return system.service.enable_recovery(request)


def test_real_catalog_sync_creates_workspace_before_item_and_records_intent(system):
    result = system.service.synchronize(system.request)
    assert result.groups[0].metadata_applied
    assert not result.groups[0].access_enabled
    assert not result.groups[0].active
    posts = [path for method, path in system.estate.calls if method == "POST"]
    assert posts[0] == "workspaces"
    assert posts[-1].endswith("/items")
    applied = system.catalog.applied_items()[0]
    history = system.catalog.operation_history(applied.operation_id)
    assert history[0].state == OperationState.INTENT
    assert history[-1].state == OperationState.SUCCEEDED
    assert history[-1].target == applied.target
    assert system.catalog.state().controller_id is None


def test_unchanged_generation_observes_without_update(system):
    system.service.synchronize(system.request)
    count = len([call for call in system.estate.calls if call[0] in {"POST", "PATCH"}])
    result = system.service.synchronize(system.request)
    assert result.groups[0].metadata_applied
    assert len([call for call in system.estate.calls if call[0] in {"POST", "PATCH"}]) == count
    assert system.catalog.applied_items()[0].capture_generation_id == result.generation_id


def test_drift_is_blocked_not_overwritten(system):
    system.service.synchronize(system.request)
    row = system.catalog.applied_items()[0]
    system.observed[row.target.key] = digest(b"operator DR edit")
    result = system.service.synchronize(system.request)
    assert result.exit_code == 2
    assert "drift" in " ".join(result.groups[0].blockers)
    assert not any(method == "PATCH" for method, _ in system.estate.calls)


def test_lost_create_keeps_intent_and_does_not_repeat(system):
    system.estate.lost_create = True
    with pytest.raises(RecoveryBlocked, match="reconcile"):
        system.service.synchronize(system.request)
    assert len(system.estate.items) == 1
    assert system.catalog.pending_operations()[0].state == OperationState.AMBIGUOUS
    assert system.catalog.state().controller_id is not None
    with pytest.raises(CatalogConflict):
        system.service.synchronize(system.request)
    assert len(system.estate.items) == 1


def test_enable_is_source_offline_and_blocks_sync_and_pause(system):
    result = system.service.synchronize(system.request)
    system.c.source = Mock(side_effect=AssertionError("source is unavailable"))
    system.c.source.get.side_effect = AssertionError("source is unavailable")
    enabled = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=result.generation_id,
            group_ids=(result.groups[0].group_id,),
            readiness=proofs(system, result.generation_id),
        )
    )
    assert enabled.groups[0].access_enabled
    assert enabled.mode == RecoveryMode.ENABLING_RECOVERY
    with pytest.raises(RecoveryBlocked, match="mode"):
        system.service.synchronize(system.request.model_copy(update={"park": True}))
    assert not any(isinstance(event, tuple) and event[0] == "park" for event in system.capacities.events)
    assert not system.c.source.mock_calls


def test_cutover_requires_writer_fence_and_current_identity_bound_evidence(system):
    result = enable(system)
    request = CutoverRequest(
        generation_id=result.generation_id,
        group_ids=(result.groups[0].group_id,),
        readiness=proofs(system, result.generation_id),
        writer_fence=fence(),
    )
    with pytest.raises(ValueError, match="Fence every external writer"):
        system.service.cutover(request.model_copy(update={"writer_fence": fence(epoch=9)}))
    stale = request.readiness[0].model_copy(update={"target_observed_sha256": digest(b"stale")})
    with pytest.raises(RecoveryBlocked, match="evidence"):
        system.service.cutover(request.model_copy(update={"readiness": (stale,)}))
    active = system.service.cutover(request)
    assert active.mode == RecoveryMode.ACTIVE_RECOVERY
    assert active.groups[0].active
    assert system.c.runtime.get("lifecycle", "writer")["epoch"] == 1
    assert not any("jobs" in path for _, path in system.estate.calls)


def test_plan_and_status_report_authoritative_inventory(system):
    synced = system.service.synchronize(system.request)
    plan = system.service.plan(
        PlanRequest(
            generation_id=synced.generation_id,
            capacity_routes=system.request.capacity_routes,
        )
    )
    assert plan.details["inventory"]["workspaces"][0]["display_name"] == "Source"
    assert system.service.status().details["inventory"]["items"][0]["display_name"] == "A notebook"


def test_target_item_grant_drift_blocks_before_update(system):
    system.service.synchronize(system.request)
    system.estate.unexpected_item_grant = guid()
    with pytest.raises(RecoveryBlocked, match="Unexpected item access"):
        system.service.synchronize(system.request)
    assert not any(method == "PATCH" for method, _ in system.estate.calls)


def test_explicit_park_follows_committed_results(system):
    result = system.service.synchronize(system.request.model_copy(update={"park": True}))
    assert result.mode == RecoveryMode.PARKING
    assert system.catalog.state().mode == RecoveryMode.PARKING
    assert system.catalog.applied_items()
    assert not system.catalog.pending_operations()
    assert system.catalog.state().controller_id is not None


def test_tombstone_never_deletes_standby(system):
    first = system.service.synchronize(system.request)
    generation = system.catalog.load_generation(first.generation_id)

    def deleted(*args, **kwargs):
        return CapturedGeneration(
            generation.snapshot.model_copy(
                update={
                    "generation_id": guid(),
                    "parent_generation_id": first.generation_id,
                    "items": (),
                }
            ),
            (),
        )

    system.c.capture = deleted
    result = system.service.synchronize(system.request)
    assert system.catalog.load_generation().snapshot.items[0].tombstone
    assert any("tombstone" in warning for warning in result.warnings)
    assert not any(method == "DELETE" for method, _ in system.estate.calls)


def test_cross_workspace_dependency_creation_order(system):
    first = system.captured
    second = snapshot(system.config)
    edge = DependencyEdge(
        edge_id=guid(),
        consumer=second.items[0].identity,
        prerequisite=first.items[0].identity,
        phase="create",
        provenance="definition field",
        detail="Notebook depends on its store",
    )
    combined = first.model_copy(
        update={
            "workspaces": (*first.workspaces, *second.workspaces),
            "items": (*first.items, second.items[0].model_copy(update={"display_name": "dependent"})),
            "dependencies": (edge,),
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(combined, ())
    result = system.service.synchronize(system.request)
    assert all(group.metadata_applied for group in result.groups)
    assert [row["displayName"] for row in system.estate.items.values()] == ["A notebook", "dependent"]
    mutations = [path for method, path in system.estate.calls if method == "POST"]
    assert max(i for i, path in enumerate(mutations) if path == "workspaces") < min(
        i for i, path in enumerate(mutations) if path.endswith("/items")
    )


def test_catalog_loss_prevents_next_business_mutation(system, monkeypatch):
    def lost(*args, **kwargs):
        raise ConnectionError("control Warehouse unavailable")

    monkeypatch.setattr(system.catalog, "begin_operation", lost)
    with pytest.raises(ConnectionError, match="Warehouse"):
        system.service.synchronize(system.request)
    assert not any(method in {"POST", "PATCH", "DELETE"} for method, _ in system.estate.calls)


def test_no_runtime_identity_grant_in_standby(system):
    result = system.service.synchronize(system.request)
    permitted = {grant.principal.object_id for grant in system.config.access_policy.workspace_grants()}
    assert all(row["principal"]["id"] in permitted for rows in system.estate.roles.values() for row in rows)
    assert not result.groups[0].access_enabled


def test_optional_data_missing_does_not_abort_independent_metadata_group(system):
    first = system.captured
    second = snapshot(system.config)
    store = second.items[0].model_copy(update={"item_type": "SQLDatabase", "display_name": "unprotected SQL"})
    captured = first.model_copy(
        update={
            "workspaces": (*first.workspaces, *second.workspaces),
            "items": (*first.items, store),
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(captured, ())
    synced = system.service.synchronize(system.request)
    result = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=tuple(row.group_id for row in synced.groups),
            readiness=proofs(system, synced.generation_id),
        )
    )
    assert len(result.groups) == 2
    assert sum(row.access_enabled for row in result.groups) == 1
    blocked = next(row for row in result.groups if not row.access_enabled)
    assert "unprotected SQL" in " ".join(blocked.blockers)
    assert result.exit_code == 2
