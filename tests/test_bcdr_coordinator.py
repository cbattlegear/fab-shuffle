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
from tests import test_bcdr_protection_lakehouse as lakehouse_cases
from tests.test_bcdr_contracts import guid, recovery_set, snapshot
from tests.test_bcdr_warehouse_catalog import SqlHarness


class Capacities:
    def __init__(self):
        self.events = []

    def resume_catalog(self):
        self.events.append("resume_catalog")

    def close(self):
        self.events.append("close")

    def observations(self):
        return (
            {"state": "Suspended" if any(isinstance(event, tuple) for event in self.events) else "Active"},
        )

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
        self.operation_results = {}
        self.shortcuts = {}

    def handler(self, request):
        path = request.url.path.removeprefix("/v1/")
        self.calls.append((request.method, path))
        import json

        body = json.loads(request.content) if request.content else {}
        if "/shortcuts" in path:
            parts = path.split("/")
            identifier = parts[3]
            shortcuts = self.shortcuts.setdefault(identifier, [])
            if request.method == "POST":
                assert request.url.params["shortcutConflictPolicy"] == "Abort"
                shortcuts.append(body)
                return httpx.Response(201, json=body)
            if path.endswith("/shortcuts"):
                return httpx.Response(200, json={"value": shortcuts})
            key = "/".join(parts[5:])
            match = next(row for row in shortcuts if f"{row['path']}/{row['name']}" == key)
            return httpx.Response(200, json=match)
        if "/lakehouses/" in path and request.method == "GET":
            return httpx.Response(200, json=self.items[path.split("/")[-1]])
        if path.startswith("connections/") and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "id": path.split("/")[1],
                    "connectionDetails": {"type": "Web", "path": "https://external.example"},
                },
            )
        if path.endswith("/jobs/instances") and request.method == "GET":
            return httpx.Response(200, json={"value": []})
        if path.endswith("/getDefinition"):
            return httpx.Response(
                200,
                json={
                    "definition": self.items[path.split("/")[3]].get("definition", {"parts": []}),
                },
            )
        if path.endswith("/updateDefinition"):
            self.items[path.split("/")[3]]["definition"] = body["definition"]
            return httpx.Response(200, json={})
        if path.startswith("operations/"):
            identifier = path.split("/")[1]
            return httpx.Response(
                200,
                json=(
                    self.operation_results[identifier]
                    if path.endswith("/result")
                    else {"status": "Succeeded"}
                ),
            )
        if path.startswith("capacities/"):
            return httpx.Response(200, json={"id": path.split("/")[1], "state": "Active"})
        if path == "admin/workspaces":
            return httpx.Response(
                200,
                json={
                    "workspaces": [
                        row
                        for row in self.workspaces.values()
                        if row["capacityId"] == request.url.params.get("capacityId")
                    ]
                },
            )
        if path == "workspaces" and request.method == "POST":
            identifier = guid()
            self.workspaces[identifier] = {
                "id": identifier,
                "type": "Workspace",
                "capacityAssignmentProgress": "Completed",
                **body,
            }
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
            self.items[identifier] = {"id": identifier, "workspaceId": path.split("/")[1], **body}
            if self.lost_create:
                raise httpx.ReadError("connection lost after destination accepted create", request=request)
            return httpx.Response(201, json=self.items[identifier])
        if "/items/" in path and request.method == "PATCH":
            identifier = path.split("/")[-1]
            self.items[identifier].update(body)
            return httpx.Response(200, json=self.items[identifier])
        if "/items/" in path and request.method == "GET":
            return httpx.Response(200, json=self.items[path.split("/")[-1]])
        if path.endswith("/items") and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "value": [row for row in self.items.values() if row["workspaceId"] == path.split("/")[1]]
                },
            )
        if path.startswith("workspaces/") and request.method == "GET":
            identifier = path.split("/")[1]
            return httpx.Response(200, json=self.workspaces.get(identifier, {"id": identifier}))
        raise AssertionError((request.method, path))


@pytest.fixture
def system(tmp_path, monkeypatch, request):
    config = recovery_set()
    if getattr(request, "param", {}).get("catalog_separate"):
        config = config.model_copy(update={"target_capacity_ids": (*config.target_capacity_ids, guid())})
    harness = SqlHarness(tmp_path / "catalog.db")
    catalog = WarehouseCatalog(harness.connect, config, sleep=Mock())
    catalog.initialize()
    estate = Estate(config)
    tokens = Mock()
    tokens.token.return_value = "unit-test"
    tokens.tenant_id.return_value = config.tenant_id
    tokens.object_id.return_value = config.access_policy.recovery_spn.object_id
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
                target_capacity_id=config.target_capacity_ids[-1],
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
    generation = system.catalog.load_generation(generation_id)
    groups = system.c._groups(generation_id)
    writer = system.c.runtime.get("lifecycle", "writer") or {"epoch": 0}
    return tuple(
        ItemReadiness(
            source=row.source,
            target=row.target,
            generation_id=generation_id,
            writer_epoch=writer["epoch"],
            issuer=system.config.access_policy.recovery_spn,
            target_observed_sha256=system.observed[row.target.key],
            data_verified=True,
            references_verified=True,
            security_verified=True,
            effective_principals=system.c.intended_runtime_principals(
                generation,
                next(group for group in groups if row.source in group.items),
            ),
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


def production_capacities(system, tmp_path, monkeypatch):
    from fabshuffle.bcdr.bootstrap import BootstrapDescriptor, BootstrapStore, CapacityAuthorization
    from fabshuffle.bcdr.capacity import ArmCapacityClient, CapacityCoordinator
    from fabshuffle.bcdr.production import ProductionCapacities

    config = system.config
    resource = (
        f"/subscriptions/{config.tenant_id}/resourcegroups/recovery/"
        "providers/microsoft.fabric/capacities/catalog"
    )
    descriptor = BootstrapDescriptor(
        recovery_set_id=config.recovery_set_id,
        tenant_id=config.tenant_id,
        application_id=system.c.tokens.principal.client_id,
        controller_id=system.c.runtime.controller_id,
        capacities=(
            CapacityAuthorization(
                arm_resource_id=resource,
                fabric_capacity_id=config.target_capacity_ids[0],
                dedicated_recovery=True,
                authorized_for_suspend=True,
            ),
        ),
        catalog_capacity_id=resource,
        control_workspace_id=config.control_workspace.workspace_id,
        control_warehouse_id=config.control_warehouse.item_id,
        tds_host="example.datawarehouse.fabric.microsoft.com",
        tds_catalog=config.control_warehouse.item_id,
    )
    store = BootstrapStore(tmp_path / "bootstrap.json")
    store.save(descriptor, expected_revision=None)
    state = {"value": "Active", "calls": []}

    def arm_handler(request):
        state["calls"].append((request.method, request.url.path))
        if request.method == "POST":
            state["value"] = "Suspended" if request.url.path.endswith("/suspend") else "Active"
            return httpx.Response(200)
        return httpx.Response(
            200,
            json={
                "id": resource,
                "type": "Microsoft.Fabric/capacities",
                "sku": {"name": "F2", "tier": "Fabric"},
                "properties": {"state": state["value"], "provisioningState": "Succeeded"},
            },
        )

    capacities = ProductionCapacities(store, system.c.tokens, system.c.destination.client)
    capacities.arm.close()
    capacities.arm = ArmCapacityClient(system.c.tokens, transport=httpx.MockTransport(arm_handler))
    capacities.driver = CapacityCoordinator(store, capacities.arm)
    capacities.catalog = system.catalog
    monkeypatch.setattr(WarehouseCatalog, "open_from_endpoint", lambda *args, **kwargs: system.catalog)
    return capacities, store, state


def test_production_parking_then_source_free_resume_reconciles_epoch(system, tmp_path, monkeypatch):
    system.service.synchronize(system.request)
    capacities, store, state = production_capacities(system, tmp_path, monkeypatch)
    runtime = system.c.runtime
    with runtime.controller({RecoveryMode.STANDBY}):
        capacities.park(runtime, tuple(system.c.workspace_mappings().values()))
    assert system.catalog.state().mode == RecoveryMode.PARKING
    assert system.catalog.pending_operations()[0].kind == "suspend_capacity"
    assert state["value"] == "Suspended"
    assert store.load().parking.epoch == system.catalog.state().epoch
    capacities.resume_catalog()
    assert state["value"] == "Active"
    assert system.catalog.state().mode == RecoveryMode.STANDBY
    assert not system.catalog.pending_operations()
    assert system.catalog.state().controller_id is None
    assert store.load().parking is None
    capacities.close()


def test_production_pause_refuses_unrelated_workspace(system, tmp_path, monkeypatch):
    system.service.synchronize(system.request)
    capacities, _, state = production_capacities(system, tmp_path, monkeypatch)
    rogue = guid()
    system.estate.workspaces[rogue] = {"id": rogue, "capacityId": system.config.target_capacity_ids[0]}
    with system.c.runtime.controller({RecoveryMode.STANDBY}):
        with pytest.raises(RecoveryBlocked, match="unowned workspace"):
            capacities.park(system.c.runtime, tuple(system.c.workspace_mappings().values()))
    assert not any(method == "POST" for method, _ in state["calls"])
    assert system.catalog.state().mode == RecoveryMode.STANDBY
    capacities.close()


def test_production_pause_refuses_untracked_item(system, tmp_path, monkeypatch):
    system.service.synchronize(system.request)
    capacities, _, state = production_capacities(system, tmp_path, monkeypatch)
    workspace = next(iter(system.c.workspace_mappings().values()))
    rogue = guid()
    system.estate.items[rogue] = {"id": rogue, "workspaceId": workspace.workspace_id}
    with system.c.runtime.controller({RecoveryMode.STANDBY}):
        with pytest.raises(RecoveryBlocked, match="untracked item"):
            capacities.park(system.c.runtime, (workspace,))
    assert not any(method == "POST" for method, _ in state["calls"])
    capacities.close()


@pytest.mark.parametrize("owned_endpoint", [False, True])
def test_parking_accepts_only_exact_owned_lakehouse_derived_endpoint(
    system,
    tmp_path,
    monkeypatch,
    owned_endpoint,
):
    source = system.captured.items[0].model_copy(update={"item_type": "Lakehouse"})
    model = source.model_copy(
        update={
            "identity": source.identity.model_copy(update={"item_id": guid()}),
            "item_type": "SemanticModel",
            "display_name": "Independent former-default model",
        }
    )
    captured = system.captured.model_copy(update={"items": (source, model)})
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(captured, ())
    result = system.service.synchronize(system.request)
    assert all(group.metadata_applied for group in result.groups)
    mappings = system.c.item_mappings()
    parent = mappings[source.identity.key].target
    endpoint = guid()
    system.estate.items[parent.item_id]["properties"] = {
        "sqlEndpointProperties": {
            "id": endpoint,
            "connectionString": "owned.sql",
            "provisioningStatus": "Success",
        },
    }
    raw_id = endpoint if owned_endpoint else guid()
    system.estate.items[raw_id] = {
        "id": raw_id,
        "workspaceId": parent.workspace_id,
        "type": "SQLEndpoint",
    }
    capacities, _, state = production_capacities(system, tmp_path, monkeypatch)
    with system.c.runtime.controller({RecoveryMode.STANDBY}):
        if owned_endpoint:
            capacities.park(system.c.runtime, tuple(system.c.workspace_mappings().values()))
            assert state["value"] == "Suspended"
        else:
            with pytest.raises(RecoveryBlocked, match="untracked item"):
                capacities.park(system.c.runtime, tuple(system.c.workspace_mappings().values()))
            assert state["value"] == "Active"
    capacities.close()


def test_deployment_lock_is_exclusive_and_released(tmp_path):
    from fabshuffle.bcdr.production import DeploymentLock

    path = tmp_path / "bootstrap.json"
    first = DeploymentLock(path)
    with pytest.raises(RecoveryBlocked, match="Another worker"):
        DeploymentLock(path)
    first.close()
    second = DeploymentLock(path)
    second.close()


def test_production_factory_opens_sql_only_after_arm_resume(system, tmp_path, monkeypatch):
    from fabshuffle.bcdr import production
    from fabshuffle.bcdr.capacity import ArmCapacityClient
    from fabshuffle.bcdr.service import create_service

    capacities, store, state = production_capacities(system, tmp_path, monkeypatch)
    transport = capacities.arm.http._transport
    state["value"] = "Suspended"
    actual_arm = ArmCapacityClient
    monkeypatch.setattr(
        production, "ArmCapacityClient", lambda tokens: actual_arm(tokens, transport=transport)
    )
    monkeypatch.setattr(production, "FabricClient", lambda tokens: system.c.destination.client)

    def opened(*args, **kwargs):
        assert state["value"] == "Active"
        assert kwargs["expected_recovery_set_id"] == system.config.recovery_set_id
        return system.catalog

    monkeypatch.setattr(WarehouseCatalog, "open_from_endpoint", opened)
    service = create_service(store.path, target_tokens=system.c.tokens)
    assert service.coordinator.source is None
    assert service.coordinator.catalog is system.catalog
    service.close()
    capacities.close()


@pytest.mark.parametrize("reference_kind", ["workspace", "sql-host"])
def test_source_reference_connection_is_rejected_before_mutation(system, reference_kind):
    from fabshuffle.bcdr.contracts import ConnectionIdentity
    from fabshuffle.bcdr.service import ConnectionRoute

    captured = system.captured
    if reference_kind == "sql-host":
        captured = captured.model_copy(
            update={
                "items": (
                    captured.items[0].model_copy(
                        update={"properties": {"serverFqdn": "source.database.fabric.microsoft.com,1433"}}
                    ),
                )
            }
        )
        system.c.capture = lambda *args, **kwargs: CapturedGeneration(captured, ())
    synced = system.service.synchronize(system.request)
    source = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    target = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    original_get = system.c.destination.get
    system.c.destination.get = lambda path, **kwargs: (
        {
            "id": target.connection_id,
            "connectionDetails": {
                "path": (
                    system.captured.workspaces[0].identity.workspace_id
                    if reference_kind == "workspace"
                    else "source.database.fabric.microsoft.com;source_database"
                )
            },
        }
        if path.startswith("connections/")
        else original_get(path, **kwargs)
    )
    with pytest.raises(RecoveryBlocked, match="still names the source"):
        system.service.plan(
            PlanRequest(
                generation_id=synced.generation_id,
                capacity_routes=system.request.capacity_routes,
                connection_mappings=(
                    ConnectionRoute(source=source, target=target, evidence="operator-approval"),
                ),
            )
        )


@pytest.mark.parametrize("new_workspace", [False, True])
def test_production_setup_wires_real_provisioners_and_sql_catalog(
    system,
    tmp_path,
    monkeypatch,
    new_workspace,
):
    from functools import partial

    from fabshuffle.bcdr import production
    from fabshuffle.bcdr.bootstrap import BootstrapStore
    from fabshuffle.bcdr.service import SetupCapacity, SetupRequest, setup

    capacities, descriptor_store, _ = production_capacities(system, tmp_path, monkeypatch)
    descriptor = descriptor_store.load()
    system.c.tokens.object_id.return_value = system.config.access_policy.recovery_spn.object_id
    control_id = system.config.control_workspace.workspace_id
    system.estate.workspaces[control_id] = {
        "id": control_id,
        "type": "Workspace",
        "capacityId": system.config.target_capacity_ids[0],
        "capacityAssignmentProgress": "Completed",
    }
    warehouse_id = guid()

    def fabric_handler(request):
        path = request.url.path.removeprefix("/v1/")
        if path.endswith("/warehouses") or "/warehouses/" in path:
            return httpx.Response(
                201 if request.method == "POST" else 200,
                json={
                    "id": warehouse_id,
                    "type": "Warehouse",
                    "workspaceId": path.split("/")[1],
                    "properties": {"connectionString": "example.datawarehouse.fabric.microsoft.com"},
                },
            )
        return system.estate.handler(request)

    transport = httpx.MockTransport(fabric_handler)
    monkeypatch.setattr(production, "FabricClient", partial(FabricClient, transport=transport))
    monkeypatch.setattr(
        production,
        "ControlWorkspaceProvisioner",
        partial(production.ControlWorkspaceProvisioner, transport=transport),
    )
    monkeypatch.setattr(
        production,
        "ControlWarehouseProvisioner",
        partial(production.ControlWarehouseProvisioner, transport=transport),
    )
    monkeypatch.setattr(production, "ArmCapacityClient", lambda tokens: capacities.arm)
    created = []

    def new_catalog(server, database, tokens, config):
        harness = SqlHarness(tmp_path / "setup-catalog.db")
        catalog = WarehouseCatalog(harness.connect, config, sleep=Mock())
        created.append(catalog)
        return catalog

    monkeypatch.setattr(WarehouseCatalog, "from_endpoint", new_catalog)
    path = tmp_path / "new-deployment.json"
    request = SetupRequest(
        control_workspace_name="Recovery control" if new_workspace else None,
        control_workspace_id=None if new_workspace else control_id,
        source_capacity_ids=system.config.source_capacity_ids,
        recovery_capacities=tuple(
            SetupCapacity(
                arm_resource_id=row.arm_resource_id,
                fabric_capacity_id=row.fabric_capacity_id,
                dedicated_recovery=True,
                authorized_for_suspend=True,
            )
            for row in descriptor.capacities
        ),
        catalog_capacity_id=descriptor.catalog_capacity_id,
        access_policy=system.config.access_policy,
    )
    result = setup(request, path, target_tokens=system.c.tokens)
    stored = BootstrapStore(path).load()
    assert stored.control_warehouse_id == warehouse_id
    assert result.details["control_warehouse_id"] == warehouse_id
    assert created[0].state().mode == RecoveryMode.STANDBY
    assert created[0].recovery_set.control_workspace.workspace_id == stored.control_workspace_id
    assert "not-a-real-secret" not in path.read_text()


def test_reconcile_uses_exact_lro_receipt_without_recreating_item(system):
    from fabshuffle.bcdr.service import ReconcileOperationRequest
    from fabshuffle.fabric.client import OperationTimeout

    original_apply = system.c.apply
    original_request = system.c.destination.client.request
    service_id = guid()

    def accepted(method, path, **kwargs):
        response = original_request(method, path, **kwargs)
        if method == "POST" and path.endswith("/items"):
            document = response.json()
            system.estate.operation_results[service_id] = document
            return httpx.Response(202, headers={"x-ms-operation-id": service_id}, request=response.request)
        return response

    system.c.destination.client.request = accepted

    def timeout(client, item, payloads, **kwargs):
        client.request(
            "POST",
            f"workspaces/{kwargs['target_workspace'].workspace_id}/items",
            json={"displayName": item.display_name, "type": item.item_type},
        )
        raise OperationTimeout("poll interrupted while the service continues")

    system.c.apply = timeout
    with pytest.raises(RecoveryBlocked, match="reconcile"):
        system.service.synchronize(system.request)
    pending = system.catalog.pending_operations()[0]
    before = system.catalog.state()
    assert len(system.estate.items) == 1
    system.c.apply = original_apply
    system.c.destination.client.request = original_request
    system.c.source = None
    result = system.service.reconcile_operation(
        ReconcileOperationRequest(
            operation_id=pending.operation_id,
            expected_controller_id=before.controller_id,
            expected_epoch=before.epoch,
            previous_controller_stopped=True,
            fencing_evidence="worker-process-terminated",
            target_quiescence_evidence="target-writers-fenced",
        )
    )
    assert result.details["reconciled_operation"] == pending.operation_id
    assert len(system.estate.items) == 1
    assert not system.catalog.pending_operations()
    assert len(system.catalog.applied_items()) == 1


def test_reconcile_refuses_name_only_unknown_create(system):
    from fabshuffle.bcdr.service import ReconcileOperationRequest

    system.estate.lost_create = True
    with pytest.raises(RecoveryBlocked):
        system.service.synchronize(system.request)
    state = system.catalog.state()
    pending = system.catalog.pending_operations()[0]
    with pytest.raises(RecoveryBlocked, match="same-name"):
        system.service.reconcile_operation(
            ReconcileOperationRequest(
                operation_id=pending.operation_id,
                expected_controller_id=state.controller_id,
                expected_epoch=state.epoch,
                previous_controller_stopped=True,
                fencing_evidence="worker-terminated",
                target_quiescence_evidence="target-stopped",
            )
        )
    assert len(system.estate.items) == 1
    assert system.catalog.pending_operations()


def test_reconcile_saved_success_after_catalog_loss_does_not_write_fabric(system, monkeypatch):
    import pyodbc

    from fabshuffle.bcdr.catalog import CatalogError
    from fabshuffle.bcdr.service import ReconcileOperationRequest

    original_pending = system.catalog.pending_operations
    unavailable = False

    def lost(*args, **kwargs):
        nonlocal unavailable
        unavailable = True
        raise pyodbc.OperationalError("08S01", "catalog connection lost")

    def pending():
        if unavailable:
            raise pyodbc.OperationalError("08S01", "catalog connection lost")
        return original_pending()

    with monkeypatch.context() as patch:
        patch.setattr(system.catalog, "record_applied", lost)
        patch.setattr(system.catalog, "pending_operations", pending)
        with pytest.raises(CatalogError, match="catalog connection lost"):
            system.service.synchronize(system.request)
    state = system.catalog.state()
    operation = next(row for row in system.catalog.operations() if row.kind == "item-apply")
    assert operation.state == OperationState.SUCCEEDED
    writes = [call for call in system.estate.calls if call[0] != "GET"]
    system.service.reconcile_operation(
        ReconcileOperationRequest(
            operation_id=operation.operation_id,
            expected_controller_id=state.controller_id,
            expected_epoch=state.epoch,
            previous_controller_stopped=True,
            fencing_evidence="worker-stopped",
            target_quiescence_evidence="target-stopped",
        )
    )
    assert system.catalog.applied_items()[0].operation_id == operation.operation_id
    assert writes == [call for call in system.estate.calls if call[0] != "GET"]


def test_changed_connection_rebinds_unchanged_item(system):
    from fabshuffle.bcdr.contracts import ConnectionIdentity
    from fabshuffle.bcdr.service import ConnectionRoute

    source = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    first = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    second = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    captured = system.captured.model_copy(
        update={
            "dependencies": (
                DependencyEdge(
                    edge_id=guid(),
                    consumer=system.captured.items[0].identity,
                    prerequisite=source,
                    phase="bind",
                    provenance="captured connection",
                    detail="Use a verified external connection",
                ),
            )
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(
        captured.model_copy(
            update={
                "generation_id": guid(),
                "parent_generation_id": kwargs.get("parent_generation_id"),
            }
        ),
        (),
    )
    original_get = system.c.destination.get
    system.c.destination.get = lambda path, **kwargs: (
        {
            "id": path.split("/")[-1],
            "connectionDetails": {"type": "Web", "path": "https://independent.example"},
        }
        if path.startswith("connections/")
        else original_get(path, **kwargs)
    )
    system.c.apply = Mock(wraps=system.c.apply)
    for target in (first, second):
        result = system.service.synchronize(
            system.request.model_copy(
                update={
                    "connection_mappings": (
                        ConnectionRoute(source=source, target=target, evidence="approved"),
                    ),
                }
            )
        )
        assert result.groups[0].metadata_applied
    assert system.c.apply.call_count == 2
    assert system.c.apply.call_args.kwargs["connection_mappings"] == ((source, second),)


def test_corrected_preflight_refusal_retries_pinned_generation(system):
    original = system.c.apply
    system.c.apply = lambda *args, **kwargs: AdapterResult(
        RecoveryOutcome.BLOCKED,
        diagnostics=("Supply target quiescence",),
    )
    first = system.service.synchronize(system.request)
    assert not first.groups[0].metadata_applied
    assert (
        next(row for row in system.catalog.operations() if row.kind == "item-apply").state
        == OperationState.FAILED
    )
    system.c.apply = original
    second = system.service.synchronize(
        system.request.model_copy(
            update={
                "capture": False,
                "generation_id": first.generation_id,
                "target_quiescence_evidence": "writers-fenced",
            }
        )
    )
    assert second.groups[0].metadata_applied
    assert len(system.estate.items) == 1


def test_partial_enable_rechecks_corrected_group_evidence(system):
    second = snapshot(system.config)
    capture = system.captured.model_copy(
        update={
            "workspaces": (*system.captured.workspaces, *second.workspaces),
            "items": (*system.captured.items, *second.items),
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(capture, ())
    synced = system.service.synchronize(system.request)
    all_proofs = proofs(system, synced.generation_id)
    first = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=tuple(row.group_id for row in synced.groups),
            readiness=(all_proofs[0],),
        )
    )
    failed = next(row for row in first.groups if not row.access_enabled)
    assert sum(row.access_enabled for row in first.groups) == 1
    retried = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(failed.group_id,),
            readiness=all_proofs,
        )
    )
    assert retried.groups[0].access_enabled
    assert retried.groups[0].blockers == ()


def test_deferred_schedule_stays_stopped_without_blocking_acl_admission(system):
    from dataclasses import replace

    original = system.c.apply
    system.c.apply = lambda *args, **kwargs: replace(
        original(*args, **kwargs),
        deferred_grants=({"scope": "schedule", "path": ".schedules"},),
    )
    result = enable(system)
    assert result.groups[0].access_enabled
    assert not any("jobs" in path for _, path in system.estate.calls)


def test_blocked_group_can_enable_after_another_group_cutover(system):
    second = snapshot(system.config)
    capture = system.captured.model_copy(
        update={
            "workspaces": (*system.captured.workspaces, *second.workspaces),
            "items": (*system.captured.items, *second.items),
        }
    )
    system.c.capture = Mock(return_value=CapturedGeneration(capture, ()))
    synced = system.service.synchronize(system.request)
    evidence = proofs(system, synced.generation_id)
    enabled = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=tuple(row.group_id for row in synced.groups),
            readiness=(evidence[0],),
        )
    )
    first = next(row for row in enabled.groups if row.access_enabled)
    blocked = next(row for row in enabled.groups if not row.access_enabled)
    system.service.cutover(
        CutoverRequest(
            generation_id=synced.generation_id,
            group_ids=(first.group_id,),
            readiness=proofs(system, synced.generation_id),
            writer_fence=fence(),
        )
    )
    writes = [row for row in system.estate.calls if row[0] != "GET"]
    result = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(blocked.group_id,),
            readiness=proofs(system, synced.generation_id),
        )
    )
    assert result.mode == RecoveryMode.ACTIVE_RECOVERY
    assert result.groups[0].access_enabled
    assert next(
        row for row in system.c._groups(synced.generation_id) if row.group_id == first.group_id
    ).active
    assert writes == [row for row in system.estate.calls if row[0] != "GET"]
    assert system.c.capture.call_count == 1


@pytest.mark.parametrize("invalid_field", ["generation_id", "writer_epoch", "issuer", "effective_principals"])
def test_readiness_is_generation_epoch_issuer_and_runtime_bound(system, invalid_field):
    first = system.service.synchronize(system.request)
    old = proofs(system, first.generation_id)[0]
    second = system.service.synchronize(system.request)
    good = proofs(system, second.generation_id)[0]
    stranger = system.config.access_policy.recovery_spn.model_copy(update={"object_id": guid()})
    replacements = {
        "generation_id": old.generation_id,
        "writer_epoch": good.writer_epoch + 1,
        "issuer": stranger,
        "effective_principals": (stranger,),
    }
    result = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=second.generation_id,
            group_ids=(second.groups[0].group_id,),
            readiness=(good.model_copy(update={invalid_field: replacements[invalid_field]}),),
        )
    )
    assert not result.groups[0].access_enabled
    assert result.groups[0].blockers


@pytest.mark.parametrize("reuse_external", [False, True])
def test_exact_lro_reconciliation_replays_pinned_routes_with_real_adapter(system, reuse_external):
    from fabshuffle.bcdr.adapters import apply_captured_item, observe_target
    from fabshuffle.bcdr.capture import make_payload
    from fabshuffle.bcdr.contracts import ConnectionIdentity, PayloadPurpose
    from fabshuffle.bcdr.service import ConnectionRoute, ReconcileOperationRequest
    from fabshuffle.fabric.client import OperationTimeout
    from fabshuffle.fabric.definitions import decode_json_part

    source = ConnectionIdentity(tenant_id=system.config.tenant_id, connection_id=guid())
    target = source if reuse_external else source.model_copy(update={"connection_id": guid()})
    item = system.captured.items[0]
    payload = make_payload(
        item.identity,
        "definition.json",
        canonical_json({"connectionId": source.connection_id}),
        PayloadPurpose.DEFINITION,
    )
    item = item.model_copy(
        update={
            "payload_ids": (payload.descriptor.payload_id,),
            "properties": {"bcdr": {"connections": [{"id": source.connection_id}]}},
        }
    )
    generation = system.captured.model_copy(
        update={
            "items": (item,),
            "payloads": (payload.descriptor,),
            "dependencies": (
                DependencyEdge(
                    edge_id=guid(),
                    consumer=item.identity,
                    prerequisite=source,
                    phase="bind",
                    provenance="definition",
                    detail="Use an independently approved connection",
                ),
            ),
        }
    )
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(generation, (payload,))
    service_id = guid()
    original_request = system.c.destination.client.request

    def accepted(method, path, **kwargs):
        response = original_request(method, path, **kwargs)
        if method == "POST" and path.endswith("/items"):
            system.estate.operation_results[service_id] = response.json()
            return httpx.Response(202, headers={"x-ms-operation-id": service_id}, request=response.request)
        return response

    system.c.destination.client.request = accepted

    def interrupted(client, item, payloads, **kwargs):
        client.request(
            "POST",
            f"workspaces/{kwargs['target_workspace'].workspace_id}/items",
            json={"type": "Notebook", "displayName": item.display_name},
        )
        raise OperationTimeout("interrupted before target metadata binding")

    system.c.apply = interrupted
    with pytest.raises(RecoveryBlocked, match="reconcile"):
        system.service.synchronize(
            system.request.model_copy(
                update={
                    "connection_mappings": (
                        ConnectionRoute(source=source, target=target, evidence="approved"),
                    ),
                }
            )
        )
    operation = system.catalog.pending_operations()[0]
    state = system.catalog.state()
    system.c.destination.client.request = original_request
    system.c.apply = apply_captured_item
    system.c.observe = observe_target
    system.c.source = None
    result = system.service.reconcile_operation(
        ReconcileOperationRequest(
            operation_id=operation.operation_id,
            expected_controller_id=state.controller_id,
            expected_epoch=state.epoch,
            previous_controller_stopped=True,
            fencing_evidence="previous worker stopped",
            target_quiescence_evidence="target scheduler fenced",
        )
    )
    definition = next(iter(system.estate.items.values()))["definition"]["parts"][0]
    assert decode_json_part(definition["payload"])["connectionId"] == target.connection_id
    assert len(system.estate.items) == 1
    assert result.details["applied"]["target"]["item_id"] == next(iter(system.estate.items))


def configured_replica(system):
    from fabshuffle.bcdr.contracts import AclScope, DesiredAcl, Principal, RecoveryDataBinding
    from fabshuffle.bcdr.replica import ReplicaAccessEvidence, binding_digest
    from fabshuffle.bcdr.service import ConfigureReplicaRequest, ReplicaAttachment

    source = system.captured.items[0]
    metadata = {
        "schema_enabled": False,
        "schemas": [{"name": "dbo"}],
        "files_inventory": [],
        "shortcuts": [],
        "tables": [
            {
                "name": "orders",
                "schema_name": "dbo",
                "data_source_format": "DELTA",
                "storage_location": (
                    f"https://onelake.dfs.fabric.microsoft.com/{source.identity.workspace_id}/"
                    f"{source.identity.item_id}/Tables/orders"
                ),
            }
        ],
    }
    runtime_principal = Principal(tenant_id=source.identity.tenant_id, object_id=guid(), kind="User")
    grant = DesiredAcl(
        acl_id=guid(),
        scope=AclScope.WORKSPACE,
        workspace=system.captured.workspaces[0].identity,
        principal=runtime_principal,
        permission="Viewer",
        provenance="captured reader",
    )
    item = source.model_copy(update={"item_type": "Lakehouse", "properties": {"bcdr": metadata}})
    captured = system.captured.model_copy(update={"items": (item,), "desired_acls": (grant,)})
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(captured, ())
    result = system.service.synchronize(system.request)
    applied = system.c.item_mappings()[item.identity.key]
    old_observe = system.c.observe

    def observed(client, target, item_type):
        shortcuts = system.estate.shortcuts.get(target.item_id, [])
        return (
            digest(canonical_json({"shortcuts": shortcuts}))
            if shortcuts
            else old_observe(client, target, item_type)
        )

    system.c.observe = observed
    system.c.replica.observe = observed
    binding = RecoveryDataBinding(
        generation_id=result.generation_id,
        source=item.identity,
        source_path="Tables/orders",
        consumer=applied.target,
        strategy="temporary_continuity",
        qualification="verified",
        read_only_verified=True,
        retention_acknowledged=True,
        evidence="external-read-only-enforcement",
    )
    request = ConfigureReplicaRequest(
        generation_id=result.generation_id,
        source=item.identity,
        attachments=(
            ReplicaAttachment(
                binding=binding,
                shortcut_path="Tables",
                shortcut_name="orders",
                access_evidence=ReplicaAccessEvidence(
                    qualification_id=guid(),
                    binding_sha256=binding_digest(binding),
                    principal=runtime_principal,
                    verified_at=now(),
                    valid_until=now() + timedelta(hours=1),
                    enforcement_reference=binding.evidence,
                ),
            ),
        ),
        qualified_at=now(),
        valid_until=now() + timedelta(hours=1),
        qualification_evidence="incident caller-only qualification",
    )
    return result, request, grant


def current_replica_proofs(system, generation_id):
    return tuple(
        row.model_copy(
            update={
                "target_observed_sha256": system.c.observe(system.c.destination, row.target, "Lakehouse"),
                "observed_at": now(),
            }
        )
        for row in proofs(system, generation_id)
    )


def test_configure_and_enable_real_replica_attachment_requires_fresh_runtime_proof(system):
    synced, configured, grant = configured_replica(system)
    before = list(system.estate.calls)
    config_result = system.service.configure_replica(configured)
    assert not config_result.details["data_ready"]
    assert before == system.estate.calls
    system.c.source = None
    first = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(synced.groups[0].group_id,),
            approved_acl_ids=(grant.acl_id,),
            readiness=proofs(system, synced.generation_id),
        )
    )
    assert not first.groups[0].access_enabled
    attachments = first.details["temporary_attachments"][0]
    assert not attachments["data_ready"] and not attachments["endpoint_ready"]
    assert sum(len(rows) for rows in system.estate.shortcuts.values()) == 1
    proof = current_replica_proofs(system, synced.generation_id)
    enabled = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(synced.groups[0].group_id,),
            approved_acl_ids=(grant.acl_id,),
            readiness=proof,
        )
    )
    assert enabled.groups[0].access_enabled
    assert (
        len(
            [path for method, path in system.estate.calls if method == "POST" and path.endswith("/shortcuts")]
        )
        == 1
    )
    cutover = system.service.cutover(
        CutoverRequest(
            generation_id=synced.generation_id,
            group_ids=(synced.groups[0].group_id,),
            readiness=current_replica_proofs(system, synced.generation_id),
            writer_fence=fence(),
        )
    )
    assert cutover.groups[0].active
    assert system.c.runtime.get("lifecycle", "writer")["access_mode"] == "read_only"
    assert not any(configured.source.workspace_id in path for _, path in system.estate.calls)


@pytest.mark.parametrize("invalid", ["target", "expired", "binding-hash", "path-alias", "wrong-principal"])
def test_replica_configuration_rejects_unqualified_scope_without_mutations(system, invalid):
    from fabshuffle.bcdr.replica import ReplicaAttachmentError

    _, request, _ = configured_replica(system)
    entry = request.attachments[0]
    if invalid == "target":
        entry = entry.model_copy(
            update={
                "binding": entry.binding.model_copy(
                    update={
                        "consumer": entry.binding.consumer.model_copy(update={"item_id": guid()}),
                    }
                )
            }
        )
    elif invalid == "expired":
        request = request.model_copy(update={"valid_until": now() - timedelta(seconds=1)})
    elif invalid == "binding-hash":
        entry = entry.model_copy(
            update={
                "access_evidence": entry.access_evidence.model_copy(
                    update={
                        "binding_sha256": digest(b"wrong binding"),
                    }
                )
            }
        )
    elif invalid == "path-alias":
        entry = entry.model_copy(update={"shortcut_name": "not-orders"})
    else:
        entry = entry.model_copy(
            update={
                "access_evidence": entry.access_evidence.model_copy(
                    update={
                        "principal": system.config.access_policy.recovery_spn,
                    }
                )
            }
        )
        # No captured read-only enforcement for a source workspace administrator.
        request = request.model_copy(update={"qualified_at": now()})
    request = request.model_copy(update={"attachments": (entry,)})
    before = list(system.estate.calls)
    if invalid == "wrong-principal":
        # Admission still needs the intended business readers, not merely a qualified operator.
        system.service.configure_replica(request)
        synced = system.c._groups(request.generation_id)[0]
        result = system.service.enable_recovery(
            EnableRecoveryRequest(
                generation_id=request.generation_id,
                group_ids=(synced.group_id,),
                readiness=proofs(system, request.generation_id),
            )
        )
        assert not result.groups[0].access_enabled
    else:
        with pytest.raises((RecoveryBlocked, ReplicaAttachmentError)):
            system.service.configure_replica(request)
        assert before == system.estate.calls
        assert not system.catalog.pending_operations()


@pytest.fixture
def lakehouse_setup(monkeypatch):
    return lakehouse_cases.setup.__wrapped__(monkeypatch)


def test_independent_lakehouse_real_copy_finalizes_metadata_then_requires_new_proof(
    system,
    lakehouse_setup,
    tmp_path,
    monkeypatch,
):
    from dataclasses import replace

    from fabshuffle.bcdr.protection import DataIdentity
    from fabshuffle.bcdr.protection_binding import (
        ConfigureProtectionRequest,
        ProtectionConfiguration,
        build_data_recovery,
    )
    from tests import test_bcdr_protection_lakehouse as lake_tests

    lake, template = lakehouse_setup
    source = system.captured.items[0].model_copy(
        update={
            "item_type": "Lakehouse",
            "properties": {"defaultSchema": "dbo"},
        }
    )
    captured = system.captured.model_copy(update={"items": (source,)})
    system.c.capture = lambda *args, **kwargs: CapturedGeneration(captured, ())
    point = captured.captured_at - timedelta(minutes=1)
    descriptor = template.model_copy(
        update={
            "source": DataIdentity(
                source.identity.tenant_id, source.identity.workspace_id, source.identity.item_id
            ),
            "captured_at": point,
            "completed_at": point,
            "consistency": replace(
                template.consistency,
                verified_at=point - timedelta(minutes=1),
                valid_until=now() + timedelta(hours=1),
            ),
        }
    )
    system.service.configure_protection(
        ConfigureProtectionRequest(
            source=source.identity,
            configuration=ProtectionConfiguration(
                provider="lakehouse",
                descriptor=descriptor,
                max_age_seconds=86400,
                target_approval_ref="fresh-owned",
            ),
        )
    )
    original_apply = system.c.apply
    modes = []

    def applying(client, item, payloads, **kwargs):
        modes.append(kwargs.get("shell_only", False))
        result = original_apply(client, item, payloads, **kwargs)
        system.estate.items[result.applied.target.item_id]["properties"] = {"defaultSchema": "dbo"}
        return replace(result, metadata_applied=not kwargs.get("shell_only", False))

    system.c.apply = applying
    synced = system.service.synchronize(system.request)
    assert not synced.groups[0].metadata_applied and modes == [True]
    target = system.c.item_mappings()[source.identity.key].target
    system.estate.workspaces[target.workspace_id]["capacityRegion"] = "West US"
    monkeypatch.setattr(lake_tests, "SOURCE", source.identity)
    monkeypatch.setattr(lake_tests, "TARGET", target)
    system.c.data_recovery = build_data_recovery(
        client=system.c.destination,
        tokens=system.c.tokens,
        protected_root=None,
        scratch=tmp_path,
        limits=lake_tests.LIMITS,
    )
    system.c.source = None
    first = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(synced.groups[0].group_id,),
            readiness=proofs(system, synced.generation_id),
        )
    )
    assert not first.groups[0].access_enabled
    assert {key: bytes(value) for key, value in lake.target.items()} == lake_tests.CONTENT
    assert modes == [True, False]
    prepared = system.c.runtime.get("data-prepared", f"{synced.generation_id}/{source.identity.key}")
    assert prepared["byte_copy_complete"] and not prepared["data_ready"] and not prepared["endpoint_ready"]
    writes = len(lake.writes)
    postcopy = tuple(
        proof.model_copy(update={"observed_at": now()}) for proof in proofs(system, synced.generation_id)
    )
    second = system.service.enable_recovery(
        EnableRecoveryRequest(
            generation_id=synced.generation_id,
            group_ids=(synced.groups[0].group_id,),
            readiness=postcopy,
        )
    )
    assert second.groups[0].access_enabled
    assert len(lake.writes) == writes
    assert all(
        request.method in ("GET", "HEAD")
        for request in lake.calls
        if source.identity.workspace_id in request.url.path
    )
