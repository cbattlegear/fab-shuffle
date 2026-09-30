from __future__ import annotations

import io
import json
import zipfile
from datetime import UTC, datetime
from unittest.mock import Mock

import httpx
import pytest

from fabshuffle.bcdr import adapters
from fabshuffle.bcdr.adapters import (
    adapter_capabilities,
    apply_captured_item,
    captured_hashes,
    observe_target,
)
from fabshuffle.bcdr.capture import make_payload
from fabshuffle.bcdr.contracts import (
    ConnectionIdentity,
    EndpointIdentity,
    ItemIdentity,
    ItemRecord,
    PayloadPurpose,
    RecoveryOutcome,
    WorkspaceIdentity,
)
from fabshuffle.bcdr.registry import TYPE_REGISTRY
from fabshuffle.fabric.client import FabricApiError, FabricError, OperationTimeout
from fabshuffle.fabric.definitions import decode_json_part, part
from fabshuffle.fabric.support import POWER_BI_TYPES, REBUILT_TYPES

TENANT = "10000000-0000-0000-0000-000000000001"
SOURCE = "20000000-0000-0000-0000-000000000001"
TARGET = "20000000-0000-0000-0000-000000000002"
OTHER_SOURCE = "20000000-0000-0000-0000-000000000003"
OTHER_TARGET = "20000000-0000-0000-0000-000000000004"
ITEM = "30000000-0000-0000-0000-000000000001"
CREATED = "30000000-0000-0000-0000-000000000002"
DEPENDENCY = "30000000-0000-0000-0000-000000000003"
REPLACEMENT = "30000000-0000-0000-0000-000000000004"
CONNECTION = "60000000-0000-0000-0000-000000000001"
NEW_CONNECTION = "60000000-0000-0000-0000-000000000002"
GENERATION = "70000000-0000-0000-0000-000000000001"
OPERATION = "80000000-0000-0000-0000-000000000001"
IDENTITY = ItemIdentity(tenant_id=TENANT, workspace_id=SOURCE, item_id=ITEM)
TARGET_WORKSPACE = WorkspaceIdentity(tenant_id=TENANT, workspace_id=TARGET)


def captured(item_type="Notebook", content=None, path="definition.json", *, properties=None):
    body = json.dumps(content if content is not None else {})
    payload = make_payload(IDENTITY, path, body.encode(), PayloadPurpose.DEFINITION)
    item = ItemRecord(
        identity=IDENTITY,
        item_type=item_type,
        display_name="Orders",
        captured_at=datetime.now(UTC),
        api_version="v1",
        definition_format="TMSL" if item_type == "SemanticModel" else None,
        payload_ids=(payload.descriptor.payload_id,),
        capture_complete=True,
        properties=properties or {},
    )
    return item, (payload,)


class Destination:
    """Every source request fails, including fallback metadata and definition reads."""

    def __init__(self):
        self.mutations = []
        self.reads = []
        self.definition = [part("definition.json", {})]
        self.item_type = "Notebook"
        self.fail = None

    def tenant_id(self):
        return TENANT

    def _target_only(self, path):
        assert SOURCE not in path and OTHER_SOURCE not in path, f"Source call during recovery: {path}"

    def get(self, path, **kwargs):
        self._target_only(path)
        self.reads.append(path)
        if path.count("/") == 1:
            return {"id": path.split("/")[-1]}
        return {
            "id": CREATED,
            "type": self.item_type,
            "displayName": "Orders",
            "properties": {
                "connectionString": "target.sql",
                "defaultSchema": "dbo",
                "sqlEndpointProperties": {"connectionString": "target.sql", "id": NEW_CONNECTION},
            },
        }

    def post(self, path, json=None, params=None, **kwargs):
        self._target_only(path)
        if path.endswith("/getDefinition"):
            return {"definition": {"parts": self.definition}}
        self.mutations.append((path, json))
        if self.fail:
            raise self.fail
        if json and "definition" in json:
            self.definition = json["definition"]["parts"]
        return {"id": CREATED}

    def list_all(self, path, **kwargs):
        self._target_only(path)
        return []

    def request(self, method, path, params=None, **kwargs):
        self._target_only(path)
        if method == "GET":
            if path.startswith("connections/"):
                return httpx.Response(200, json=self.get(path))
            if path.endswith("/sparkcompute"):
                return httpx.Response(200, json={})
            if path.endswith("/libraries"):
                return httpx.Response(200, json={"libraries": []})
            return httpx.Response(200, json={"value": self.list_all(path)})
        self.mutations.append((path, kwargs.get("content")))
        return httpx.Response(200)

    def patch(self, path, **kwargs):
        self._target_only(path)
        self.mutations.append((path, kwargs.get("json")))
        return {}


def apply(item, payloads, client=None, **kwargs):
    client = client or Destination()
    client.item_type = item.item_type
    arguments = {
        "generation_id": GENERATION,
        "operation_id": OPERATION,
        "target_workspace": TARGET_WORKSPACE,
        "source_items": [item],
        "item_mappings": {},
        "workspace_mappings": {(TENANT, SOURCE): TARGET_WORKSPACE},
    }
    arguments.update(kwargs)
    return apply_captured_item(client, item, payloads, **arguments)


def test_source_independent_apply_returns_real_target_and_hashes():
    item, payloads = captured("Notebook", {"workspaceId": SOURCE})
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.RESTORED_STOPPED
    assert result.metadata_applied
    assert result.applied.target.item_id == CREATED
    assert (result.applied.definition_sha256, result.applied.properties_sha256) == captured_hashes(
        item, payloads
    )
    assert decode_json_part(client.definition[0]["payload"])["workspaceId"] == TARGET
    assert all(SOURCE not in path for path in client.reads)


def test_update_reuses_explicit_id_and_never_adopts_by_name():
    item, payloads = captured()
    client = Destination()
    result = apply(item, payloads, client, target_id=CREATED, destination_quiescence="test-owned-stopped")
    assert result.applied.target.item_id == CREATED
    assert len(client.mutations) == 1
    assert client.mutations[0][0].endswith(f"/{CREATED}/updateDefinition")


@pytest.mark.parametrize("item_type", sorted(REBUILT_TYPES | POWER_BI_TYPES))
def test_registry_surface_has_explicit_adapter_capability(item_type):
    item, _ = captured(item_type)
    capability = adapter_capabilities(item)
    assert isinstance(capability.inactive_create, bool)
    assert capability.inactive_create or capability.reason
    assert item_type in TYPE_REGISTRY


@pytest.mark.parametrize("item_type", sorted(adapters.BLOCKED_TYPES))
def test_unsafe_or_reassign_only_types_never_mutate(item_type):
    item, payloads = captured(item_type)
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.BLOCKED and result.diagnostics
    assert result.applied is None and client.mutations == []


def test_tmsl_role_members_deferred_while_rls_filters_preserved():
    role = {
        "name": "Sales",
        "members": [{"memberName": "human@example.org", "memberId": CONNECTION}],
        "tablePermissions": [{"name": "Orders", "filterExpression": '[Region] = "West"'}],
    }
    item, payloads = captured("SemanticModel", {"model": {"roles": [role]}}, "model.bim")
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.RESTORED_STOPPED
    assert result.deferred_grants[0]["member"]["memberId"] == CONNECTION
    copied_role = decode_json_part(client.definition[0]["payload"])["model"]["roles"][0]
    assert "members" not in copied_role and copied_role["tablePermissions"] == role["tablePermissions"]
    assert decode_json_part(part("model.bim", payloads[0].data)["payload"])["model"]["roles"][0]["members"]


@pytest.mark.parametrize("field", ["roleAssignments", "acl", "permissions", "principals", "grants"])
def test_unhandled_grant_shapes_block_before_mutation(field):
    item, payloads = captured("GraphQLApi", {field: [{"principalId": CONNECTION}]})
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.BLOCKED
    assert "grant-bearing" in result.diagnostics[0]
    assert not client.mutations


def test_reflex_stops_rules_and_historical_rule_application():
    entities = [
        {"type": "container-v1", "payload": {"name": "container"}},
        {
            "type": "timeSeriesView-v1",
            "payload": {
                "definition": {
                    "type": "Rule",
                    "settings": {"shouldRun": True, "shouldApplyRuleOnUpdate": True},
                }
            },
        },
    ]
    item, payloads = captured("Reflex", entities, "ReflexEntities.json")
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.RESTORED_STOPPED
    settings = decode_json_part(client.definition[0]["payload"])[1]["payload"]["definition"]["settings"]
    assert settings == {"shouldRun": False, "shouldApplyRuleOnUpdate": False}


def test_missing_reflex_entities_block_instead_of_claiming_stopped():
    item, payloads = captured("Reflex")
    client = Destination()
    assert apply(item, payloads, client).outcome == RecoveryOutcome.BLOCKED
    assert not client.mutations


def test_catalog_autosync_disabled_and_connection_mapped():
    item, payloads = captured(
        "MirroredAzureDatabricksCatalog",
        {
            "autoSync": "Enabled",
            "databricksWorkspaceConnectionId": CONNECTION,
        },
    )
    client = Destination()
    result = apply(
        item,
        payloads,
        client,
        connection_mappings=(
            (
                ConnectionIdentity(tenant_id=TENANT, connection_id=CONNECTION),
                ConnectionIdentity(tenant_id=TENANT, connection_id=NEW_CONNECTION),
            ),
        ),
    )
    assert result.applied
    document = decode_json_part(client.definition[0]["payload"])
    assert document["autoSync"] == "Disabled"
    assert document["databricksWorkspaceConnectionId"] == NEW_CONNECTION


@pytest.mark.parametrize(
    "content",
    [
        {"workspaceId": SOURCE, "itemId": DEPENDENCY},
        {"connectionId": CONNECTION},
        {"itemId": ITEM},
    ],
)
def test_unmapped_hidden_connection_item_or_self_reference_is_blocked(content):
    item, payloads = captured(content=content)
    client = Destination()
    assert apply(item, payloads, client).outcome == RecoveryOutcome.BLOCKED
    assert not client.mutations


def test_cross_workspace_dependency_uses_qualified_mappings():
    item, payloads = captured(content={"workspaceId": OTHER_SOURCE, "itemId": DEPENDENCY})
    dependency = item.model_copy(
        update={
            "identity": ItemIdentity(
                tenant_id=TENANT,
                workspace_id=OTHER_SOURCE,
                item_id=DEPENDENCY,
            )
        }
    )
    client = Destination()
    result = apply(
        item,
        payloads,
        client,
        source_items=[item, dependency],
        item_mappings={
            (TENANT, OTHER_SOURCE, DEPENDENCY): ItemIdentity(
                tenant_id=TENANT,
                workspace_id=OTHER_TARGET,
                item_id=REPLACEMENT,
            ),
        },
        workspace_mappings={
            (TENANT, SOURCE): TARGET_WORKSPACE,
            (TENANT, OTHER_SOURCE): WorkspaceIdentity(tenant_id=TENANT, workspace_id=OTHER_TARGET),
        },
    )
    assert result.applied
    assert decode_json_part(client.definition[0]["payload"]) == {
        "workspaceId": OTHER_TARGET,
        "itemId": REPLACEMENT,
    }


def test_ambiguous_bare_ids_across_workspaces_are_not_flattened():
    item, payloads = captured()
    duplicate = item.model_copy(
        update={
            "identity": ItemIdentity(
                tenant_id=TENANT,
                workspace_id=OTHER_SOURCE,
                item_id=ITEM,
            )
        }
    )
    client = Destination()
    result = apply(item, payloads, client, source_items=[item, duplicate])
    assert result.outcome == RecoveryOutcome.BLOCKED and "Ambiguous" in result.diagnostics[0]
    assert not client.mutations


def test_target_drift_hash_includes_definition():
    client = Destination()
    identity = ItemIdentity(tenant_id=TENANT, workspace_id=TARGET, item_id=CREATED)
    before = observe_target(client, identity, "Notebook")
    client.definition = [part("definition.json", {"changed": True})]
    assert observe_target(client, identity, "Notebook") != before


@pytest.mark.parametrize(
    "error",
    [
        FabricApiError("POST", "target", 403, '{"errorCode":"Denied","message":"No target permission"}'),
        OperationTimeout("target operation still running"),
    ],
)
def test_destination_errors_propagate_without_retry_or_synthetic_success(error):
    item, payloads = captured()
    client = Destination()
    client.fail = error
    with pytest.raises(type(error)) as caught:
        apply(item, payloads, client)
    assert caught.value is error
    assert len(client.mutations) == 1


def test_missing_payload_or_wrong_owner_is_not_restored():
    item, payloads = captured()
    with pytest.raises(FabricError, match="Missing captured payload"):
        apply(item, ())
    wrong = type(payloads[0])(
        payloads[0].descriptor.model_copy(
            update={
                "owner": ItemIdentity(
                    tenant_id=TENANT,
                    workspace_id=SOURCE,
                    item_id=DEPENDENCY,
                )
            }
        ),
        payloads[0].data,
    )
    with pytest.raises(FabricError, match="owner"):
        apply(item, (wrong,))


def test_cosmos_container_ttl_disabled_without_claiming_data_recovered():
    item, payloads = captured(
        "CosmosDBDatabase",
        {
            "containers": [
                {"resource": {"id": "orders", "partitionKey": {"paths": ["/customer"]}, "defaultTtl": 60}},
            ]
        },
    )
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.PARTIAL
    assert "defaultTtl" not in decode_json_part(client.definition[0]["payload"])["containers"][0]["resource"]


def test_kql_policy_or_ingestion_never_executed_by_definition_import():
    item, payloads = captured("KQLDatabase", {}, "DatabaseProperties.json")
    dangerous = make_payload(
        IDENTITY,
        "DatabaseSchema.kql",
        b'.set-or-append orders <| cluster("source").database("prod").orders',
        PayloadPurpose.DEFINITION,
    )
    item = item.model_copy(update={"payload_ids": (*item.payload_ids, dangerous.descriptor.payload_id)})
    client = Destination()
    result = apply(item, (*payloads, dangerous), client)
    assert result.outcome == RecoveryOutcome.BLOCKED
    assert not client.mutations


def test_lakehouse_roles_deferred_and_alm_disabled():
    parts = [
        part("data-access-roles.json", [{"name": "HumanRole", "members": {"users": [CONNECTION]}}]),
        part("alm.settings.json", {"objectTypes": [{"name": "DataAccessRoles", "state": "Enabled"}]}),
    ]
    prepared, deferred = adapters._defer_grants(parts)
    assert len(deferred) == 1 and deferred[0]["scope"] == "onelake"
    assert [entry["path"] for entry in prepared] == ["alm.settings.json"]
    assert decode_json_part(prepared[0]["payload"])["objectTypes"][0]["state"] == "Disabled"


def schema_capture(item_type="Warehouse", *, trigger=False):
    item, _ = captured(
        item_type, properties={"bcdr": {"sql": {"sql_dependencies": []}, "schema_enabled": True}}
    )
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("model.xml", '<Element Type="SqlDmlTrigger"/>' if trigger else "<DataSchemaModel/>")
        archive.writestr("postdeploy.sql", "GRANT SELECT TO [human]")
    payload = make_payload(
        IDENTITY, "schema/source.dacpac", data.getvalue(), PayloadPurpose.SQL_SCHEMA, encoding="binary"
    )
    return item.model_copy(update={"payload_ids": (payload.descriptor.payload_id,)}), (payload,)


def test_sql_schema_uses_captured_dacpac_and_only_destination_credentials(monkeypatch):
    item, payloads = schema_capture()
    client = Destination()
    tokens = object()
    calls = []
    guard = Mock()

    def script(source, output, **kwargs):
        assert source.read_bytes() == payloads[0].data
        assert kwargs["exclude_security"] is True and kwargs["tokens"] is tokens
        assert kwargs["server"] == "target.sql"
        output.write_text("CREATE TABLE orders (id int);", encoding="utf-8")
        calls.append(kwargs)
        return output

    monkeypatch.setattr(adapters.sqlschema, "script_dacpac", script)
    monkeypatch.setattr(
        adapters.sqlschema, "extract_dacpac", Mock(side_effect=AssertionError("source unavailable"))
    )

    def apply_script(path, **kwargs):
        assert kwargs["cancel_requested"]() is False
        return []

    monkeypatch.setattr(adapters.sqlschema, "apply_script", apply_script)
    result = apply(item, payloads, client, tokens=tokens, mutation_guard=guard)
    guard.assert_called_once_with()
    assert result.outcome == RecoveryOutcome.PARTIAL and calls
    assert result.target_properties["properties"]["connectionString"] == "target.sql"
    assert result.deferred_grants[0]["scope"] == "sql"
    assert all("updateDefinition" not in path for path, _ in client.mutations)


def test_sql_apply_failure_not_reported_as_synchronized(monkeypatch):
    item, payloads = schema_capture()

    def script(source, output, **kwargs):
        output.write_text("CREATE TABLE orders (id int);", encoding="utf-8")
        return output

    monkeypatch.setattr(adapters.sqlschema, "script_dacpac", script)
    monkeypatch.setattr(
        adapters.sqlschema, "apply_script", lambda path, **kwargs: ["Service says SQL denied"]
    )
    with pytest.raises(FabricError, match="SQL denied"):
        apply(item, payloads, tokens=object())


def test_opaque_active_sql_trigger_blocks_before_create():
    item, payloads = schema_capture(trigger=True)
    client = Destination()
    result = apply(item, payloads, client, tokens=object())
    assert result.outcome == RecoveryOutcome.BLOCKED
    assert not client.mutations


def test_endpoint_owner_must_be_mapped_before_rebinding():
    item, payloads = captured(content={"server": "source.sql"})
    endpoint = EndpointIdentity(item=IDENTITY, endpoint_kind="sql", endpoint_id="source.sql")
    client = Destination()
    result = apply(item, payloads, client, endpoint_mappings=((endpoint, "target.sql"),))
    assert result.outcome == RecoveryOutcome.BLOCKED and not client.mutations


def test_sql_protection_shell_does_not_apply_schema_first(monkeypatch):
    item, payloads = schema_capture("SQLDatabase")
    client = Destination()
    monkeypatch.setattr(
        adapters.sqlschema, "script_dacpac", Mock(side_effect=AssertionError("no schema yet"))
    )
    result = apply(item, payloads, client, shell_only=True)
    assert result.outcome == RecoveryOutcome.PARTIAL
    assert result.applied.target.item_id == CREATED
    assert "NOT synchronized" in result.diagnostics[0]
    assert result.metadata_applied is False
    assert len(client.mutations) == 1 and client.mutations[0][0].endswith("/sqlDatabases")


def test_destination_reference_read_errors_preserve_service_error():
    item, payloads = captured(content={"workspaceId": SOURCE})
    error = FabricApiError(
        "GET", "target", 403, '{"errorCode":"TargetDenied","message":"Access not granted"}'
    )

    class DeniedDestination(Destination):
        def get(self, path, **kwargs):
            raise error

    client = DeniedDestination()
    with pytest.raises(FabricApiError) as caught:
        apply(item, payloads, client)
    assert caught.value is error and not client.mutations


def test_unknown_workspace_is_blocked_without_probing_it():
    item, payloads = captured(content={"workspaceId": OTHER_SOURCE})
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.BLOCKED
    assert client.reads == [] and client.mutations == []


def test_report_by_path_requires_exact_captured_model_mapping():
    item, payloads = captured(
        "Report",
        {"datasetReference": {"byPath": {"path": "../Orders.SemanticModel"}}},
        "definition.pbir",
    )
    client = Destination()
    result = apply(item, payloads, client)
    assert result.outcome == RecoveryOutcome.BLOCKED
    assert "exact captured semantic model" in result.diagnostics[0]
    assert not client.mutations


def test_reflex_rules_remain_stopped_on_repeat_definition_update():
    item, payloads = captured(
        "Reflex",
        [
            {
                "type": "timeSeriesView-v1",
                "payload": {"definition": {"type": "Rule", "settings": {"shouldRun": True}}},
            }
        ],
        "ReflexEntities.json",
    )
    client = Destination()
    result = apply(item, payloads, client, target_id=CREATED)
    assert result.applied.target.item_id == CREATED
    assert decode_json_part(client.definition[0]["payload"])[0]["payload"]["definition"]["settings"] == {
        "shouldRun": False,
        "shouldApplyRuleOnUpdate": False,
    }


def test_dacpac_export_timestamp_does_not_masquerade_as_schema_drift():
    payloads = []
    for stamp in ("first-export", "second-export"):
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("model.xml", "<Model>same schema</Model>")
            archive.writestr("Origin.xml", f"<export>{stamp}</export>")
        payloads.append(data.getvalue())
    assert payloads[0] != payloads[1]
    assert adapters._definition_digest("schema.dacpac", payloads[0]) == adapters._definition_digest(
        "schema.dacpac",
        payloads[1],
    )


def test_json_unicode_escaped_source_workspace_is_rewritten_without_source_reads():
    item, _ = captured()
    escaped_workspace = OTHER_SOURCE.replace("-", r"\u002d")
    payload = make_payload(
        IDENTITY,
        "definition.json",
        ('{"workspaceId":"' + escaped_workspace + '"}').encode(),
        PayloadPurpose.DEFINITION,
    )
    item = item.model_copy(update={"payload_ids": (payload.descriptor.payload_id,)})
    other = item.model_copy(
        update={
            "identity": ItemIdentity(
                tenant_id=TENANT,
                workspace_id=OTHER_SOURCE,
                item_id=DEPENDENCY,
            )
        }
    )
    client = Destination()
    result = apply(
        item,
        (payload,),
        client,
        source_items=[item, other],
        workspace_mappings={
            (TENANT, SOURCE): TARGET_WORKSPACE,
            (TENANT, OTHER_SOURCE): WorkspaceIdentity(tenant_id=TENANT, workspace_id=OTHER_TARGET),
        },
    )
    assert result.metadata_applied
    assert decode_json_part(client.definition[0]["payload"])["workspaceId"] == OTHER_TARGET
    assert not any(OTHER_SOURCE in path for path in client.reads)


def test_definition_schedule_is_never_imported_and_is_exposed_for_later_enablement():
    item, payloads = captured()
    schedule = make_payload(IDENTITY, ".schedules", b'{"enabled":true}', PayloadPurpose.DEFINITION)
    item = item.model_copy(update={"payload_ids": (*item.payload_ids, schedule.descriptor.payload_id)})
    client = Destination()
    result = apply(item, (*payloads, schedule), client)
    assert result.metadata_applied
    assert any(grant["scope"] == "schedule" for grant in result.deferred_grants)
    assert all(entry["path"] != ".schedules" for entry in client.definition)


def test_existing_actor_update_requires_quiescence_evidence_before_read_or_mutation():
    item, payloads = captured("Notebook")
    client = Destination()
    result = apply(item, payloads, client, target_id=CREATED)
    assert result.outcome == RecoveryOutcome.BLOCKED
    assert client.reads == [] and client.mutations == []


def test_reuse_of_explicitly_verified_external_connection_is_allowed():
    item, payloads = captured(content={"connectionId": CONNECTION})
    connection = ConnectionIdentity(tenant_id=TENANT, connection_id=CONNECTION)
    client = Destination()
    result = apply(
        item,
        payloads,
        client,
        connection_mappings=((connection, connection),),
        verified_external_connections=(connection,),
    )
    assert result.metadata_applied
    assert decode_json_part(client.definition[0]["payload"])["connectionId"] == CONNECTION


def test_unverified_connection_reuse_is_still_blocked():
    item, payloads = captured(content={"connectionId": CONNECTION})
    connection = ConnectionIdentity(tenant_id=TENANT, connection_id=CONNECTION)
    client = Destination()
    result = apply(item, payloads, client, connection_mappings=((connection, connection),))
    assert result.outcome == RecoveryOutcome.BLOCKED and not client.mutations


def test_nondefault_captured_definition_format_survives_offline_apply():
    item, payloads = captured("VariableLibrary")
    item = item.model_copy(update={"definition_format": "JSON"})
    client = Destination()
    result = apply(item, payloads, client)
    assert result.metadata_applied
    assert client.mutations[0][1]["definition"]["format"] == "JSON"


def test_environment_uses_only_staging_endpoints_never_generic_definition_or_publish():
    item, payloads = captured(
        "Environment",
        None,
        "Setting/Sparkcompute.yml",
        properties={
            "bcdr": {"environment": {"staging_compute": {"driverCores": 4, "sparkProperties": []}}},
        },
    )
    client = Destination()
    result = apply(item, payloads, client)
    assert result.metadata_applied
    assert any(path.endswith("/staging/sparkcompute") for path, _ in client.mutations)
    assert all(
        "publish" not in path.lower() and "updateDefinition" not in path for path, _ in client.mutations
    )


def test_lakehouse_shortcut_zero_ids_remain_destination_local(monkeypatch):
    item, payloads = schema_capture("Lakehouse")
    shortcuts = [
        {
            "name": "local",
            "path": "Files",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "workspaceId": adapters.EMPTY_GUID,
                    "itemId": adapters.EMPTY_GUID,
                    "path": "Files/data",
                },
            },
        }
    ]
    metadata = make_payload(
        IDENTITY, "lakehouse.metadata.json", b'{"defaultSchema":"dbo"}', PayloadPurpose.DEFINITION
    )
    shortcut_payload = make_payload(
        IDENTITY, "shortcuts.metadata.json", json.dumps(shortcuts).encode(), PayloadPurpose.DEFINITION
    )
    item = item.model_copy(
        update={
            "payload_ids": (
                *item.payload_ids,
                metadata.descriptor.payload_id,
                shortcut_payload.descriptor.payload_id,
            ),
            "properties": {
                "bcdr": {"sql": {"sql_dependencies": []}, "schema_enabled": True, "shortcuts": shortcuts}
            },
        }
    )
    client = Destination()
    monkeypatch.setattr(adapters, "_apply_sql_schema", lambda *args, **kwargs: None)
    result = apply(item, (*payloads, metadata, shortcut_payload), client, tokens=object())
    assert result.metadata_applied
    payload = next(entry for entry in client.definition if entry["path"] == "shortcuts.metadata.json")
    target = decode_json_part(payload["payload"])[0]["target"]["oneLake"]
    assert target["workspaceId"] == TARGET and target["itemId"] == adapters.EMPTY_GUID
