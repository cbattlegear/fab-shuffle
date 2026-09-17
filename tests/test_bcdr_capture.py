from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime

import httpx
import pytest

from fabshuffle.bcdr.capture import (
    capture_item,
    capture_workspaces,
    definition_parts,
    make_payload,
)
from fabshuffle.bcdr.capture_sources import MetadataReaders
from fabshuffle.bcdr.contracts import (
    ItemIdentity,
    PayloadPurpose,
    Principal,
    RecoverySet,
    StandbyAccessPolicy,
    WorkspaceIdentity,
)
from fabshuffle.bcdr.registry import TYPE_REGISTRY
from fabshuffle.fabric.client import FabricApiError, FabricError
from fabshuffle.fabric.definitions import part

TENANT = "10000000-0000-0000-0000-000000000001"
SOURCE = "20000000-0000-0000-0000-000000000001"
CONTROL = "20000000-0000-0000-0000-000000000002"
ITEM = "30000000-0000-0000-0000-000000000001"
CAPACITY = "40000000-0000-0000-0000-000000000001"
TARGET_CAPACITY = "40000000-0000-0000-0000-000000000002"
SPN = "50000000-0000-0000-0000-000000000001"
CONNECTION = "60000000-0000-0000-0000-000000000001"
IDENTITY = ItemIdentity(tenant_id=TENANT, workspace_id=SOURCE, item_id=ITEM)


def dacpac():
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("model.xml", "<DataSchemaModel/>")
    return data.getvalue()


class Readers:
    def lakehouse_tables(self, identity):
        assert identity == IDENTITY
        return {
            "schemas": [{"name": "sales"}],
            "tables": [
                {
                    "name": "orders",
                    "schema_name": "sales",
                    "storage_location": f"https://onelake.dfs.fabric.microsoft.com/{SOURCE}/{ITEM}/"
                    "Tables/sales/actual-storage-path",
                }
            ],
        }

    def files_inventory(self, identity):
        return [{"name": f"{ITEM}/Files/customers.csv", "contentLength": 80}]

    def sql_metadata(self, server, database):
        assert server == "source.sql"
        return {
            "tables": [{"schema_name": "sales", "table_name": "orders"}],
            "sql_dependencies": [],
            "sql_roles": [],
            "sql_permissions": [],
        }

    def sql_schema(self, server, database):
        return dacpac()

    def kql_metadata(self, cluster, database, *, follower):
        return {
            "schema": [{"DatabaseSchema": ".create table x (n:long)"}],
            "principals": [],
            "follower": [{"OriginalDatabaseName": ITEM}] if follower else [],
        }


class SourceClient:
    def __init__(self, item_type="Notebook", parts=None, properties=None):
        self.item_type = item_type
        self.parts = parts if parts is not None else [part("notebook-content.py", "# no source dependencies")]
        self.properties = properties or {}
        self.calls = []
        self.connections = []
        self.files = {}

    def tenant_id(self):
        return TENANT

    def get(self, path):
        self.calls.append(("GET", path, None))
        if path == f"workspaces/{SOURCE}":
            return {
                "id": SOURCE,
                "displayName": "Production",
                "capacityId": CAPACITY,
                "capacityRegion": "East US",
                "workspaceIdentity": {"servicePrincipalId": SPN},
            }
        if path.endswith("/spark/settings"):
            return {"environment": {"name": "default"}}
        return {"id": ITEM, "type": self.item_type, "displayName": "Orders", "properties": self.properties}

    def list_all(self, path, params=None, value_key="value"):
        self.calls.append(("LIST", path, params))
        if path == "workspaces":
            return [{"id": SOURCE, "capacityId": CAPACITY}, {"id": CONTROL, "capacityId": TARGET_CAPACITY}]
        if path.endswith("/items"):
            return [{"id": ITEM, "type": self.item_type, "displayName": "Orders"}]
        if path.endswith("/connections"):
            return self.connections
        if path.endswith("/roleAssignments"):
            return [{"role": "Viewer", "principal": {"id": SPN, "type": "ServicePrincipal"}}]
        if path.endswith("/files"):
            return [{"filePath": path, "sizeInBytes": len(data)} for path, data in self.files.items()]
        if path.endswith("/tables"):
            raise AssertionError("Never call the generic table API for schema-enabled lakehouses")
        return []

    def post(self, path, params=None):
        self.calls.append(("POST", path, params))
        if path.endswith("/getMirroringStatus"):
            return {"status": "Stopped"}
        assert path.endswith("/getDefinition")
        return {"definition": {"parts": self.parts, **({"format": params["format"]} if params else {})}}

    def request(self, method, path, params=None):
        assert method == "GET" and params == {"beta": "true"}
        return httpx.Response(200, content=self.files[path.split("/files/")[1]])


def recovery_set():
    workspace = WorkspaceIdentity(tenant_id=TENANT, workspace_id=CONTROL)
    return RecoverySet(
        recovery_set_id="70000000-0000-0000-0000-000000000001",
        tenant_id=TENANT,
        control_workspace=workspace,
        control_warehouse=ItemIdentity(tenant_id=TENANT, workspace_id=CONTROL, item_id=ITEM),
        access_policy=StandbyAccessPolicy(
            recovery_spn=Principal(
                tenant_id=TENANT,
                object_id=SPN,
                kind="ServicePrincipal",
            )
        ),
        source_capacity_ids=(CAPACITY,),
        target_capacity_ids=(TARGET_CAPACITY,),
    )


def test_complete_generation_retains_identity_roles_config_and_connections():
    client = SourceClient()
    client.connections = [
        {
            "id": CONNECTION,
            "connectivityType": "ShareableCloud",
            "connectionDetails": {"type": "SQL", "path": "source.sql;Orders"},
        }
    ]
    result = capture_workspaces(client, recovery_set(), tokens=object(), readers=Readers())
    snapshot = result.snapshot
    snapshot.require_publishable(recovery_set())
    assert len(snapshot.workspaces) == 1
    workspace = snapshot.workspaces[0]
    assert workspace.properties["workspaceIdentity"]["servicePrincipalId"] == SPN
    assert workspace.properties["capacityRegion"] == "East US"
    assert workspace.properties["bcdr"]["control_workspace"]["workspace_id"] == CONTROL
    assert {acl.scope for acl in snapshot.desired_acls} == {"workspace", "connection"}
    assert any(
        edge.prerequisite and getattr(edge.prerequisite, "connection_id", None) == CONNECTION
        for edge in snapshot.dependencies
    )
    assert result.payloads[0].data == b"# no source dependencies"


def test_semantic_models_use_tmsl_and_never_skip_former_defaults():
    client = SourceClient("SemanticModel", [part("model.bim", {"model": {"roles": [], "tables": []}})])
    captured = capture_item(client, IDENTITY, "SemanticModel", readers=Readers())
    assert captured.record.capture_complete
    assert captured.record.definition_format == "TMSL"
    assert ("POST", f"workspaces/{SOURCE}/items/{ITEM}/getDefinition", {"format": "TMSL"}) in client.calls


def test_spark_job_v2_retains_all_inline_runtime_parts():
    client = SourceClient(
        "SparkJobDefinition",
        [
            part("SparkJobDefinitionV1.json", {"executableFile": "Main/main.py"}),
            part("Main/main.py", "print('standby')"),
            part("Libs/helper.py", "VALUE = 1"),
        ],
    )
    captured = capture_item(client, IDENTITY, client.item_type, readers=Readers())
    assert captured.record.definition_format == "SparkJobDefinitionV2"
    assert [payload.descriptor.purpose for payload in captured.payloads] == [
        PayloadPurpose.DEFINITION,
        PayloadPurpose.RUNTIME_FILE,
        PayloadPurpose.RUNTIME_FILE,
    ]
    assert len(definition_parts(captured.record, captured.payloads)) == 3


def test_airflow_reads_separate_files_without_running_dags():
    client = SourceClient("ApacheAirflowJob", [part("AirflowJob.json", {})])
    client.files = {
        "dags/orders.py": b"raise RuntimeError('never execute')",
        "requirements.txt": b"example==1",
    }
    captured = capture_item(client, IDENTITY, client.item_type, readers=Readers())
    runtime = [payload for payload in captured.payloads if payload.descriptor.purpose == "runtime_file"]
    assert [payload.descriptor.path for payload in runtime] == [
        "runtime/dags/orders.py",
        "runtime/requirements.txt",
    ]
    assert runtime[0].data == client.files["dags/orders.py"]
    assert len(definition_parts(captured.record, captured.payloads)) == 1


def test_schema_lakehouse_uses_onelake_actual_paths_and_separate_sql_metadata():
    client = SourceClient(
        "Lakehouse",
        [part("lakehouse.metadata.json", {"defaultSchema": "dbo"})],
        {
            "defaultSchema": "dbo",
            "sqlEndpointProperties": {"connectionString": "source.sql", "id": CONNECTION},
        },
    )
    captured = capture_item(client, IDENTITY, client.item_type, readers=Readers())
    metadata = captured.record.properties["bcdr"]
    assert metadata["schema_enabled"]
    assert metadata["tables"][0]["storage_location"].endswith("/sales/actual-storage-path")
    assert metadata["sql"]["tables"][0]["table_name"] == "orders"
    assert metadata["schemas"] == [{"name": "sales"}]
    assert any(payload.descriptor.purpose == "sql_schema" for payload in captured.payloads)


def test_missing_required_model_metadata_cannot_publish():
    client = SourceClient("SemanticModel", [part("definition/model.tmdl", "model Model")])
    result = capture_workspaces(client, recovery_set(), tokens=object(), readers=Readers())
    assert not result.snapshot.items[0].capture_complete
    with pytest.raises(ValueError, match="metadata"):
        result.snapshot.require_publishable(recovery_set())


@pytest.mark.parametrize("operation", ["GET", "LIST", "POST"])
def test_required_read_failure_preserves_service_error(operation, monkeypatch):
    client = SourceClient()
    error = FabricApiError(operation, "source", 403, '{"errorCode":"NoRead","message":"Grant source access"}')
    method = {"GET": "get", "LIST": "list_all", "POST": "post"}[operation]

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(client, method, fail)
    with pytest.raises(FabricApiError) as caught:
        capture_workspaces(client, recovery_set(), tokens=object(), readers=Readers())
    assert caught.value is error
    assert caught.value.error_code == "NoRead"


@pytest.mark.parametrize("paths", [[CONTROL], ["20000000-0000-0000-0000-000000000099"]])
def test_explicit_control_or_invisible_selection_is_rejected(paths):
    with pytest.raises(FabricError):
        capture_workspaces(
            SourceClient(), recovery_set(), tokens=object(), readers=Readers(), workspace_ids=paths
        )


@pytest.mark.parametrize("path", ["../escape.py", "Main\\escape.py", "Main/%2e%2e/escape.py", "/absolute"])
def test_runtime_paths_cannot_escape(path):
    client = SourceClient("SparkJobDefinition", [part(path, "x")])
    with pytest.raises(ValueError):
        capture_item(client, IDENTITY, client.item_type, readers=Readers())


@pytest.mark.parametrize(
    "invalid",
    [
        [part("x.json", "{}"), part("x.json", "{}")],
        [{"path": "x.json", "payloadType": "Unknown", "payload": "e30="}],
        [{"path": "x.json", "payloadType": "InlineBase64", "payload": "!bad!"}],
    ],
)
def test_corrupt_or_duplicate_definition_parts_fail_capture(invalid):
    with pytest.raises((ValueError, FabricError)):
        capture_item(SourceClient(parts=invalid), IDENTITY, "Notebook", readers=Readers())


def test_embedded_secrets_are_not_published():
    with pytest.raises(ValueError, match="credential"):
        make_payload(IDENTITY, "definition.json", b'{"password":"do-not-save"}', PayloadPurpose.DEFINITION)


def test_source_sql_failure_is_not_replaced_by_empty_schema():
    class FailedReader(Readers):
        def sql_metadata(self, server, database):
            raise RuntimeError("SQL permission denied")

    client = SourceClient("Warehouse", properties={"connectionString": "source.sql"})
    with pytest.raises(RuntimeError, match="SQL permission denied"):
        capture_item(client, IDENTITY, client.item_type, readers=FailedReader())


def test_kql_follower_state_is_captured_before_outage():
    client = SourceClient(
        "KQLDatabase",
        [part("DatabaseProperties.json", {})],
        {"databaseType": "Shortcut", "queryServiceUri": "https://source.kusto"},
    )
    captured = capture_item(client, IDENTITY, client.item_type, readers=Readers())
    assert captured.record.properties["bcdr"]["kql"]["follower"][0]["OriginalDatabaseName"] == ITEM


def test_one_lake_table_reader_handles_schema_and_table_pagination_without_sql():
    requests = []

    def respond(request):
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer storage"
        if request.url.path.endswith("/schemas"):
            return httpx.Response(200, json={"schemas": [{"name": "sales.a"}], "next_page_token": None})
        assert request.url.params["schema_name"] == "sales.a"
        name = "two" if request.url.params.get("page_token") else "one"
        return httpx.Response(
            200,
            json={
                "tables": [
                    {"name": name, "schema_name": "sales.a", "storage_location": f"https://actual/{name}"}
                ],
                "next_page_token": None if name == "two" else "next",
            },
        )

    class Tokens:
        def storage_token(self):
            return "storage"

    reader = MetadataReaders(Tokens(), transport=httpx.MockTransport(respond))
    result = reader.lakehouse_tables(IDENTITY)
    assert [table["name"] for table in result["tables"]] == ["one", "two"]
    assert len(requests) == 3


def test_onelake_missing_collection_is_not_an_empty_inventory():
    class Tokens:
        def storage_token(self):
            return "storage"

    reader = MetadataReaders(Tokens(), transport=httpx.MockTransport(lambda _: httpx.Response(200, json={})))
    with pytest.raises(FabricError, match="empty inventory"):
        reader.lakehouse_tables(IDENTITY)


@pytest.mark.parametrize("item_type", sorted(TYPE_REGISTRY))
def test_every_registry_type_has_an_explicit_capture_route(item_type):
    # Required specialized metadata can be unavailable, but no registry type is silently filtered.
    client = SourceClient(
        item_type,
        [part("definition.json", {"containers": []})],
        {
            "connectionString": "source.sql",
            "serverFqdn": "source.sql",
            "databaseName": "Orders",
            "sqlEndpointProperties": {"connectionString": "source.sql"},
            "queryServiceUri": "https://source.kusto",
            "databasesItemIds": [],
        },
    )
    captured = capture_item(client, IDENTITY, item_type, readers=Readers(), captured_at=datetime.now(UTC))
    assert captured.record.item_type == item_type
    if not TYPE_REGISTRY[item_type].migration_rebuild:
        assert captured.record.properties["bcdr"]["unsupported_reason"]
