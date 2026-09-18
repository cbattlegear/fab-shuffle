"""Credential-free optional protection providers; recovery clients cannot read primary."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import zipfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from fabshuffle.bcdr import protection as common
from fabshuffle.bcdr import protection_cosmos as cosmos
from fabshuffle.bcdr import protection_kql as kql
from fabshuffle.bcdr import protection_sql as sql
from fabshuffle.bcdr.protection import (
    ConsistencyEvidence,
    DataArtifact,
    DataEndpoint,
    DataIdentity,
    ProtectedLocation,
    ProtectionError,
    ProtectionLimits,
    artifact_path,
    missing_protection,
    staged_artifacts,
    validate_manifest,
)
from fabshuffle.run import CancelledError
from fabshuffle.transfer.common import StagingBudgetError

TENANT = "11111111-1111-1111-1111-111111111111"
SOURCE = DataIdentity(TENANT, "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333")
TARGET = DataIdentity(TENANT, "44444444-4444-4444-4444-444444444444", "55555555-5555-5555-5555-555555555555")
SRC = DataEndpoint(SOURCE, "https://source.example.test", "original")
DST = DataEndpoint(TARGET, "https://target.example.test", "standby")
NOW = datetime.now(UTC)
EVIDENCE = ConsistencyEvidence("freeze-123", NOW - timedelta(hours=1), NOW + timedelta(hours=1), True, True)
TOKENS = SimpleNamespace(
    sql_token=lambda: "TEST_ACCESS_TOKEN", principal=object(), assert_active=lambda: None,
)
LIMITS = ProtectionLimits(max_disk_bytes=1024**2, max_record_bytes=65536)


@pytest.fixture
def location(tmp_path):
    root = tmp_path / "protected"
    root.mkdir()
    return ProtectedLocation(root, "eastus", "westus", "storage-approval")


def options(location, tmp_path):
    return dict(
        source=SOURCE,
        target=DST,
        tokens=TOKENS,
        location=location,
        scratch=tmp_path,
        max_age=timedelta(days=1),
        target_approval_ref="restricted-empty-target",
        limits=LIMITS,
    )


def package(path, *, model='<Model><Element Type="SqlTable" /></Model>', extra=None):
    with zipfile.ZipFile(path, "w") as writer:
        writer.writestr("model.xml", model)
        writer.writestr("Data/Orders.BCP", b"captured-data")
        for name, payload in (extra or {}).items():
            writer.writestr(name, payload)


@pytest.fixture
def sql_capture(monkeypatch, location):
    calls = []
    tables = (sql.SqlTableRows("sales", "orders", 2),)

    def run(arguments, **kwargs):
        calls.append(arguments)
        destination = next(arg.split(":", 1)[1] for arg in arguments if arg.startswith("/TargetFile:"))
        package(Path(destination))

    monkeypatch.setattr(sql, "_run_sqlpackage", run)
    monkeypatch.setattr(sql, "_table_rows", lambda *_: tables)
    captured = sql.capture_sql(
        source=SRC,
        tokens=TOKENS,
        location=location,
        consistency=EVIDENCE,
        schema_approval_ref="self-contained-schema",
        limits=LIMITS,
    )
    return captured, calls


@pytest.mark.parametrize("kind", ["SQLDatabase", "CosmosDBDatabase", "KQLDatabase"])
def test_missing_optional_input_is_named_and_not_ready(kind):
    result = missing_protection("Orders", kind)
    assert result.state == "protection_missing"
    assert not result.data_ready and not result.ready_for_cutover
    assert "Orders" in result.warnings[0] and "Supply" in result.warnings[0]


def test_sql_capture_is_data_bearing_permission_filtered_atomic_and_serializable(sql_capture, location):
    captured, calls = sql_capture
    assert captured.manifest.complete
    assert "/p:ExtractAllTableData=True" in calls[0]
    assert "/p:IgnorePermissions=True" in calls[0]
    assert "/p:ExtractReferencedServerScopedElements=False" in calls[0]
    assert len(list(location.root.iterdir())) == 1
    assert not next(location.root.iterdir()).name.endswith(".partial")
    adapter = TypeAdapter(sql.SqlProtection)
    serialized = adapter.dump_json(captured)
    assert b"TEST_ACCESS_TOKEN" not in serialized
    assert adapter.validate_json(serialized) == captured


@pytest.mark.parametrize(
    "field,value",
    [
        ("complete", False),
        ("schema_version", 2),
        ("source", replace(SRC, identity=TARGET)),
        ("completed_at", NOW + timedelta(days=1)),
    ],
)
def test_manifest_rejects_incomplete_wrong_identity_or_future(sql_capture, location, field, value):
    manifest = replace(sql_capture[0].manifest, **{field: value})
    with pytest.raises(ProtectionError):
        validate_manifest(manifest, source=SOURCE, location=location, max_age=timedelta(days=1))


def test_manifest_stale_is_not_ready(sql_capture, location):
    manifest = sql_capture[0].manifest
    result = validate_manifest(
        manifest,
        source=SOURCE,
        location=location,
        max_age=timedelta(days=1),
        now=NOW + timedelta(days=2),
    )
    assert result.state == "protection_stale" and not result.data_ready


@pytest.mark.parametrize("path", ["../secret", "/etc/passwd", "C:\\data", "a/../b", "./data", "a//b"])
def test_artifact_paths_are_bounded(path):
    with pytest.raises(ValueError):
        DataArtifact(path, "0" * 64, 1)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://target.test",
        "https://u:p@target.test",
        "https://target.test/?sig=token",
        "https://target.test/path",
        "https://target.test#token",
    ],
)
def test_endpoints_cannot_embed_credentials(endpoint):
    with pytest.raises(ValueError):
        DataEndpoint(TARGET, endpoint, "db")


def test_artifact_symlinks_and_hash_changes_are_refused(sql_capture, location):
    artifact = sql_capture[0].manifest.artifacts[0]
    path = artifact_path(location.root, artifact.path)
    sibling = path.with_suffix(".renamed")
    path.rename(sibling)
    path.symlink_to(sibling)
    with pytest.raises(ProtectionError, match="symbolic"):
        artifact_path(location.root, artifact.path)
    path.unlink()
    sibling.rename(path)
    path.write_bytes(b"corrupt")
    with pytest.raises(ProtectionError, match="checksum"):
        validate_manifest(
            sql_capture[0].manifest, source=SOURCE, location=location, max_age=timedelta(days=1)
        )


def test_staging_revalidates_changed_bytes_and_cleans_up(sql_capture, location, tmp_path):
    captured = sql_capture[0]
    path = artifact_path(location.root, captured.manifest.artifacts[0].path)
    path.write_bytes(b"replacement")
    with pytest.raises(ProtectionError, match="changed"):
        with staged_artifacts(captured.manifest, location, tmp_path, LIMITS, None):
            pytest.fail("Changed data must not reach a destination.")
    assert not list(tmp_path.glob("bcdr-data-*"))


def test_restore_sql_is_source_offline_and_excludes_grants(sql_capture, monkeypatch, location, tmp_path):
    captured, _ = sql_capture
    calls = []
    targets = []
    monkeypatch.setattr(sql, "_require_empty_target", lambda target, *_: targets.append(target))

    def table_rows(endpoint, *_):
        assert endpoint == DST, "Restore must never query the failed primary."
        return captured.tables

    monkeypatch.setattr(sql, "_table_rows", table_rows)
    monkeypatch.setattr(sql, "_run_sqlpackage", lambda arguments, **_: calls.append(arguments))
    result = sql.restore_sql(captured, **options(location, tmp_path))
    assert result.data_ready and not result.ready_for_cutover and result.target == TARGET
    assert targets == [DST]
    command = calls[0]
    assert "/Action:Publish" in command
    for value in (
        "/p:IgnorePermissions=True",
        "/p:IgnoreAuthorizer=True",
        "/p:IgnorePreDeployScript=True",
        "/p:IgnorePostDeployScript=True",
    ):
        assert value in command
    exclusions = next(arg for arg in command if arg.startswith("/p:ExcludeObjectTypes="))
    for security in ("Permissions", "Users", "RoleMembership", "Logins", "Credentials"):
        assert security in exclusions
    assert all("SourceServer" not in arg for arg in command)
    assert not list(tmp_path.glob("bcdr-data-*"))


def test_bacpac_import_is_deferred_not_unfiltered(sql_capture, monkeypatch, location, tmp_path):
    captured = replace(sql_capture[0], manifest=replace(sql_capture[0].manifest, provider="sql-bacpac"))
    monkeypatch.setattr(sql, "_run_sqlpackage", lambda *_a, **_k: pytest.fail("No unsafe BACPAC import."))
    result = sql.restore_sql(captured, **options(location, tmp_path))
    assert not result.data_ready and result.state == "deferred"
    assert "grants" in result.warnings[0]


def test_sql_restore_refuses_source_or_bad_row_counts(sql_capture, monkeypatch, location, tmp_path):
    captured = sql_capture[0]
    kwargs = options(location, tmp_path)
    with pytest.raises(ProtectionError, match="independently"):
        sql.restore_sql(captured, **dict(kwargs, target=SRC))
    monkeypatch.setattr(sql, "_require_empty_target", lambda *_: None)
    monkeypatch.setattr(sql, "_run_sqlpackage", lambda *_a, **_k: None)
    monkeypatch.setattr(sql, "_table_rows", lambda *_: ())
    with pytest.raises(ProtectionError, match="counts"):
        sql.restore_sql(captured, **kwargs)


@pytest.mark.parametrize("kind", ["SqlDmlTrigger", "SqlExternalDataSource", "SqlSynonym", "SqlAssembly"])
def test_sql_unsafe_models_are_blocked_before_destination(kind, tmp_path):
    artifact = tmp_path / "input.dacpac"
    package(artifact, model=f'<Model><Element Type="{kind}" /></Model>')
    with pytest.raises(ProtectionError, match=kind):
        sql._inspect_package(artifact, LIMITS, SRC)


def test_sql_package_rejects_zip_traversal_xml_entities_and_bombs(tmp_path):
    path = tmp_path / "bad.dacpac"
    package(path, extra={"../startup.sh": "bad"})
    with pytest.raises(ProtectionError, match="unsafe"):
        sql._inspect_package(path, LIMITS, SRC)
    package(path, model='<!DOCTYPE data [<!ENTITY x "hello">]><Model>&x;</Model>')
    with pytest.raises(ProtectionError, match="entity"):
        sql._inspect_package(path, LIMITS, SRC)
    package(path, extra={"huge": "x" * 500})
    with pytest.raises(StagingBudgetError):
        sql._inspect_package(path, replace(LIMITS, max_disk_bytes=100), SRC)


def test_sql_capture_quiescence_and_cancel_cleanup(monkeypatch, location):
    with pytest.raises(ProtectionError, match="Quiesce"):
        sql.capture_sql(
            source=SRC,
            tokens=TOKENS,
            location=location,
            consistency=replace(EVIDENCE, writes_quiesced=False),
            schema_approval_ref="review",
        )
    monkeypatch.setattr(sql, "_table_rows", lambda *_: ())

    def cancelled(arguments, **_):
        path = next(arg.split(":", 1)[1] for arg in arguments if arg.startswith("/TargetFile:"))
        Path(path).write_bytes(b"partial")
        raise CancelledError("cancelled")

    monkeypatch.setattr(sql, "_run_sqlpackage", cancelled)
    with pytest.raises(CancelledError):
        sql.capture_sql(
            source=SRC, tokens=TOKENS, location=location, consistency=EVIDENCE, schema_approval_ref="review"
        )
    assert list(location.root.iterdir()) == []


def test_sql_tool_errors_preserve_service_code_but_redact_token(monkeypatch, tmp_path):
    monkeypatch.setattr(sql, "SETTINGS", SimpleNamespace(sqlpackage_path=sys.executable))
    with pytest.raises(ProtectionError) as failure:
        sql._run_sqlpackage(
            ["-c", "import sys; print('ServiceCode: bad request', sys.argv); sys.exit(2)"],
            token="TEST_ACCESS_TOKEN",
            staging=tmp_path,
            limits=LIMITS,
            cancel=None,
        )
    assert "ServiceCode: bad request" in str(failure.value)
    assert "TEST_ACCESS_TOKEN" not in str(failure.value)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("failure", ["cancel", "timeout", "disk", "output"])
def test_sql_tool_cancellation_and_resource_budgets(monkeypatch, tmp_path, failure):
    monkeypatch.setattr(sql, "SETTINGS", SimpleNamespace(sqlpackage_path=sys.executable))
    code = "import time; time.sleep(60)"
    limits = replace(LIMITS, timeout_seconds=1)
    if failure == "disk":
        code = "from pathlib import Path; Path('large').write_bytes(b'x'*10000)"
        limits = replace(limits, max_disk_bytes=100)
    if failure == "output":
        code = "print('x'*10000)"
        limits = replace(limits, max_record_bytes=100)
    expected = (
        CancelledError
        if failure == "cancel"
        else (StagingBudgetError if failure in ("disk", "output") else ProtectionError)
    )
    with pytest.raises(expected):
        sql._run_sqlpackage(
            ["-c", code],
            token="TEST_ACCESS_TOKEN",
            staging=tmp_path,
            limits=limits,
            cancel=(lambda: True) if failure == "cancel" else None,
        )


def test_installed_sqlpackage_supports_used_publish_controls():
    result = subprocess.run(
        ["sqlpackage", "/Action:Publish", "/?"], capture_output=True, text=True, check=True
    )
    for name in (
        "IgnorePermissions",
        "IgnoreRoleMembership",
        "IgnoreAuthorizer",
        "IgnorePreDeployScript",
        "IgnorePostDeployScript",
        "ExcludeObjectTypes",
    ):
        assert name in result.stdout


class FakeContainer:
    def __init__(self, name="orders", documents=(), *, default_ttl=None, partition="/customer/id"):
        self.properties = {"id": name, "partitionKey": {"paths": [partition], "kind": "Hash", "version": 2}}
        if default_ttl is not None:
            self.properties["defaultTtl"] = default_ttl
        self.documents = copy.deepcopy(list(documents))
        self.writes = []

    def read(self):
        return copy.deepcopy(self.properties)

    def query_items(self, query, **kwargs):
        assert kwargs["enable_cross_partition_query"] and kwargs["max_item_count"] == 1
        yield from copy.deepcopy(self.documents[:1] if "TOP 1" in query else self.documents)

    def create_item(self, body):
        self.writes.append(copy.deepcopy(body))
        self.documents.append(copy.deepcopy(body))
        return body


class FakeCosmos:
    def __init__(self, *containers):
        self.containers = {c.properties["id"]: c for c in containers}
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def get_database_client(self, name):
        assert name in ("original", "standby")
        return self

    def list_containers(self):
        yield from (c.read() for c in self.containers.values())

    def get_container_client(self, name):
        return self.containers[name]


DOCUMENTS = [
    {
        "id": "same-id",
        "customer": {"id": "p1"},
        "ttl": 3600,
        "_ts": 123,
        "_etag": "old",
        "custom": {"_etag": "user-field"},
        "value": 1,
    },
    {"id": "same-id", "customer": {"id": "p2"}, "ttl": -1, "_rid": "source-rid", "value": 2},
]


@pytest.fixture
def cosmos_capture(monkeypatch, location):
    client = FakeCosmos(FakeContainer(documents=DOCUMENTS))
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: client)
    captured = cosmos.capture_cosmos(
        source=SRC,
        tokens=TOKENS,
        location=location,
        consistency=EVIDENCE,
        limits=LIMITS,
    )
    assert client.closed
    return captured


def test_cosmos_stream_export_preserves_system_metadata_and_manifest(cosmos_capture, location):
    artifact = cosmos_capture.manifest.artifacts[0]
    assert artifact.records == 2
    lines = artifact_path(location.root, artifact.path).read_text().splitlines()
    assert [json.loads(line) for line in lines] == DOCUMENTS
    adapter = TypeAdapter(cosmos.CosmosProtection)
    assert adapter.validate_json(adapter.dump_json(cosmos_capture)) == cosmos_capture


def test_cosmos_restore_offline_preserves_user_ids_partitions_and_strips_only_system(
    cosmos_capture,
    monkeypatch,
    location,
    tmp_path,
):
    target = FakeContainer()
    client = FakeCosmos(target)

    def destination_only(endpoint, _):
        assert endpoint == DST.endpoint, "Primary must not be opened during recovery."
        return client

    monkeypatch.setattr(cosmos, "cosmos_client", destination_only)
    result = cosmos.restore_cosmos(cosmos_capture, **options(location, tmp_path))
    assert result.state == "restored_stopped" and result.data_ready and not result.ready_for_cutover
    assert len(target.writes) == 2
    assert {d["customer"]["id"] for d in target.writes} == {"p1", "p2"}
    assert all(d["id"] == "same-id" for d in target.writes)
    assert target.writes[0]["custom"] == {"_etag": "user-field"}
    assert target.writes[0]["ttl"] == 3600 and "_ts" not in target.writes[0]
    assert "deletes" in result.warnings[1] and "TTL OFF" in result.warnings[0]


@pytest.mark.parametrize(
    "change,message",
    [
        ({"default_ttl": -1}, "TTL OFF"),
        ({"partition": "/other"}, "partition-key"),
        ({"documents": DOCUMENTS}, "not empty"),
    ],
)
def test_cosmos_target_ttl_identity_and_empty_requirements(
    cosmos_capture,
    monkeypatch,
    location,
    tmp_path,
    change,
    message,
):
    target = FakeContainer(**change)
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: FakeCosmos(target))
    with pytest.raises(ProtectionError, match=message):
        cosmos.restore_cosmos(cosmos_capture, **options(location, tmp_path))
    assert target.writes == []


def test_cosmos_corrupt_last_record_does_not_write_earlier_records(
    cosmos_capture,
    monkeypatch,
    location,
    tmp_path,
):
    artifact = cosmos_capture.manifest.artifacts[0]
    path = artifact_path(location.root, artifact.path)
    data = path.read_bytes() + b'{"bad":"no-id"}\n'
    path.write_bytes(data)
    changed = replace(artifact, sha256=hashlib.sha256(data).hexdigest(), size_bytes=len(data), records=3)
    captured = replace(cosmos_capture, manifest=replace(cosmos_capture.manifest, artifacts=(changed,)))
    monkeypatch.setattr(
        cosmos, "cosmos_client", lambda *_: pytest.fail("Invalid data must not open the target.")
    )
    with pytest.raises(ProtectionError, match="id"):
        cosmos.restore_cosmos(captured, **options(location, tmp_path))


@pytest.mark.parametrize("ttl", [3600, -1])
def test_cosmos_source_ttl_expiration_is_not_quiescence(monkeypatch, location, ttl):
    client = FakeCosmos(FakeContainer(documents=DOCUMENTS, default_ttl=ttl))
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: client)
    with pytest.raises(ProtectionError, match=r"TTL|expiring"):
        cosmos.capture_cosmos(
            source=SRC, tokens=TOKENS, location=location, consistency=EVIDENCE, limits=LIMITS
        )
    assert list(location.root.iterdir()) == []


@pytest.mark.parametrize(
    "limits",
    [
        replace(LIMITS, max_disk_bytes=10),
        replace(LIMITS, max_record_bytes=10),
    ],
)
def test_cosmos_export_budgets_are_atomic(monkeypatch, location, limits):
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: FakeCosmos(FakeContainer(documents=DOCUMENTS)))
    with pytest.raises(StagingBudgetError):
        cosmos.capture_cosmos(
            source=SRC, tokens=TOKENS, location=location, consistency=EVIDENCE, limits=limits
        )
    assert list(location.root.iterdir()) == []


def test_cosmos_export_cancel_and_service_error_never_publish(monkeypatch, location):
    class BrokenContainer(FakeContainer):
        def query_items(self, query, **kwargs):
            yield DOCUMENTS[0]
            raise RuntimeError("HTTP 429 ServiceCode: retry after 10")

    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: FakeCosmos(BrokenContainer()))
    with pytest.raises(RuntimeError, match="HTTP 429 ServiceCode"):
        cosmos.capture_cosmos(
            source=SRC, tokens=TOKENS, location=location, consistency=EVIDENCE, limits=LIMITS
        )
    assert list(location.root.iterdir()) == []
    with pytest.raises(CancelledError):
        cosmos.capture_cosmos(
            source=SRC,
            tokens=TOKENS,
            location=location,
            consistency=EVIDENCE,
            limits=LIMITS,
            cancel=lambda: True,
        )
    assert list(location.root.iterdir()) == []


@pytest.fixture
def prepared():
    return kql.KqlProtection(
        SRC,
        DST,
        "eastus",
        "westus",
        NOW,
        NOW - timedelta(minutes=2),
        "dataset-check",
        "schema-policy-check",
        "target-restricted",
        (kql.KqlTable("events", 10),),
        (kql.KqlInput("eventhub-2", "input-access-check"),),
    )


def kql_options():
    return dict(
        source=SOURCE,
        target=DST,
        tokens=TOKENS,
        probe_inputs=lambda _: True,
        max_age=timedelta(hours=1),
        recovery_capacity_paused=False,
        limits=LIMITS,
    )


def test_kql_paused_continuous_and_stale_are_explicitly_unready(prepared):
    result = kql.restore_kql(replace(prepared, continuous=True), **kql_options())
    assert not result.data_ready and "active compute" in result.warnings[0]
    result = kql.restore_kql(prepared, **dict(kql_options(), recovery_capacity_paused=True))
    assert not result.data_ready and "Resume" in result.warnings[0]
    result = kql.restore_kql(
        replace(prepared, data_as_of=NOW - timedelta(days=2)),
        **kql_options(),
    )
    assert result.state == "protection_stale" and not result.data_ready


def test_kql_requires_inputs_and_data_not_empty_schema(prepared):
    with pytest.raises(ProtectionError, match="Supply"):
        kql.restore_kql(replace(prepared, tables=()), **kql_options())
    result = kql.restore_kql(prepared, **dict(kql_options(), probe_inputs=lambda _: False))
    assert result.state == "deferred" and not result.data_ready


def test_kql_restores_prepared_target_without_primary_reads_or_ingestion(prepared, monkeypatch):
    queries = []

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute_query(self, database, query, **kwargs):
            queries.append((database, query))
            return SimpleNamespace(primary_results=[[[12]]])

        def execute_mgmt(self, database, query, **kwargs):
            assert query == '.show table ["events"] details | project TableName'
            return SimpleNamespace(primary_results=[[{"TableName": "events"}]])

    def target_only(endpoint, _, *, tokens):
        assert endpoint == DST.endpoint
        assert tokens is TOKENS
        return Client()

    monkeypatch.setattr(kql, "kql_client", target_only)
    result = kql.restore_kql(prepared, **kql_options())
    assert result.data_ready and not result.ready_for_cutover
    assert queries == [("standby", '["events"] | count')]
    assert "export" in result.warnings[0]


def test_kql_fails_if_standby_is_empty_or_wrong_mapping(prepared, monkeypatch):
    with pytest.raises(ProtectionError):
        kql.restore_kql(prepared, **dict(kql_options(), target=SRC))
    with pytest.raises(ProtectionError, match="another"):
        kql.restore_kql(replace(prepared, target_region="eastus"), **kql_options())
    monkeypatch.setattr(
        kql, "_probe_tables", lambda *_: (_ for _ in ()).throw(ProtectionError("events not populated"))
    )
    with pytest.raises(ProtectionError, match="not populated"):
        kql.restore_kql(prepared, **kql_options())


@pytest.mark.parametrize("permission,objects", [(0, 0), (None, 0), (1, 2), (1, 0)])
def test_sql_empty_target_requires_complete_metadata_visibility_and_closes_connection(
    monkeypatch,
    permission,
    objects,
):
    class Connection:
        closed = False

        def cursor(self):
            return self

        def execute(self, query):
            self.value = permission if "HAS_PERMS" in query else objects
            return self

        def fetchone(self):
            return (self.value,)

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(sql, "connect", lambda *_a, **_k: connection)
    if permission != 1 or objects:
        with pytest.raises(ProtectionError):
            sql._require_empty_target(DST, TOKENS, LIMITS)
    else:
        sql._require_empty_target(DST, TOKENS, LIMITS)
    assert connection.closed


def test_utf16_xml_cannot_bypass_entity_check(tmp_path):
    path = tmp_path / "utf16.dacpac"
    model = '<!DOCTYPE data [<!ENTITY x "hello">]><Model>&x;</Model>'.encode("utf-16-le")
    package(path, model=model)
    with pytest.raises(ProtectionError, match="entity"):
        sql._inspect_package(path, LIMITS, SRC)


def test_cosmos_total_operation_deadline(monkeypatch, location):
    values = iter((0, 2))
    monkeypatch.setattr(common.time, "monotonic", lambda: next(values))
    with pytest.raises(ProtectionError, match="timeout_seconds"):
        cosmos.capture_cosmos(
            source=SRC,
            tokens=TOKENS,
            location=location,
            consistency=EVIDENCE,
            limits=replace(LIMITS, timeout_seconds=1),
        )
    assert list(location.root.iterdir()) == []


@pytest.mark.parametrize(
    "line",
    [
        b'{"id":"1","customer":{"id":"p"},"v":1e999}\n',
        b'{"id":"1","id":"2","customer":{"id":"p"}}\n',
        b'{"id":"1","customer":{"id":"p"}}',
    ],
)
def test_cosmos_json_rejects_overflow_duplicate_and_partial_records(tmp_path, line):
    path = tmp_path / "data.jsonl"
    path.write_bytes(line)
    container = cosmos._container(FakeContainer().read())
    with pytest.raises(ProtectionError):
        list(cosmos._documents(path, container, LIMITS, None))


def test_cosmos_failed_restore_preserves_error_and_does_not_upsert_retry(
    cosmos_capture,
    monkeypatch,
    location,
    tmp_path,
):
    class FailingTarget(FakeContainer):
        def create_item(self, body):
            if self.writes:
                raise RuntimeError("ServiceError: destination write denied")
            return super().create_item(body)

    target = FailingTarget()
    monkeypatch.setattr(cosmos, "cosmos_client", lambda *_: FakeCosmos(target))
    with pytest.raises(RuntimeError, match="ServiceError: destination write denied"):
        cosmos.restore_cosmos(cosmos_capture, **options(location, tmp_path))
    assert len(target.writes) == 1
    with pytest.raises(ProtectionError, match="not empty"):
        cosmos.restore_cosmos(cosmos_capture, **options(location, tmp_path))
    assert len(target.writes) == 1


def test_kql_same_cluster_is_not_an_independent_region(prepared):
    same_cluster = replace(DST, endpoint=SRC.endpoint)
    with pytest.raises(ProtectionError, match="independent regional cluster"):
        kql.restore_kql(
            replace(prepared, target=same_cluster),
            **dict(kql_options(), target=same_cluster),
        )
