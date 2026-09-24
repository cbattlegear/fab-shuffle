"""Pinned OneLake byte recovery is executable without claiming Delta engine readiness."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from fabshuffle.bcdr import protection_binding as binding
from fabshuffle.bcdr import protection_lakehouse as lh
from fabshuffle.bcdr.catalog import CatalogError
from fabshuffle.bcdr.contracts import OperationState
from fabshuffle.bcdr.protection import ProtectionError
from fabshuffle.run import CancelledError
from fabshuffle.transfer.common import StagingBudgetError
from fabshuffle.transfer.files import DeltaValidationRequired, FileTransferError
from tests.test_bcdr_protection import DST, EVIDENCE, LIMITS, NOW, SRC
from tests.test_bcdr_protection_binding import SOURCE, TARGET, Runtime, generation
from tests.test_cross_tenant_delta import METADATA, PROTOCOL, lines

TOKENS = SimpleNamespace(storage_token=lambda: "storage-token")
CONTENT = {
    "Files/input.txt": b"business-file",
    "Tables/dbo/Events/part.parquet": b"data",
    "Tables/dbo/Events/_delta_log/00000000000000000000.json": lines(
        PROTOCOL,
        METADATA,
        {"add": {"path": "part.parquet", "size": 4}},
    ),
}


class OneLake:
    def __init__(self):
        self.source = dict(CONTENT)
        self.target = {}
        self.target_dirs = {"Tables", "Files"}
        self.source_dirs = set()
        self.calls = []
        self.reject_read = False
        self.race = False
        self.bad_hash = False
        self.versions = {}
        self.after_write = None
        self.redirect = False
        self.directory_versions = {}

    def handle(self, request):
        self.calls.append(request)
        assert request.url.host == "onelake.dfs.fabric.microsoft.com"
        workspace, *parts = request.url.path.strip("/").split("/")
        source = workspace == SOURCE.workspace_id
        identity = SOURCE if source else TARGET
        assert workspace == identity.workspace_id
        data = self.source if source else self.target
        if source:
            assert request.method in ("GET", "HEAD")
            if self.redirect:
                return httpx.Response(307, headers={"Location": "https://unapproved.example.test/secret"})
        if not parts:
            directory = request.url.params["directory"].removeprefix(identity.item_id + "/")
            entries = {}
            paths = set(data) | (self.source_dirs if source else self.target_dirs)
            for path in paths:
                if not path.startswith(directory + "/"):
                    continue
                remainder = path[len(directory) + 1 :]
                name = directory + "/" + remainder.split("/")[0]
                entries[name] = (
                    {"name": identity.item_id + "/" + name, "isDirectory": True}
                    if "/" in remainder or name not in data
                    else {
                        "name": identity.item_id + "/" + name,
                        "etag": '"v0"',
                        "contentLength": len(data[name]),
                    }
                )
            return httpx.Response(200, json={"paths": list(entries.values())})
        assert parts[0] == identity.item_id
        path = "/".join(parts[1:])
        etag = f'"v{self.versions.get(path, 0)}"'
        if request.method in ("GET", "HEAD"):
            if not source and path in self.target_dirs and request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "ETag": f'"dir{self.directory_versions.get(path, 0)}"',
                        "x-ms-resource-type": "directory",
                    },
                )
            if source and self.reject_read:
                return httpx.Response(
                    403, json={"error": {"code": "ReplicaUnavailable", "message": "Not routed"}}
                )
            if path not in data:
                return httpx.Response(404, json={"error": {"code": "PathNotFound", "message": "No file"}})
            if request.headers.get("If-Match", etag) != etag:
                return httpx.Response(
                    412, json={"error": {"code": "ConditionNotMet", "message": "Version changed"}}
                )
            content = bytes(data[path])
            if request.method == "HEAD":
                return httpx.Response(200, headers={"ETag": etag, "Content-Length": str(len(content))})
            begin, end = map(int, request.headers["Range"].removeprefix("bytes=").split("-"))
            if not source and self.bad_hash:
                content = b"x" * len(content)
            return httpx.Response(
                206,
                content=content[begin : end + 1],
                headers={
                    "ETag": etag,
                    "Content-Range": f"bytes {begin}-{end}/{len(content)}",
                },
            )
        if request.method == "PUT":
            assert request.headers["If-None-Match"] == "*", "Never replace an existing destination path."
            if path in data or path in self.target_dirs or self.race:
                return httpx.Response(
                    412, json={"error": {"code": "ConditionNotMet", "message": "Target exists"}}
                )
            if request.url.params["resource"] == "directory":
                self.target_dirs.add(path)
            else:
                data[path] = bytearray()
            return httpx.Response(201, headers={"ETag": etag})
        assert request.method == "PATCH" and path in data
        if request.url.params["action"] == "append":
            data[path].extend(request.content)
            if self.after_write:
                self.after_write()
        return httpx.Response(200, headers={"ETag": etag})

    @property
    def writes(self):
        return [request for request in self.calls if request.method not in ("GET", "HEAD")]


@pytest.fixture
def setup(monkeypatch):
    lake = OneLake()
    original = lh._PinnedClient
    client = httpx.Client
    monkeypatch.setattr(
        lh, "_PinnedClient", lambda *args: original(*args, transport=httpx.MockTransport(lake.handle))
    )
    monkeypatch.setattr(
        lh.httpx, "Client", lambda **kw: client(transport=httpx.MockTransport(lake.handle), **kw)
    )
    directories = {"Tables", "Files", "Tables/dbo", "Tables/dbo/Events", "Tables/dbo/Events/_delta_log"}
    protection = lh.LakehouseProtection(
        source=SRC.identity,
        captured_at=NOW,
        completed_at=NOW,
        consistency=EVIDENCE,
        storage_read_approval_ref="replica-access-approved",
        snapshot_qualification_ref="quiesced-input-qualified",
        source_region="eastus",
        recovery_region="westus",
        source_paths_verified_local=True,
        files=tuple(
            lh.OneLakeFile(
                path=path, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), etag='"v0"'
            )
            for path, data in CONTENT.items()
        ),
        directories=tuple(sorted(directories)),
    )
    return lake, protection


class TargetFabric:
    def __init__(self):
        self.default_schema = "dbo"
        self.shortcuts = []
        self.calls = []
        self.hook = None
        self.shortcut_error = None

    def get(self, path):
        assert path == f"workspaces/{TARGET.workspace_id}/lakehouses/{TARGET.item_id}"
        self.calls.append(path)
        if self.hook:
            self.hook()
        return {
            "id": TARGET.item_id,
            "workspaceId": TARGET.workspace_id,
            "type": "Lakehouse",
            "properties": {"defaultSchema": self.default_schema} if self.default_schema else {},
        }

    def list_all(self, path):
        assert path == f"workspaces/{TARGET.workspace_id}/items/{TARGET.item_id}/shortcuts"
        self.calls.append(path)
        if self.shortcut_error:
            raise self.shortcut_error
        return self.shortcuts


def restore(protection, tmp_path, **extra):
    return lh.restore_lakehouse(
        protection,
        source=SRC.identity,
        target=DST.identity,
        tokens=TOKENS,
        scratch=tmp_path,
        max_age=timedelta(days=1),
        target_approval_ref="fresh-owned-target",
        target_client=extra.pop("target_client", TargetFabric()),
        limits=LIMITS,
        **extra,
    )


def test_real_bounded_relay_copies_exact_pins_without_source_compute(setup, tmp_path):
    lake, protection = setup
    receipt = restore(protection, tmp_path)
    assert {path: bytes(data) for path, data in lake.target.items()} == CONTENT
    assert receipt.copied_files == len(CONTENT) and receipt.copied_bytes == sum(map(len, CONTENT.values()))
    assert receipt.byte_copy_complete and not receipt.data_ready and not receipt.endpoint_ready
    assert "not the complete active-file set" in receipt.warnings[0]
    assert all(request.url.host == "onelake.dfs.fabric.microsoft.com" for request in lake.calls)
    assert all(request.headers["If-None-Match"] == "*" for request in lake.writes if request.method == "PUT")
    assert not list(tmp_path.glob("delta-preflight-*"))


def test_capture_returns_complete_hash_and_version_pins_not_local_metadata_archive(setup, tmp_path):
    lake, _ = setup
    captured = lh.capture_lakehouse(
        source=SRC.identity,
        tokens=TOKENS,
        consistency=EVIDENCE,
        storage_read_approval_ref="replica-access-approved",
        snapshot_qualification_ref="snapshot-qualified",
        source_region="eastus",
        recovery_region="westus",
        source_paths_verified_local=True,
        scratch=tmp_path,
        limits=LIMITS,
    )
    assert len(captured.files) == 3 and all(file.etag == '"v0"' for file in captured.files)
    assert not lake.writes and list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("problem", ["hash", "etag", "missing", "extra"])
def test_source_pins_fail_before_any_destination_writes(setup, tmp_path, problem):
    lake, protection = setup
    if problem == "hash":
        lake.source["Files/input.txt"] = b"changed-file!"
    elif problem == "etag":
        lake.versions["Files/input.txt"] = 1
    elif problem == "missing":
        del lake.source["Files/input.txt"]
    else:
        lake.source["Files/not-in-snapshot.txt"] = b"unexpected"
    with pytest.raises((ProtectionError, FileTransferError)):
        restore(protection, tmp_path)
    assert not lake.writes


def test_structural_delta_reference_failure_does_not_copy_or_repair_logs(setup, tmp_path):
    lake, protection = setup
    path = next(path for path in lake.source if path.endswith(".json"))
    lake.source[path] = lines(PROTOCOL, METADATA, {"add": {"path": "../escape.parquet"}})
    pinned = [
        row.model_copy(
            update={
                "sha256": hashlib.sha256(lake.source[path]).hexdigest(),
                "size_bytes": len(lake.source[path]),
            }
        )
        if row.path == path
        else row
        for row in protection.files
    ]
    protection = protection.model_copy(update={"files": tuple(pinned)})
    with pytest.raises(DeltaValidationRequired):
        restore(protection, tmp_path)
    assert not lake.writes
    assert lake.source[path].endswith(b'../escape.parquet"}}\n')


def test_nonempty_or_raced_destination_is_not_overwritten(setup, tmp_path):
    lake, protection = setup
    lake.target["Files/input.txt"] = bytearray(b"original")
    with pytest.raises(ProtectionError, match="fresh owned"):
        restore(protection, tmp_path)
    assert bytes(lake.target["Files/input.txt"]) == b"original" and not lake.writes
    lake.target.clear()
    lake.race = True
    with pytest.raises(FileTransferError, match="ConditionNotMet"):
        restore(protection, tmp_path)
    assert not lake.target


def test_destination_digest_failure_remains_unready(setup, tmp_path):
    lake, protection = setup
    lake.bad_hash = True
    with pytest.raises(ProtectionError, match="checksum"):
        restore(protection, tmp_path)


def test_replica_unavailable_preserves_service_error_not_empty_success(setup, tmp_path):
    lake, protection = setup
    lake.reject_read = True
    with pytest.raises(FileTransferError, match="ReplicaUnavailable"):
        restore(protection, tmp_path)
    assert not lake.writes


def test_cancellation_after_append_prevents_later_writes(setup, tmp_path):
    lake, protection = setup
    cancelled = False

    def stop():
        nonlocal cancelled
        cancelled = True

    lake.after_write = stop
    with pytest.raises(CancelledError):
        restore(protection, tmp_path, cancel=lambda: cancelled)
    assert len([request for request in lake.writes if request.method == "PATCH"]) == 1


@pytest.mark.parametrize("path", ["Tables/../secret", "Files/%2fsecret", "Files/a\\b", "Elsewhere/data"])
def test_lakehouse_pins_refuse_path_escape(path):
    with pytest.raises(ValidationError):
        lh.OneLakeFile(path=path, size_bytes=1, sha256="0" * 64, etag='"v0"')


def test_transport_guards_methods_queries_encoding_and_unapproved_hosts(setup):
    lake, protection = setup
    with lh._PinnedClient(protection, DST.identity, LIMITS, None) as client:
        for method, url in (
            ("PUT", f"{lh._root(SRC.identity)}/Files/input.txt"),
            ("GET", f"{lh._root(SRC.identity)}/Files/input.txt?sig=anything"),
            ("GET", f"{lh._root(SRC.identity)}/Files/%252finput.txt"),
            ("GET", f"https://arbitrary.example.test/{SOURCE.workspace_id}/{SOURCE.item_id}/Files/input.txt"),
            ("GET", f"{lh._root(SRC.identity)}/Files/input.txt#fragment"),
            (
                "GET",
                f"https://user:password@onelake.dfs.fabric.microsoft.com/"
                f"{SOURCE.workspace_id}/{SOURCE.item_id}/Files/input.txt",
            ),
        ):
            with pytest.raises(ProtectionError):
                client.request(method, url)
    assert not lake.calls


def test_pinned_client_does_not_follow_redirects_even_if_caller_requests_it(setup):
    lake, protection = setup
    lake.redirect = True
    with lh._PinnedClient(protection, DST.identity, LIMITS, None) as client:
        response = client.get(f"{lh._root(SRC.identity)}/Files/input.txt", follow_redirects=True)
    assert response.status_code == 307 and len(lake.calls) == 1


def test_shared_copy_preserves_injected_client_ownership(setup, tmp_path):
    lake, protection = setup
    with lh._PinnedClient(protection, DST.identity, LIMITS, None) as client:
        client.writes_allowed = True
        client.owned_directories = {"Files", "Tables"}
        lh.files.copy_tree_streaming(
            source_path=lh._root(SRC.identity) + "/Files",
            target_path=lh._root(DST.identity) + "/Files",
            tokens=TOKENS,
            target_tokens=TOKENS,
            scratch_dir=tmp_path,
            transport_client=client,
            max_memory_bytes=LIMITS.max_record_bytes,
            max_disk_staging_bytes=LIMITS.max_disk_bytes,
        )
        assert not client.is_closed
    assert client.is_closed and bytes(lake.target["Files/input.txt"]) == CONTENT["Files/input.txt"]


def test_inventory_budget_checked_before_service_reads(setup, tmp_path):
    lake, protection = setup
    with pytest.raises(StagingBudgetError):
        lh.restore_lakehouse(
            protection,
            source=SRC.identity,
            target=DST.identity,
            tokens=TOKENS,
            scratch=tmp_path,
            max_age=timedelta(days=1),
            target_approval_ref="approved",
            target_client=TargetFabric(),
            limits=LIMITS.__class__(max_manifest_entries=1),
        )
    assert not lake.calls


def test_binder_publishes_preparation_only_after_success_and_preserves_copy_time(setup, tmp_path):
    lake, protection = setup
    runtime = Runtime()

    def put(namespace, key, document):
        runtime.fence()
        assert all(op.state == OperationState.SUCCEEDED for op in runtime.catalog.journal.values())
        existing = runtime.catalog.get_record(namespace, key)
        runtime.catalog.put_record(
            runtime.lease, namespace, key, document, expected_revision=existing.revision if existing else None
        )

    runtime.put = put

    class Fabric:
        def get(self, path):
            assert SOURCE.workspace_id not in path and runtime.current_operation is not None
            runtime.fence()
            if "/lakehouses/" in path:
                return {
                    "id": TARGET.item_id,
                    "workspaceId": TARGET.workspace_id,
                    "type": "Lakehouse",
                    "properties": {"defaultSchema": "dbo"},
                }
            return {
                "id": TARGET.workspace_id,
                "capacityRegion": "West US",
                "capacityAssignmentProgress": "Completed",
            }

        def list_all(self, path):
            assert path == f"workspaces/{TARGET.workspace_id}/items/{TARGET.item_id}/shortcuts"
            runtime.fence()
            return []

    configuration = binding.ProtectionConfiguration(
        provider="lakehouse",
        descriptor=protection,
        max_age_seconds=86400,
        target_approval_ref="fresh-owned",
    )
    record = binding.configure_protection(
        runtime,
        binding.ConfigureProtectionRequest(source=SOURCE, configuration=configuration),
        protected_root=None,
    )
    captured, item = generation(record, item_type="Lakehouse")
    lake.target_dirs.add("Tables/dbo")
    recovery = binding.build_data_recovery(
        client=Fabric(),
        tokens=TOKENS,
        protected_root=None,
        scratch=tmp_path,
        limits=LIMITS,
    )
    ready, warnings = recovery.restore(captured, item, TARGET, runtime)
    assert not ready and warnings
    key = f"{captured.snapshot.generation_id}/{SOURCE.key}"
    prepared = runtime.catalog.get_record("data-prepared", key).document
    assert prepared["byte_copy_complete"] and prepared["data_ready"] is False
    assert prepared["endpoint_ready"] is False and prepared["configuration_digest"] == record.sha256
    assert runtime.catalog.journal[prepared["operation_id"]].state == OperationState.SUCCEEDED
    count = len(lake.writes)
    recovery.restore(captured, item, TARGET, runtime)
    assert len(lake.writes) == count
    assert (
        runtime.catalog.get_record("data-prepared", key).document["completed_at"] == prepared["completed_at"]
    )


def test_catalog_loss_interrupts_actual_one_lake_copy_without_further_mutation(setup, tmp_path):
    lake, protection = setup
    runtime = Runtime()
    lake.after_write = lambda: setattr(runtime.catalog, "available", False)

    def fence():
        runtime.fence()
        return False

    with pytest.raises(CatalogError):
        restore(protection, tmp_path, cancel=fence)
    assert len([request for request in lake.writes if request.method == "PATCH"]) == 1


@pytest.mark.parametrize("schemas", [("dbo",), ("dbo", "sales")])
def test_empty_captured_schema_scaffolding_is_preserved_without_put_or_delete(setup, tmp_path, schemas):
    lake, protection = setup
    extra = {f"Tables/{schema}" for schema in schemas}
    lake.target_dirs |= extra
    lake.source_dirs |= extra
    protection = protection.model_copy(
        update={
            "directories": tuple(sorted(set(protection.directories) | extra)),
        }
    )
    fabric = TargetFabric()
    receipt = restore(protection, tmp_path, target_client=fabric)
    assert receipt.byte_copy_complete and not receipt.data_ready and not receipt.endpoint_ready
    assert {name: bytes(content) for name, content in lake.target.items()} == CONTENT
    assert lake.target_dirs == set(protection.directories)
    assert not any(
        request.url.path.removeprefix(f"/{TARGET.workspace_id}/{TARGET.item_id}/") in extra
        for request in lake.writes
    ), "Do not PUT, delete or overwrite existing immutable schema directories."
    assert fabric.calls and all(SOURCE.workspace_id not in path for path in fabric.calls)


@pytest.mark.parametrize(
    "directory",
    [
        "Tables/unrelated",
        "Tables/dbo/Events",
        "Files/empty",
        "Tables/dbo/Events/_delta_log",
    ],
)
def test_only_empty_captured_top_level_schema_scaffolds_may_preexist(setup, tmp_path, directory):
    lake, protection = setup
    lake.target_dirs.add("Tables/dbo")
    lake.target_dirs.add(directory)
    with pytest.raises(ProtectionError):
        restore(protection, tmp_path)
    assert not lake.writes


def test_schema_scaffold_requires_live_schema_mode(setup, tmp_path):
    lake, protection = setup
    lake.target_dirs.add("Tables/dbo")
    fabric = TargetFabric()
    fabric.default_schema = None
    with pytest.raises(ProtectionError, match="schema-enabled"):
        restore(protection, tmp_path, target_client=fabric)
    assert not lake.writes


def test_empty_schema_shortcuts_are_not_accepted_as_local_scaffolding(setup, tmp_path):
    lake, protection = setup
    lake.target_dirs.add("Tables/dbo")
    fabric = TargetFabric()
    fabric.shortcuts = [{"path": "Tables", "name": "dbo", "target": {"oneLake": {}}}]
    with pytest.raises(ProtectionError, match="shortcuts"):
        restore(protection, tmp_path, target_client=fabric)
    assert not lake.writes


def test_failed_shortcut_inventory_is_not_empty_proof(setup, tmp_path):
    from fabshuffle.fabric.client import FabricApiError

    lake, protection = setup
    fabric = TargetFabric()
    fabric.shortcut_error = FabricApiError("GET", "target", 404, '{"errorCode":"ItemNotFound"}')
    with pytest.raises(FabricApiError, match="ItemNotFound"):
        restore(protection, tmp_path, target_client=fabric)
    assert not lake.writes


@pytest.mark.parametrize("drift", ["etag", "file", "directory", "shortcut", "mode"])
def test_scaffold_drift_during_source_preflight_stops_before_copy(setup, tmp_path, drift):
    lake, protection = setup
    lake.target_dirs.add("Tables/dbo")
    fabric = TargetFabric()
    count = 0

    def change():
        nonlocal count
        count += 1
        if count != 2:
            return
        if drift == "etag":
            lake.directory_versions["Tables/dbo"] = 1
        elif drift == "file":
            lake.target["Files/input.txt"] = bytearray(b"new")
        elif drift == "directory":
            lake.target_dirs.add("Tables/dbo/Events")
        elif drift == "shortcut":
            fabric.shortcuts = [{"name": "dbo"}]
        else:
            fabric.default_schema = None

    fabric.hook = change
    with pytest.raises(ProtectionError):
        restore(protection, tmp_path, target_client=fabric)
    assert not lake.writes


def test_scaffold_is_checked_again_immediately_before_first_mutation(setup, tmp_path):
    lake, protection = setup
    lake.target_dirs.add("Tables/dbo")
    fabric = TargetFabric()
    count = 0

    def change():
        nonlocal count
        count += 1
        if count == 3:
            lake.directory_versions["Tables/dbo"] = 1

    fabric.hook = change
    with pytest.raises(ProtectionError, match="drifted"):
        restore(protection, tmp_path, target_client=fabric)
    assert not lake.writes
