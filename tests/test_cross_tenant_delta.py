"""Real Arrow checkpoints and mocked OneLake requests exercise the complete preflight/upload boundary."""

import json
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fabshuffle.run import CancelledError
from fabshuffle.transfer import delta, files
from fabshuffle.transfer.common import StagingBudgetError

ROOT = "lake/Tables"
TABLE = ROOT + "/dbo/Events"
LOG = TABLE + "/_delta_log/"
COMMIT = LOG + "00000000000000000000.json"
CHECKPOINT = LOG + "00000000000000000000.checkpoint.parquet"
PROTOCOL = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
METADATA = {
    "metaData": {
        "id": "table-id",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": '{"type":"struct","fields":[]}',
        "partitionColumns": [],
        "configuration": {},
    }
}


def lines(*actions):
    return ("\n".join(json.dumps(action) for action in actions) + "\n").encode()


def parquet(*actions, **options):
    # from_pylist otherwise infers only the first row's fields.
    keys = set().union(*(action.keys() for action in actions))
    rows = [{key: action.get(key) for key in keys} for action in actions]
    table = pa.Table.from_pylist(rows)
    if "metaData" in keys:
        map_type = pa.map_(pa.string(), pa.string())
        metadata_type = pa.struct(
            [
                ("id", pa.string()),
                ("format", pa.struct([("provider", pa.string()), ("options", map_type)])),
                ("schemaString", pa.string()),
                ("partitionColumns", pa.list_(pa.string())),
                ("configuration", map_type),
            ]
        )
        table = table.set_column(
            table.schema.get_field_index("metaData"),
            "metaData",
            pa.array([row.get("metaData") for row in rows], type=metadata_type),
        )
    output = pa.BufferOutputStream()
    pq.write_table(table, output, **options)
    return output.getvalue().to_pybytes()


class Lake:
    def __init__(self, contents):
        self.contents = contents
        self.versions = {}
        self.uploads = {}
        self.requests = []
        self.root_lists = 0
        self.hook = None
        self.reject_metadata = False

    def etag(self, name):
        return f'"v{self.versions.get(name, 0)}"'

    def handle(self, request):
        self.requests.append(request)
        if self.hook:
            self.hook(request)
        source = request.url.path.startswith("/source")
        assert request.headers["Authorization"] == ("Bearer source" if source else "Bearer target")
        if source:
            assert request.method in {"GET", "HEAD"}
            if request.url.path == "/source":
                directory = request.url.params["directory"]
                if directory == ROOT:
                    self.root_lists += 1
                entries = {}
                for name, content in self.contents.items():
                    if not name.startswith(directory + "/"):
                        continue
                    local = name[len(directory) + 1 :]
                    first = local.split("/")[0]
                    child = directory + "/" + first
                    entries[child] = (
                        {"name": child, "isDirectory": True}
                        if "/" in local
                        else {"name": name, "etag": self.etag(name), "contentLength": len(content)}
                    )
                return httpx.Response(200, json={"paths": [entries[name] for name in sorted(entries)]})
            name = request.url.path.removeprefix("/source/")
            content = self.contents[name]
            if request.method == "HEAD":
                return httpx.Response(
                    200, headers={"Content-Length": str(len(content)), "ETag": self.etag(name)}
                )
            if self.reject_metadata and "_delta_log/" in name:
                return httpx.Response(
                    403, json={"error": {"code": "MetadataReadDenied", "message": "No read role"}}
                )
            if request.headers["If-Match"] != self.etag(name):
                return httpx.Response(
                    412, json={"error": {"code": "ConditionNotMet", "message": "ETag changed"}}
                )
            begin, end = map(int, request.headers["Range"].removeprefix("bytes=").split("-"))
            return httpx.Response(
                206,
                content=content[begin : end + 1],
                headers={
                    "Content-Range": f"bytes {begin}-{end}/{len(content)}",
                },
            )
        name = request.url.path.removeprefix("/target/")
        if request.method == "PUT":
            if request.url.params["resource"] == "file":
                self.uploads[name] = bytearray()
            return httpx.Response(201)
        if request.method == "PATCH":
            if request.url.params["action"] == "append":
                assert int(request.url.params["position"]) == len(self.uploads[name])
                self.uploads[name].extend(request.content)
            else:
                assert int(request.url.params["position"]) == len(self.uploads[name])
            return httpx.Response(200)
        assert request.method == "HEAD"
        return httpx.Response(200, headers={"Content-Length": str(len(self.uploads[name]))})


def install(monkeypatch, contents):
    lake = Lake(contents)
    client = httpx.Client
    monkeypatch.setattr(
        files.httpx, "Client", lambda **kw: client(transport=httpx.MockTransport(lake.handle), **kw)
    )
    return lake


def copy(tmp_path, **kwargs):
    return files.copy_tree_streaming(
        source_path="https://onelake.dfs.fabric.microsoft.com/source/" + ROOT,
        target_path="https://onelake.dfs.fabric.microsoft.com/target/" + ROOT,
        tokens=SimpleNamespace(storage_token=lambda: "source"),
        target_tokens=SimpleNamespace(storage_token=lambda: "target"),
        kind="lakehouse",
        scratch_dir=tmp_path,
        **kwargs,
    )


def no_uploads(lake, tmp_path):
    assert not any(request.url.path.startswith("/target") for request in lake.requests)
    assert not list(tmp_path.glob("delta-preflight-*"))


@pytest.mark.parametrize("checkpoint", [False, True])
def test_valid_relative_delta_is_preflighted_before_byte_identical_upload(monkeypatch, tmp_path, checkpoint):
    actions = [PROTOCOL, METADATA, {"add": {"path": "part.parquet"}}, {"remove": {"path": "old.parquet"}}]
    contents = {TABLE + "/part.parquet": b"data", COMMIT: lines(*actions)}
    if checkpoint:
        contents[CHECKPOINT] = parquet(*actions)
        contents[LOG + "_last_checkpoint"] = json.dumps({"version": 0, "size": 4}).encode()
    lake = install(monkeypatch, contents)
    completed = []
    outcome = copy(tmp_path, max_staging_bytes=256 * 1024, on_complete=completed.append)
    assert {name: bytes(content) for name, content in lake.uploads.items()} == contents
    assert completed == [outcome] and not outcome.empty
    first_write = next(
        index for index, request in enumerate(lake.requests) if request.url.path.startswith("/target")
    )
    reads = [request for request in lake.requests[:first_write] if request.headers.get("Range")]
    assert any(request.url.path.endswith(".json") for request in reads)
    if checkpoint:
        assert any(request.url.path.endswith(".parquet") for request in reads)
    assert not list(tmp_path.glob("delta-preflight-*"))


@pytest.mark.parametrize(
    "path",
    [
        "abfss://source@onelake.dfs.fabric.microsoft.com/item/data.parquet",
        "https://source/path",
        "file:///C:/data",
        "/rooted/data",
        "../other/data",
        "partition/../../data",
        "%2e%2e/data",
        "%252e%252e/data",
        "//server/share",
        r"..\data",
        "C:/data",
        "https%3A%2F%2Fsource/file",
    ],
)
@pytest.mark.parametrize("action", ["add", "remove", "cdc"])
def test_unsafe_json_file_paths_never_upload(monkeypatch, tmp_path, path, action):
    lake = install(monkeypatch, {COMMIT: lines(PROTOCOL, METADATA, {action: {"path": path}})})
    with pytest.raises(files.DeltaValidationRequired, match="absolute/external or parent-escape"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


@pytest.mark.parametrize("action", ["add", "remove"])
def test_unsafe_parquet_checkpoint_paths_never_upload(monkeypatch, tmp_path, action):
    lake = install(monkeypatch, {CHECKPOINT: parquet(PROTOCOL, METADATA, {action: {"path": "../outside"}})})
    with pytest.raises(files.DeltaValidationRequired, match="parent-escape"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_v2_json_checkpoint_and_parquet_sidecar_are_both_inspected(monkeypatch, tmp_path):
    main = LOG + "00000000000000000000.checkpoint.abc.json"
    sidecar = LOG + "_sidecars/part.parquet"
    source = {
        main: lines(
            PROTOCOL,
            METADATA,
            {"checkpointMetadata": {"version": 0}},
            {"sidecar": {"path": "part.parquet", "sizeInBytes": 1}},
        ),
        sidecar: parquet({"add": {"path": "part.parquet"}}),
        TABLE + "/part.parquet": b"data",
        LOG + "_last_checkpoint": json.dumps(
            {
                "version": 0,
                "size": 4,
                "v2Checkpoint": {
                    "path": main.rsplit("/", 1)[1],
                    "nonFileActions": [PROTOCOL, METADATA],
                    "sidecarFiles": [{"path": "_sidecars/part.parquet"}],
                },
            }
        ).encode(),
    }
    lake = install(monkeypatch, source)
    copy(tmp_path)
    assert {name: bytes(content) for name, content in lake.uploads.items()} == source


@pytest.mark.parametrize("location", ["sidecar", "embedded-sidecar", "v2-pointer", "sidecar-content"])
def test_unsafe_v2_references_are_not_ignored(monkeypatch, tmp_path, location):
    main = LOG + "00000000000000000000.checkpoint.abc.json"
    sidecar = LOG + "_sidecars/part.parquet"
    last = {"version": 0, "size": 3, "v2Checkpoint": {"path": main.rsplit("/", 1)[1]}}
    source = {
        main: lines(
            PROTOCOL,
            METADATA,
            {"sidecar": {"path": "../part.parquet" if location == "sidecar" else "part.parquet"}},
        ),
        sidecar: parquet(
            {"add": {"path": "s3://old/data" if location == "sidecar-content" else "data.parquet"}}
        ),
    }
    if location == "v2-pointer":
        last["v2Checkpoint"]["path"] = "https://source/old.checkpoint.parquet"
    if location == "embedded-sidecar":
        last["v2Checkpoint"]["sidecarFiles"] = [{"path": "../old.parquet"}]
    source[LOG + "_last_checkpoint"] = json.dumps(last).encode()
    lake = install(monkeypatch, source)
    with pytest.raises(files.DeltaValidationRequired):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


@pytest.mark.parametrize(
    "storage,path",
    [
        ("p", "s3://source/vector.bin"),
        ("x", "unknown"),
        ("u", "../" + "0" * 20),
        ("u", "//host/" + "0" * 20),
    ],
)
@pytest.mark.parametrize("checkpoint", [False, True])
def test_deletion_vector_external_and_escape_paths_are_refused(
    monkeypatch, tmp_path, storage, path, checkpoint
):
    action = {
        "add": {"path": "data.parquet", "deletionVector": {"storageType": storage, "pathOrInlineDv": path}}
    }
    content = parquet(PROTOCOL, METADATA, action) if checkpoint else lines(PROTOCOL, METADATA, action)
    lake = install(monkeypatch, {CHECKPOINT if checkpoint else COMMIT: content})
    with pytest.raises(files.DeltaValidationRequired):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_inline_and_z85_uuid_vectors_are_not_treated_as_uris(monkeypatch, tmp_path):
    source = {
        COMMIT: lines(
            PROTOCOL,
            METADATA,
            {
                "add": {
                    "path": "one.parquet",
                    "deletionVector": {
                        "storageType": "u",
                        "pathOrInlineDv": "ab^-aqEH.-t@S}K{vb[*k^",
                    },
                }
            },
            {
                "remove": {
                    "path": "old.parquet",
                    "deletionVector": {
                        "storageType": "i",
                        "pathOrInlineDv": "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
                    },
                }
            },
        )
    }
    lake = install(monkeypatch, source)
    copy(tmp_path)
    assert lake.uploads


@pytest.mark.parametrize(
    "action",
    [
        {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["catalogManaged"]}},
        {"futureAction": {"path": "relative"}},
        {"add": {"path": "part.parquet", "remoteLocation": "unknown"}},
        {"domainMetadata": {"domain": "unknown.plugin", "configuration": '{"path":"external"}'}},
    ],
)
def test_unknown_path_features_fail_closed(monkeypatch, tmp_path, action):
    lake = install(monkeypatch, {COMMIT: lines(PROTOCOL, METADATA, action)})
    with pytest.raises(files.DeltaValidationRequired):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_checksum_embedded_add_paths_are_inspected(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA),
            LOG + "00000000000000000000.crc": json.dumps(
                {
                    "metadata": METADATA["metaData"],
                    "protocol": PROTOCOL["protocol"],
                    "allFiles": [{"path": "abfss://old/file"}],
                }
            ).encode(),
        },
    )
    with pytest.raises(files.DeltaValidationRequired, match="absolute/external"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_all_tables_are_validated_before_first_destination_write(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA),
            COMMIT.replace("Events", "LaterTable"): lines(
                PROTOCOL, METADATA, {"add": {"path": "../external"}}
            ),
        },
    )
    with pytest.raises(files.DeltaValidationRequired):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_oversized_checkpoint_never_exceeds_disk_budget(monkeypatch, tmp_path):
    content = parquet(PROTOCOL, METADATA) + b"x" * 65536
    lake = install(monkeypatch, {CHECKPOINT: content})
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("keep")
    with pytest.raises(StagingBudgetError, match="staging bytes"):
        copy(tmp_path, max_memory_bytes=256 * 1024, max_disk_staging_bytes=32768)
    no_uploads(lake, tmp_path)
    assert sentinel.read_text() == "keep"
    assert not any(request.headers.get("Range") for request in lake.requests)


def test_checkpoint_disk_budget_can_exceed_arrow_memory_budget(monkeypatch, tmp_path):
    content = parquet(
        PROTOCOL,
        METADATA,
        {"add": {"path": "part.parquet", "stats": "".join(chr(33 + i % 80) for i in range(50000))}},
        compression="NONE",
    )
    assert len(content) > 8192
    source = {CHECKPOINT: content, TABLE + "/part.parquet": b"data"}
    lake = install(monkeypatch, source)

    copy(tmp_path, max_memory_bytes=8192, max_disk_staging_bytes=len(content) + 1024)

    assert {name: bytes(value) for name, value in lake.uploads.items()} == source
    assert not list(tmp_path.glob("delta-preflight-*"))


def test_cancel_during_checkpoint_decode_always_cleans_staging(monkeypatch, tmp_path):
    lake = install(monkeypatch, {CHECKPOINT: parquet(PROTOCOL, METADATA)})
    original = delta._parquet
    cancelled = False

    def cancel(path, *args):
        nonlocal cancelled
        assert path.stat().st_size <= 65536
        cancelled = True
        return original(path, *args)

    monkeypatch.setattr(delta, "_parquet", cancel)
    with pytest.raises(CancelledError):
        copy(tmp_path, max_staging_bytes=65536, cancel_requested=lambda: cancelled)
    no_uploads(lake, tmp_path)


def test_metadata_etags_are_pinned_between_preflight_and_upload(monkeypatch, tmp_path):
    lake = install(monkeypatch, {COMMIT: lines(PROTOCOL, METADATA)})

    def mutate(request):
        if request.url.path == "/source" and request.url.params["directory"] == ROOT and lake.root_lists == 2:
            lake.contents[COMMIT] = lines(PROTOCOL, METADATA, {"add": {"path": "https://old/file"}})
            lake.versions[COMMIT] = 1

    lake.hook = mutate
    completed = []
    with pytest.raises(files.DeltaValidationRequired, match="changed after preflight"):
        copy(tmp_path, on_complete=completed.append)
    assert COMMIT not in lake.uploads and not completed
    assert not list(tmp_path.glob("delta-preflight-*"))


def test_native_read_error_is_preserved_and_no_checkpoint_is_written(monkeypatch, tmp_path):
    lake = install(monkeypatch, {COMMIT: lines(PROTOCOL, METADATA)})
    lake.reject_metadata = True
    with pytest.raises(files.FileTransferError, match="MetadataReadDenied: No read role"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_referenced_sidecar_must_exist_and_be_inspected(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            LOG + "00000000000000000000.checkpoint.abc.json": lines(
                PROTOCOL, METADATA, {"sidecar": {"path": "missing.parquet"}}
            ),
        },
    )
    with pytest.raises(files.DeltaValidationRequired, match="was not inspected"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_v2_parquet_checkpoint_and_multipart_classic_are_supported(monkeypatch, tmp_path):
    first = LOG + "00000000000000000000.checkpoint.0000000001.0000000002.parquet"
    second = LOG + "00000000000000000000.checkpoint.0000000002.0000000002.parquet"
    v2 = LOG + "00000000000000000001.checkpoint.abc.parquet"
    source = {
        first: parquet(PROTOCOL, METADATA),
        second: parquet({"add": {"path": "part.parquet"}}),
        v2: parquet(PROTOCOL, METADATA, {"sidecar": {"path": "@sidecar.parquet"}}),
        LOG + "_sidecars/@sidecar.parquet": parquet({"remove": {"path": "old.parquet"}}),
        LOG + "_last_checkpoint": json.dumps({"version": 0, "parts": 2, "size": 3}).encode(),
    }
    lake = install(monkeypatch, source)
    copy(tmp_path)
    assert {name: bytes(content) for name, content in lake.uploads.items()} == source


def test_many_checkpoints_are_staged_one_at_a_time_not_as_a_whole_table(monkeypatch, tmp_path):
    source = {LOG + f"{version:020}.checkpoint.parquet": parquet(PROTOCOL, METADATA) for version in range(20)}
    assert sum(map(len, source.values())) > 16384
    original = delta._parquet
    sizes = []

    def observe(path, *args):
        staged = list(tmp_path.rglob("checkpoint.parquet"))
        assert staged == [path]
        sizes.append(path.stat().st_size)
        return original(path, *args)

    monkeypatch.setattr(delta, "_parquet", observe)
    lake = install(monkeypatch, source)
    copy(tmp_path, max_staging_bytes=16384)
    assert len(sizes) == 20 and max(sizes) <= 16384
    assert len(lake.uploads) == 20
    assert not list(tmp_path.glob("delta-preflight-*"))


def test_checkpoint_decode_uses_bounded_arrow_batches(tmp_path, monkeypatch):
    path = tmp_path / "input.parquet"
    path.write_bytes(
        parquet(PROTOCOL, METADATA, *({"add": {"path": f"part-{i}.parquet"}} for i in range(600)))
    )
    original = pq.ParquetFile.iter_batches
    sizes = []

    def tracked(self, *args, **kwargs):
        assert kwargs["batch_size"] <= 128 and kwargs["use_threads"] is False
        for batch in original(self, *args, **kwargs):
            sizes.append(batch.num_rows)
            yield batch

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", tracked)
    snapshot = delta.Snapshot(budget=1024 * 1024)
    snapshot.tables[TABLE] = set()
    delta._parquet(
        path,
        delta.Inspector(snapshot, TABLE, str(path), lambda name: False),
        1024 * 1024,
        None,
    )
    assert sum(sizes) == 602 and max(sizes) <= 128 and len(sizes) > 1


def test_corrupt_checkpoint_is_removed_after_parser_failure(monkeypatch, tmp_path):
    lake = install(monkeypatch, {CHECKPOINT: b"not a parquet checkpoint"})
    with pytest.raises(files.DeltaValidationRequired, match="malformed or unsupported metadata"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_missing_multipart_fragment_prevents_upload(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            LOG + "00000000000000000000.checkpoint.0000000001.0000000002.parquet": parquet(
                PROTOCOL, METADATA
            ),
            LOG + "_last_checkpoint": json.dumps({"version": 0, "parts": 2, "size": 2}).encode(),
        },
    )
    with pytest.raises(files.DeltaValidationRequired, match="was not inspected"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_relative_reference_into_excluded_shortcut_is_rejected(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA, {"add": {"path": "shortcut/part.parquet"}}),
            TABLE + "/shortcut/part.parquet": b"external shortcut contents",
        },
    )
    with pytest.raises(files.DeltaValidationRequired, match="excluded shortcut"):
        copy(tmp_path, exclude_paths=["dbo/Events/shortcut"])
    no_uploads(lake, tmp_path)


def test_shortcut_tables_are_excluded_before_reading_their_metadata(monkeypatch, tmp_path):
    external = ROOT + "/dbo/External/_delta_log/00000000000000000000.json"
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA),
            external: lines(PROTOCOL, METADATA, {"add": {"path": "https://external/file"}}),
        },
    )
    copy(tmp_path, exclude_paths=["Tables/dbo/External"])
    assert external not in lake.uploads
    assert not any(request.url.path.endswith(external) for request in lake.requests)


def test_duplicate_json_keys_cannot_hide_an_unsafe_reference(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA) + b'{"add":{"path":"https://source/file","path":"relative"}}\n',
        },
    )
    with pytest.raises(files.DeltaValidationRequired, match="duplicate JSON key"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


def test_null_optional_last_checkpoint_fields_are_not_unknown_features(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            CHECKPOINT: parquet(PROTOCOL, METADATA),
            LOG + "_last_checkpoint": json.dumps(
                {
                    "version": 0,
                    "size": 2,
                    "parts": None,
                    "checkpointType": None,
                    "amtCheckpoint": None,
                }
            ).encode(),
        },
    )
    copy(tmp_path)
    assert lake.uploads


def test_json_record_budget_is_enforced_without_staging_the_log(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA, {"add": {"path": "part.parquet", "stats": "x" * 20000}}),
        },
    )
    with pytest.raises(StagingBudgetError, match="Delta JSON record"):
        copy(tmp_path, max_memory_bytes=8192, max_disk_staging_bytes=10 * 1024 * 1024)
    no_uploads(lake, tmp_path)


def test_checkpoint_decompression_is_bounded_before_reading_batches(monkeypatch, tmp_path):
    content = parquet(PROTOCOL, METADATA, {"add": {"path": "p" * 30000 + ".parquet"}})
    assert len(content) < 8192
    lake = install(monkeypatch, {CHECKPOINT: content})
    with pytest.raises(StagingBudgetError, match="Projected Delta checkpoint row group"):
        copy(tmp_path, max_memory_bytes=8192, max_disk_staging_bytes=10 * 1024 * 1024)
    no_uploads(lake, tmp_path)


def test_catalog_coordinated_commit_directories_require_manual_migration(monkeypatch, tmp_path):
    lake = install(
        monkeypatch,
        {
            COMMIT: lines(PROTOCOL, METADATA),
            LOG + "_commits/00000000000000000001.uuid.json": lines({"add": {"path": "part.parquet"}}),
        },
    )
    with pytest.raises(files.DeltaValidationRequired, match="unsupported metadata directory"):
        copy(tmp_path)
    no_uploads(lake, tmp_path)


@pytest.mark.parametrize(
    "path", ["p=space%20value/part.parquet", "p=hash%23value/part.parquet", "p=100%25/part.parquet"]
)
def test_uri_encoded_literal_partition_values_remain_valid(path):
    assert delta.relative_path(path, "test").endswith("/part.parquet")


def test_invalid_uri_percent_escapes_are_rejected():
    with pytest.raises(files.DeltaValidationRequired, match="percent encoding"):
        delta.relative_path("bad%GG/path", "test")


# A Files/ root is not restricted to managed tables the way Tables/ is: a notebook can write
# an unmanaged Delta table anywhere under Files/ (for example ``df.write.format("delta")
# .save("Files/unmanaged/events")``), and its commit log is exactly as capable of an
# escaping/unsafe storage reference as a managed table's. Files/ copies must still discover
# and preflight it, while leaving ordinary, non-Delta content free to copy unvalidated.
FILES_ROOT = "lake/Files"
UNMANAGED_TABLE = FILES_ROOT + "/unmanaged/events"
UNMANAGED_LOG = UNMANAGED_TABLE + "/_delta_log/"
UNMANAGED_COMMIT = UNMANAGED_LOG + "00000000000000000000.json"


def copy_files_root(tmp_path, **kwargs):
    return files.copy_tree_streaming(
        source_path="https://onelake.dfs.fabric.microsoft.com/source/" + FILES_ROOT,
        target_path="https://onelake.dfs.fabric.microsoft.com/target/" + FILES_ROOT,
        tokens=SimpleNamespace(storage_token=lambda: "source"),
        target_tokens=SimpleNamespace(storage_token=lambda: "target"),
        scratch_dir=tmp_path,
        **kwargs,
    )


def test_unmanaged_delta_table_under_files_root_still_rejects_escaping_references(monkeypatch, tmp_path):
    lake = install(
        monkeypatch, {UNMANAGED_COMMIT: lines(PROTOCOL, METADATA, {"add": {"path": "/rooted/data"}})}
    )
    with pytest.raises(files.DeltaValidationRequired, match="absolute/external or parent-escape"):
        copy_files_root(tmp_path)
    no_uploads(lake, tmp_path)


def test_ordinary_files_copy_alongside_a_validated_unmanaged_delta_table(monkeypatch, tmp_path):
    actions = [PROTOCOL, METADATA, {"add": {"path": "part.parquet"}}]
    contents = {
        FILES_ROOT + "/reports/summary.csv": b"a,b,c\n1,2,3\n",
        UNMANAGED_TABLE + "/part.parquet": b"data",
        UNMANAGED_COMMIT: lines(*actions),
    }
    lake = install(monkeypatch, contents)
    outcome = copy_files_root(tmp_path, max_staging_bytes=256 * 1024)
    assert not outcome.empty
    assert {name: bytes(content) for name, content in lake.uploads.items()} == contents
    assert not list(tmp_path.glob("delta-preflight-*"))


def test_ordinary_files_root_with_no_delta_tables_still_copies_unvalidated(monkeypatch, tmp_path):
    contents = {FILES_ROOT + "/notes/readme.txt": b"nothing Delta about this at all"}
    lake = install(monkeypatch, contents)
    outcome = copy_files_root(tmp_path)
    assert not outcome.empty
    assert {name: bytes(content) for name, content in lake.uploads.items()} == contents
