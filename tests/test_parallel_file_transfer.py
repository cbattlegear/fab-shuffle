"""Files/ is relayed through RAM by a pool of workers, with no AzCopy and no local staging."""

from __future__ import annotations

import threading

import httpx
import pytest

from fabshuffle.transfer import files
from fabshuffle.transfer.files import FileTransferError


class Tokens:
    def __init__(self, value):
        self.value = value

    def storage_token(self):
        return self.value


class Tree:
    """A source Files/ root with nested directories and a shortcut, and a recording target."""

    def __init__(self, count=12):
        self.payloads = {f"dir{i % 3}/file{i}.bin": bytes([i]) * (10 + i) for i in range(count)}
        self.payloads["top.bin"] = b"top"
        self.shortcut_payload = b"must-not-copy"
        self.target: dict[str, bytearray] = {}
        self.directories: list[str] = []
        self.chunk_sizes: list[int] = []
        self.fail_on: str | None = None
        self.throttle_first = 0
        self.active = 0
        self.peak = 0
        self.lock = threading.Lock()

    def listing(self, directory):
        base = "lake/Files"
        prefix = f"{base}/{directory}/" if directory else f"{base}/"
        entries, seen = [], set()
        for name, data in self.payloads.items():
            if not f"{base}/{name}".startswith(prefix):
                continue
            rest = f"{base}/{name}"[len(prefix):]
            head = rest.split("/", 1)[0]
            if "/" in rest:
                if head not in seen:
                    seen.add(head)
                    entries.append({"name": prefix + head, "isDirectory": True})
            else:
                entries.append({"name": prefix + head, "etag": '"e"', "contentLength": len(data)})
        if not directory:
            entries.append({"name": f"{base}/shortcut", "isDirectory": True})
        return entries

    def handle(self, request):
        path = request.url.path
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            if path.startswith("/source"):
                return self.source(request)
            return self.destination(request)
        finally:
            with self.lock:
                self.active -= 1

    def source(self, request):
        if path_is_listing := (request.url.path == "/source"):
            directory = request.url.params.get("directory", "")
            directory = directory.removeprefix("lake/Files").strip("/")
            assert path_is_listing
            return httpx.Response(200, json={"paths": self.listing(directory)})
        name = request.url.path.removeprefix("/source/lake/Files/")
        data = self.payloads[name]
        if request.method == "HEAD":
            return httpx.Response(200, headers={"Content-Length": str(len(data)), "ETag": '"e"'})
        begin, end = map(int, request.headers["Range"].removeprefix("bytes=").split("-"))
        return httpx.Response(206, content=data[begin:end + 1], headers={
            "Content-Range": f"bytes {begin}-{end}/{len(data)}",
        })

    def destination(self, request):
        name = request.url.path.removeprefix("/target/lake/Files").strip("/")
        if request.method == "PUT" and request.url.params["resource"] == "directory":
            with self.lock:
                self.directories.append(name)
            return httpx.Response(201)
        if request.method == "PUT":
            with self.lock:
                if self.throttle_first:
                    self.throttle_first -= 1
                    return httpx.Response(429, headers={"Retry-After": "0"}, json={
                        "error": {"code": "ServerBusy", "message": "slow down"},
                    })
                if name == self.fail_on:
                    return httpx.Response(403, json={
                        "error": {"code": "AuthorizationPermissionMismatch", "message": "Write denied"},
                    })
                self.target[name] = bytearray()
            return httpx.Response(201)
        if request.method == "PATCH":
            if request.url.params["action"] == "append":
                with self.lock:
                    self.chunk_sizes.append(len(request.content))
                    self.target[name].extend(request.content)
            return httpx.Response(200)
        return httpx.Response(200, headers={"Content-Length": str(len(self.target[name]))})


_REAL_CLIENT = httpx.Client


def run(monkeypatch, tree, **kwargs):
    client = _REAL_CLIENT
    monkeypatch.setattr(
        files.httpx, "Client",
        lambda **kw: client(transport=httpx.MockTransport(tree.handle), **kw),
    )
    return files.copy_tree_streaming(
        source_path="https://onelake.dfs.fabric.microsoft.com/source/lake/Files",
        target_path="https://onelake.dfs.fabric.microsoft.com/target/lake/Files",
        tokens=Tokens("s"), target_tokens=Tokens("t"),
        exclude_paths=["Files/shortcut"], **kwargs,
    )


@pytest.mark.parametrize("workers", [1, 4])
def test_every_file_arrives_byte_for_byte_and_the_shortcut_is_left_alone(monkeypatch, workers):
    tree = Tree()
    outcome = run(monkeypatch, tree, workers=workers)
    assert not outcome.empty
    assert {name: bytes(data) for name, data in tree.target.items()} == tree.payloads
    assert "shortcut" not in tree.directories


def test_parallel_and_serial_copies_produce_identical_trees(monkeypatch):
    serial, parallel = Tree(), Tree()
    run(monkeypatch, serial, workers=1)
    run(monkeypatch, parallel, workers=4)
    assert serial.target == parallel.target


def test_memory_budget_is_divided_between_workers_not_multiplied(monkeypatch):
    tree = Tree()
    run(monkeypatch, tree, workers=4, max_memory_bytes=64)
    assert max(tree.chunk_sizes) <= 64 // (2 * 4)


def test_a_failed_file_reports_the_service_error_and_stops_the_rest(monkeypatch):
    tree = Tree(count=30)
    tree.fail_on = "dir1/file1.bin"
    with pytest.raises(FileTransferError, match="AuthorizationPermissionMismatch: Write denied"):
        run(monkeypatch, tree, workers=3)
    assert len(tree.target) < len(tree.payloads)


def test_throttling_is_retried_using_the_service_retry_after(monkeypatch):
    tree = Tree(count=2)
    tree.throttle_first = 2
    run(monkeypatch, tree, workers=1)
    assert {name: bytes(data) for name, data in tree.target.items()} == tree.payloads


def test_cancellation_stops_without_completing(monkeypatch):
    from fabshuffle.run import CancelledError

    tree = Tree()
    with pytest.raises(CancelledError):
        run(monkeypatch, tree, workers=2, cancel_requested=lambda: True)
    assert not tree.target


def test_no_azcopy_remains_in_the_transfer_module():
    assert not hasattr(files, "_azcopy") and not hasattr(files, "copy_files")
