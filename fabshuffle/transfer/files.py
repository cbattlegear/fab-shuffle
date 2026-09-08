"""OneLake Files transfer with separate source/destination identities.

The legacy path uses AzCopy with local staging; paired transfers relay bounded byte ranges.
Managed Tables undergo bounded Delta log/checkpoint reference validation before any uploads.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from collections.abc import Callable, Collection, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import httpx

from fabshuffle.auth import ServicePrincipal, TokenProvider
from fabshuffle.config import SETTINGS
from fabshuffle.lifecycle import CopyOutcome
from fabshuffle.transfer.common import (
    DEFAULT_MAX_STAGING_BYTES,
    check_budget,
    check_cancelled,
)

logger = logging.getLogger(__name__)

TRUSTED_SUFFIXES = "onelake.dfs.fabric.microsoft.com"


class FileTransferError(RuntimeError):
    """The OneLake transfer could not be completed safely."""


class DeltaValidationRequired(FileTransferError):
    """A raw byte relay cannot prove Delta log/checkpoint references are self-contained."""


def _azcopy_env(principal: ServicePrincipal) -> dict[str, str]:
    import os

    return {
        **os.environ,
        "AZCOPY_AUTO_LOGIN_TYPE": "SPN",
        "AZCOPY_SPA_APPLICATION_ID": principal.client_id,
        "AZCOPY_SPA_CLIENT_SECRET": principal.client_secret,
        "AZCOPY_TENANT_ID": principal.tenant_id,
    }


def copy_files(
    *,
    source_files_path: str,
    target_files_path: str,
    principal: ServicePrincipal,
    scratch_dir: Path,
    target_principal: ServicePrincipal | None = None,
    target_tokens: TokenProvider | None = None,
    tokens: TokenProvider | None = None,
    max_staging_bytes: int = DEFAULT_MAX_STAGING_BYTES,
    cancel_requested: Callable[[], bool] | None = None,
    on_complete: Callable[[CopyOutcome], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> CopyOutcome:
    """Stage ``Files/`` from the source lakehouse locally, then upload to the target."""
    if target_principal is not None or target_tokens is not None:
        return copy_tree_streaming(
            source_path=source_files_path, target_path=target_files_path,
            tokens=tokens or TokenProvider(principal),
            target_tokens=target_tokens or TokenProvider(target_principal),
            max_staging_bytes=max_staging_bytes, cancel_requested=cancel_requested,
            on_progress=on_progress, on_complete=on_complete,
        )
    check_cancelled(cancel_requested)
    staging = scratch_dir / "files"
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    try:
        if on_progress:
            on_progress("Downloading OneLake files")
        _azcopy(["copy", f"{source_files_path.rstrip('/')}/*", str(staging), "--recursive"], principal)
        check_cancelled(cancel_requested)

        if not any(staging.iterdir()):
            if on_progress:
                on_progress("No files to transfer")
            outcome = CopyOutcome("files", empty=True)
            if on_complete:
                on_complete(outcome)
            return outcome

        if on_progress:
            on_progress("Uploading OneLake files")
        check_cancelled(cancel_requested)
        _azcopy(["copy", f"{staging}/*", target_files_path, "--recursive"], principal)
        check_cancelled(cancel_requested)
        outcome = CopyOutcome("files", empty=False)
        if on_complete:
            on_complete(outcome)
        return outcome
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _azcopy(args: list[str], principal: ServicePrincipal) -> None:
    command = [SETTINGS.azcopy_path, *args, f"--trusted-microsoft-suffixes={TRUSTED_SUFFIXES}"]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=False, env=_azcopy_env(principal)
        )
    except FileNotFoundError as error:
        raise FileTransferError(
            f"OneLake file transfer needs azcopy, but '{SETTINGS.azcopy_path}' is not installed "
            "in this image."
        ) from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-1500:]
        raise FileTransferError(f"azcopy {args[0]} failed with exit code {result.returncode}: {detail}")


def _tree_location(path: str) -> tuple[str, str]:
    parsed = urlsplit(path.rstrip("/"))
    if (
        parsed.scheme != "https" or parsed.username or parsed.password
        or parsed.query or parsed.fragment
        or not parsed.hostname or not parsed.hostname.endswith(".fabric.microsoft.com")
    ):
        raise FileTransferError("Use an HTTPS OneLake Files/ or Tables/ URL without credentials.")
    parts = unquote(parsed.path).strip("/").split("/")
    if len(parts) < 3 or parts[2] not in {"Files", "Tables"} or any(
        part in {"", ".", ".."} for part in parts
    ):
        raise FileTransferError("The transfer must stay inside a Fabric-created Files/ or Tables/ root.")
    return f"{parsed.scheme}://{parsed.netloc}/{quote(parts[0], safe='')}", "/".join(parts[1:])


def _headers(tokens: TokenProvider) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {tokens.storage_token()}",
        "x-ms-version": "2021-06-08",
        "Accept-Encoding": "identity",
    }


def _error(response: httpx.Response) -> FileTransferError:
    try:
        body = response.json().get("error", {})
        detail = f"{body.get('code', '')}: {body.get('message', '')}".strip(": ")
    except (ValueError, AttributeError):
        detail = response.text
    return FileTransferError(f"OneLake HTTP {response.status_code}: {detail}")


def _request(
    client: httpx.Client, method: str, url: str, tokens: TokenProvider, **kwargs: Any,
) -> httpx.Response:
    response = client.request(method, url, headers=_headers(tokens), **kwargs)
    if not response.is_success:
        raise _error(response)
    return response


def _listed_paths(
    client: httpx.Client, filesystem: str, directory: str, tokens: TokenProvider,
    cancel_requested: Callable[[], bool] | None,
) -> Iterator[dict[str, Any]]:
    continuation = ""
    while True:
        check_cancelled(cancel_requested)
        params = {
            "resource": "filesystem", "directory": directory,
            "recursive": "false", "maxResults": "100",
        }
        if continuation:
            params["continuation"] = continuation
        response = _request(client, "GET", filesystem, tokens, params=params)
        body = response.json()
        if not isinstance(body, dict) or not isinstance(body.get("paths"), list):
            raise FileTransferError(
                "OneLake returned an incomplete path listing; no empty-tree assumption is safe."
            )
        for entry in body["paths"]:
            name = str(entry.get("name", ""))
            relative = name.removeprefix(directory + "/")
            if relative == name or "/" in relative or relative in {"", ".", ".."}:
                raise FileTransferError(f"OneLake returned a path outside the requested directory: {name}")
            yield entry
        next_token = response.headers.get("x-ms-continuation", "")
        if not next_token:
            break
        if next_token == continuation:
            raise FileTransferError("OneLake returned a repeated continuation token; listing is incomplete.")
        continuation = next_token


def _file_properties(
    client: httpx.Client, source: str, entry: dict[str, Any], tokens: TokenProvider,
) -> tuple[int, str]:
    properties = _request(client, "HEAD", source, tokens)
    size = int(properties.headers["Content-Length"])
    etag = properties.headers.get("ETag")
    if not etag:
        raise FileTransferError(
            f"OneLake did not return an ETag for {entry['name']}; retry after freezing writes."
        )
    listed_etag = entry.get("etag")
    if listed_etag and str(listed_etag).strip('"') != etag.strip('"'):
        raise FileTransferError(
            f"{entry['name']} changed during enumeration; freeze source writes and retry."
        )
    if size < 0 or (entry.get("_validated_size") is not None and size != entry["_validated_size"]):
        raise FileTransferError(f"OneLake length changed for {entry['name']}; keep source writes frozen.")
    return size, etag


def _read_chunks(
    client: httpx.Client, source: str, tokens: TokenProvider, size: int, etag: str,
    chunk_size: int, cancel_requested: Callable[[], bool] | None,
) -> Iterator[bytes]:
    offset = 0
    while offset < size:
        check_cancelled(cancel_requested)
        end = min(offset + chunk_size, size) - 1
        headers = {**_headers(tokens), "Range": f"bytes={offset}-{end}", "If-Match": etag}
        with client.stream("GET", source, headers=headers) as response:
            if not response.is_success:
                response.read()
                raise _error(response)
            expected_range = f"bytes {offset}-{end}/{size}"
            if response.status_code != 206 or response.headers.get("Content-Range") != expected_range:
                raise FileTransferError(
                    f"OneLake did not honor byte range {expected_range}; no chunk was uploaded."
                )
            content = bytearray()
            for chunk in response.iter_bytes(chunk_size=chunk_size):
                check_cancelled(cancel_requested)
                if len(content) + len(chunk) > end - offset + 1:
                    raise FileTransferError("OneLake returned more bytes than requested; transfer stopped.")
                content.extend(chunk)
            if len(content) != end - offset + 1:
                raise FileTransferError(f"OneLake returned an incomplete byte range for {source}.")
        yield bytes(content)
        offset += len(content)


def _copy_file_streaming(
    client: httpx.Client, source: str, target: str, entry: dict[str, Any],
    tokens: TokenProvider, target_tokens: TokenProvider, chunk_size: int,
    cancel_requested: Callable[[], bool] | None,
) -> None:
    # Validated metadata carries its preflight ETag, not the later listing's version.
    size, etag = _file_properties(client, source, entry, tokens)
    check_cancelled(cancel_requested)
    _request(client, "PUT", target, target_tokens, params={"resource": "file"}, content=b"")
    offset = 0
    for content in _read_chunks(client, source, tokens, size, etag, chunk_size, cancel_requested):
        check_cancelled(cancel_requested)
        _request(
            client, "PATCH", target, target_tokens,
            params={"action": "append", "position": str(offset)}, content=bytes(content),
        )
        offset += len(content)
    check_cancelled(cancel_requested)
    _request(
        client, "PATCH", target, target_tokens,
        params={"action": "flush", "position": str(size), "close": "true"}, content=b"",
    )
    target_properties = _request(client, "HEAD", target, target_tokens)
    if int(target_properties.headers["Content-Length"]) != size:
        raise FileTransferError(f"OneLake target length differs for {entry['name']}; retry the transfer.")


def copy_tree_streaming(
    *,
    source_path: str,
    target_path: str,
    tokens: TokenProvider,
    target_tokens: TokenProvider,
    max_staging_bytes: int = DEFAULT_MAX_STAGING_BYTES,
    exclude_paths: Collection[str] = (),
    kind: str = "files",
    scratch_dir: Path | None = None,
    on_progress: Callable[[str], None] | None = None,
    on_complete: Callable[[CopyOutcome], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> CopyOutcome:
    """Relay a frozen OneLake tree through bounded RAM, using no staging disk.

    Fabric must already have created the item and its Files/Tables roots. The caller must
    exclude shortcut paths and keep source writes frozen through successful completion.
    ``exclude_paths`` contains case-sensitive paths relative to this root, or Files/Tables
    qualified paths obtained from the source shortcut inventory.
    Every copy undergoes a read-only Delta reference preflight before destination I/O, since
    an unmanaged Delta table (for example one a notebook wrote directly under Files/) is just
    as capable of an escaping/unsafe storage reference as a managed one. A Tables/ root is
    strict: Fabric restricts it to managed tables only, so any file outside a discovered one
    is unexpected and refused. A Files/ root is not strict: ordinary content may freely mix
    with zero or more unmanaged Delta tables, and only files inside a discovered table's
    ``_delta_log`` are inspected.
    Checkpoints are staged individually within the budget and inspected in Arrow batches;
    unsafe/unknown path features fail without rewriting logs. Source writes must remain frozen.
    ``scratch_dir`` defaults to the working directory; private checkpoint staging is always removed.
    A successful byte copy does not establish destination SQL catalog readiness.
    The destination must be fresh, or a retry of the same operator-frozen source snapshot.
    On retry each file is recreated before appending; no uncertain append is retried alone.

    https://learn.microsoft.com/fabric/onelake/onelake-api-parity
    https://learn.microsoft.com/rest/api/storageservices/datalakestoragegen2/path/read
    https://learn.microsoft.com/rest/api/storageservices/datalakestoragegen2/path/update
    """
    check_budget(max_staging_bytes)
    check_cancelled(cancel_requested)
    source_fs, source_root = _tree_location(source_path)
    target_fs, target_root = _tree_location(target_path)
    if source_fs == target_fs and source_root == target_root:
        raise FileTransferError("Source and destination OneLake paths must differ.")
    strict_tables = kind == "lakehouse" or any(
        root.split("/", 2)[1] == "Tables" for root in (source_root, target_root)
    )
    # Leave room for the bytearray and immutable HTTP request payload simultaneously.
    chunk_size = max(1, min(4 * 1024 * 1024, max_staging_bytes // 2))
    excluded = {value.strip("/") for value in exclude_paths}
    count = 0
    copied_metadata: set[str] = set()

    def is_excluded(name: str) -> bool:
        local = name[len(source_root) + 1:]
        candidates = {local, name, name.split("/", 1)[-1]}
        return any(value == ex or value.startswith(ex + "/") for ex in excluded for value in candidates)

    def visit(client: httpx.Client, relative: str = "") -> None:
        nonlocal count
        directory = source_root + (f"/{relative}" if relative else "")
        for entry in _listed_paths(client, source_fs, directory, tokens, cancel_requested):
            check_cancelled(cancel_requested)
            name = str(entry["name"])
            local = name[len(source_root) + 1:]
            if is_excluded(name):
                continue
            target = f"{target_fs}/{quote(target_root + '/' + local, safe='/')}"
            if str(entry.get("isDirectory", False)).lower() == "true":
                response = client.put(
                    target, params={"resource": "directory"}, headers=_headers(target_tokens), content=b"",
                )
                if not response.is_success:
                    try:
                        code = response.json().get("error", {}).get("code")
                    except ValueError:
                        code = None
                    if response.status_code != 409 or code != "PathAlreadyExists":
                        raise _error(response)
                visit(client, local)
            else:
                entry = snapshot.pin(entry)
                if on_progress:
                    on_progress(f"Copying OneLake {local}")
                _copy_file_streaming(
                    client, f"{source_fs}/{quote(name, safe='/')}", target, entry,
                    tokens, target_tokens, chunk_size, cancel_requested,
                )
                count += 1
                if name in snapshot.files:
                    copied_metadata.add(name)

    with httpx.Client(timeout=120, follow_redirects=False) as client:
        from fabshuffle.transfer.delta import preflight

        snapshot = preflight(
            client, source_fs, source_root, tokens, max_staging_bytes=max_staging_bytes,
            scratch_dir=scratch_dir, is_excluded=is_excluded, on_progress=on_progress,
            cancel_requested=cancel_requested, strict=strict_tables,
        )
        visit(client)
    if copied_metadata != snapshot.files.keys():
        raise DeltaValidationRequired(
            "Delta metadata disappeared after preflight; keep source writes frozen and retry."
        )
    check_cancelled(cancel_requested)
    outcome = CopyOutcome(kind, empty=count == 0)
    if on_complete:
        on_complete(outcome)
    return outcome


__all__ = ["DeltaValidationRequired", "FileTransferError", "copy_files", "copy_tree_streaming"]
