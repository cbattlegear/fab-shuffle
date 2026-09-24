"""Deployment exclusion before bootstrap/ARM/SQL, not a replacement for Warehouse epochs.

REST contracts: Microsoft Learn /rest/api/storageservices/{lease-blob,put-blob,
specifying-conditional-headers-for-blob-service-operations}. Never break a lease.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from xml.etree import ElementTree

import httpx

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.activity import report_activity
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.lifecycle import safe_text

LEASE_ENV = "FAB_SHUFFLE_BCDR_LEASE_BLOB_URL"


def validate_blob_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        any(ord(char) <= 32 or ord(char) >= 127 for char in value)
        or parsed.scheme != "https"
        or not re.fullmatch(r"[a-z0-9]{3,24}\.blob\.core\.windows\.net", parsed.netloc)
        or parsed.query or parsed.fragment or "?" in value or "#" in value
        or not re.fullmatch(r"/[a-z0-9](?:[a-z0-9-]{1,61})[a-z0-9]/[A-Za-z0-9_./-]+", parsed.path)
        or "--" in parsed.path.split("/")[1]
        or any(part in {"", ".", ".."} for part in parsed.path.split("/")[1:])
    ):
        raise RecoveryBlocked(
            f"Set {LEASE_ENV} to a fixed public Azure Blob HTTPS URL with container/blob; "
            "SAS, credentials, query strings, fragments, custom domains and encoded paths are forbidden"
        )
    return value


class BlobLeaseError(RecoveryBlocked):
    def __init__(self, response: httpx.Response, action: str) -> None:
        code = response.headers.get("x-ms-error-code", "")
        message = response.text
        try:
            root = ElementTree.fromstring(response.content)
            code = root.findtext("Code") or code
            message = root.findtext("Message") or message
        except ElementTree.ParseError:
            pass
        self.error_code = code
        self.status_code = response.status_code
        self.request_id = response.headers.get("x-ms-request-id", "")
        super().__init__(safe_text(
            f"Deployment lease {action}: HTTP {response.status_code}; {code}: {message}"
            f" (request ID {self.request_id}). Stop this worker; inspect the lease/previous operation "
            "and Storage Blob Data Contributor access before retrying."
        ))


class BlobLease:
    """A finite lease with a conservative monotonic deadline and sticky loss.

    A failed/late renewal is terminal even though Azure permits renewing an expired
    lease. An in-flight external effect may have completed; never replay it here.
    """

    DURATION = 60
    RENEW_INTERVAL = 20
    SAFETY_MARGIN = 5

    def __init__(
        self, url: str, tokens: TokenProvider, *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.url = validate_blob_url(url)
        self.tokens = tokens
        self.clock = clock
        self.lease_id = str(uuid4())
        self.http = httpx.Client(transport=transport, timeout=10, follow_redirects=False)
        self._mutex = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._deadline = 0.0
        self._held = False
        self._closed = False
        self._loss: str | None = None
        try:
            created = self._request(None)
            if created.status_code != 201:
                error = BlobLeaseError(created, "conditional create")
                if (error.status_code, error.error_code) not in {
                    (412, "ConditionNotMet"), (409, "BlobAlreadyExists"),
                }:
                    raise error
            started = self.clock()
            response = self._request("acquire")
            if response.status_code != 201:
                raise BlobLeaseError(response, "acquire")
            if response.headers.get("x-ms-lease-id") != self.lease_id:
                raise RecoveryBlocked("Deployment lease acquire returned an unexpected lease ID; stop")
            self._held = True
            self._deadline = started + self.DURATION - self.SAFETY_MARGIN
            self.assert_held()
            self._thread = threading.Thread(
                target=self._heartbeat, name="bcdr-deployment-lease", daemon=True,
            )
            self._thread.start()
        except BaseException:
            # An unknown acquire is not retried or released speculatively; it expires in 60s.
            self.http.close()
            raise

    def _request(self, action: str | None) -> httpx.Response:
        for attempt in range(2):
            token = self.tokens.storage_token()
            if action in {"renew", "release"}:
                self.assert_held()
            headers = {
                "Authorization": f"Bearer {token}",
                "x-ms-version": "2023-11-03",
                "x-ms-date": format_datetime(datetime.now(UTC), usegmt=True),
            }
            if action is None:
                headers.update({"x-ms-blob-type": "BlockBlob", "If-None-Match": "*"})
            else:
                headers["x-ms-lease-action"] = action
                if action == "acquire":
                    headers.update({
                        "x-ms-lease-duration": str(self.DURATION),
                        "x-ms-proposed-lease-id": self.lease_id,
                    })
                else:
                    headers["x-ms-lease-id"] = self.lease_id
            try:
                response = self.http.put(
                    self.url + ("?comp=lease" if action else ""), headers=headers, content=b"",
                )
            except httpx.TransportError as error:
                raise RecoveryBlocked(
                    f"Deployment lease {action or 'conditional create'} response was lost "
                    f"({type(error).__name__}); stop and reconcile any in-flight work. "
                    "Do not retry effects or take over Warehouse ownership."
                ) from error
            if response.status_code != 401 or attempt:
                return response
            self.tokens.invalidate()
        raise AssertionError("Lease request attempts exhausted")

    def assert_held(self) -> None:
        with self._mutex:
            if self._held and self.clock() >= self._deadline:
                self._loss = self._loss or "Deployment lease deadline expired"
            if self._closed or not self._held or self._loss:
                raise RecoveryBlocked(
                    (self._loss or "Deployment lease is not held")
                    + "; stop new work and reconcile in-flight effects before an explicit retry"
                )

    def renew(self) -> None:
        try:
            self.assert_held()
            started = self.clock()
            response = self._request("renew")
            if response.status_code != 200:
                raise BlobLeaseError(response, "renew")
            if response.headers.get("x-ms-lease-id") != self.lease_id:
                raise RecoveryBlocked("Deployment lease renewal returned an unexpected lease ID")
            self.assert_held()
            with self._mutex:
                self._deadline = started + self.DURATION - self.SAFETY_MARGIN
        except Exception as error:
            # A heartbeat must publish failure to the foreground, not die silently.
            with self._mutex:
                self._loss = self._loss or safe_text(str(error))
            self._stop.set()
            raise

    def _heartbeat(self) -> None:
        try:
            while not self._stop.wait(self.RENEW_INTERVAL):
                try:
                    self.renew()
                except Exception:
                    return  # renew stored the actionable failure; every fence and close raises it.
        finally:
            if self._closed:
                self.http.close()

    def close(self) -> None:
        if self._closed:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=25)
            if self._thread.is_alive():
                with self._mutex:
                    self._loss = self._loss or "Deployment lease heartbeat did not stop"
                # Do not close a client still in use or race renew against release.
                self._closed = True
                raise RecoveryBlocked(self._loss + "; stop this worker and let the finite lease expire")
        try:
            self.assert_held()
            response = self._request("release")
            if response.status_code != 200:
                raise BlobLeaseError(response, "release")
            self.assert_held()
        finally:
            self._closed = True
            self.http.close()


class DeploymentLock:
    """Local flock plus optional cloud exclusion; cloud errors never fall back locally."""

    def __init__(self, path: Path, *, tokens: TokenProvider | None = None) -> None:
        import fcntl

        self.remote: BlobLease | None = None
        self.fd: int | None = None
        self.acquired = False
        report_activity("Acquiring deployment ownership")
        url = os.environ.get(LEASE_ENV)
        if url is not None:
            validate_blob_url(url)
            if tokens is None:
                raise RecoveryBlocked("Configure deployment lease credentials before opening the service")
            self.remote = BlobLease(url, tokens)
        try:
            if self.remote is not None:
                # Azure Files does not support advisory locks. Use local disk only
                # for the additional same-host flock, never as cross-host exclusion.
                key = hashlib.sha256(str(path.absolute()).encode()).hexdigest()
                lock_path = Path(tempfile.gettempdir()) / f"fab-shuffle-{key}.controller.lock"
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                lock_path = path.with_suffix(path.suffix + ".controller.lock")
            self.fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.acquired = True
            report_activity(detail="Exclusive deployment lock acquired", owns_deployment=True)
        except BaseException as error:
            self.close()
            if isinstance(error, BlockingIOError):
                raise RecoveryBlocked(
                    "Another worker owns this deployment; wait for its durable result"
                ) from error
            raise

    def assert_held(self) -> None:
        if self.fd is None:
            raise RecoveryBlocked("Deployment lock is closed; stop this worker")
        if self.remote is not None:
            self.remote.assert_held()

    def close(self) -> None:
        try:
            if self.remote is not None:
                self.remote.close()
        finally:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
                if self.acquired:
                    self.acquired = False
                    report_activity(detail="Deployment lock released", owns_deployment=False)


class GuardedTokens(TokenProvider):
    """Fence each backend's token use, including retries and refreshed tokens."""

    def __init__(self, tokens: TokenProvider, guard: Callable[[], None]) -> None:
        self.principal = tokens.principal
        self._delegate = tokens
        self._guard = guard

    def _call(self, action: Callable[[], str]) -> str:
        self._guard()
        value = action()
        self._guard()
        return value

    def assert_active(self) -> None:
        self._guard()

    def invalidate(self) -> None:
        self._guard()
        self._delegate.invalidate()
        self._guard()

    def token(self, scope: str) -> str:
        return self._call(lambda: self._delegate.token(scope))

    def storage_token(self) -> str:
        return self._call(self._delegate.storage_token)

    def fabric_token(self) -> str:
        return self._call(self._delegate.fabric_token)

    def sql_token(self) -> str:
        return self._call(self._delegate.sql_token)

    def kusto_token(self) -> str:
        return self._call(self._delegate.kusto_token)

    def powerbi_token(self) -> str:
        return self._call(self._delegate.powerbi_token)

    def tenant_id(self) -> str:
        return self._call(self._delegate.tenant_id)

    def object_id(self) -> str:
        return self._call(self._delegate.object_id)
