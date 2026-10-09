"""Bounded binary persistence and exact round-trip validation for captured parts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from fabshuffle.bcdr.contracts import PayloadDescriptor, digest, reject_embedded_secrets

# Learn: fabric/data-warehouse/data-types documents a 16 MB per-value limit.
# Stay well below it; split encoded bytes, not characters, and decode only after assembly.
CHUNK_BYTES = 1024 * 1024


class IntegrityError(ValueError):
    """Stored records do not form the exact generation that was captured."""


@dataclass(frozen=True, slots=True)
class PayloadChunk:
    ordinal: int
    byte_length: int
    sha256: str
    data: bytes


@dataclass(frozen=True, slots=True)
class CapturedPayload:
    descriptor: PayloadDescriptor
    data: bytes

    def validate(self) -> None:
        if self.descriptor.byte_length != len(self.data) or self.descriptor.sha256 != digest(self.data):
            raise IntegrityError(f"Payload length/hash mismatch: {self.descriptor.path}")
        if self.descriptor.chunk_count != chunk_count(len(self.data)):
            raise IntegrityError(f"Payload chunk count mismatch: {self.descriptor.path}")
        if self.descriptor.encoding == "utf-8":
            self.data.decode("utf-8")
        reject_embedded_secrets(self.data)


def chunk_count(byte_length: int) -> int:
    return max(1, (byte_length + CHUNK_BYTES - 1) // CHUNK_BYTES)


def split_payload(data: bytes) -> Iterable[PayloadChunk]:
    for ordinal in range(chunk_count(len(data))):
        chunk = data[ordinal * CHUNK_BYTES:(ordinal + 1) * CHUNK_BYTES]
        yield PayloadChunk(ordinal, len(chunk), digest(chunk), chunk)


def join_payload(
    chunks: Iterable[PayloadChunk], *, byte_length: int, sha256: str, count: int,
) -> bytes:
    if byte_length < 0 or count != chunk_count(byte_length):
        raise IntegrityError("Invalid payload byte length/chunk count")
    ordered = sorted(chunks, key=lambda chunk: chunk.ordinal)
    if len(ordered) != count:
        raise IntegrityError("Missing or duplicate payload chunks")
    for ordinal, chunk in enumerate(ordered):
        expected_size = min(CHUNK_BYTES, max(0, byte_length - ordinal * CHUNK_BYTES))
        if (
            chunk.ordinal != ordinal or chunk.byte_length != expected_size
            or len(chunk.data) != expected_size or digest(chunk.data) != chunk.sha256
        ):
            raise IntegrityError("Corrupt, duplicate or out-of-order payload chunk")
    data = b"".join(chunk.data for chunk in ordered)
    if len(data) != byte_length or digest(data) != sha256:
        raise IntegrityError("Payload total length/hash mismatch")
    return data
