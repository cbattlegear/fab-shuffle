from dataclasses import replace
from uuid import uuid4

import pytest

from fabshuffle.bcdr.contracts import ItemIdentity, PayloadDescriptor, PayloadPurpose, digest
from fabshuffle.bcdr.payloads import (
    CHUNK_BYTES,
    CapturedPayload,
    IntegrityError,
    chunk_count,
    join_payload,
    split_payload,
)


@pytest.mark.parametrize("data", [
    b"", bytes(range(256)) * 5000, ("\u96ea\U0001f680" * 2_500_001).encode("utf-8"),
])
def test_large_binary_and_utf8_round_trip_without_splitting_characters_on_decode(data):
    chunks = list(split_payload(data))
    assert all(len(chunk.data) <= CHUNK_BYTES for chunk in chunks)
    assert join_payload(
        reversed(chunks), byte_length=len(data), sha256=digest(data), count=chunk_count(len(data)),
    ) == data


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "corrupt", "length", "ordinal", "total_hash"])
def test_chunk_validation_does_not_trust_only_part_count(mutation):
    data = b"x" * (CHUNK_BYTES + 4)
    chunks = list(split_payload(data))
    sha256 = digest(data)
    if mutation == "duplicate":
        chunks[1] = chunks[0]
    elif mutation == "missing":
        chunks.pop()
    elif mutation == "corrupt":
        chunks[1] = replace(chunks[1], data=b"yyyy")
    elif mutation == "length":
        chunks[0] = replace(chunks[0], byte_length=3)
    elif mutation == "ordinal":
        chunks[1] = replace(chunks[1], ordinal=9)
    else:
        sha256 = "0" * 64
    with pytest.raises(IntegrityError):
        join_payload(chunks, byte_length=len(data), sha256=sha256, count=2)


def test_captured_utf8_payload_must_be_valid_and_matches_descriptor():
    data = b"\xff"
    descriptor = PayloadDescriptor(
        payload_id=str(uuid4()),
        owner=ItemIdentity(tenant_id=str(uuid4()), workspace_id=str(uuid4()), item_id=str(uuid4())),
        path="definition/model.json", purpose=PayloadPurpose.DEFINITION, media_type="application/json",
        encoding="utf-8", byte_length=len(data), sha256=digest(data), chunk_count=1,
    )
    with pytest.raises(UnicodeDecodeError):
        CapturedPayload(descriptor, data).validate()
    CapturedPayload(descriptor.model_copy(update={"encoding": "binary"}), data).validate()
