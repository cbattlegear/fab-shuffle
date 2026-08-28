"""Helpers for Fabric item definitions.

Definitions are transported as a list of ``parts``, each holding a base64 encoded
payload. Every item also carries a ``.platform`` part describing its type and name.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterable, Mapping
from typing import Any

PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
)
EMPTY_LOGICAL_ID = "00000000-0000-0000-0000-000000000000"


def encode_payload(content: str | bytes | Mapping[str, Any] | list[Any]) -> str:
    """Base64 encode a definition part payload."""
    if isinstance(content, (Mapping, list)):
        raw = json.dumps(content, indent=2).encode("utf-8")
    elif isinstance(content, str):
        raw = content.encode("utf-8")
    else:
        raw = content
    return base64.b64encode(raw).decode("ascii")


def decode_payload(payload: str) -> bytes:
    return base64.b64decode(payload)


def decode_json_part(payload: str) -> Any:
    return json.loads(decode_payload(payload).decode("utf-8"))


def part(path: str, content: str | bytes | Mapping[str, Any] | list[Any]) -> dict[str, str]:
    return {"path": path, "payload": encode_payload(content), "payloadType": "InlineBase64"}


def platform_part(item_type: str, display_name: str, description: str = "") -> dict[str, str]:
    """Build the ``.platform`` part every Fabric item definition needs."""
    metadata: dict[str, Any] = {"type": item_type, "displayName": display_name}
    if description:
        metadata["description"] = description
    return part(
        ".platform",
        {
            "$schema": PLATFORM_SCHEMA,
            "metadata": metadata,
            "config": {"version": "2.0", "logicalId": EMPTY_LOGICAL_ID},
        },
    )


def definition(parts: Iterable[Mapping[str, str]], fmt: str | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"parts": list(parts)}
    if fmt:
        body["format"] = fmt
    return body


def find_part(parts: Iterable[Mapping[str, Any]], path: str) -> dict[str, Any] | None:
    for candidate in parts:
        if candidate.get("path") == path:
            return dict(candidate)
    return None


def replace_part(
    parts: Iterable[Mapping[str, Any]],
    path: str,
    content: str | bytes | Mapping[str, Any] | list[Any],
) -> list[dict[str, Any]]:
    """Return the part list with ``path`` swapped for freshly encoded ``content``."""
    replaced = False
    updated: list[dict[str, Any]] = []
    for candidate in parts:
        if candidate.get("path") == path:
            updated.append(part(path, content))
            replaced = True
        else:
            updated.append(dict(candidate))
    if not replaced:
        updated.append(part(path, content))
    return updated


__all__ = [
    "decode_json_part",
    "decode_payload",
    "definition",
    "encode_payload",
    "find_part",
    "part",
    "platform_part",
    "replace_part",
]
