"""Helpers for Fabric item definitions.

Definitions are transported as a list of ``parts``, each holding a base64 encoded
payload. Every item also carries a ``.platform`` part describing its type and name.
"""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
)
EMPTY_LOGICAL_ID = "00000000-0000-0000-0000-000000000000"

# Definition parts Fab Shuffle rewrites references inside. Anything else (images, custom
# visuals, other binary resources under StaticResources/) is copied through untouched.
TEXT_SUFFIXES = frozenset(
    {
        ".bim",
        ".json",
        ".kql",
        ".m",
        ".md",
        ".pbir",
        ".pbism",
        ".pq",
        ".tmdl",
        ".txt",
        ".xml",
    }
)


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


def is_text_part(path: str) -> bool:
    """Whether a definition part holds text that references other items."""
    name = path.rsplit("/", 1)[-1]
    if name.startswith("."):
        # Dot files such as .platform are JSON.
        return True
    suffix = name[name.rfind(".") :].lower() if "." in name else ""
    return suffix in TEXT_SUFFIXES


def build_rewriter(replacements: Mapping[str, str]) -> Any:
    """Compile a single-pass, case-insensitive replacer for a source -> target id map.

    Two details matter here. Matching is case-insensitive because Fabric is inconsistent
    about GUID casing between endpoints, and the whole map is applied in *one* pass over
    alternation so a value that was just substituted can never be rewritten again by a
    later, shorter key (a bare item GUID also appears inside SQL endpoint strings).
    """
    usable = {key: value for key, value in replacements.items() if key and value and key != value}
    if not usable:
        return None

    # Longest first so the most specific reference wins at any given position.
    keys = sorted(usable, key=len, reverse=True)
    pattern = re.compile("|".join(re.escape(key) for key in keys), re.IGNORECASE)
    lookup = {key.lower(): value for key, value in usable.items()}

    def rewrite(text: str) -> str:
        return pattern.sub(lambda match: lookup[match.group(0).lower()], text)

    return rewrite


def rewrite_parts(
    parts: Iterable[Mapping[str, Any]],
    replacements: Mapping[str, str],
) -> tuple[list[dict[str, Any]], int]:
    """Repoint an exported definition at the migrated items.

    Returns the rewritten parts and how many of them actually changed, so callers can
    tell the difference between "rebound" and "nothing referenced the old workspace".
    """
    rewrite = build_rewriter(replacements)
    updated: list[dict[str, Any]] = []
    changed = 0

    for candidate in parts:
        path = candidate.get("path", "")
        payload = candidate.get("payload", "")

        if rewrite is None or not is_text_part(path) or not payload:
            updated.append(dict(candidate))
            continue

        try:
            original = decode_payload(payload).decode("utf-8")
        except UnicodeDecodeError:
            updated.append(dict(candidate))
            continue

        rewritten = rewrite(original)
        if rewritten == original:
            updated.append(dict(candidate))
            continue

        updated.append(part(path, rewritten))
        changed += 1

    return updated, changed


def strip_part(parts: Iterable[Mapping[str, Any]], path: str) -> list[dict[str, Any]]:
    return [dict(candidate) for candidate in parts if candidate.get("path") != path]


__all__ = [
    "TEXT_SUFFIXES",
    "build_rewriter",
    "decode_json_part",
    "decode_payload",
    "definition",
    "encode_payload",
    "find_part",
    "is_text_part",
    "part",
    "platform_part",
    "replace_part",
    "rewrite_parts",
    "strip_part",
]
