"""Reading the service principal's own object id out of its token.

Fabric identifies a principal by object id, but a service principal is configured with its
application id, and the two are different GUIDs. Asking Microsoft Graph for the mapping would
need a directory permission this application otherwise has no use for. The token it already
holds carries the answer: for an app-only token the ``oid`` claim is the service principal.
"""

from __future__ import annotations

import base64
import json

from fabshuffle.auth import token_claim

OBJECT_ID = "3f2b9c71-55aa-4f0e-9c1d-8e77b0a41d22"
APP_ID = "9b57d15c-f03f-4112-adb8-b480df80bd02"


def jwt(claims: dict) -> str:
    """A token shaped like the real thing. Only the payload segment is ever read."""
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def test_the_object_id_comes_from_the_oid_claim() -> None:
    token = jwt({"oid": OBJECT_ID, "appid": APP_ID, "tid": "tenant"})

    assert token_claim(token, "oid") == OBJECT_ID


def test_the_object_id_is_not_the_application_id() -> None:
    """Using appid here is the obvious mistake, and it fails confusingly at the API."""
    token = jwt({"oid": OBJECT_ID, "appid": APP_ID})

    assert token_claim(token, "oid") != token_claim(token, "appid")


def test_padding_is_restored_before_decoding() -> None:
    """Base64url in a JWT is unpadded, so lengths that need padding must still decode."""
    for length in range(1, 12):
        token = jwt({"oid": "x" * length})
        assert token_claim(token, "oid") == "x" * length


def test_a_missing_claim_is_empty_rather_than_an_error() -> None:
    assert token_claim(jwt({"appid": APP_ID}), "oid") == ""


def test_a_malformed_token_is_empty_rather_than_an_error() -> None:
    # The script falls back to a directory lookup, so this must degrade rather than raise.
    assert token_claim("not-a-jwt", "oid") == ""
    assert token_claim("", "oid") == ""


def test_an_undecodable_payload_is_empty_rather_than_an_error() -> None:
    assert token_claim("header.!!!not-base64!!!.signature", "oid") == ""


def test_a_payload_that_is_not_json_is_empty_rather_than_an_error() -> None:
    payload = base64.urlsafe_b64encode(b"plain text").decode().rstrip("=")

    assert token_claim(f"header.{payload}.signature", "oid") == ""


def test_a_non_string_claim_is_returned_as_text() -> None:
    assert token_claim(jwt({"oid": 12345}), "oid") == "12345"
