from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from fabshuffle.bcdr.bootstrap import ARM_VERSION, BootstrapError
from fabshuffle.bcdr.capacity import ARM_BASE, ARM_SCOPE, ArmCapacityClient, CapacityError
from fabshuffle.bcdr.discovery import match_recovery_capacities

TENANT = "00000000-0000-0000-0000-000000000001"
SUB = "00000000-0000-0000-0000-000000000002"
OTHER = "00000000-0000-0000-0000-000000000003"
FABRIC = "00000000-0000-0000-0000-000000000004"
ARM = f"/subscriptions/{SUB}/resourcegroups/dr/providers/microsoft.fabric/capacities/recovery"
FABRIC_ROW = {"id": FABRIC, "displayName": "Recovery", "region": "West US", "sku": "F4"}
ARM_ROW = {"id": ARM, "name": "recovery", "location": "westus",
           "subscriptionName": "QA subscription", "resourceGroup": "dr"}


def test_unique_name_region_matching_returns_exact_internal_ids():
    result = match_recovery_capacities([{**FABRIC_ROW, "displayName": " RECOVERY "}], [ARM_ROW])
    assert result[0]["arm_resource_id"] == ARM
    assert result[0]["id"] == FABRIC
    assert result[0]["matchStatus"] == "matched"
    assert result[0]["matchMethod"] == "name_region"
    assert result[0]["subscriptionName"] == "QA subscription"


@pytest.mark.parametrize("fabric,azure,status", [
    ([FABRIC_ROW], [], "unmatched"),
    ([{**FABRIC_ROW, "region": "East US"}], [ARM_ROW], "unmatched"),
    ([{**FABRIC_ROW, "displayName": "Recovery2"}], [ARM_ROW], "unmatched"),
    ([{**FABRIC_ROW, "region": ""}], [ARM_ROW], "unmatched"),
    ([{**FABRIC_ROW, "sku": "Trial"}], [ARM_ROW], "unsupported"),
    ([FABRIC_ROW, {**FABRIC_ROW, "id": OTHER}], [ARM_ROW], "ambiguous"),
    ([FABRIC_ROW], [ARM_ROW, {**ARM_ROW, "id": ARM.replace(SUB, OTHER)}], "ambiguous"),
])
def test_missing_or_ambiguous_matches_never_choose_first(fabric, azure, status):
    choices = match_recovery_capacities(fabric, azure)
    assert all(row["matchStatus"] == status for row in choices)
    assert all(row["arm_resource_id"] is None and row["matchMessage"] for row in choices)


def test_duplicate_page_entry_is_not_a_second_resource():
    assert match_recovery_capacities([FABRIC_ROW], [ARM_ROW, ARM_ROW])[0]["matchStatus"] == "matched"


def tokens(scopes):
    def token(scope):
        scopes.append(scope)
        return "never-log-this-token"
    return SimpleNamespace(tenant_id=lambda: TENANT, token=token)


def arm_document():
    return {**ARM_ROW, "type": "Microsoft.Fabric/capacities", "sku": {"name": "F4", "tier": "Fabric"},
            "properties": {"administration": {"members": ["do-not-return"]}}}


def test_arm_discovery_is_paginated_read_only_same_tenant_and_nonsecret():
    scopes, calls = [], []
    path = f"/subscriptions/{SUB}/providers/Microsoft.Fabric/capacities"

    def handler(request):
        calls.append(str(request.url))
        assert request.method == "GET"
        if request.url.path == "/subscriptions":
            if "page" in request.url.params:
                return httpx.Response(200, json={"value": [
                    {"subscriptionId": OTHER, "tenantId": OTHER, "displayName": "Other tenant"},
                ]})
            return httpx.Response(200, json={"value": [
                {"subscriptionId": SUB, "tenantId": TENANT, "displayName": "QA subscription"},
            ], "nextLink": f"{ARM_BASE}/subscriptions?api-version=2022-12-01&page=2"})
        assert request.url.path == path
        if "page" in request.url.params:
            return httpx.Response(200, json={"value": []})
        return httpx.Response(200, json={"value": [arm_document()],
            "nextLink": f"{ARM_BASE}{path}?api-version={ARM_VERSION}&page=2"})

    with ArmCapacityClient(tokens(scopes), transport=httpx.MockTransport(handler)) as arm:
        result = arm.list_capacities()
    assert result == [ARM_ROW]
    assert scopes == [ARM_SCOPE] * 4 and len(calls) == 4
    assert "do-not-return" not in str(result)


@pytest.mark.parametrize("next_link", [
    "https://evil.example/subscriptions",
    f"{ARM_BASE}/subscriptions/{SUB}/providers/Microsoft.Compute/virtualMachines",
    f"{ARM_BASE}/subscriptions?api-version=2022-12-01",
    f"{ARM_BASE}/subscriptions#fragment",
])
def test_unsafe_or_repeated_discovery_paging_is_rejected_before_token(next_link):
    scopes = []
    with ArmCapacityClient(tokens(scopes), transport=httpx.MockTransport(
        lambda _: httpx.Response(200, json={"value": [], "nextLink": next_link}),
    )) as arm:
        with pytest.raises(BootstrapError):
            arm.list_capacities()
    assert scopes == [ARM_SCOPE]


@pytest.mark.parametrize("payload", [{}, {"value": {}}, {"value": [None]}, {"value": [], "nextLink": 42}])
def test_malformed_discovery_is_not_empty_success(payload):
    with ArmCapacityClient(tokens([]), transport=httpx.MockTransport(
        lambda _: httpx.Response(200, json=payload),
    )) as arm:
        with pytest.raises(BootstrapError):
            arm.list_capacities()


@pytest.mark.parametrize("status", [302, 403])
def test_discovery_does_not_follow_redirects_or_hide_service_failure(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, headers={"Location": "https://evil.example/subscriptions"},
                              json={"error": {"code": "AuthorizationFailed", "message": "Grant read access"}})

    with ArmCapacityClient(tokens([]), transport=httpx.MockTransport(
        handler,
    )) as arm:
        with pytest.raises(CapacityError, match="AuthorizationFailed: Grant read access"):
            arm.list_capacities()
    assert len(calls) == 1 and calls[0].url.host == "management.azure.com"
