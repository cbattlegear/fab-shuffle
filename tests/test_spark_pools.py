"""Custom Spark pools and workspace Spark settings.

A pool belongs to the workspace it was created in, so an environment that pins one would
otherwise land in the migrated workspace referencing a pool that does not exist. Pools are
recreated first and their new ids go into the id map, which repoints the environment.
"""

from __future__ import annotations

from fabshuffle.fabric import spark
from fabshuffle.fabric.client import FabricApiError

POOL = {
    "id": "old-pool",
    "name": "pool1",
    "type": "Workspace",
    "nodeFamily": "MemoryOptimized",
    "nodeSize": "Small",
    "autoScale": {"enabled": True, "minNodeCount": 1, "maxNodeCount": 2},
    "dynamicExecutorAllocation": {"enabled": True, "minExecutors": 1, "maxExecutors": 1},
}
STARTER = {"id": "starter", "name": "Starter Pool", "type": "Workspace"}
CAPACITY_POOL = {"id": "cap-pool", "name": "shared", "type": "Capacity", "nodeSize": "Large"}


class FakeClient:
    def __init__(self, *, fail_names: set[str] | None = None) -> None:
        self.fail_names = fail_names or set()
        self.posted: list[tuple[str, dict]] = []
        self.patched: list[tuple[str, dict]] = []

    def post(self, path, json=None, params=None, wait=True):
        if json.get("name") in self.fail_names:
            raise FabricApiError("POST", path, 400, "node size unavailable")
        self.posted.append((path, json))
        return {"id": f"new-{json['name']}", **json}

    def patch(self, path, json=None, params=None):
        self.patched.append((path, json))
        return json

    def list_all(self, path, params=None, value_key="value"):
        return []

    def get(self, path, params=None):
        return {}


# --------------------------------------------------------------------- pools


def test_a_custom_pool_is_recreated_with_its_settings():
    client = FakeClient()
    id_map, created, warnings = spark.copy_pools(client, "src", "dst", pools=[POOL])

    assert warnings == []
    assert created == ["pool1"]
    assert id_map == {"old-pool": "new-pool1"}

    path, payload = client.posted[0]
    assert path == "workspaces/dst/spark/pools"
    assert payload["nodeFamily"] == "MemoryOptimized"
    assert payload["autoScale"] == {"enabled": True, "minNodeCount": 1, "maxNodeCount": 2}
    # Fabric assigns these, so sending them back would be rejected.
    assert "id" not in payload and "type" not in payload


def test_the_starter_pool_is_never_recreated():
    # "Starter Pool" is a reserved name that every workspace already has.
    client = FakeClient()
    id_map, created, warnings = spark.copy_pools(client, "src", "dst", pools=[STARTER])

    assert client.posted == []
    assert created == [] and id_map == {} and warnings == []


def test_a_capacity_level_pool_is_reported_rather_than_recreated():
    client = FakeClient()
    _, created, warnings = spark.copy_pools(client, "src", "dst", pools=[CAPACITY_POOL])

    assert created == []
    assert len(warnings) == 1
    assert "Capacity level pool" in warnings[0]
    assert client.posted == []


def test_a_pool_that_cannot_be_created_is_reported_and_the_rest_continue():
    client = FakeClient(fail_names={"pool1"})
    other = {**POOL, "id": "old-2", "name": "pool2"}
    id_map, created, warnings = spark.copy_pools(client, "src", "dst", pools=[POOL, other])

    assert created == ["pool2"]
    assert id_map == {"old-2": "new-pool2"}
    assert len(warnings) == 1 and "pool1" in warnings[0]


def test_unreadable_pools_are_not_fatal():
    class Denied:
        def list_all(self, path, params=None, value_key="value"):
            raise FabricApiError("GET", path, 403, "forbidden")

    assert spark.list_pools(Denied(), "src") == []


# ------------------------------------------------------------------ settings


def test_the_default_pool_is_repointed_at_the_recreated_pool():
    settings = {
        "pool": {
            "customizeComputeEnabled": True,
            "defaultPool": {"name": "pool1", "type": "Workspace", "id": "old-pool"},
            "starterPool": {"maxNodeCount": 3, "maxExecutors": 1},
        },
        "automaticLog": {"enabled": True},
        "job": {"sessionTimeoutInMinutes": 20},
    }
    payload, warnings = spark.build_settings_payload(settings, {"old-pool": "new-pool"})

    assert warnings == []
    assert payload["pool"]["defaultPool"] == {"id": "new-pool"}
    assert payload["pool"]["starterPool"] == {"maxNodeCount": 3, "maxExecutors": 1}
    assert payload["automaticLog"] == {"enabled": True}
    assert payload["job"] == {"sessionTimeoutInMinutes": 20}


def test_a_default_pool_that_did_not_transfer_is_dropped_and_reported():
    settings = {"pool": {"defaultPool": {"name": "pool1", "type": "Workspace", "id": "old-pool"}}}
    payload, warnings = spark.build_settings_payload(settings, {})

    # Better to fall back to the starter pool than to point at the old region.
    assert "defaultPool" not in payload.get("pool", {})
    assert len(warnings) == 1 and "falls back to the starter pool" in warnings[0]


def test_the_starter_pool_default_is_carried_across_by_name():
    settings = {"pool": {"defaultPool": {"name": "Starter Pool", "type": "Workspace", "id": "x"}}}
    payload, warnings = spark.build_settings_payload(settings, {})

    assert payload["pool"]["defaultPool"] == {"name": "Starter Pool", "type": "Workspace"}
    assert warnings == []


def test_the_default_environment_is_carried_by_name():
    # The migrated environment keeps its name, so the reference still resolves.
    settings = {"environment": {"name": "environment1", "runtimeVersion": "1.3"}}
    payload, _ = spark.build_settings_payload(settings, {})
    assert payload["environment"] == {"name": "environment1", "runtimeVersion": "1.3"}


def test_an_empty_default_environment_is_not_sent():
    payload, _ = spark.build_settings_payload({"environment": {"name": ""}}, {})
    assert "environment" not in payload


def test_empty_settings_produce_no_patch():
    payload, warnings = spark.build_settings_payload({}, {})
    assert payload == {} and warnings == []
