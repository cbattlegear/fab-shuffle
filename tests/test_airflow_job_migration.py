"""Migrating an Apache Airflow job.

The definition is configuration only. A job created from it alone looks migrated in the item
list and has nothing to run, so the DAG files have to follow, over a separate beta API that
speaks file bytes rather than JSON.
"""

from __future__ import annotations

import json

from fabshuffle import orchestrator
from fabshuffle.auth import ServicePrincipal
from fabshuffle.fabric import airflow
from fabshuffle.fabric.client import FabricApiError
from fabshuffle.fabric.definitions import decode_payload, part
from fabshuffle.run import MigrationRun

SOURCE_WS = "ws-source"
TARGET_WS = "ws-target"
JOB = "airflow-source"
NEW_JOB = "airflow-target"

CONFIG = {
    "computeProperties": {"location": "Central US", "nodeSize": "Small"},
    "environmentVariables": {"LAKEHOUSE": "lh-source"},
}
JOB_ITEM = {"id": JOB, "displayName": "NightlyDags", "type": "ApacheAirflowJob"}


class FakeClient:
    def __init__(self, files=None, config=None, **flags) -> None:
        self.files = files if files is not None else [{"filePath": "dags/my_dag.py", "sizeInBytes": 120}]
        self.config = config if config is not None else CONFIG
        self.flags = flags
        self.written: list[tuple[str, bytes]] = []
        self.requests: list[tuple[str, str, dict]] = []
        self.created: list[dict] = []

    def list_all(self, path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/items":
            return [JOB_ITEM]
        if path.endswith("/files"):
            if self.flags.get("list_fails"):
                raise FabricApiError("GET", path, 403, "{}")
            assert (params or {}).get("beta") == "true", "the file APIs are beta"
            return self.files
        return []

    def get(self, path, params=None):
        raise AssertionError(f"unexpected GET {path}")

    def post(self, path, json=None, params=None, wait=True):
        if path.endswith("/getDefinition"):
            return {
                "definition": {
                    "parts": [part("ApacheAirflowJob.json", self.config), part(".platform", "{}")]
                }
            }
        if path.endswith("/items"):
            self.created.append(json or {})
            return {"id": NEW_JOB}
        return {}

    def request(self, method, path, *, content=None, params=None, headers=None, expected=None):
        self.requests.append((method, path, params or {}))
        if method == "GET":
            if self.flags.get("read_fails"):
                raise FabricApiError("GET", path, 404, "{}")

            class Response:
                pass

            response = Response()
            response.content = b"print('hello')"
            return response
        if self.flags.get("write_fails"):
            raise FabricApiError("PUT", path, 400, "{}")
        self.written.append((path, content))
        return None

    def delete(self, path, params=None):
        return None


def make_ctx(client, region="West US 2"):
    plan = orchestrator.MigrationPlan(
        capacity_id="cap",
        capacity_name="F64",
        capacity_region="westus2",
        capacity_display_region=region,
        source_workspace_id=SOURCE_WS,
        source_workspace_name="src",
        target_workspace_name="dst",
    )
    ctx = orchestrator._Context(
        client=client,
        tokens=object(),
        principal=ServicePrincipal("t", "c", "s"),
        plan=plan,
        run=MigrationRun(source_workspace_name="src", capacity_name="F64"),
        scratch_dir=None,
    )
    ctx.target_workspace_id = TARGET_WS
    ctx.run.start_step("orchestration", "Migrating")
    return ctx


def migrate(client, region="West US 2"):
    ctx = make_ctx(client, region)
    return ctx, *orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _m: None)


def sent_config(client):
    body = client.created[0]
    payload = next(
        p for p in body["definition"]["parts"] if p["path"] == "ApacheAirflowJob.json"
    )["payload"]
    return json.loads(decode_payload(payload))


# ------------------------------------------------------------ configuration


def test_the_compute_location_is_moved_to_the_new_region():
    client = FakeClient()
    migrate(client)

    # A display string, not the normalised region, because that is what the job stores.
    assert sent_config(client)["computeProperties"]["location"] == "West US 2"


def test_an_unknown_region_leaves_the_location_alone():
    client = FakeClient()
    migrate(client, region="")

    assert sent_config(client)["computeProperties"]["location"] == "Central US"


def test_a_config_without_a_location_is_not_given_one():
    client = FakeClient(config={"computeProperties": {"nodeSize": "Small"}})
    migrate(client)

    assert sent_config(client)["computeProperties"] == {"nodeSize": "Small"}


def test_the_source_platform_part_is_not_carried_over():
    client = FakeClient()
    migrate(client)

    assert all(p["path"] != ".platform" for p in client.created[0]["definition"]["parts"])


def test_secrets_are_reported_because_their_values_never_come_back():
    client = FakeClient(config={"secrets": [{"name": "api_key"}]})
    _, _, warnings = migrate(client)

    assert any("re-enter them" in w for w in warnings)


def test_environment_variables_are_surfaced_rather_than_silently_rewritten():
    _, _, warnings = migrate(FakeClient())

    assert any("environment variable" in w for w in warnings)


# -------------------------------------------------------------------- files


def test_dag_files_are_copied_into_the_new_job():
    client = FakeClient()
    _, migrated, warnings = migrate(client)

    assert (migrated, warnings and [w for w in warnings if "did not copy" in w]) == (1, [])
    path, content = client.written[0]
    assert path == f"workspaces/{TARGET_WS}/apacheAirflowJobs/{NEW_JOB}/files/dags/my_dag.py"
    assert content == b"print('hello')"


def test_every_file_request_marks_itself_as_beta():
    client = FakeClient()
    migrate(client)

    assert all(params.get("beta") == "true" for _, path, params in client.requests if "/files" in path)


def test_a_path_with_characters_needing_encoding_is_encoded():
    client = FakeClient(files=[{"filePath": "dags/my dag+1.py", "sizeInBytes": 10}])
    migrate(client)

    assert client.written[0][0].endswith("/files/dags/my%20dag%2B1.py")


def test_a_job_whose_files_cannot_be_listed_says_it_has_no_dags():
    client = FakeClient(list_fails=True)
    _, migrated, warnings = migrate(client)

    # The item still exists, so the failure has to be loud: an Airflow job with no DAGs is
    # not obviously broken until someone runs it.
    assert migrated == 1
    assert any("it has no DAGs" in w for w in warnings)


def test_one_file_failing_does_not_stop_the_others():
    client = FakeClient(
        files=[
            {"filePath": "dags/a.py", "sizeInBytes": 10},
            {"filePath": "dags/b.py", "sizeInBytes": 10},
        ],
        write_fails=True,
    )
    _, _, warnings = migrate(client)

    assert len([w for w in warnings if "did not copy" in w]) == 2


def test_a_file_too_large_to_be_a_dag_is_reported_not_streamed():
    client = FakeClient(files=[{"filePath": "data/dump.parquet", "sizeInBytes": airflow.MAX_FILE_BYTES + 1}])
    _, _, warnings = migrate(client)

    assert any("larger than" in w for w in warnings)
    assert client.written == []


def test_a_job_with_no_files_at_all_is_still_called_out():
    client = FakeClient(files=[])
    _, _, warnings = migrate(client)

    assert any("no files to copy" in w for w in warnings)


# ------------------------------------------------------------------ failure


def test_a_job_that_cannot_be_created_is_reported_and_the_phase_goes_on():
    client = FakeClient()

    def post(path, json=None, params=None, wait=True):
        if path.endswith("/items"):
            raise FabricApiError("POST", path, 400, '{"errorCode":"Nope","message":"no"}')
        return FakeClient.post(client, path, json, params, wait)

    client.post = post
    _, migrated, warnings = migrate(client)

    assert migrated == 0
    assert any("NightlyDags" in w for w in warnings)
    assert client.written == []
