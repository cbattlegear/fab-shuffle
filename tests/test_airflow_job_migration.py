"""Migrating an Apache Airflow job.

The definition is configuration only. A job created from it alone looks migrated in the item
list and has nothing to run, so the DAG files have to follow, over a separate beta API that
speaks file bytes rather than JSON.
"""

from __future__ import annotations

import json

import pytest

from fabshuffle import journal, orchestrator
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
        self.contents: dict[str, bytes] = {}

    def list_all(self, path, params=None, value_key="value"):
        if path == f"workspaces/{SOURCE_WS}/items":
            return [JOB_ITEM]
        if path.endswith("/files"):
            if self.flags.get("list_fails"):
                raise FabricApiError("GET", path, 403, '{"errorCode":"Denied","message":"grant file access"}')
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
            response.content = self.contents.get(path.rsplit("/files/", 1)[-1], b"print('hello')")
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


def test_resume_adopts_airflow_and_continues_incomplete_files():
    client = FakeClient()
    ctx = make_ctx(client)
    ctx.prior = journal.Replay(id_map={JOB: NEW_JOB})
    ctx.id_map.update(ctx.prior.id_map)
    migrated, _ = orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _: None)
    assert migrated == 1
    assert client.created == []
    assert client.written[0][0] == (
        f"workspaces/{TARGET_WS}/apacheAirflowJobs/{NEW_JOB}/files/dags/my_dag.py"
    )


def test_resume_keeps_finished_airflow_files():
    client = FakeClient()
    ctx = make_ctx(client)
    ctx.prior = journal.Replay(id_map={JOB: NEW_JOB}, data_done={(JOB, "airflow-files", "")})
    ctx.id_map.update(ctx.prior.id_map)
    orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _: None)
    assert client.created == client.written == []


def test_rebinding_airflow_refreshes_even_previously_finished_files():
    client = FakeClient()
    ctx = make_ctx(client)
    ctx.prior = journal.Replay(id_map={JOB: NEW_JOB}, data_done={(JOB, "airflow-files", "")})
    ctx.id_map.update(ctx.prior.id_map)
    ctx.refresh_needed.add(JOB)
    orchestrator._migrate_airflow_jobs(ctx, "orchestration", [JOB_ITEM], lambda _: None)
    assert client.created == []
    assert len(client.written) == 1
    assert JOB not in ctx.refresh_needed


def test_every_file_request_marks_itself_as_beta():
    client = FakeClient()
    migrate(client)

    assert all(params.get("beta") == "true" for _, path, params in client.requests if "/files" in path)


def test_a_path_with_characters_needing_encoding_is_encoded():
    client = FakeClient(files=[{"filePath": "dags/my dag+1.py", "sizeInBytes": 10}])
    migrate(client)

    assert client.written[0][0].endswith("/files/dags/my%20dag%2B1.py")


def test_a_job_whose_files_cannot_be_listed_is_not_created():
    client = FakeClient(list_fails=True)
    _, migrated, warnings = migrate(client)

    assert migrated == 0 and client.created == []
    assert any("Denied" in w and "grant file access" in w for w in warnings)


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


SOURCE_GUID = "aaaabbbb-1111-2222-3333-444455556666"
TARGET_GUID = "bbbbcccc-1111-2222-3333-444455556666"
NOTEBOOK_GUID = "ccccdddd-1111-2222-3333-444455556666"
NEW_NOTEBOOK_GUID = "ddddeeee-1111-2222-3333-444455556666"


def test_literal_urls_config_and_binary_files_are_preflighted_before_create():
    client = FakeClient(
        files=[{"filePath": "dags/main.py"}, {"filePath": "libs/data.bin"}],
        config={"environmentVariables": {"WORKSPACE": SOURCE_GUID, "NOTEBOOK": NOTEBOOK_GUID}},
    )
    literal = f"https://api.fabric.microsoft.com/v1/workspaces/{SOURCE_GUID.upper()}/items/{NOTEBOOK_GUID}"
    client.contents = {"dags/main.py": literal.encode(), "libs/data.bin": b"\xff" + literal.encode()}
    ctx = make_ctx(client)
    ctx.id_map.update({SOURCE_GUID: TARGET_GUID, NOTEBOOK_GUID: NEW_NOTEBOOK_GUID})
    ctx.source_items[NOTEBOOK_GUID] = {
        "id": NOTEBOOK_GUID, "displayName": "Daily notebook", "type": "Notebook",
    }
    count, warnings = orchestrator._migrate_airflow_jobs(
        ctx, "orchestration", [JOB_ITEM], lambda _m: None
    )
    assert count == 1
    assert sent_config(client)["environmentVariables"] == {
        "WORKSPACE": TARGET_GUID, "NOTEBOOK": NEW_NOTEBOOK_GUID,
    }
    assert client.written[0][1] == literal.replace(
        SOURCE_GUID.upper(), TARGET_GUID
    ).replace(NOTEBOOK_GUID, NEW_NOTEBOOK_GUID).encode()
    assert client.written[1][1] == client.contents["libs/data.bin"]
    assert not any("did not copy" in warning for warning in warnings)


@pytest.mark.parametrize("in_config", [False, True])
def test_an_unmapped_literal_dependency_prevents_job_creation(in_config):
    client = FakeClient(config={"notebookId": NOTEBOOK_GUID} if in_config else {})
    client.contents["dags/my_dag.py"] = (
        b"print('hello')" if in_config else
        f"run_notebook('{SOURCE_GUID}', '{NOTEBOOK_GUID.upper()}')".encode()
    )
    ctx = make_ctx(client)
    ctx.id_map[SOURCE_GUID] = TARGET_GUID
    ctx.source_items[NOTEBOOK_GUID] = {
        "id": NOTEBOOK_GUID, "displayName": "Daily notebook", "type": "Notebook",
    }
    count, warnings = orchestrator._migrate_airflow_jobs(
        ctx, "orchestration", [JOB_ITEM], lambda _m: None
    )
    assert count == 0 and client.created == [] and client.written == []
    assert "Daily notebook" in warnings[0] and "retry" in warnings[0]
    assert JOB not in ctx.id_map


def test_an_unreadable_text_file_prevents_job_creation():
    client = FakeClient()
    client.contents["dags/my_dag.py"] = b"\xff"
    _, count, warnings = migrate(client)
    assert count == 0 and not client.created
    assert "UTF-8" in warnings[0]


def test_copying_without_preflight_or_reference_context_is_refused():
    client = FakeClient()
    count, warnings = airflow.copy_files(
        client, source_workspace_id=SOURCE_WS, source_job_id=JOB,
        target_workspace_id=TARGET_WS, target_job_id=NEW_JOB, job_name="NightlyDags",
    )
    assert count == 0 and not client.written
    assert "preflighted files" in warnings[0]


@pytest.mark.parametrize("where", ["config", "dag", "both"])
def test_fresh_job_self_references_are_refused_before_creation(where):
    job = {**JOB_ITEM, "id": NOTEBOOK_GUID}
    config = {"environmentVariables": {"JOB_ID": NOTEBOOK_GUID.upper()}} if where != "dag" else {}
    client = FakeClient(config=config)
    if where != "config":
        client.contents["dags/my_dag.py"] = (
            f"url = 'https://api.fabric.microsoft.com/v1/workspaces/{SOURCE_GUID}"
            f"/items/{NOTEBOOK_GUID.upper()}'"
        ).encode()
    ctx = make_ctx(client)
    ctx.id_map[SOURCE_GUID] = TARGET_GUID
    ctx.source_items[job["id"]] = job
    count, warnings = orchestrator._migrate_airflow_jobs(
        ctx, "orchestration", [job], lambda _m: None,
    )
    assert count == 0 and not client.created and not client.written
    assert job["id"] not in ctx.id_map
    assert "own source job ID" in warnings[0]
    assert "Remove hardcoded self references" in warnings[0]
    assert "retry" in warnings[0]


def test_existing_target_self_references_are_preflighted_and_rebound():
    client = FakeClient(files=[{"filePath": "dags/my_dag.py"}])
    literal = f"/workspaces/{SOURCE_GUID}/items/{NOTEBOOK_GUID.upper()}"
    client.contents["dags/my_dag.py"] = literal.encode()
    mapping = {SOURCE_GUID: TARGET_GUID, NOTEBOOK_GUID: NEW_NOTEBOOK_GUID}
    config = [part("ApacheAirflowJob.json", {"environmentVariables": {"JOB_ID": NOTEBOOK_GUID}})]
    airflow.preflight_references(
        config, source_job_id=NOTEBOOK_GUID, job_name="NightlyDags", id_map=mapping,
        source_items={},
    )
    prepared = airflow.preflight_files(
        client, source_workspace_id=SOURCE_GUID, source_job_id=NOTEBOOK_GUID,
        job_name="NightlyDags", id_map=mapping, source_items={},
    )
    count, warnings = airflow.copy_files(
        client, source_workspace_id=SOURCE_GUID, source_job_id=NOTEBOOK_GUID,
        target_workspace_id=TARGET_GUID, target_job_id=NEW_NOTEBOOK_GUID,
        job_name="NightlyDags", prepared_files=prepared,
    )
    assert count == 1 and not warnings
    assert client.written[0][1] == f"/workspaces/{TARGET_GUID}/items/{NEW_NOTEBOOK_GUID}".encode()
