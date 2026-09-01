"""Item types whose migration is not a plain definition round trip.

Most items are copied by exporting their definition, rewriting the ids inside it, and
creating the copy. These four need something else first:

* a Snowflake database is created from a payload rather than from its definition, because the
  definition's own fields are documented as having to be empty on create;
* a mirrored Azure Databricks catalog and a Reflex both describe something *live*, so they
  are created switched off to avoid two of them acting on the same source at once;
* a Spark job definition keeps its code in extra definition parts that only appear if the
  right format is requested.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from fabshuffle.fabric.definitions import decode_json_part, find_part, part, replace_part

logger = logging.getLogger(__name__)

# Spark job definitions come in two formats that share a payload schema *and* a part
# filename. Only V2 carries the Main/ and Libs/ parts holding the actual code, so exporting
# with the default V1 silently produces a job whose executable does not exist.
SPARK_JOB_DEFINITION_FORMAT = "SparkJobDefinitionV2"
SPARK_JOB_PAYLOAD_PART = "SparkJobDefinitionV1.json"

SNOWFLAKE_PROPERTIES_PART = "SnowflakeDatabaseProperties.json"
ADB_CATALOG_PART = "definition.json"
REFLEX_ENTITIES_PART = "ReflexEntities.json"


# --------------------------------------------------------------- Snowflake database


def snowflake_creation_payload(
    item: Mapping[str, Any],
    parts: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Build the payload that points a new item at the same Snowflake database.

    The definition article marks ``snowflakeDatabaseName`` as required and then says it "should
    be empty in create requests", which only makes sense for creating *with a definition*. The
    creation payload takes exactly the two fields the definition holds, so that is the route
    used here and the definition is read only for its values.
    """
    properties = (item.get("properties") or {}).copy()
    payload_part = find_part(list(parts), SNOWFLAKE_PROPERTIES_PART)
    if payload_part:
        properties = {**decode_json_part(payload_part["payload"]), **properties}

    database = properties.get("snowflakeDatabaseName") or ""
    connection = properties.get("connectionId") or ""
    if not database or not connection:
        return None
    # The connection is tenant scoped, so the copy follows the same one without any rewriting.
    return {"snowflakeDatabaseName": database, "connectionId": connection}


# ------------------------------------------- mirrored Azure Databricks catalog


def disable_catalog_autosync(parts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    """Create the catalog with syncing off, and report what it was set to.

    Two Fabric catalogs syncing the same Databricks Unity Catalog at once is the same problem
    as two mirrors on one database, so the operator turns it on once they are ready to cut
    over rather than having both live at the moment of migration.
    """
    payload_part = find_part(parts, ADB_CATALOG_PART)
    if not payload_part:
        return parts, ""

    content = decode_json_part(payload_part["payload"])
    was = str(content.get("autoSync") or "")
    if was.casefold() == "disabled":
        return parts, was

    content["autoSync"] = "Disabled"
    return replace_part(parts, ADB_CATALOG_PART, content), was


# ------------------------------------------------------------------------ Reflex


def disable_reflex_rules(parts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Switch every rule off, and report how many were running.

    A Reflex reacts to data. Copying a live one leaves two of them watching the same source,
    so every alert is sent twice and every pipeline it triggers runs twice.

    ``ReflexEntities.json`` is a top level array of entities; rules are ``timeSeriesView-v1``
    entities whose payload definition has type ``Rule``. Only ``settings.shouldRun`` is
    touched. The entity ids in this document wire the Reflex to itself and mean nothing
    outside it, so they are left exactly as they are.
    """
    payload_part = find_part(parts, REFLEX_ENTITIES_PART)
    if not payload_part:
        return parts, 0

    entities = decode_json_part(payload_part["payload"])
    if not isinstance(entities, list):
        return parts, 0

    running = 0
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        payload = entity.get("payload")
        if not isinstance(payload, dict):
            continue
        definition = payload.get("definition")
        if not isinstance(definition, dict) or definition.get("type") != "Rule":
            continue
        settings = definition.get("settings")
        if not isinstance(settings, dict):
            settings = {}
            definition["settings"] = settings
        if settings.get("shouldRun") is not False:
            running += 1
        settings["shouldRun"] = False

    if not running:
        return parts, 0
    return replace_part(parts, REFLEX_ENTITIES_PART, entities), running


# ------------------------------------------------------- Spark job definition


def spark_job_warnings(parts: Iterable[Mapping[str, Any]], source_workspace_id: str) -> list[str]:
    """Report the parts of a Spark job definition that a copy cannot carry.

    Inline upload does not accept ``.jar`` files, so a JVM job references its jar by an
    absolute path instead. If that path is inside the workspace being migrated then the id in
    it gets rewritten to the new workspace, where nothing ever wrote the file.
    """
    payload_part = find_part(list(parts), SPARK_JOB_PAYLOAD_PART)
    if not payload_part:
        return []

    content = decode_json_part(payload_part["payload"])
    references = [content.get("executableFile") or ""]
    references.extend(content.get("additionalLibraryUris") or [])

    warnings: list[str] = []
    if any(str(reference).casefold().endswith(".jar") for reference in references):
        warnings.append(
            "it runs a .jar, which cannot be carried inside the definition. Upload the jar to "
            "the new workspace and repoint the job at it"
        )
    if any(source_workspace_id.casefold() in str(reference).casefold() for reference in references):
        warnings.append(
            "it refers to a file by a path inside the workspace being migrated. That path is "
            "repointed at the new workspace, where the file does not exist, so upload it"
        )
    return warnings


def encode_part(path: str, content: Mapping[str, Any] | list[Any]) -> dict[str, str]:
    """A definition part carrying JSON, for tests and for rebuilding a payload."""
    return part(path, content)


__all__ = [
    "ADB_CATALOG_PART",
    "REFLEX_ENTITIES_PART",
    "SNOWFLAKE_PROPERTIES_PART",
    "SPARK_JOB_DEFINITION_FORMAT",
    "SPARK_JOB_PAYLOAD_PART",
    "disable_catalog_autosync",
    "disable_reflex_rules",
    "encode_part",
    "snowflake_creation_payload",
    "spark_job_warnings",
]
