"""Live, read-only data-plane metadata readers. Never used during standby apply."""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import quote

import httpx
from azure.kusto.data import KustoClient

from fabshuffle.auth import TokenProvider
from fabshuffle.bcdr.contracts import ItemIdentity, logical_path, reject_embedded_secrets
from fabshuffle.fabric.client import FabricApiError, FabricError
from fabshuffle.transfer import sqlschema
from fabshuffle.transfer.kql import kusto_connection

MAX_METADATA_BYTES = 64 * 1024 * 1024
MAX_METADATA_ROWS = 100_000


def inspect_schema_archive(data: bytes) -> dict[str, bytes]:
    """Bound decompression and inspect credentials before cataloging an opaque DACPAC."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        names = [entry.filename for entry in entries]
        if len(names) != len(set(names)) or "model.xml" not in names:
            raise FabricError("Captured DACPAC needs one model.xml and unique archive members")
        if len(entries) > 10_000 or sum(entry.file_size for entry in entries) > MAX_METADATA_BYTES:
            raise FabricError("Expanded DACPAC exceeds the metadata inspection budget")
        inspected: dict[str, bytes] = {}
        for entry in entries:
            content = archive.read(entry)
            reject_embedded_secrets(content)
            if entry.filename.lower().endswith((".xml", ".sql")):
                if content.startswith((b"\xff\xfe", b"\xfe\xff")):
                    content = content.decode("utf-16").encode("utf-8")
                reject_embedded_secrets(content)
                inspected[entry.filename] = content
        return inspected


class MetadataReaders:
    """Real default readers; tests may override individual data-plane methods.

    OneLake table metadata uses Storage tokens, not Fabric tokens:
    https://learn.microsoft.com/fabric/onelake/table-apis/table-apis-overview
    """

    def __init__(
        self,
        tokens: TokenProvider,
        *,
        transport: httpx.BaseTransport | None = None,
        max_bytes: int = MAX_METADATA_BYTES,
        max_rows: int = MAX_METADATA_ROWS,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> None:
        if max_bytes < 1 or max_rows < 1:
            raise ValueError("Metadata capture limits must be positive")
        self.tokens = tokens
        self.transport = transport
        self.max_bytes = max_bytes
        self.max_rows = max_rows
        self.cancel_requested = cancel_requested

    def _json(self, url: str, params: dict[str, str]) -> tuple[dict[str, Any], httpx.Headers]:
        with (
            httpx.Client(transport=self.transport, timeout=120, follow_redirects=False) as client,
            client.stream(
                "GET",
                url,
                params=params,
                headers={
                    "Authorization": f"Bearer {self.tokens.storage_token()}",
                    "x-ms-version": "2021-06-08",
                },
            ) as response,
        ):
            body = bytearray()
            for chunk in response.iter_bytes():
                if len(body) + len(chunk) > self.max_bytes:
                    raise FabricError(f"Metadata response exceeds {self.max_bytes} bytes: {url}")
                body.extend(chunk)
            if not response.is_success:
                raise FabricApiError("GET", str(response.url), response.status_code, body.decode("utf-8"))
            document = json.loads(body)
            if not isinstance(document, dict):
                raise FabricError(f"Metadata endpoint returned a non-object response: {url}")
            return document, response.headers

    def _table_pages(
        self,
        identity: ItemIdentity,
        collection: str,
        **params: str,
    ) -> list[dict[str, Any]]:
        # The Delta metadata API returns next_page_token, NOT Fabric continuationToken.
        # https://learn.microsoft.com/fabric/onelake/table-apis/delta-table-apis-get-started
        url = (
            "https://onelake.table.fabric.microsoft.com/delta/"
            f"{identity.workspace_id}/{identity.item_id}/api/2.1/unity-catalog/{collection}"
        )
        query = {"catalog_name": identity.item_id, **params}
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        while True:
            document, _ = self._json(url, query)
            values = document.get(collection)
            if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
                raise FabricError(f"Missing '{collection}' metadata; an empty inventory cannot be assumed")
            rows.extend(values)
            if len(rows) > self.max_rows:
                raise FabricError("Metadata inventory exceeds the configured row limit")
            token = document.get("next_page_token")
            if not token:
                return rows
            if not isinstance(token, str) or token in seen:
                raise FabricError("Repeated or invalid OneLake metadata page token; capture is incomplete")
            seen.add(token)
            query["page_token"] = token

    def lakehouse_tables(self, identity: ItemIdentity) -> dict[str, Any]:
        schemas = self._table_pages(identity, "schemas")
        tables: list[dict[str, Any]] = []
        names: set[str] = set()
        for schema in schemas:
            name = schema.get("name")
            if not isinstance(name, str) or not name or name in names:
                raise FabricError("OneLake returned a missing or duplicate schema name")
            names.add(name)
            for table in self._table_pages(identity, "tables", schema_name=name):
                if not table.get("name") or not table.get("storage_location"):
                    raise FabricError("OneLake table metadata omitted its name or actual storage location")
                if table.get("schema_name") != name:
                    raise FabricError("OneLake table metadata belongs to a different schema")
                tables.append(table)
                if len(tables) > self.max_rows:
                    raise FabricError("Table inventory exceeds the configured row limit")
        keys = [(table["schema_name"], table["name"]) for table in tables]
        if len(keys) != len(set(keys)):
            raise FabricError("OneLake returned duplicate table identities")
        return {"schemas": schemas, "tables": tables}

    def files_inventory(self, identity: ItemIdentity) -> list[dict[str, Any]]:
        """List names/lengths/ETags only, not business file contents."""
        url = f"https://onelake.dfs.fabric.microsoft.com/{identity.workspace_id}"
        root = f"{identity.item_id}/Files"
        query = {"resource": "filesystem", "directory": root, "recursive": "true", "maxResults": "1000"}
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        while True:
            document, headers = self._json(url, query)
            rows = document.get("paths")
            if not isinstance(rows, list):
                raise FabricError("OneLake omitted its Files listing; no empty-files assumption is safe")
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("name"), str):
                    raise FabricError("OneLake returned a file without a path")
                if not row["name"].startswith(root + "/"):
                    raise FabricError("OneLake returned a path outside the captured Files root")
                logical_path(row["name"][len(root) + 1 :])
                result.append(row)
            if len(result) > self.max_rows:
                raise FabricError("Files inventory exceeds the configured row limit")
            token = headers.get("x-ms-continuation")
            if not token:
                return result
            if token in seen:
                raise FabricError("Repeated OneLake Files continuation token; capture is incomplete")
            seen.add(token)
            query["continuation"] = token

    def sql_metadata(self, server: str, database: str) -> dict[str, Any]:
        """Capture names, permissions and unresolved cross-database dependencies."""
        if not server or not database:
            raise FabricError("Capture needs the source SQL server and its actual database catalog")
        queries = {
            "tables": (
                "SELECT s.name AS schema_name,t.name AS table_name "
                "FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.is_ms_shipped=0"
            ),
            "schemas": "SELECT name FROM sys.schemas",
            "sql_dependencies": (
                "SELECT referenced_server_name,referenced_database_name,referenced_schema_name,"
                "referenced_entity_name,is_caller_dependent,is_ambiguous "
                "FROM sys.sql_expression_dependencies"
            ),
            "sql_permissions": (
                "SELECT p.name AS principal_name,p.type_desc AS principal_type,"
                "d.state_desc,d.permission_name,d.class_desc,d.major_id,d.minor_id "
                "FROM sys.database_permissions d JOIN sys.database_principals p "
                "ON d.grantee_principal_id=p.principal_id"
            ),
            "sql_roles": (
                "SELECT r.name AS role_name,m.name AS member_name "
                "FROM sys.database_role_members rm "
                "JOIN sys.database_principals r ON rm.role_principal_id=r.principal_id "
                "JOIN sys.database_principals m ON rm.member_principal_id=m.principal_id"
            ),
        }
        result: dict[str, Any] = {}
        with closing(sqlschema.connect(server, database, self.tokens)) as connection:
            cursor = connection.cursor()
            self.tokens.assert_active()
            permission = cursor.execute(
                "SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION')"
            ).fetchone()
            self.tokens.assert_active()
            if not permission or permission[0] != 1:
                raise FabricError("Grant source database VIEW DEFINITION for complete SQL metadata capture")
            for key, query in queries.items():
                self.tokens.assert_active()
                cursor.execute(query)
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchmany(self.max_rows + 1)
                self.tokens.assert_active()
                if len(rows) > self.max_rows:
                    raise FabricError(f"SQL metadata '{key}' exceeds the configured row limit")
                result[key] = [dict(zip(columns, row, strict=True)) for row in rows]
        return result

    def sql_schema(self, server: str, database: str) -> bytes:
        """Stage a schema-only DACPAC transiently; the Warehouse owns its durable bytes."""
        self.tokens.assert_active()
        with TemporaryDirectory(prefix="fab-bcdr-schema-") as folder:
            root = Path(folder)
            path = sqlschema.extract_dacpac(
                server=server,
                database=database,
                principal=self.tokens.principal,
                tokens=self.tokens,
                output=root / "source.dacpac",
                staging_root=root,
                max_disk_staging_bytes=self.max_bytes,
                cancel_requested=self.cancel_requested,
            )
            self.tokens.assert_active()
            if path.stat().st_size > self.max_bytes:
                raise FabricError("SQL schema payload exceeds the configured metadata byte limit")
            return path.read_bytes()

    def kql_metadata(self, cluster: str, database: str, *, follower: bool) -> dict[str, Any]:
        if not cluster or not database:
            raise FabricError("Capture needs the source KQL query endpoint and database ID")
        self.tokens.assert_active()
        builder = kusto_connection(cluster, self.tokens, use_token_provider=True)
        result: dict[str, Any] = {}
        with KustoClient(builder) as client:
            self.tokens.assert_active()
            identity = client.execute_mgmt(database, ".show database identity")
            self.tokens.assert_active()
            identity_rows = list(identity.primary_results[0])
            if len(identity_rows) != 1 or not identity_rows[0]["DatabaseName"]:
                raise FabricError("KQL did not return an unambiguous actual database identity")
            actual_database = str(identity_rows[0]["DatabaseName"])
            name = f"[{json.dumps(actual_database)}]"
            result["database_name"] = actual_database
            # These are the documented CSL options, not the nonexistent IncludePolicies switch.
            # https://learn.microsoft.com/kusto/management/show-schema-database
            commands = {
                "schema": f".show database {name} schema as csl script with "
                "(IncludeEncodingPolicies=true, IncludeSecuritySettings=false, "
                "IncludeIngestionMappings=true, ShowObfuscatedStrings=false)",
                "schema_json": f".show database {name} schema as json",
                "security_schema": f".show database {name} schema as csl script with "
                "(IncludeSecuritySettings=true, ShowObfuscatedStrings=false)",
                "policies": ".show database policies",
                "principals": f".show database {name} principals",
            }
            if follower:
                commands["follower"] = f".show follower database {name}"
                result["follower_source_qualification"] = (
                    "Unverified: Fabric support and recreatable source identity are not established by "
                    "the ADX follower override command. Preserve rows; never infer a source URI."
                )
            for key, command in commands.items():
                self.tokens.assert_active()
                response = client.execute_mgmt(actual_database, command)
                self.tokens.assert_active()
                rows = list(response.primary_results[0])
                if len(rows) > self.max_rows:
                    raise FabricError(f"KQL metadata '{key}' exceeds the configured row limit")
                result[key] = [
                    {
                        column.column_name: row[column.column_name]
                        for column in response.primary_results[0].columns
                    }
                    for row in rows
                ]
        return result


def file_api_path(path: str) -> str:
    """Validate before constructing a file API URL, including encoded separators."""
    return quote(logical_path(path), safe="/")
