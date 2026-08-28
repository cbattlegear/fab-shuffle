# Fab Shuffle

Region transfer tool for Microsoft Fabric workspaces.

Fab Shuffle recreates a Fabric workspace on a capacity in a different region and moves the
data across. Fabric blocks reassigning a workspace that contains Fabric items to a capacity
in another region, so the only way to "move" a workspace is to rebuild it — that is what
this tool automates.

## Current state

v2 is a Python application driven almost entirely by the
[Fabric REST API](https://learn.microsoft.com/en-us/rest/api/fabric/articles/), wrapped in a
small web UI that walks you through the move. (v1 was a PowerShell script around the `fab`
CLI; it still lives on the `main` branch's history.)

### What gets migrated

| Item | Schema | Data | Notes |
| --- | --- | --- | --- |
| Lakehouse | ✅ | ✅ tables + files | Schema-enabled lakehouses supported |
| Lakehouse SQL analytics endpoint | ✅ | n/a | Views, procedures, and functions |
| Warehouse | ✅ | ✅ | Collation preserved |
| Eventhouse | ✅ | n/a | |
| KQL database (`ReadWrite`) | ✅ | ✅ | |
| OneLake shortcuts | ✅ | n/a | Internal targets remapped to the new workspace |
| Workspace folders | ✅ | n/a | Hierarchy recreated |
| Workspace permissions | ✅ | n/a | Role assignments replayed |

Anything else in the source workspace is reported on the review screen as not yet migrated.
KQL *shortcut* (follower) databases are skipped, because Fabric does not expose the follower
source through the API.

## Usage

### Requirements

- Docker
- A service principal

Fab Shuffle uses a service principal to automate data movement and to avoid a pile of
permission problems.

Steps to set up your service principal:

1. [Create a service principal](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal#register-an-application-with-microsoft-entra-id-and-create-a-service-principal)
   and a [client secret](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal#option-3-create-a-new-client-secret).
   **Record your tenant ID, client ID, and client secret.**
2. Add the service principal to an [Entra ID group](https://learn.microsoft.com/en-us/entra/fundamentals/quickstart-create-group-add-members).
3. Enable [service principal access to Fabric APIs](https://learn.microsoft.com/en-us/fabric/admin/enable-service-principal-admin-apis)
   in the Fabric admin portal, including "Service principals can create workspaces,
   connections, and deployment pipelines".

   ![Screenshot showing Service Principal settings in Fabric Admin Portal](docs/images/service_principal_fabric.png)

4. Give the service principal the Reader role on the target Fabric capacity in Azure.
5. Give the service principal Admin on the Fabric workspace you want to move.
6. Make the service principal Owner of any connections used by your shortcuts.

### Run it

```bash
docker run --rm -p 8080:8080 ghcr.io/cbattlegear/fab-shuffle:latest
```

Open <http://localhost:8080> and follow the wizard:

1. **Sign in** with the service principal's tenant ID, client ID, and secret.
2. **Pick the target capacity** — its region is the destination region.
3. **Pick the source workspace.**
4. **Review** the plan, adjust the new workspace name, and choose what to copy.
5. **Migrate**, watching each step report progress live, then delete the temporary artifacts.

Credentials are held in the container's memory for the life of the session and are never
written to disk.

> Lakehouse file transfer stages files on local disk inside the container. Mount a volume at
> `/app/local` if you are moving more data than the container's writable layer can hold.

### Configuration

Every setting has a sensible default; override with environment variables when needed.

| Variable | Default | Purpose |
| --- | --- | --- |
| `FAB_SHUFFLE_PORT` | `8080` | Web UI port |
| `FAB_SHUFFLE_HOST` | `0.0.0.0` | Bind address |
| `FAB_SHUFFLE_SCRATCH` | `/app/local` | Local staging directory |
| `FAB_SHUFFLE_MAX_RETRIES` | `6` | Retries for throttled/transient Fabric calls |
| `FAB_SHUFFLE_COPY_JOB_TIMEOUT_SECONDS` | `43200` | Copy Job budget |
| `FAB_SHUFFLE_LRO_TIMEOUT_SECONDS` | `3600` | Long-running-operation budget |
| `FAB_SHUFFLE_SQL_ENDPOINT_TIMEOUT_SECONDS` | `1800` | Wait for SQL endpoint provisioning |

## How it works

1. Create the target workspace on the chosen capacity, plus a short-lived scratch workspace
   that holds the Copy Jobs (so they never pollute the migrated workspace).
2. Recreate eventhouses, then import each KQL database definition retargeted at the new
   eventhouse, and copy table data with a cross-cluster `.set-or-replace`.
3. Recreate lakehouses, copy table data with Copy Jobs, and copy `Files/` with azcopy.
4. Recreate warehouses, transfer their T-SQL schema, and copy table data with Copy Jobs.
5. Recreate shortcuts, refresh the SQL analytics endpoints, then copy their schema.
6. Replay workspace role assignments.
7. Delete the scratch workspace and local staging.

### Why some things are not pure REST

Three gaps in the Fabric REST API are covered by external tooling bundled in the image:

- **Warehouse / SQL endpoint schema** — there is no definition or schema-export API for
  `Warehouse`, so `sqlpackage` extracts a DACPAC and the generated script is applied over TDS.
- **OneLake files** — OneLake has no server-side copy API, so `azcopy` moves `Files/`.
- **KQL data** — no REST API copies KQL table data, so the Kusto control plane is used.

## Development

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest        # tests
.venv/bin/python -m ruff check .  # lint
.venv/bin/python -m fabshuffle    # run the UI on http://localhost:8080
```

## Planned features

- Migration of the remaining definition-backed item types (notebooks, pipelines, semantic
  models, eventstreams, and friends)
- Connection remapping for external shortcuts and pipelines
- Configurable parallelism for data transfers
- Multiple workspace support in a single run
