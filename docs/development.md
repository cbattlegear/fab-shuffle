# Development and release maintenance

Use the [Docker-only validation workflow](../README.md#development) for application
execution, tests and lint. A native Python environment does not reproduce the shipped
SQL, OneLake and authentication tooling.

## Locked inputs

The [Dockerfile](../Dockerfile) pins the Python base image and validation-only Node and
PowerShell images. [uv.lock](../uv.lock) and the hashed [requirements](../requirements)
exports fix Python dependency selection. [tools.lock.json](../tools.lock.json) records
external tool versions, artifacts, hashes and upstream sources.

Python and NuGet mirrors must serve the pinned artifacts unchanged. Missing packages,
unavailable wheels and checksum mismatches must fail rather than trigger an unreviewed
source build or upgrade. `uv.lock` retains platform markers; that metadata is not a promise
of native Windows application support.

The final/default Dockerfile target is `production`; it excludes test sources, Node,
PowerShell and Python test dependencies. The `test` target derives from the same runtime
and adds the locked validation tools.

## Azure deployment contracts

Changes to [`deploy/azuredeploy.json`](../deploy/azuredeploy.json) and
[`deploy/azuredeploy-sync-job.json`](../deploy/azuredeploy-sync-job.json) must preserve the
explicit boundary between browser operator EasyAuth and Fabric runtime identity. Consult
the versioned resource schemas, not a similarly named App Service resource:
[Container Apps authConfigs 2024-03-01](https://learn.microsoft.com/en-us/azure/templates/microsoft.app/2024-03-01/containerapps/authconfigs)
and [jobs 2024-03-01](https://learn.microsoft.com/en-us/azure/templates/microsoft.app/2024-03-01/jobs).
`globalValidation.unauthenticatedClientAction` expresses required authentication in that
auth schema; there is no `requireAuthentication` property. Nonempty individual operator
object IDs and the application guard must remain mandatory, including without a UAMI.
Initial/redeployed ingress is internal and `FAB_SHUFFLE_EASYAUTH_READY=false`; header
validation alone cannot defend a caller before the auth sidecar is configured. Final
activation is explicitly ordered: verify the deployed auth config, enable external ingress
while still not ready, read the actual external FQDN and register its callback, then set
readiness true with the targeted CLI environment update. Never substitute a partial ARM
app PUT that could discard identity, other environment variables or mounted storage.

The scheduled job is a coordinated worker of the existing recovery deployment: same
UAMI/bootstrap, shared Azure Files and exact remote Blob lease URL, one central Warehouse.
Its command is a direct argument array, never an arbitrary shell command. ARM must not
initialize, replace or export over the persisted bootstrap/request, and `parallelism=1`
must not be described as cross-execution exclusion. The only automatically assigned
runtime data role is on the dedicated lock container, not an entire account/subscription.

Validate these changes in the Linux test image without live mounts, credentials or network:

```text
docker build --platform linux/amd64 --target test -t fab-shuffle:azure-deploy-test .
docker run --rm --platform linux/amd64 --network none fab-shuffle:azure-deploy-test tests/test_azure_files_deploy.py tests/test_azure_auth_and_sync_deploy.py
```

The standard entrypoint also checks tools, dependency locks, runtime smoke and Ruff.
Template tests are static contract/regression checks, **not an Azure ARM deployment
validation**. Before release, separately qualify real single-tenant callback/hybrid login,
sidecar tenant/object claims, denial of non-allowlisted users, UAMI/Fabric authorization,
cross-container Blob lease exclusion, scheduled results and timeout/reconciliation in a
disposable Azure environment. Never use production recovery state for validation.

## Deliberate release upgrades

Review dependency and tool updates at least monthly, and promptly for security fixes.
A scheduled image rebuild does **not** update the pins. Make updates on a branch.

For dependency maintenance only, start a shell in the test image with your checkout mounted
at `/workspace` (replace the placeholder with its absolute host path). This container needs
network access to fetch the explicitly selected package versions; do not mount recovery
volumes or provide Fabric credentials.

```text
docker run --rm -it --mount "type=bind,source=<absolute-checkout-path>,target=/workspace" --workdir /workspace --entrypoint /bin/sh fab-shuffle:test
```

1. Inside that container, for a selected dependency run
   `python -m uv lock --upgrade-package NAME==VERSION`; use `python -m uv lock --upgrade`
   only for a deliberate full refresh. Preserve the declared dependency compatibility.
   To update the resolver/installer, edit the exact `lock` group pins and
   `tool.uv.required-version` together, install that exact uv version into this isolated
   environment, and regenerate. To update the build backend, change both
   `build-system.requires` and the `build` group. The lock includes their transitives.
2. Run `python scripts/lock_dependencies.py` to regenerate all four hashed exports.
   Exit and rebuild the test target with the updated inputs, then run its validation
   entrypoint. Commit `pyproject.toml`, `uv.lock`, and the exports together.
   Do not edit generated hashes or replace them with `pip freeze`.
3. Review `tools.lock.json` and the Dockerfile base digest using the official upstream
   release metadata linked in the manifest's `sources`. Select each exact version's
   `Filename`/`SHA256` stanza from both Microsoft package indexes, including the SDK's
   Microsoft runtime/host/targeting-pack dependency closure. For NuGet, download the exact
   `.nupkg` from the official flat-container feed, compare its SHA-512 with the catalog,
   and record its SHA-256. Use the versioned AzCopy release assets and their published
   `digest` values, never an evergreen download link. Verify both architectures' downloads
   and hashes before changing pins. Resolve the Python image's multi-architecture digest
   with `docker buildx imagetools inspect python:VERSION-bookworm`. Check Microsoft Learn
   per-tool pages for supported architectures and runtime changes. Review the validation
   Node/PowerShell image digests and workflow action/tool pins explicitly too.
4. Run the test image's validation entrypoint before building or publishing the production
   target. Build a unique local tag with
   `docker build --platform linux/amd64 --target production -t fab-shuffle:release-review-UNIQUE .`;
   repeat for `linux/arm64` with a different tag on an ARM runner or under emulation.
   Both builds must pass the embedded CLI/import/HTTP smoke checks. Do not push from the
   dependency-refresh procedure.

For an application release, align the version in `fabshuffle/__init__.py`, `pyproject.toml`
and the root project entry in `uv.lock` before tagging. The
[release workflow](../.github/workflows/docker-publish.yml) gates publication on
[Linux-container validation](../.github/workflows/tests.yml). Semver `v*.*.*` tags control
versioned images, `latest` and signing. A merge to `main` alone does not publish a new stable
`latest` release.

## Reproducibility boundary

The guarantee is **dependency/artifact selection**, not byte-for-byte OCI images. The
base image is immutable, Python wheels are hash checked, and external tool inputs are
pinned. Debian apt repositories and OS dependency packages are not fully snapshotted;
their transitive updates, maintainer scripts, timestamps and hosted runner updates can
change image bytes. A withdrawn pinned artifact makes the build fail until an operator
deliberately refreshes it.

For an identical deployed image, retain and run its published digest rather than
rebuilding a tag. ARM64 image smoke checks do not establish vendor-supported live
SqlPackage transfers or live Fabric qualification.
