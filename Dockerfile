FROM python:3.12.14-bookworm@sha256:581429e3df12d76e6af4be5ab7d0e7fc2013eb57dc23d2de691411c8efdbb970 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    # Major roll-forward is explicit; only the pinned .NET 10 runtime is installed.
    DOTNET_ROLL_FORWARD=Major \
    PATH="/root/.dotnet/tools:${PATH}"

WORKDIR /app
ARG TARGETARCH
COPY tools.lock.json ./
COPY scripts/install_tools.py ./scripts/

# Microsoft .debs (including SDK/runtime dependencies) come from exact, hashed feed
# artifacts. Debian OS dependencies are deliberately NOT claimed to be snapshotted.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    python scripts/install_tools.py apt --architecture "${TARGETARCH}" && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# mssql-tools18 installs outside the default PATH.
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

RUN python scripts/install_tools.py azcopy --architecture "${TARGETARCH}"

# Override when nuget.org is unreachable, for example behind a corporate feed proxy:
#   docker build --build-arg NUGET_SOURCE=https://internal.example/nuget/v3/index.json .
ARG NUGET_SOURCE=https://api.nuget.org/v3/index.json
RUN python scripts/install_tools.py nuget --architecture "${TARGETARCH}" && \
    rm -rf /root/.nuget/packages /tmp/NuGetScratch

# Override alongside NUGET_SOURCE when pypi.org is unreachable:
#   docker build --build-arg PIP_INDEX_URL=https://internal.example/pypi/simple/ .
ARG PIP_INDEX_URL=https://pypi.org/simple/

COPY pyproject.toml uv.lock README.md ./
COPY requirements ./requirements
COPY scripts/lock_dependencies.py ./scripts/
RUN python -m pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" \
        --require-hashes --only-binary=:all: -r requirements/bootstrap.txt && \
    python scripts/lock_dependencies.py --check && \
    python -m uv pip sync --system --require-hashes --only-binary=:all: \
        --default-index "${PIP_INDEX_URL}" \
        requirements/bootstrap.txt requirements/build.txt requirements/runtime.txt

COPY fabshuffle ./fabshuffle
RUN python -m uv pip install --system --no-build-isolation --no-deps --offline .
COPY scripts/smoke_image.py ./scripts/
RUN python -m uv pip check --system && python scripts/smoke_image.py

RUN mkdir -p /app/local
ENV FAB_SHUFFLE_SCRATCH=/app/local \
    FAB_SHUFFLE_HOST=0.0.0.0 \
    FAB_SHUFFLE_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=4).status == 200 else 1)"

CMD ["python", "-m", "fabshuffle"]

# Validation adds tools to the actual runtime, never substitutes a host Python environment.
FROM node:24-bookworm-slim@sha256:ba849c60be29959425b8734d57b8b4b7d56f98edd9504c9af091d5281095a71e AS test-node
FROM mcr.microsoft.com/powershell:7.4-ubuntu-22.04@sha256:62300a213a9293916333df2b014cd3a8f22fb0b0b65f2bb446aaf436bcf8c868 AS test-powershell

FROM runtime AS test
ARG PIP_INDEX_URL=https://pypi.org/simple/
COPY --from=test-node /usr/local/bin/node /usr/local/bin/node
COPY --from=test-powershell /opt/microsoft/powershell/7 /opt/microsoft/powershell/7
ENV PATH="/opt/microsoft/powershell/7:${PATH}" \
    FAB_SHUFFLE_SCRATCH=/tmp/fab-shuffle-tests \
    PYTEST_ADDOPTS="-o cache_dir=/tmp/pytest-cache" \
    RUFF_CACHE_DIR=/tmp/ruff-cache
RUN python -m uv pip install --system --require-hashes --only-binary=:all: \
        --default-index "${PIP_INDEX_URL}" -r requirements/dev.txt
COPY tests ./tests
COPY scripts ./scripts
COPY deploy ./deploy
COPY .github ./.github
COPY Dockerfile .dockerignore ./
HEALTHCHECK NONE
ENTRYPOINT ["python", "scripts/run_container_tests.py"]
CMD ["tests"]

# A default build/publish must never ship the validation dependencies or test entrypoint.
FROM runtime AS production
