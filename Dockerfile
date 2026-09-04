FROM python:3.12-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    # sqlpackage and UnpackDacPac target net8.0; let them roll forward onto the .NET 10 runtime.
    DOTNET_ROLL_FORWARD=Major \
    PATH="/root/.dotnet/tools:${PATH}"

# The Microsoft package feed provides the .NET SDK and the ODBC driver on both
# architectures. Fabric has no REST API for T-SQL schema or OneLake file copy yet,
# so sqlpackage, unpackdacpac, azcopy, and bcp stay in the image.
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates gnupg && \
    wget -q https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        dotnet-sdk-10.0 \
        msodbcsql18 \
        mssql-tools18 \
        unixodbc \
        unixodbc-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# mssql-tools18 installs outside the default PATH.
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# azcopy has no arm64 apt package, so take the architecture-matched tarball.
ARG TARGETARCH
RUN case "${TARGETARCH}" in \
      amd64) AZCOPY_URL="https://aka.ms/downloadazcopy-v10-linux" ;; \
      arm64) AZCOPY_URL="https://aka.ms/downloadazcopy-v10-linux-arm64" ;; \
      *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    wget -q "${AZCOPY_URL}" -O /tmp/azcopy.tar.gz && \
    tar -xzf /tmp/azcopy.tar.gz -C /tmp && \
    install -m 0755 /tmp/azcopy_linux_*/azcopy /usr/local/bin/azcopy && \
    rm -rf /tmp/azcopy*

# Override when nuget.org is unreachable, for example behind a corporate feed proxy:
#   docker build --build-arg NUGET_SOURCE=https://internal.example/nuget/v3/index.json .
ARG NUGET_SOURCE=https://api.nuget.org/v3/index.json
RUN printf '<?xml version="1.0" encoding="utf-8"?>\n<configuration>\n<packageSources>\n<clear />\n<add key="primary" value="%s" />\n</packageSources>\n</configuration>\n' \
        "${NUGET_SOURCE}" > /tmp/NuGet.config && \
    dotnet tool install --global microsoft.sqlpackage --no-cache --configfile /tmp/NuGet.config && \
    dotnet tool install --global UnpackDacPac --no-cache --configfile /tmp/NuGet.config && \
    rm /tmp/NuGet.config && \
    rm -rf /root/.nuget/packages /tmp/NuGetScratch

WORKDIR /app

# Override alongside NUGET_SOURCE when pypi.org is unreachable:
#   docker build --build-arg PIP_INDEX_URL=https://internal.example/pypi/simple/ .
ARG PIP_INDEX_URL=https://pypi.org/simple/

COPY pyproject.toml README.md ./
COPY fabshuffle ./fabshuffle
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" .

RUN mkdir -p /app/local
ENV FAB_SHUFFLE_SCRATCH=/app/local \
    FAB_SHUFFLE_HOST=0.0.0.0 \
    FAB_SHUFFLE_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=4).status == 200 else 1)"

CMD ["python", "-m", "fabshuffle"]
