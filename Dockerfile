FROM python:3.12-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    PATH="/root/.dotnet/tools:${PATH}"

# Microsoft package feed provides azcopy, the ODBC driver, and the .NET SDK that
# sqlpackage/unpackdacpac need. Fabric has no REST API for T-SQL schema or OneLake
# file copy yet, so those two tools stay in the image.
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates gnupg && \
    wget -q https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        dotnet-sdk-8.0 \
        azcopy \
        msodbcsql18 \
        unixodbc \
        unixodbc-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN dotnet tool install --global UnpackDacPac --no-cache && \
    dotnet tool install --global microsoft.sqlpackage --no-cache

WORKDIR /app

COPY pyproject.toml README.md ./
COPY fabshuffle ./fabshuffle
RUN pip install --no-cache-dir .

RUN mkdir -p /app/local
ENV FAB_SHUFFLE_SCRATCH=/app/local \
    FAB_SHUFFLE_HOST=0.0.0.0 \
    FAB_SHUFFLE_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=4).status == 200 else 1)"

CMD ["python", "-m", "fabshuffle"]
