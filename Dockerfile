FROM python:3.12-bookworm

# Install fab-cli and use fallback encryption because of linux limitations
RUN pip install --no-cache-dir ms-fabric-cli==0.2.0 && fab config set encryption_fallback_enabled true

RUN apt update && apt install -y wget && \
    wget -q https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt update && \
    apt install -y dotnet-sdk-8.0 powershell azcopy

RUN pwsh -Command "Install-Module -Name Az.Accounts -Repository PSGallery -RequiredVersion 5.1.0 -Force"
RUN pwsh -Command "Install-Module -Name SqlServer -Repository PSGallery -RequiredVersion 22.3.0 -Force"
RUN dotnet tool install --global UnpackDacPac --no-cache && dotnet tool install --global microsoft.sqlpackage --no-cache

RUN pwsh -Command "Install-Package -Name Microsoft.Azure.Kusto.Ingest -RequiredVersion 13.0.2 -Source 'nuget.org' -SkipDependencies -Force"
RUN pwsh -Command "Install-Package -Name Microsoft.Azure.Kusto.Data -RequiredVersion 13.0.2 -Source 'nuget.org' -SkipDependencies -Force"
RUN pwsh -Command "Install-Package -Name Microsoft.Azure.Kusto.Cloud.Platform -RequiredVersion 13.0.2 -Source 'nuget.org' -SkipDependencies -Force"
RUN pwsh -Command "Install-Package -Name Microsoft.Azure.Kusto.Cloud.Platform.Msal -RequiredVersion 13.0.2 -Source 'nuget.org' -SkipDependencies -Force"
RUN pwsh -Command "Install-Package -Name Azure.Core -RequiredVersion 1.46.1 -Source 'nuget.org' -SkipDependencies -Force"
RUN pwsh -Command "Install-Package -Name Microsoft.Identity.Client -RequiredVersion 4.72.1 -Source 'nuget.org' -SkipDependencies -Force"
RUN pwsh -Command "Install-Package -Name Microsoft.IdentityModel.Abstractions -RequiredVersion 8.9.0 -Source 'nuget.org' -SkipDependencies -Force"

WORKDIR /app
COPY CopyJobTemplates CopyJobTemplates/
COPY *.ps1 .
COPY run.sh .
RUN chmod +x run.sh
RUN mkdir ./local/

ENTRYPOINT [ "bash", "/app/run.sh" ]