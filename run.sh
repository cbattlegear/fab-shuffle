#!/usr/bin/env bash
set -e

if [ -z "${SERVICE_PRINCIPAL_CLIENT_ID}" ]; then
  echo "SERVICE_PRINCIPAL_CLIENT_ID is not set"
  exit 1
fi
if [ -z "${SERVICE_PRINCIPAL_CLIENT_SECRET}" ]; then
  echo "SERVICE_PRINCIPAL_CLIENT_SECRET is not set"
  exit 1
fi
if [ -z "${SERVICE_PRINCIPAL_TENANT_ID}" ]; then
  echo "SERVICE_PRINCIPAL_TENANT_ID is not set"
  exit 1
fi
if [ -z "${TARGET_CAPACITY_NAME}" ]; then
  echo "TARGET_CAPACITY_NAME is not set"
  exit 1
fi
if [ -z "${SOURCE_WORKSPACE_NAME}" ]; then
  echo "SOURCE_WORKSPACE_NAME is not set"
  exit 1
fi

exec pwsh -File fab-shuffle.ps1 -spnClientId $SERVICE_PRINCIPAL_CLIENT_ID -spnClientSecret $SERVICE_PRINCIPAL_CLIENT_SECRET -spnTenantId $SERVICE_PRINCIPAL_TENANT_ID -capacityName $TARGET_CAPACITY_NAME -workspaceName $SOURCE_WORKSPACE_NAME