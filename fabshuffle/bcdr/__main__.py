"""External-scheduler entrypoint: ``python -m fabshuffle.bcdr`` inside Linux Docker."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from pydantic import ValidationError

from fabshuffle.auth import AuthError, ServicePrincipal, TokenProvider, managed_identity_principal
from fabshuffle.bcdr.backend import RecoveryBlocked
from fabshuffle.bcdr.catalog import CatalogError
from fabshuffle.bcdr.protection_binding import ConfigureProtectionRequest
from fabshuffle.bcdr.scheduled import read_request, scheduled_sync
from fabshuffle.bcdr.service import (
    BcdrService,
    ConfigureReplicaRequest,
    CutbackRequest,
    CutoverRequest,
    EnableRecoveryRequest,
    FailbackExecuteRequest,
    FailbackRequest,
    PlanRequest,
    RearmRequest,
    ReconcileOperationRequest,
    ServiceResult,
    SetupRequest,
    SyncRequest,
    create_service,
)
from fabshuffle.bcdr.service import (
    setup as setup_recovery,
)
from fabshuffle.fabric.client import FabricApiError, FabricError
from fabshuffle.lifecycle import safe_text

REQUESTS = {
    "reconcile-operation": ReconcileOperationRequest,
    "configure-replica": ConfigureReplicaRequest,
    "setup": SetupRequest,
    "configure-protection": ConfigureProtectionRequest,
    "plan": PlanRequest,
    "synchronize": SyncRequest,
    "scheduled-sync": SyncRequest,
    "enable-recovery": EnableRecoveryRequest,
    "cutover": CutoverRequest,
    "plan-failback": FailbackRequest,
    "execute-failback": FailbackExecuteRequest,
    "cutback": CutbackRequest,
    "rearm": RearmRequest,
}
CONSEQUENTIAL = frozenset({
    "setup", "configure-protection", "synchronize", "enable-recovery",
    "cutover", "execute-failback", "cutback", "rearm", "reconcile-operation", "configure-replica",
    "scheduled-sync",
})


class SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse ordinarily echoes unknown arguments, which might be a mistakenly
        # supplied password. Credentials are never command-line options.
        if message.startswith("unrecognized arguments"):
            message = (
                "Unrecognized options. Use --help; supply credentials through the environment or secret file."
            )
        super().error(message)


def credential_provider(environ: Mapping[str, str]) -> TokenProvider:
    mode = environ.get("FAB_SHUFFLE_BCDR_AUTH_MODE", "service_principal")
    if mode == "managed_identity":
        fields = (
            "FAB_SHUFFLE_BCDR_TENANT_ID", "FAB_SHUFFLE_BCDR_CLIENT_ID",
            "FAB_SHUFFLE_BCDR_CLIENT_SECRET", "FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE",
        )
        if any(environ.get(name) for name in fields):
            raise ValueError(
                "Managed identity mode cannot include BCDR service-principal credentials. "
                "Remove FAB_SHUFFLE_BCDR_TENANT_ID, FAB_SHUFFLE_BCDR_CLIENT_ID, "
                "FAB_SHUFFLE_BCDR_CLIENT_SECRET and FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE."
            )
        principal = managed_identity_principal(environ)
        if principal is None:
            raise AuthError(
                "Set FAB_SHUFFLE_MANAGED_IDENTITY_TENANT_ID and "
                "FAB_SHUFFLE_MANAGED_IDENTITY_CLIENT_ID for managed_identity mode."
            )
        return TokenProvider(principal)
    if mode != "service_principal":
        raise ValueError("FAB_SHUFFLE_BCDR_AUTH_MODE must be service_principal or managed_identity.")
    tenant = environ.get("FAB_SHUFFLE_BCDR_TENANT_ID", "")
    client = environ.get("FAB_SHUFFLE_BCDR_CLIENT_ID", "")
    secret = environ.get("FAB_SHUFFLE_BCDR_CLIENT_SECRET", "")
    secret_file = environ.get("FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE", "")
    if secret and secret_file:
        raise ValueError("Supply CLIENT_SECRET or CLIENT_SECRET_FILE, not both.")
    if secret_file:
        secret = Path(secret_file).read_text(encoding="utf-8").strip()
    if not tenant or not client or not secret:
        raise ValueError(
            "Set FAB_SHUFFLE_BCDR_TENANT_ID, FAB_SHUFFLE_BCDR_CLIENT_ID and "
            "FAB_SHUFFLE_BCDR_CLIENT_SECRET (or FAB_SHUFFLE_BCDR_CLIENT_SECRET_FILE)."
        )
    return TokenProvider(ServicePrincipal(tenant, client, secret))


def dispatch(service: BcdrService, command: str, text: str) -> ServiceResult:
    """Explicit typed calls; unsupported commands cannot reach a dynamic service member."""
    if command == "status":
        return service.status()
    if command == "reconcile-operation":
        return service.reconcile_operation(ReconcileOperationRequest.model_validate_json(text))
    if command == "configure-replica":
        return service.configure_replica(ConfigureReplicaRequest.model_validate_json(text))
    if command == "configure-protection":
        return service.configure_protection(ConfigureProtectionRequest.model_validate_json(text))
    if command == "plan":
        return service.plan(PlanRequest.model_validate_json(text))
    if command == "synchronize":
        return service.synchronize(SyncRequest.model_validate_json(text))
    if command == "enable-recovery":
        return service.enable_recovery(EnableRecoveryRequest.model_validate_json(text))
    if command == "cutover":
        return service.cutover(CutoverRequest.model_validate_json(text))
    if command == "plan-failback":
        return service.plan_failback(FailbackRequest.model_validate_json(text))
    if command == "execute-failback":
        return service.execute_failback(FailbackExecuteRequest.model_validate_json(text))
    if command == "cutback":
        return service.cutback(CutbackRequest.model_validate_json(text))
    if command == "rearm":
        return service.rearm(RearmRequest.model_validate_json(text))
    raise ValueError("Choose a supported BCDR command; use --help.")


def main(argv: Sequence[str] | None = None) -> int:
    parser = SafeParser(
        description="Operate same-tenant BCDR from the Linux container, outside paused recovery capacity."
    )
    parser.add_argument("command", choices=["status", *REQUESTS])
    parser.add_argument(
        "--bootstrap", type=Path,
        help="Durable non-secret bootstrap descriptor, not a recovery metadata archive.",
    )
    parser.add_argument("--request", type=Path, help="Typed JSON request file; default: standard input.")
    parser.add_argument(
        "--confirm", choices=sorted(CONSEQUENTIAL), help="Explicitly approve this named action.",
    )
    parser.add_argument(
        "--schema", action="store_true", help="Print the command's request schema without sign-in.",
    )
    args = parser.parse_args(argv)
    if args.schema:
        model = REQUESTS.get(args.command)
        print(json.dumps(model.model_json_schema() if model else {}, indent=2))
        return 0
    try:
        if args.bootstrap is None:
            raise ValueError("Specify --bootstrap with the durable non-secret controller descriptor.")
        if args.command in CONSEQUENTIAL and args.confirm != args.command:
            raise ValueError(f"Review the request and supply --confirm {args.command} for this action.")
        if args.confirm is not None and args.confirm != args.command:
            raise ValueError("--confirm must match the command being executed.")
        text = ""
        request = None
        if args.command == "status":
            if args.request:
                raise ValueError(
                    "status takes no request; it uses the bootstrap without source metadata reads."
                )
        elif args.command == "scheduled-sync":
            if args.request is None:
                raise ValueError(
                    "scheduled-sync requires an approved --request JSON file, not standard input."
                )
            request = read_request(args.request)
        else:
            text = args.request.read_text(encoding="utf-8") if args.request else sys.stdin.read()
            request = REQUESTS[args.command].model_validate_json(text)
        tokens = credential_provider(os.environ)
        if args.command == "scheduled-sync":
            result = scheduled_sync(args.bootstrap, request, target_tokens=tokens)
        elif args.command == "setup":
            result = setup_recovery(
                SetupRequest.model_validate_json(text), args.bootstrap, target_tokens=tokens,
            )
        else:
            service = create_service(
                args.bootstrap, target_tokens=tokens,
                source_tokens=tokens if (
                    (isinstance(request, SyncRequest) and request.capture)
                    or isinstance(request, FailbackRequest)
                ) else None,
            )
            try:
                result = dispatch(service, args.command, text)
            finally:
                service.close()
        print(result.model_dump_json(indent=2))
        return result.exit_code
    except ValidationError as error:
        details = [
            {"field": ".".join(map(str, entry["loc"])), "message": entry["msg"]}
            for entry in error.errors(include_input=False, include_context=False, include_url=False)
        ]
        print(json.dumps({"error": "Invalid BCDR request", "details": details}), file=sys.stderr)
        return 2
    except FabricApiError as error:
        print(json.dumps({"error": safe_text(error.body or str(error))}), file=sys.stderr)
        return 1
    except (AuthError, FabricError, CatalogError, RecoveryBlocked, ValueError, OSError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
