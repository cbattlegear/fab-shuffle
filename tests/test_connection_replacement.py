"""Recreating connections that point into the workspace being migrated.

A connection's target is immutable, so repointing one means building a replacement. That is
only possible unattended when the credential type needs no secret, because Fabric never
returns an existing connection's credentials.
"""

from __future__ import annotations

from fabshuffle.fabric import connections

SQL_METADATA = {
    "type": "SQL",
    "creationMethods": [
        {
            "name": "SQL",
            "parameters": [
                {"name": "server", "dataType": "Text", "required": True},
                {"name": "database", "dataType": "Text", "required": False},
            ],
        }
    ],
    "supportedCredentialTypes": ["Basic", "OAuth2", "ServicePrincipal", "WorkspaceIdentity"],
}


def connection(**overrides):
    base = {
        "id": "conn-old",
        "displayName": "Bronze SQL",
        "connectivityType": "ShareableCloud",
        "privacyLevel": "Organizational",
        "connectionDetails": {"type": "SQL", "path": "old.datawarehouse.fabric.microsoft.com;bronze"},
        "credentialDetails": {
            "credentialType": "WorkspaceIdentity",
            "singleSignOnType": "None",
            "connectionEncryption": "Encrypted",
            "skipTestConnection": False,
        },
    }
    base.update(overrides)
    return base


# ------------------------------------------------------------- parameters


def test_a_path_maps_onto_the_declared_parameters_in_order():
    method = SQL_METADATA["creationMethods"][0]
    parameters = connections.build_parameters("new.datawarehouse.fabric.microsoft.com;bronze", method)

    assert parameters == [
        {"name": "server", "dataType": "Text", "value": "new.datawarehouse.fabric.microsoft.com"},
        {"name": "database", "dataType": "Text", "value": "bronze"},
    ]


def test_an_omitted_optional_parameter_is_allowed():
    method = SQL_METADATA["creationMethods"][0]
    parameters = connections.build_parameters("only-a-server", method)
    assert parameters == [{"name": "server", "dataType": "Text", "value": "only-a-server"}]


def test_a_missing_required_parameter_is_refused():
    method = {"name": "SQL", "parameters": [{"name": "server", "required": True}]}
    assert connections.build_parameters("", method) is None


def test_more_path_segments_than_parameters_is_refused():
    # Guessing at a payload that does not match the declared shape would be worse than
    # telling the operator to build it themselves.
    method = {"name": "SQL", "parameters": [{"name": "server", "required": True}]}
    assert connections.build_parameters("a;b;c", method) is None


# --------------------------------------------------------------- eligibility


def test_a_workspace_identity_cloud_connection_can_be_recreated():
    assert connections.can_recreate(connection(), SQL_METADATA) is None


def test_a_secret_bearing_credential_type_is_refused():
    secret = connection(
        credentialDetails={"credentialType": "ServicePrincipal", "singleSignOnType": "None"}
    )
    reason = connections.can_recreate(secret, SQL_METADATA)
    assert reason and "never returns" in reason


def test_a_gateway_connection_is_refused():
    gateway = connection(connectivityType="OnPremisesGateway")
    reason = connections.can_recreate(gateway, SQL_METADATA)
    assert reason and "gateway" in reason


def test_an_unknown_connection_type_is_refused():
    reason = connections.can_recreate(connection(), None)
    assert reason and "does not report as supported" in reason


def test_a_credential_type_the_source_does_not_support_is_refused():
    metadata = {**SQL_METADATA, "supportedCredentialTypes": ["Basic"]}
    reason = connections.can_recreate(connection(), metadata)
    assert reason and "cannot use WorkspaceIdentity" in reason


# ------------------------------------------------------------------ payload


def test_the_creation_payload_carries_the_new_target_and_original_settings():
    payload = connections.build_creation_payload(
        connection(),
        "new.datawarehouse.fabric.microsoft.com;bronze",
        SQL_METADATA,
        display_name="Bronze SQL (westus)",
    )

    assert payload["connectivityType"] == "ShareableCloud"
    assert payload["displayName"] == "Bronze SQL (westus)"
    assert payload["privacyLevel"] == "Organizational"
    assert payload["connectionDetails"]["creationMethod"] == "SQL"
    assert payload["connectionDetails"]["parameters"][0]["value"].startswith("new.")
    # Credentials are set by type only; there is no secret to carry across.
    assert payload["credentialDetails"]["credentials"] == {"credentialType": "WorkspaceIdentity"}
    assert payload["credentialDetails"]["connectionEncryption"] == "Encrypted"


def test_an_ambiguous_creation_method_is_refused():
    ambiguous = {
        "type": "Thing",
        "creationMethods": [
            {"name": "A", "parameters": [{"name": "x"}]},
            {"name": "B", "parameters": [{"name": "y"}]},
        ],
        "supportedCredentialTypes": ["WorkspaceIdentity"],
    }
    source = connection(connectionDetails={"type": "Thing", "path": "value"})
    assert connections.build_creation_payload(source, "value", ambiguous, display_name="x") is None


def test_a_single_creation_method_is_used_even_when_unnamed_after_the_type():
    single = {
        "type": "Thing",
        "creationMethods": [{"name": "OnlyWay", "parameters": [{"name": "x", "required": True}]}],
        "supportedCredentialTypes": ["WorkspaceIdentity"],
    }
    source = connection(connectionDetails={"type": "Thing", "path": "value"})
    payload = connections.build_creation_payload(source, "value", single, display_name="x")

    assert payload["connectionDetails"]["creationMethod"] == "OnlyWay"
