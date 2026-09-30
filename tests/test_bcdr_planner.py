"""Estate planning uses captured evidence, never a live source or guessed service capability."""

from dataclasses import replace

import pytest

from fabshuffle.bcdr.planner import (
    AdapterCapabilities,
    CapacityMapping,
    Dependency,
    EstateInventory,
    Evidence,
    EvidenceState,
    Item,
    Phase,
    PlanConfigurationError,
    Replacement,
    Requirement,
    Resource,
    ResourceKey,
    Selection,
    TargetMapping,
    Workspace,
    build_operation_plan,
    mapping_invalidation,
)

TENANT = "tenant"
CAPACITY = (TENANT, "source-capacity")
TARGET_CAPACITY = (TENANT, "recovery-capacity")
CONTROL = (TENANT, "control")
WAREHOUSE = (*CONTROL, "metadata")
COMPLETE = Evidence(EvidenceState.COMPLETE, ("captured definitions and operator declaration",))
QUALIFIED = AdapterCapabilities(inactive_create=True, provenance="adapter fixture qualification")
SHELL = replace(QUALIFIED, shell_then_bind=True)
STORE = replace(QUALIFIED, is_store=True)


def workspace(identifier, name=None, *, tenant=TENANT, capacity="source-capacity", eligible=True):
    return Workspace((tenant, identifier), (tenant, capacity), name or identifier, eligible)


def item(identifier, ws="a", *, name=None, tenant=TENANT, **kwargs):
    return Item(
        (tenant, ws, identifier),
        name or identifier,
        kwargs.pop("item_type", "Notebook"),
        evidence=kwargs.pop("evidence", COMPLETE),
        capabilities=kwargs.pop("capabilities", QUALIFIED),
        **kwargs,
    )


def dependency(consumer, prerequisite, **kwargs):
    return Dependency(
        consumer.key,
        prerequisite.key if isinstance(prerequisite, Item | Resource) else prerequisite,
        kwargs.pop("provenance", "captured definition"),
        **kwargs,
    )


def inventory(*items, workspaces=None, dependencies=(), resources=()):
    if workspaces is None:
        keys = sorted({row.key[:2] for row in items})
        workspaces = tuple(workspace(key[1], tenant=key[0]) for key in keys)
    return EstateInventory(tuple(workspaces), tuple(items), tuple(dependencies), tuple(resources))


def plan(estate, selection=None, **kwargs):
    return build_operation_plan(
        estate,
        selection or Selection((CAPACITY,)),
        capacity_mappings=kwargs.pop("capacity_mappings", (CapacityMapping(CAPACITY, TARGET_CAPACITY),)),
        control_workspace=CONTROL,
        control_warehouse=WAREHOUSE,
        **kwargs,
    )


def op(stage, subject):
    key = subject.key if isinstance(subject, Item | Resource) else subject
    if isinstance(key, ResourceKey):
        return stage, key.kind, key.tenant_id, key.workspace_id, key.item_id, key.resource_id
    return stage, *key


def operation(result, stage, subject):
    return next(row for row in result.operations if row.key == op(stage, subject))


def codes(result, stage, subject):
    return {issue.code for issue in operation(result, stage, subject).blockers}


def assert_before(result, earlier, later):
    assert result.executable_order.index(earlier) < result.executable_order.index(later)


def assert_valid_order(result):
    placed = set()
    operations = {row.key: row for row in result.operations}
    for key in result.executable_order:
        assert not operations[key].blockers
        assert set(operations[key].prerequisites) <= placed
        placed.add(key)
    assert placed == {row.key for row in result.operations if not row.blockers}


def test_empty_include_selects_only_eligible_source_capacity_not_tenant_or_control():
    a, b, disabled, metadata = item("a"), item("b", "b"), item("x", "disabled"), item("metadata", "control")
    estate = inventory(
        a,
        b,
        disabled,
        metadata,
        workspaces=(
            workspace("a"),
            workspace("b", capacity="another-capacity"),
            workspace("disabled", eligible=False),
            workspace("control"),
        ),
    )
    result = plan(estate)
    assert result.selected_workspaces == ((TENANT, "a"),)
    assert [binding.source for binding in result.bindings] == [a.key]
    assert not result.required_additions
    assert_valid_order(result)


def test_exact_include_does_not_match_name_and_contains_is_positive_case_insensitive_union():
    estate = inventory(
        item("one"),
        item("two", "b"),
        item("three", "c"),
        item("four", "d"),
        workspaces=(
            workspace("a", "sales"),
            workspace("b", "NORTH Revenue operations"),
            workspace("c", "revenue exclusions"),
            workspace("d", "Elsewhere"),
        ),
    )
    result = plan(
        estate,
        Selection(
            (CAPACITY,),
            include=((TENANT, "d"),),
            keywords=("ReVeNuE",),
            exclude=((TENANT, "c"),),
        ),
    )
    assert result.selected_workspaces == ((TENANT, "b"), (TENANT, "d"))
    exact = plan(estate, Selection((CAPACITY,), include=((TENANT, "a"),)))
    assert exact.selected_workspaces == ((TENANT, "a"),)
    with pytest.raises(PlanConfigurationError, match="Unknown workspace"):
        plan(estate, Selection((CAPACITY,), include=((TENANT, "sales"),)))


def test_keyword_match_never_expands_capacity_scope_and_no_match_is_empty():
    estate = inventory(
        item("one"),
        item("two", "b"),
        workspaces=(workspace("a", "Local"), workspace("b", "Revenue", capacity="elsewhere")),
    )
    result = plan(estate, Selection((CAPACITY,), keywords=("Revenue",)))
    assert not result.selected_workspaces
    assert not result.operations


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"source_capacities": ()}, "explicit source capacities"),
        ({"source_capacities": (CAPACITY, CAPACITY)}, "duplicate"),
        ({"include": ((TENANT, "a"), (TENANT, "a"))}, "duplicate"),
        ({"include": ((TENANT, "a"),), "exclude": ((TENANT, "a"),)}, "conflicting"),
        ({"approved_additions": ((TENANT, "a"),), "exclude": ((TENANT, "a"),)}, "conflicting"),
        ({"keywords": (" ",)}, "empty keywords"),
        ({"include": (("a",),)}, "qualified identity"),
    ],
)
def test_selection_rejects_ambiguous_configuration(kwargs, match):
    with pytest.raises(PlanConfigurationError, match=match):
        Selection(**({"source_capacities": (CAPACITY,)} | kwargs))


@pytest.mark.parametrize(
    "selection,match",
    [
        (Selection(((TENANT, "absent"),)), "Unknown source capacities"),
        (Selection((CAPACITY,), include=((TENANT, "absent"),)), "Unknown workspace"),
        (Selection((CAPACITY,), exclude=((TENANT, "absent"),)), "Unknown workspace"),
        (Selection((CAPACITY,), include=(CONTROL,)), "control, ineligible or outside"),
        (Selection((CAPACITY,), include=((TENANT, "b"),)), "control, ineligible or outside"),
    ],
)
def test_unknown_ids_and_explicit_control_or_capacity_escape_are_errors(selection, match):
    estate = inventory(
        item("one"),
        item("two", "b"),
        item("metadata", "control"),
        workspaces=(workspace("a"), workspace("b", capacity="elsewhere"), workspace("control")),
    )
    with pytest.raises(PlanConfigurationError, match=match):
        plan(estate, selection)


def test_capacity_mappings_must_be_explicit_same_tenant_unique_and_outside_source_scope():
    estate = inventory(item("a"))
    with pytest.raises(PlanConfigurationError, match="exactly one"):
        plan(estate, capacity_mappings=())
    mapping = CapacityMapping(CAPACITY, TARGET_CAPACITY)
    with pytest.raises(PlanConfigurationError, match="Duplicate capacity mapping"):
        plan(estate, capacity_mappings=(mapping, mapping))
    with pytest.raises(PlanConfigurationError, match="distinct destination"):
        CapacityMapping(CAPACITY, CAPACITY)
    with pytest.raises(PlanConfigurationError, match="same tenant"):
        CapacityMapping(CAPACITY, ("another-tenant", "capacity"))
    second_capacity = (TENANT, "second-source")
    estate = inventory(
        item("a"),
        item("b", "b"),
        workspaces=(workspace("a"), workspace("b", capacity="second-source")),
    )
    with pytest.raises(PlanConfigurationError, match="overlap"):
        plan(
            estate,
            Selection((CAPACITY, second_capacity)),
            capacity_mappings=(
                CapacityMapping(CAPACITY, second_capacity),
                CapacityMapping(second_capacity, TARGET_CAPACITY),
            ),
        )


def test_names_and_equal_item_ids_remain_qualified_across_workspaces_and_tenants():
    a = item("same-id", name="Same name")
    b = item("same-id", "b", name="Same name")
    c = item("same-id", tenant="other-tenant", name="Same name")
    estate = inventory(a, b, c, dependencies=(dependency(b, a),))
    other_capacity = ("other-tenant", "source-capacity")
    result = plan(
        estate,
        Selection((CAPACITY, other_capacity)),
        capacity_mappings=(
            CapacityMapping(CAPACITY, TARGET_CAPACITY),
            CapacityMapping(other_capacity, ("other-tenant", "recovery-capacity")),
        ),
    )
    assert {binding.source for binding in result.bindings} == {a.key, b.key, c.key}
    assert_before(result, op("create", a), op("create", b))
    assert not operation(result, "create", c).blockers
    assert_valid_order(result)


def test_guid_casing_normalizes_inventory_and_selection_without_lowercasing_names():
    guid = "AABBCCDD-0000-4000-8000-AABBCCDDEEFF"
    row = item(guid, guid, tenant=guid, name="CaseSensitive")
    estate = inventory(row, workspaces=(workspace(guid, tenant=guid, capacity=guid),))
    selected = Selection(((guid, guid),), include=((guid, guid),))
    result = build_operation_plan(
        estate,
        selected,
        capacity_mappings=(CapacityMapping((guid, guid), (guid, "other-capacity")),),
        control_workspace=(guid, "control"),
        control_warehouse=(guid, "control", "catalog"),
    )
    assert result.selected_workspaces == ((guid.lower(), guid.lower()),)
    assert result.bindings[0].source == (guid.lower(),) * 3
    assert row.display_name == "CaseSensitive"
    with pytest.raises(PlanConfigurationError, match="Duplicate item"):
        plan(inventory(item(guid), item(guid.lower())))


@pytest.mark.parametrize("kind", ["item", "workspace", "resource"])
def test_duplicate_qualified_identities_are_not_silently_overwritten(kind):
    row = item("one")
    resource = Resource(ResourceKey("connection", TENANT, "conn"))
    estate = inventory(row)
    if kind == "item":
        estate = replace(estate, items=(row, replace(row, display_name="Different")))
    elif kind == "workspace":
        estate = replace(estate, workspaces=(workspace("a"), workspace("a", "Different")))
    else:
        estate = replace(estate, resources=(resource, resource))
    with pytest.raises(PlanConfigurationError, match=f"Duplicate {kind}"):
        plan(estate)


def test_cross_workspace_chain_requires_approval_then_orders_items_not_workspaces():
    report, model, lake = item("report"), item("model", "b"), item("lake", "c", capabilities=STORE)
    notebook, pipeline = item("notebook"), item("pipeline", "c")
    estate = inventory(
        report,
        model,
        lake,
        notebook,
        pipeline,
        dependencies=(
            dependency(report, model),
            dependency(model, lake),
            dependency(pipeline, notebook),
        ),
    )
    selection = Selection((CAPACITY,), include=((TENANT, "a"),))
    preview = plan(estate, selection)
    assert preview.required_additions == ((TENANT, "b"), (TENANT, "c"))
    assert "approval_required" in codes(preview, "create", report)
    assert op("create", notebook) in preview.executable_order
    assert op("create", model) not in {row.key for row in preview.operations}
    result = plan(estate, replace(selection, approved_additions=preview.required_additions))
    assert_before(result, op("create", lake), op("create", model))
    assert_before(result, op("create", model), op("create", report))
    # Closure adds only needed items, not every item in an approved workspace.
    assert op("create", pipeline) not in {row.key for row in result.operations}
    assert not result.cycles
    assert_valid_order(result)
    full = plan(estate)
    assert_before(full, op("create", notebook), op("create", pipeline))
    assert not full.reference_cycles
    assert not full.cycles
    all_workspaces = {op("workspace_create", key) for key in ((TENANT, "a"), (TENANT, "b"), (TENANT, "c"))}
    for row in full.operations:
        if row.kind in {"store_create", "item_create"}:
            assert all_workspaces <= set(row.prerequisites)
    assert_valid_order(full)


def test_excluded_dependency_is_never_silently_added_or_approved():
    report, model, independent = item("report"), item("model", "b"), item("independent")
    estate = inventory(report, model, independent, dependencies=(dependency(report, model),))
    selection = Selection((CAPACITY,), include=((TENANT, "a"),), exclude=((TENANT, "b"),))
    result = plan(estate, selection)
    assert not result.required_additions
    assert "excluded_dependency" in codes(result, "create", report)
    assert op("ready", independent) in result.executable_order
    blocker = operation(result, "create", report).blockers[0]
    assert model.key == blocker.subject
    assert "Remove the exclusion" in blocker.message
    with pytest.raises(PlanConfigurationError, match="conflicting"):
        replace(selection, approved_additions=((TENANT, "b"),))
    with pytest.raises(PlanConfigurationError, match="stale/redundant"):
        plan(estate, Selection((CAPACITY,), approved_additions=((TENANT, "a"),)))


def test_dependency_on_unselected_capacity_or_control_blocks_only_its_consumers():
    consumer, other, catalog, independent = (
        item("consumer"),
        item("other", "b"),
        item("metadata", "control"),
        item("ok"),
    )
    estate = inventory(
        consumer,
        other,
        catalog,
        independent,
        workspaces=(
            workspace("a"),
            workspace("b", capacity="elsewhere"),
            workspace("control"),
        ),
        dependencies=(dependency(consumer, other), dependency(consumer, catalog)),
    )
    result = plan(estate)
    assert codes(result, "create", consumer) == {"out_of_scope", "control_dependency"}
    assert not result.required_additions
    assert op("ready", independent) in result.executable_order


def test_external_replacement_can_resolve_excluded_or_uncaptured_dependency():
    consumer, excluded = item("consumer"), item("excluded", "b")
    missing = (TENANT, "unlisted-workspace", "unlisted-item")
    estate = inventory(
        consumer,
        excluded,
        dependencies=(
            dependency(consumer, excluded),
            dependency(consumer, missing),
        ),
    )
    replacements = (
        Replacement(
            TargetMapping(excluded.key, (TENANT, "standby", "model"), "inc-1"), True, True, True, "drill"
        ),
        Replacement(
            TargetMapping(missing, (TENANT, "standby", "external"), "inc-2"), True, True, True, "drill"
        ),
    )
    result = plan(estate, Selection((CAPACITY,), exclude=((TENANT, "b"),)), replacements=replacements)
    assert not result.required_additions
    assert not operation(result, "ready", consumer).blockers
    assert {row.source for row in result.bindings if row.replacement} == {excluded.key, missing}
    assert not any(row.key == op("create", excluded) for row in result.operations)
    assert_valid_order(result)


def test_replacement_validation_and_original_source_reference_guards():
    source = item("source")
    mapping = TargetMapping(source.key, (TENANT, "recovery", "target"), "inc-1")
    with pytest.raises(PlanConfigurationError, match="Validate replacement"):
        Replacement(mapping, False, True, True, "not validated")
    with pytest.raises(PlanConfigurationError, match="Validate replacement"):
        Replacement(mapping, True, False, True, "same failure domain")
    with pytest.raises(PlanConfigurationError, match="Validate replacement"):
        Replacement(mapping, True, True, True, "")
    for target in (source.key, WAREHOUSE):
        with pytest.raises(PlanConfigurationError, match="source/control"):
            plan(
                inventory(source),
                replacements=(
                    Replacement(TargetMapping(source.key, target, "inc"), True, True, True, "operator"),
                ),
            )
    with pytest.raises(PlanConfigurationError, match="Cross-tenant"):
        plan(
            inventory(source),
            replacements=(
                Replacement(
                    TargetMapping(source.key, ("other-tenant", "recovery", "target"), "inc"),
                    True,
                    True,
                    True,
                    "operator",
                ),
            ),
        )


def test_unready_replacement_allows_metadata_not_consumer_readiness():
    source, consumer = item("source", "b"), item("consumer")
    replacement = Replacement(
        TargetMapping(source.key, (TENANT, "recovery", "target"), "inc"),
        True,
        True,
        False,
        "identity and independence validated",
    )
    result = plan(
        inventory(source, consumer, dependencies=(dependency(consumer, source),)),
        Selection((CAPACITY,), include=((TENANT, "a"),)),
        replacements=(replacement,),
    )
    assert op("bind", consumer) in result.executable_order
    assert "replacement_unready" in codes(result, "ready", consumer)


def test_unprotected_optional_data_preserves_metadata_and_independent_groups():
    sql = item(
        "orders",
        capabilities=STORE,
        item_type="SQLDatabase",
        data_required=True,
        protection_action="Supply the pre-disaster off-region BACPAC.",
    )
    api, report, independent = item("api"), item("report"), item("independent")
    estate = inventory(
        sql, api, report, independent, dependencies=(dependency(api, sql), dependency(report, api))
    )
    result = plan(estate)
    for row in (sql, api, report):
        assert op("bind", row) in result.executable_order
        assert "protection_missing" in codes(result, "ready", row)
    assert op("ready", independent) in result.executable_order
    assert "BACPAC" in operation(result, "data_ready", sql).blockers[0].message
    assert not any(key[0] == "activate" for key in result.executable_order)
    assert codes(result, "activate", independent) == {"activation_approval"}
    assert_valid_order(result)


def test_explicit_data_prerequisite_withholds_definition_but_safe_shell_can_arrive():
    sql = item("orders", capabilities=STORE, data_required=True)
    consumer = item("consumer", capabilities=SHELL)
    result = plan(
        inventory(sql, consumer, dependencies=(dependency(consumer, sql, requires=Requirement.DATA_READY),))
    )
    assert op("create", consumer) in result.executable_order
    assert "protection_missing" in codes(result, "bind", consumer)
    assert "protection_missing" in codes(result, "ready", consumer)


@pytest.mark.parametrize(
    "evidence",
    [
        Evidence(),
        Evidence(EvidenceState.UNKNOWN, ("beta upstream unavailable; scanner not configured",)),
        Evidence(EvidenceState.PARTIAL, ("definition references",), "dynamic expression unresolved"),
    ],
)
def test_absent_or_partial_evidence_never_means_no_dependencies(evidence):
    source = item("source", evidence=evidence)
    dependent, independent = item("dependent"), item("independent")
    result = plan(inventory(source, dependent, independent, dependencies=(dependency(dependent, source),)))
    assert "dependency_evidence_gap" in codes(result, "create", source)
    assert "dependency_evidence_gap" in codes(result, "ready", dependent)
    assert op("ready", independent) in result.executable_order
    assert dict(result.evidence)[source.key] == evidence


def test_unknown_evidence_can_only_create_explicit_safe_shell_not_bind_source_definition():
    source = item("unknown", evidence=Evidence(), capabilities=SHELL)
    result = plan(inventory(source))
    assert op("create", source) in result.executable_order
    assert "dependency_evidence_gap" in codes(result, "bind", source)
    with pytest.raises(PlanConfigurationError, match="provenance"):
        Evidence(EvidenceState.COMPLETE)
    with pytest.raises(PlanConfigurationError, match="qualification provenance"):
        AdapterCapabilities(inactive_create=True)
    with pytest.raises(PlanConfigurationError, match="inactive-create"):
        AdapterCapabilities(shell_then_bind=True)


def test_no_inferred_item_type_support_or_automatic_endpoint_readiness():
    unknown = item("known-type", item_type="Lakehouse", capabilities=AdapterCapabilities())
    result = plan(inventory(unknown))
    assert "inactive_create_unqualified" in codes(result, "create", unknown)
    owner = item("owner")
    endpoint = Resource(ResourceKey("endpoint", TENANT, "ep", "a", "owner"), owner.key)
    consumer = item("consumer")
    result = plan(
        inventory(owner, consumer, resources=(endpoint,), dependencies=(dependency(consumer, endpoint),))
    )
    assert "resource_unqualified" in codes(result, "create", consumer)


def test_optional_unresolved_edges_are_visible_and_do_not_allow_source_bound_definitions():
    consumer = item("consumer", capabilities=SHELL)
    missing = (TENANT, "external", "dynamic")
    result = plan(
        inventory(
            consumer,
            dependencies=(
                dependency(
                    consumer,
                    missing,
                    required=False,
                    qualified=False,
                    provenance="operator dynamic reference",
                ),
            ),
        )
    )
    assert op("create", consumer) in result.executable_order
    assert "unqualified_dependency" in codes(result, "bind", consumer)
    assert "unqualified_dependency" in codes(result, "ready", consumer)
    assert result.dependencies[0].provenance == "operator dynamic reference"


def test_real_binding_cycle_is_reported_and_never_emitted_in_input_order():
    a, b, consumer, independent = item("a"), item("b"), item("consumer"), item("independent")
    result = plan(
        inventory(
            b,
            a,
            consumer,
            independent,
            dependencies=(
                dependency(a, b),
                dependency(b, a),
                dependency(consumer, a),
            ),
        )
    )
    assert result.reference_cycles == ((a.key, b.key),)
    assert len(result.cycles) == 1
    assert result.cycles[0].items == (a.key, b.key)
    assert "safe shell-then-bind" in result.cycles[0].message
    for row in (a, b, consumer):
        assert "dependency_cycle" in codes(result, "create", row)
        assert op("create", row) not in result.executable_order
    assert op("ready", independent) in result.executable_order
    assert_valid_order(result)


def test_safe_shell_then_bind_resolves_identity_cycle_only_by_explicit_capability():
    a, b = item("a", capabilities=SHELL), item("b", capabilities=SHELL)
    result = plan(inventory(a, b, dependencies=(dependency(a, b), dependency(b, a))))
    assert result.reference_cycles == ((a.key, b.key),)
    assert not result.cycles
    assert_before(result, op("create", a), op("bind", b))
    assert_before(result, op("create", b), op("bind", a))
    for row in (a, b):
        for prerequisite in (a, b):
            assert_before(result, op("data_ready", prerequisite), op("ready", row))
    assert_valid_order(result)


def test_bound_milestone_cycle_is_not_magically_resolved_by_shell_capability():
    a, b = item("a", capabilities=SHELL), item("b", capabilities=SHELL)
    result = plan(
        inventory(
            a,
            b,
            dependencies=(
                dependency(a, b, requires=Requirement.BOUND),
                dependency(b, a, requires=Requirement.BOUND),
            ),
        )
    )
    assert result.cycles
    assert op("create", a) in result.executable_order
    assert op("create", b) in result.executable_order
    assert "dependency_cycle" in codes(result, "bind", a)
    assert "dependency_cycle" in codes(result, "ready", b)
    assert_valid_order(result)


def test_self_reference_and_data_phase_cycles_have_explicit_blockers():
    a = item("self")
    result = plan(inventory(a, dependencies=(dependency(a, a),)))
    assert result.cycles
    assert result.reference_cycles == ((a.key,),)
    a, b = item("a"), item("b")
    result = plan(
        inventory(
            a,
            b,
            dependencies=(
                dependency(a, b, phase=Phase.DATA_READY, requires=Requirement.DATA_READY),
                dependency(b, a, phase=Phase.DATA_READY, requires=Requirement.DATA_READY),
            ),
        )
    )
    assert result.cycles
    for row in (a, b):
        assert op("bind", row) in result.executable_order
        assert "dependency_cycle" in codes(result, "data_ready", row)


def test_activation_dependency_does_not_force_data_before_creation_or_enable_production():
    writer, data = item("writer"), item("data", data_required=True)
    result = plan(
        inventory(
            writer,
            data,
            dependencies=(dependency(writer, data, phase=Phase.ACTIVATE, requires=Requirement.DATA_READY),),
        )
    )
    assert op("bind", writer) in result.executable_order
    assert codes(result, "activate", writer) == {"activation_approval", "protection_missing"}
    assert "protection_missing" in codes(result, "ready", writer)


def test_endpoint_connection_and_data_readiness_have_distinct_prerequisite_operations():
    store = item("store", capabilities=STORE, data_required=True)
    consumer = item("consumer")
    endpoint = Resource(
        ResourceKey("endpoint", TENANT, "ep", "a", "store"),
        store.key,
        prepare_supported=True,
        provenance="qualified endpoint adapter",
    )
    connection = Resource(
        ResourceKey("connection", TENANT, "conn"),
        prerequisites=(endpoint.key,),
        prepare_supported=True,
        provenance="qualified connection adapter",
    )
    estate = inventory(
        store, consumer, resources=(endpoint, connection), dependencies=(dependency(consumer, connection),)
    )
    result = plan(estate)
    assert_before(result, op("bind", store), op("endpoint_ready", endpoint))
    assert_before(result, op("endpoint_ready", endpoint), op("connection_ready", connection))
    assert_before(result, op("connection_ready", connection), op("create", consumer))
    assert "protection_missing" in codes(result, "endpoint_usable", endpoint)
    assert "protection_missing" in codes(result, "connection_usable", connection)
    assert "protection_missing" in codes(result, "ready", consumer)
    assert_valid_order(result)


def test_resource_closure_also_requires_workspace_approval():
    store, consumer = item("store", "b"), item("consumer")
    endpoint = Resource(
        ResourceKey("endpoint", TENANT, "ep", "b", "store"),
        store.key,
        prepare_supported=True,
        provenance="endpoint adapter",
    )
    estate = inventory(store, consumer, resources=(endpoint,), dependencies=(dependency(consumer, endpoint),))
    result = plan(estate, Selection((CAPACITY,), include=((TENANT, "a"),)))
    assert result.required_additions == ((TENANT, "b"),)
    assert "approval_required" in codes(result, "create", consumer)


def test_endpoint_and_connection_identity_collision_never_aliases_an_item():
    owner = item("same")
    endpoint = Resource(
        ResourceKey("endpoint", TENANT, "same", "a", "same"),
        owner.key,
        prepare_supported=True,
        provenance="adapter",
    )
    connection = Resource(
        ResourceKey("connection", TENANT, "same"), prepare_supported=True, provenance="adapter"
    )
    consumer = item("consumer")
    result = plan(
        inventory(
            owner,
            consumer,
            resources=(endpoint, connection),
            dependencies=(
                dependency(consumer, endpoint),
                dependency(consumer, connection),
            ),
        )
    )
    assert {row.source for row in result.bindings} == {owner.key, consumer.key, endpoint.key, connection.key}
    assert_valid_order(result)
    with pytest.raises(PlanConfigurationError, match="tenant-scoped"):
        ResourceKey("connection", TENANT, "same", "a")
    with pytest.raises(PlanConfigurationError, match="Endpoint owner"):
        ResourceKey("endpoint", TENANT, "same")


def test_mapping_invalidation_tracks_incarnation_removal_rebinding_and_resources_transitively():
    store, model, report, independent = item("store"), item("model"), item("report"), item("independent")
    endpoint = Resource(ResourceKey("endpoint", TENANT, "ep", "a", "store"), store.key)
    connection = Resource(ResourceKey("connection", TENANT, "connection"), prerequisites=(endpoint.key,))
    estate = inventory(
        store,
        model,
        report,
        independent,
        resources=(endpoint, connection),
        dependencies=(
            dependency(model, connection),
            dependency(report, model),
        ),
    )
    before = TargetMapping(store.key, (TENANT, "recovery", "new-store"), "incarnation-1")
    after = replace(before, incarnation="incarnation-2")
    expected = {store.key, model.key, report.key, endpoint.key, connection.key}
    assert set(mapping_invalidation(estate, (before,), (after,))) == expected
    assert set(mapping_invalidation(estate, (before,), ())) == expected
    assert set(mapping_invalidation(estate, (), (after,))) == expected
    assert mapping_invalidation(estate, (before,), (before,)) == ()
    assert (
        set(mapping_invalidation(estate, (before,), (replace(before, target=(TENANT, "recovery", "other")),)))
        == expected
    )
    result = plan(estate, previous_mappings=(before,), target_mappings=(after,))
    assert set(result.invalidated) == expected
    assert independent.key not in result.invalidated
    assert result.placements[0].target_workspace == (TENANT, "recovery")


def test_existing_mappings_never_imply_completion_and_uncreated_bindings_have_no_invented_ids():
    a, b = item("a"), item("b")
    mapping = TargetMapping(a.key, (TENANT, "recovery", "target-a"), "inc")
    result = plan(inventory(a, b), target_mappings=(mapping,))
    bindings = {row.source: row for row in result.bindings}
    assert bindings[a.key].target == mapping.target
    assert bindings[b.key].target is None
    assert op("create", a) in result.executable_order
    assert codes(result, "activate", a) == {"activation_approval"}


def test_target_guards_include_uncaptured_source_coordinates_and_qualified_id_collisions():
    consumer = item("consumer")
    missing = (TENANT, "uncaptured", "original")
    estate = inventory(consumer, dependencies=(dependency(consumer, missing),))
    with pytest.raises(PlanConfigurationError, match="referenced source/control"):
        plan(
            estate,
            replacements=(
                Replacement(
                    TargetMapping(missing, (TENANT, "uncaptured", "different-id"), "inc"),
                    True,
                    True,
                    True,
                    "operator",
                ),
            ),
        )
    # Reusing an item ID in a genuinely different workspace does not alias the source.
    result = plan(
        inventory(consumer),
        target_mappings=(TargetMapping(consumer.key, (TENANT, "recovery", consumer.key[2]), "inc"),),
    )
    assert result.bindings[0].target == (TENANT, "recovery", "consumer")


def test_replacement_cannot_claim_readiness_for_a_target_this_plan_might_recreate():
    a, b = item("a"), item("b")
    mapping = TargetMapping(a.key, (TENANT, "recovery", "a"), "inc")
    with pytest.raises(PlanConfigurationError, match="also owned"):
        plan(
            inventory(a, b),
            target_mappings=(mapping,),
            replacements=(Replacement(replace(mapping, source=b.key), True, True, True, "operator"),),
        )


def test_owned_endpoint_mapping_must_match_its_qualified_owner_mapping():
    owner, consumer = item("owner"), item("consumer")
    endpoint = Resource(
        ResourceKey("endpoint", TENANT, "ep", "a", "owner"),
        owner.key,
        prepare_supported=True,
        provenance="qualified endpoint adapter",
    )
    estate = inventory(
        owner,
        consumer,
        resources=(endpoint,),
        dependencies=(dependency(consumer, endpoint),),
    )
    with pytest.raises(PlanConfigurationError, match="mapped owner"):
        plan(
            estate,
            target_mappings=(
                TargetMapping(owner.key, (TENANT, "recovery", "new-owner"), "inc"),
                TargetMapping(
                    endpoint.key, ResourceKey("endpoint", TENANT, "ep2", "recovery", "wrong"), "inc"
                ),
            ),
        )


def test_connection_resource_cycle_blocks_consumers_not_independent_items():
    a_key = ResourceKey("connection", TENANT, "a")
    b_key = ResourceKey("connection", TENANT, "b")
    a = Resource(a_key, prerequisites=(b_key,), prepare_supported=True, provenance="adapter")
    b = Resource(b_key, prerequisites=(a_key,), prepare_supported=True, provenance="adapter")
    consumer, independent = item("consumer"), item("independent")
    result = plan(
        inventory(
            consumer,
            independent,
            resources=(a, b),
            dependencies=(dependency(consumer, a),),
        )
    )
    assert result.cycles
    assert "dependency_cycle" in codes(result, "create", consumer)
    assert op("ready", independent) in result.executable_order
    assert_valid_order(result)


def test_target_ownership_mapping_conflicts_fail_before_any_plan_is_executable():
    a, b = item("a"), item("b")
    a_mapping = TargetMapping(a.key, (TENANT, "recovery", "a"), "inc")
    with pytest.raises(PlanConfigurationError, match="both an owned"):
        plan(
            inventory(a),
            target_mappings=(a_mapping,),
            replacements=(Replacement(a_mapping, True, True, True, "operator"),),
        )
    with pytest.raises(PlanConfigurationError, match="Multiple owned"):
        plan(inventory(a, b), target_mappings=(a_mapping, replace(a_mapping, source=b.key)))
    with pytest.raises(PlanConfigurationError, match="one destination workspace"):
        plan(
            inventory(a, b),
            target_mappings=(
                a_mapping,
                TargetMapping(b.key, (TENANT, "other-recovery", "b"), "inc"),
            ),
        )
    b = item("b", "b")
    with pytest.raises(PlanConfigurationError, match="must not share"):
        plan(
            inventory(a, b),
            target_mappings=(
                a_mapping,
                TargetMapping(b.key, (TENANT, "recovery", "b"), "inc"),
            ),
        )
    with pytest.raises(PlanConfigurationError, match="Unknown mapping source"):
        plan(inventory(a), target_mappings=(replace(a_mapping, source=(TENANT, "unknown", "id")),))
    with pytest.raises(PlanConfigurationError, match="incarnation"):
        TargetMapping(a.key, (TENANT, "recovery", "a"), "")


def test_dependency_evidence_and_plan_are_deterministic_under_all_input_permutations():
    a, b, c = item("a"), item("b", "b"), item("c", "c")
    edges = (dependency(a, b), dependency(a, c), dependency(b, c, provenance="operator"), dependency(a, b))
    estate = inventory(a, b, c, dependencies=edges)
    selection = Selection((CAPACITY,), keywords=("B", "a", "A"))
    first = plan(estate, selection)
    second = plan(
        replace(
            estate,
            workspaces=tuple(reversed(estate.workspaces)),
            items=tuple(reversed(estate.items)),
            dependencies=tuple(reversed(edges)),
        ),
        selection,
    )
    assert first == second
    assert len(first.dependencies) == 3
    assert first.required_additions == ((TENANT, "c"),)
    assert_valid_order(first)


def test_long_dependency_chain_does_not_hit_python_recursion_limit():
    rows = tuple(item(f"node-{number:04}") for number in range(1100))
    edges = tuple(dependency(rows[index], rows[index - 1]) for index in range(1, len(rows)))
    result = plan(inventory(*reversed(rows), dependencies=tuple(reversed(edges))))
    assert not result.cycles
    assert_before(result, op("create", rows[0]), op("create", rows[-1]))
    assert op("ready", rows[-1]) in result.executable_order
    assert_valid_order(result)
