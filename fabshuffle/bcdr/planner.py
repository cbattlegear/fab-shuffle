"""Pure estate planning; this module makes no service calls or capability assumptions.

Keys are source coordinates, never names or globally unqualified item IDs. The coordinator
can construct them directly from captured identity fields. Destination IDs remain unknown
until returned by creation; a planned placement is not an adopted target.

An operation's presence is not evidence of completion. ``executable_order`` contains only
ungated operations, in prerequisite order. Activation always requires a separate approval;
execution, effective-access checks and persisted readiness are the coordinator's job.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TypeAlias

from fabshuffle.fabric.definitions import identity_key

WorkspaceKey: TypeAlias = tuple[str, str]
CapacityKey: TypeAlias = tuple[str, str]
ItemKey: TypeAlias = tuple[str, str, str]
OperationKey: TypeAlias = tuple[str, ...]


class PlanConfigurationError(ValueError):
    """The supplied inventory or configuration is contradictory or malformed."""


class EvidenceState(StrEnum):
    UNKNOWN = "unknown"
    PARTIAL = "partial"
    COMPLETE = "complete"


class Phase(StrEnum):
    CREATE = "create"
    BIND = "bind"
    DATA_READY = "data_ready"
    ACTIVATE = "activate"


class Requirement(StrEnum):
    IDENTITY = "identity"
    BOUND = "bound"
    DATA_READY = "data_ready"
    READY = "ready"


def _key(value: tuple[str, ...], size: int, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, tuple)
        or len(value) != size
        or any(not isinstance(part, str) or not part.strip() or part != part.strip() for part in value)
    ):
        raise PlanConfigurationError(f"{label} must contain {size} nonempty, qualified identity fields.")
    return tuple(identity_key(part) for part in value)


@dataclass(frozen=True, order=True)
class ResourceKey:
    """Connections are tenant-scoped; endpoints retain their owning item coordinates."""

    kind: str
    tenant_id: str
    resource_id: str
    workspace_id: str = ""
    item_id: str = ""

    def __post_init__(self) -> None:
        if self.kind not in {"connection", "endpoint"}:
            raise PlanConfigurationError("Resource kind must be 'connection' or 'endpoint'.")
        tenant, resource = _key((self.tenant_id, self.resource_id), 2, "Resource identity")
        object.__setattr__(self, "tenant_id", tenant)
        object.__setattr__(self, "resource_id", resource)
        if self.kind == "endpoint":
            workspace, item = _key((self.workspace_id, self.item_id), 2, "Endpoint owner")
            object.__setattr__(self, "workspace_id", workspace)
            object.__setattr__(self, "item_id", item)
        elif self.workspace_id or self.item_id:
            raise PlanConfigurationError("A connection is tenant-scoped, not item- or workspace-scoped.")


NodeKey: TypeAlias = ItemKey | ResourceKey


def _node(value: NodeKey) -> NodeKey:
    return value if isinstance(value, ResourceKey) else _key(value, 3, "Item identity")


def _sort_node(value: NodeKey) -> tuple[str, ...]:
    if isinstance(value, ResourceKey):
        return value.kind, value.tenant_id, value.workspace_id, value.item_id, value.resource_id
    return ("item", *value)


def _describe(value: NodeKey | WorkspaceKey) -> str:
    return "/".join(_sort_node(value) if isinstance(value, ResourceKey) else value)


@dataclass(frozen=True)
class Evidence:
    state: EvidenceState = EvidenceState.UNKNOWN
    provenance: tuple[str, ...] = ()
    detail: str = ""

    def __post_init__(self) -> None:
        if self.state not in set(EvidenceState):
            raise PlanConfigurationError(f"Unknown evidence state {self.state!r}.")
        if any(not origin.strip() for origin in self.provenance):
            raise PlanConfigurationError("Evidence provenance must name its source.")
        if self.state != EvidenceState.UNKNOWN and not self.provenance:
            raise PlanConfigurationError("Partial/complete dependency evidence needs recorded provenance.")
        object.__setattr__(self, "provenance", tuple(sorted(set(self.provenance))))


@dataclass(frozen=True)
class AdapterCapabilities:
    """Caller-qualified capabilities, not an item-type support matrix.

    Without safe shells, CREATE must submit an already rebound definition. BIND then records
    its application. With safe shells, CREATE writes no source definition and BIND applies it.
    """

    inactive_create: bool = False
    shell_then_bind: bool = False
    is_store: bool = False
    provenance: str = ""

    def __post_init__(self) -> None:
        if self.shell_then_bind and not self.inactive_create:
            raise PlanConfigurationError("Shell creation requires a qualified inactive-create capability.")
        if self.inactive_create and not self.provenance.strip():
            raise PlanConfigurationError("Inactive-create capability needs adapter qualification provenance.")


@dataclass(frozen=True)
class Workspace:
    key: WorkspaceKey
    capacity: CapacityKey
    display_name: str
    eligible: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", _key(self.key, 2, "Workspace"))
        object.__setattr__(self, "capacity", _key(self.capacity, 2, "Capacity"))
        if self.key[0] != self.capacity[0]:
            raise PlanConfigurationError(
                "A workspace and its source capacity must belong to the same tenant."
            )


@dataclass(frozen=True)
class Item:
    key: ItemKey
    display_name: str
    item_type: str
    evidence: Evidence = field(default_factory=Evidence)
    capabilities: AdapterCapabilities = field(default_factory=AdapterCapabilities)
    eligible: bool = True
    data_required: bool = False
    protection_available: bool = False
    protection_action: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", _key(self.key, 3, "Item"))


@dataclass(frozen=True)
class Resource:
    """A captured endpoint/connection and its explicitly qualified preparation operation.

    ``owner`` is required for endpoints. It does not imply automatic endpoint creation.
    ``prerequisites`` names resources which must be ready first, such as a connection's
    destination endpoint. Alternatively, supply a validated replacement for this resource.
    """

    key: ResourceKey
    owner: ItemKey | None = None
    prerequisites: tuple[ResourceKey, ...] = ()
    prepare_supported: bool = False
    provenance: str = ""

    def __post_init__(self) -> None:
        if self.owner is not None:
            object.__setattr__(self, "owner", _key(self.owner, 3, "Resource owner"))
        if self.key.kind == "endpoint":
            expected = (self.key.tenant_id, self.key.workspace_id, self.key.item_id)
            if self.owner != expected:
                raise PlanConfigurationError("Endpoint owner must match its qualified endpoint identity.")
        if self.prepare_supported and not self.provenance.strip():
            raise PlanConfigurationError("Resource preparation needs adapter qualification provenance.")
        object.__setattr__(self, "prerequisites", tuple(sorted(set(self.prerequisites))))


@dataclass(frozen=True)
class Dependency:
    """``consumer`` depends on ``prerequisite``; downstream never means writes-to.

    ``phase`` is the consumer gate; ``requires`` is the prerequisite milestone. A default
    binding needs a destination identity, whereas data/ready requirements must be explicit.
    Even optional/unresolved edges withhold readiness; they do not disappear from evidence.
    """

    consumer: ItemKey
    prerequisite: NodeKey
    provenance: str
    phase: Phase = Phase.BIND
    requires: Requirement = Requirement.IDENTITY
    required: bool = True
    qualified: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "consumer", _key(self.consumer, 3, "Dependency consumer"))
        object.__setattr__(self, "prerequisite", _node(self.prerequisite))
        if not self.provenance.strip():
            raise PlanConfigurationError("Every dependency must record its evidence provenance.")
        if self.phase not in set(Phase) or self.requires not in set(Requirement):
            raise PlanConfigurationError("Dependency phase or prerequisite milestone is unknown.")


@dataclass(frozen=True)
class EstateInventory:
    workspaces: tuple[Workspace, ...]
    items: tuple[Item, ...]
    dependencies: tuple[Dependency, ...] = ()
    resources: tuple[Resource, ...] = ()


@dataclass(frozen=True)
class Selection:
    """Exact includes and contains keywords form a union; exclusions always win.

    With neither includes nor keywords, select every eligible workspace inside the explicit
    source capacities. Required workspace additions are proposals until separately approved.
    """

    source_capacities: tuple[CapacityKey, ...]
    include: tuple[WorkspaceKey, ...] = ()
    exclude: tuple[WorkspaceKey, ...] = ()
    keywords: tuple[str, ...] = ()
    approved_additions: tuple[WorkspaceKey, ...] = ()

    def __post_init__(self) -> None:
        for name in ("source_capacities", "include", "exclude", "approved_additions"):
            values = tuple(sorted(_key(value, 2, name) for value in getattr(self, name)))
            if len(values) != len(set(values)):
                raise PlanConfigurationError(
                    f"{name} contains duplicate qualified identities; remove duplicates."
                )
            object.__setattr__(self, name, values)
        if not self.source_capacities:
            raise PlanConfigurationError("Choose explicit source capacities before selecting workspaces.")
        keywords = tuple(sorted({word.strip().casefold() for word in self.keywords}))
        if "" in keywords:
            raise PlanConfigurationError(
                "Remove empty keywords; use no selectors to include all scoped workspaces."
            )
        object.__setattr__(self, "keywords", keywords)
        conflicts = set(self.include) & set(self.exclude)
        conflicts |= set(self.approved_additions) & set(self.exclude)
        if conflicts:
            raise PlanConfigurationError(
                f"Remove conflicting include/approval and exclusion: {sorted(conflicts)!r}."
            )


@dataclass(frozen=True)
class CapacityMapping:
    source: CapacityKey
    target: CapacityKey

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _key(self.source, 2, "Source capacity"))
        object.__setattr__(self, "target", _key(self.target, 2, "Destination capacity"))
        if self.source[0] != self.target[0] or self.source == self.target:
            raise PlanConfigurationError("BCDR requires a distinct destination capacity in the same tenant.")


@dataclass(frozen=True)
class TargetMapping:
    source: NodeKey
    target: NodeKey
    incarnation: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _node(self.source))
        object.__setattr__(self, "target", _node(self.target))
        if isinstance(self.source, ResourceKey) != isinstance(self.target, ResourceKey):
            raise PlanConfigurationError("Mappings must preserve item/connection/endpoint identity kinds.")
        if isinstance(self.source, ResourceKey) and self.source.kind != self.target.kind:
            raise PlanConfigurationError("A resource mapping must preserve its resource kind.")
        if not self.incarnation.strip():
            raise PlanConfigurationError("Record the destination incarnation to detect recreated targets.")


@dataclass(frozen=True)
class Replacement:
    """A caller-validated independent target, never a request to adopt by name."""

    mapping: TargetMapping
    validated: bool
    independent: bool
    ready: bool
    provenance: str

    def __post_init__(self) -> None:
        if not self.validated or not self.independent or not self.provenance.strip():
            raise PlanConfigurationError(
                "Validate replacement identity, independence and runtime suitability, "
                "then record its evidence."
            )


@dataclass(frozen=True)
class Blocker:
    code: str
    subject: NodeKey
    message: str


@dataclass(frozen=True)
class Operation:
    key: OperationKey
    subject: NodeKey | WorkspaceKey
    kind: str
    prerequisites: tuple[OperationKey, ...]
    blockers: tuple[Blocker, ...]


@dataclass(frozen=True)
class WorkspacePlacement:
    source: WorkspaceKey
    target_capacity: CapacityKey
    target_workspace: WorkspaceKey | None = None


@dataclass(frozen=True)
class Binding:
    source: NodeKey
    target: NodeKey | None
    incarnation: str | None
    replacement: bool = False


@dataclass(frozen=True)
class Cycle:
    operations: tuple[OperationKey, ...]
    items: tuple[ItemKey, ...]
    message: str


@dataclass(frozen=True)
class ConsumerStatus:
    item: ItemKey
    metadata_blockers: tuple[Blocker, ...]
    readiness_blockers: tuple[Blocker, ...]


@dataclass(frozen=True)
class OperationPlan:
    selected_workspaces: tuple[WorkspaceKey, ...]
    required_additions: tuple[WorkspaceKey, ...]
    placements: tuple[WorkspacePlacement, ...]
    bindings: tuple[Binding, ...]
    evidence: tuple[tuple[ItemKey, Evidence], ...]
    dependencies: tuple[Dependency, ...]
    operations: tuple[Operation, ...]
    executable_order: tuple[OperationKey, ...]
    consumers: tuple[ConsumerStatus, ...]
    cycles: tuple[Cycle, ...]
    reference_cycles: tuple[tuple[ItemKey, ...], ...]
    invalidated: tuple[NodeKey, ...]


def _unique(values: Iterable, key, label: str) -> dict:
    result = {}
    for value in values:
        identity = key(value)
        if identity in result:
            raise PlanConfigurationError(
                f"Duplicate {label} identity {_describe(identity)}; reconcile the inventory."
            )
        result[identity] = value
    return result


def _inventory(inventory: EstateInventory) -> tuple[dict, dict, dict]:
    workspaces = _unique(inventory.workspaces, lambda row: row.key, "workspace")
    items = _unique(inventory.items, lambda row: row.key, "item")
    resources = _unique(inventory.resources, lambda row: row.key, "resource")
    for item in items.values():
        if item.key[:2] not in workspaces:
            raise PlanConfigurationError(f"Capture the workspace for item {_describe(item.key)} first.")
    for resource in resources.values():
        if resource.owner is not None and resource.owner not in items:
            raise PlanConfigurationError(
                f"Capture owner {_describe(resource.owner)} for {_describe(resource.key)}."
            )
    for edge in inventory.dependencies:
        if edge.consumer not in items:
            raise PlanConfigurationError(
                f"Unknown dependency consumer {_describe(edge.consumer)}; capture it first."
            )
    return workspaces, items, resources


def _dependencies(inventory: EstateInventory) -> dict[NodeKey, set[NodeKey]]:
    graph: dict[NodeKey, set[NodeKey]] = defaultdict(set)
    for item in inventory.items:
        graph[item.key]
    for edge in inventory.dependencies:
        graph[edge.consumer].add(edge.prerequisite)
    for resource in inventory.resources:
        graph[resource.key].update(resource.prerequisites)
        if resource.owner is not None:
            graph[resource.key].add(resource.owner)
    return graph


def mapping_invalidation(
    inventory: EstateInventory,
    previous: Sequence[TargetMapping],
    current: Sequence[TargetMapping],
) -> tuple[NodeKey, ...]:
    """Invalidate changed/removed/added mappings and all transitive consumers, including resources."""
    _inventory(inventory)
    before = _unique(previous, lambda row: row.source, "previous mapping")
    after = _unique(current, lambda row: row.source, "current mapping")
    changed = {key for key in before.keys() | after.keys() if before.get(key) != after.get(key)}
    downstream: dict[NodeKey, set[NodeKey]] = defaultdict(set)
    for consumer, dependencies in _dependencies(inventory).items():
        for prerequisite in dependencies:
            downstream[prerequisite].add(consumer)
    pending = list(changed)
    while pending:
        for consumer in downstream[pending.pop()] - changed:
            changed.add(consumer)
            pending.append(consumer)
    return tuple(sorted(changed, key=_sort_node))


def _components(graph: Mapping[OperationKey, set[OperationKey]]) -> list[tuple[OperationKey, ...]]:
    """Iterative Kosaraju SCCs, avoiding recursion limits on large captured estates."""
    nodes = set(graph)
    for edges in graph.values():
        nodes.update(edges)
    reverse: dict[OperationKey, set[OperationKey]] = defaultdict(set)
    for node, edges in graph.items():
        for edge in edges:
            reverse[edge].add(node)
    seen = set()
    order = []
    for root in sorted(nodes):
        if root in seen:
            continue
        stack = [(root, False)]
        while stack:
            node, finished = stack.pop()
            if finished:
                order.append(node)
            elif node not in seen:
                seen.add(node)
                stack.append((node, True))
                stack.extend(
                    (edge, False) for edge in sorted(graph.get(node, ()), reverse=True) if edge not in seen
                )
    seen.clear()
    components = []
    for root in reversed(order):
        if root in seen:
            continue
        component = set()
        pending = [root]
        while pending:
            node = pending.pop()
            if node in seen:
                continue
            seen.add(node)
            component.add(node)
            pending.extend(reverse[node] - seen)
        components.append(tuple(sorted(component)))
    return sorted(components)


def _blockers(values: Iterable[Blocker]) -> tuple[Blocker, ...]:
    return tuple(
        sorted(set(values), key=lambda issue: (issue.code, _sort_node(issue.subject), issue.message))
    )


def _op(stage: str, key: NodeKey | WorkspaceKey) -> OperationKey:
    if isinstance(key, ResourceKey):
        return stage, *_sort_node(key)
    return stage, *key


def _dependency_sort(edge: Dependency) -> tuple:
    return (
        edge.consumer,
        _sort_node(edge.prerequisite),
        edge.phase,
        edge.requires,
        edge.required,
        edge.qualified,
        edge.provenance,
    )


def build_operation_plan(
    inventory: EstateInventory,
    selection: Selection,
    *,
    capacity_mappings: Sequence[CapacityMapping],
    control_workspace: WorkspaceKey,
    control_warehouse: ItemKey,
    replacements: Sequence[Replacement] = (),
    target_mappings: Sequence[TargetMapping] = (),
    previous_mappings: Sequence[TargetMapping] | None = None,
) -> OperationPlan:
    """Build a standby plan, with no implicit dependency approvals or activation authority.

    All source workspace containers precede every item creation. Independent unblocked
    consumers remain executable. Missing optional protection blocks data/consumer readiness,
    not safe metadata preparation. Unknown/partial dependency evidence withholds binding.
    """
    workspaces, items, resources = _inventory(inventory)
    control_workspace = _key(control_workspace, 2, "Control workspace")
    control_warehouse = _key(control_warehouse, 3, "Control Warehouse")
    if control_warehouse[:2] != control_workspace:
        raise PlanConfigurationError("The recorded control Warehouse must belong to the control workspace.")
    capacities = _unique(capacity_mappings, lambda row: row.source, "capacity mapping")
    scope = set(selection.source_capacities)
    known_capacities = {workspace.capacity for workspace in workspaces.values()}
    if scope - known_capacities:
        raise PlanConfigurationError(
            f"Unknown source capacities {sorted(scope - known_capacities)!r}; capture them."
        )
    if set(capacities) != scope:
        raise PlanConfigurationError(
            "Provide exactly one destination-capacity mapping per selected source capacity."
        )
    if any(mapping.target in scope for mapping in capacities.values()):
        raise PlanConfigurationError(
            "Destination capacities must not overlap the selected source-capacity scope."
        )
    for key in (*selection.include, *selection.exclude, *selection.approved_additions):
        if key not in workspaces:
            raise PlanConfigurationError(
                f"Unknown workspace {_describe(key)}; select a captured workspace ID."
            )
    for key in (*selection.include, *selection.approved_additions):
        workspace = workspaces[key]
        if key == control_workspace or not workspace.eligible or workspace.capacity not in scope:
            raise PlanConfigurationError(
                f"Workspace '{workspace.display_name}' ({_describe(key)}) is control, ineligible or outside "
                "the selected capacities; correct the selection."
            )

    eligible = {
        workspace.key
        for workspace in workspaces.values()
        if workspace.eligible and workspace.capacity in scope and workspace.key != control_workspace
    }
    explicit = set(selection.include)
    matching = {
        key
        for key in eligible
        if any(word in workspaces[key].display_name.casefold() for word in selection.keywords)
    }
    initial = ((explicit | matching) if explicit or selection.keywords else eligible) - set(selection.exclude)
    edges = tuple(sorted(set(inventory.dependencies), key=_dependency_sort))
    graph = _dependencies(inventory)
    replacement_by_source = _unique(replacements, lambda row: row.mapping.source, "replacement")
    mappings = _unique(target_mappings, lambda row: row.source, "target mapping")
    if set(mappings) & set(replacement_by_source):
        raise PlanConfigurationError(
            "A source cannot have both an owned target mapping and an external replacement."
        )
    known_nodes = set(items) | set(resources)
    referenced = {node for dependencies in graph.values() for node in dependencies}
    source_workspaces = set(workspaces)
    for node in known_nodes | referenced:
        if isinstance(node, ResourceKey):
            if node.kind == "endpoint":
                source_workspaces.add((node.tenant_id, node.workspace_id))
        else:
            source_workspaces.add(node[:2])
    for mapping in (*target_mappings, *(row.mapping for row in replacements)):
        if mapping.source not in known_nodes | referenced:
            raise PlanConfigurationError(
                f"Unknown mapping source {_describe(mapping.source)}; capture its reference."
            )
        _validate_target(mapping, source_workspaces, control_workspace)
    owned_targets = [row.target for row in target_mappings]
    if len(owned_targets) != len(set(owned_targets)):
        raise PlanConfigurationError(
            "Multiple owned source identities map to one target; reconcile target ownership."
        )
    if set(owned_targets) & {row.mapping.target for row in replacements}:
        raise PlanConfigurationError(
            "A replacement target is also owned by this deployment; use a captured prerequisite instead."
        )
    workspace_targets: dict[WorkspaceKey, WorkspaceKey] = {}
    for mapping in target_mappings:
        if isinstance(mapping.source, ResourceKey):
            if mapping.source.kind == "endpoint":
                owner = (mapping.source.tenant_id, mapping.source.workspace_id, mapping.source.item_id)
                owner_mapping = mappings.get(owner)
                if owner_mapping and owner_mapping.target != (
                    mapping.target.tenant_id,
                    mapping.target.workspace_id,
                    mapping.target.item_id,
                ):
                    raise PlanConfigurationError(
                        "Destination endpoint identity does not match its mapped owner."
                    )
            continue
        source_workspace, target_workspace = mapping.source[:2], mapping.target[:2]
        if source_workspace in workspace_targets and workspace_targets[source_workspace] != target_workspace:
            raise PlanConfigurationError(
                "Owned items from one source workspace must map to one destination workspace."
            )
        workspace_targets[source_workspace] = target_workspace
    if len(set(workspace_targets.values())) != len(workspace_targets):
        raise PlanConfigurationError(
            "Distinct source workspaces must not share an owned destination workspace."
        )
    combined_mappings = (*target_mappings, *(row.mapping for row in replacements))

    roots = {
        item.key
        for item in items.values()
        if item.key[:2] in initial and item.eligible and item.key != control_warehouse
    }
    # Preview the full reachable closure even through unapproved additions.
    reached: set[NodeKey] = set()
    pending: list[NodeKey] = sorted(roots)
    while pending:
        node = pending.pop()
        if node in reached:
            continue
        reached.add(node)
        if node not in replacement_by_source:
            pending.extend(sorted(graph.get(node, ()), key=_sort_node))
    additions = {
        node[:2]
        for node in reached
        if not isinstance(node, ResourceKey)
        and node in items
        and node[:2] not in initial
        and node[:2] in eligible
        and node[:2] not in selection.exclude
        and node not in replacement_by_source
        and items[node].eligible
    }
    if set(selection.approved_additions) - additions:
        raise PlanConfigurationError(
            "Approve only the required workspace additions from this preview; "
            "remove stale/redundant approvals."
        )
    approved = initial | set(selection.approved_additions)

    def unavailable(node: NodeKey) -> Blocker | None:
        if node in replacement_by_source:
            return None
        if isinstance(node, ResourceKey):
            if node not in resources:
                return Blocker(
                    "missing_dependency",
                    node,
                    f"Capture resource {_describe(node)} or validate a replacement.",
                )
            return None
        item = items.get(node)
        if item is None:
            return Blocker(
                "missing_dependency", node, f"Capture item {_describe(node)} or validate a replacement."
            )
        name = f"'{item.display_name}' ({_describe(node)})"
        if node == control_warehouse or node[:2] == control_workspace:
            return Blocker(
                "control_dependency",
                node,
                f"{name} is recovery infrastructure; supply an independent target.",
            )
        if node[:2] in selection.exclude:
            return Blocker(
                "excluded_dependency", node, f"Remove the exclusion for {name}, or validate a replacement."
            )
        if not item.eligible or node[:2] not in eligible:
            return Blocker(
                "out_of_scope",
                node,
                f"Make {name} eligible and include its source capacity, or supply a replacement.",
            )
        if node[:2] not in approved:
            return Blocker(
                "approval_required", node, f"Approve required workspace {_describe(node[:2])} for {name}."
            )
        return None

    active = {node for node in reached if unavailable(node) is None}
    planned_items = {
        node for node in active if not isinstance(node, ResourceKey) and node not in replacement_by_source
    }
    planned_resources = {
        node for node in active if isinstance(node, ResourceKey) and node not in replacement_by_source
    }
    workspace_keys = initial | {node[:2] for node in planned_items}
    prerequisites: dict[OperationKey, set[OperationKey]] = {}
    issues: dict[OperationKey, set[Blocker]] = defaultdict(set)
    subjects: dict[OperationKey, NodeKey | WorkspaceKey] = {}
    kinds: dict[OperationKey, str] = {}

    def add(stage: str, node: NodeKey | WorkspaceKey, kind: str | None = None) -> OperationKey:
        key = _op(stage, node)
        prerequisites.setdefault(key, set())
        subjects[key] = node
        kinds[key] = kind or stage
        return key

    def milestone(node: NodeKey, requirement: Requirement) -> OperationKey:
        if node in replacement_by_source:
            stage = (
                "replacement_ready"
                if requirement in {Requirement.DATA_READY, Requirement.READY}
                else "replacement"
            )
        elif isinstance(node, ResourceKey):
            suffix = "usable" if requirement in {Requirement.DATA_READY, Requirement.READY} else "ready"
            stage = f"{node.kind}_{suffix}"
        else:
            stage = {
                Requirement.IDENTITY: "create",
                Requirement.BOUND: "bind",
                Requirement.DATA_READY: "data_ready",
                Requirement.READY: "ready",
            }[requirement]
        return _op(stage, node)

    workspace_ops = {add("workspace_create", key) for key in sorted(workspace_keys)}
    for node in sorted(active & set(replacement_by_source), key=_sort_node):
        replacement = replacement_by_source[node]
        existence = add("replacement", node)
        ready = add("replacement_ready", node)
        prerequisites[ready].add(existence)
        if not replacement.ready:
            issues[ready].add(
                Blocker(
                    "replacement_unready",
                    node,
                    "Establish data and runtime readiness for replacement "
                    f"{_describe(replacement.mapping.target)}.",
                )
            )
    for node in sorted(planned_items):
        item = items[node]
        create = add("create", node, "store_create" if item.capabilities.is_store else "item_create")
        bind = add("bind", node)
        data = add("data_ready", node)
        ready = add("ready", node)
        activate = add("activate", node)
        prerequisites[create].update(workspace_ops)
        prerequisites[bind].add(create)
        prerequisites[data].add(bind)
        prerequisites[ready].add(data)
        prerequisites[activate].add(ready)
        issues[activate].add(
            Blocker(
                "activation_approval",
                node,
                f"Enable recovery explicitly for '{item.display_name}' ({_describe(node)}) "
                "after runtime and ACL checks.",
            )
        )
        if not item.capabilities.inactive_create:
            issues[create].add(
                Blocker(
                    "inactive_create_unqualified",
                    node,
                    f"Qualify stopped creation for '{item.display_name}' ({_describe(node)}) "
                    "or restore it manually.",
                )
            )
        if item.evidence.state != EvidenceState.COMPLETE:
            blocker = Blocker(
                "dependency_evidence_gap",
                node,
                f"Complete dependency capture/operator declarations for '{item.display_name}' "
                f"({_describe(node)}); "
                f"evidence is {item.evidence.state}. {item.evidence.detail}".rstrip(),
            )
            issues[bind].add(blocker)
            if not item.capabilities.shell_then_bind:
                issues[create].add(blocker)
        if item.data_required and not item.protection_available:
            issues[data].add(
                Blocker(
                    "protection_missing",
                    node,
                    f"'{item.display_name}' ({_describe(node)}) has no qualified recovery data input. "
                    + (
                        item.protection_action
                        or "Supply an off-region export or independently available standby."
                    ),
                )
            )
    for node in sorted(planned_resources):
        resource = resources[node]
        operation = add(f"{node.kind}_ready", node)
        usable = add(f"{node.kind}_usable", node)
        prerequisites[usable].add(operation)
        needed: list[NodeKey] = list(resource.prerequisites)
        if resource.owner is not None:
            needed.append(resource.owner)
        for prerequisite in needed:
            blocker = unavailable(prerequisite)
            if blocker:
                issues[operation].add(blocker)
            else:
                prerequisites[operation].add(milestone(prerequisite, Requirement.BOUND))
                prerequisites[usable].add(milestone(prerequisite, Requirement.READY))
        if not resource.prepare_supported:
            issues[operation].add(
                Blocker(
                    "resource_unqualified",
                    node,
                    f"Qualify destination preparation for {_describe(node)}, "
                    "or supply a validated replacement.",
                )
            )

    # All members of a reference SCC must reach local data readiness before any member can
    # be considered ready. This resolves readiness-only cycles without erasing explicit
    # bind/data prerequisites or granting shell capability to an adapter.
    item_graph: dict[ItemKey, set[ItemKey]] = {key: set() for key in planned_items}
    for consumer in planned_items:
        frontier = list(graph.get(consumer, ()))
        visited = set()
        while frontier:
            node = frontier.pop()
            if node in visited:
                continue
            visited.add(node)
            if isinstance(node, ResourceKey) and node not in replacement_by_source:
                frontier.extend(graph.get(node, ()))
            elif node in planned_items:
                item_graph[consumer].add(node)
    components = _components(item_graph)
    component_of = {node: component for component in components for node in component}
    reference_cycles = tuple(
        component
        for component in components
        if len(component) > 1 or component[0] in item_graph.get(component[0], ())
    )
    for component in components:
        required_ready: set[OperationKey] = {milestone(node, Requirement.DATA_READY) for node in component}
        for member in component:
            for node in graph.get(member, ()):
                if unavailable(node) is None and node not in component:
                    required_ready.add(milestone(node, Requirement.READY))
        for member in component:
            prerequisites[_op("ready", member)].update(required_ready)
    for edge in edges:
        if edge.consumer not in planned_items:
            continue
        consumer = items[edge.consumer]
        gates = {_op(edge.phase, edge.consumer)}
        if edge.phase == Phase.BIND and not consumer.capabilities.shell_then_bind:
            gates.add(_op("create", edge.consumer))
        ready_gates = {_op("ready", member) for member in component_of[edge.consumer]}
        blocker = unavailable(edge.prerequisite)
        if not edge.qualified:
            blocker = Blocker(
                "unqualified_dependency",
                edge.prerequisite,
                f"Resolve/validate dependency {_describe(edge.prerequisite)} for '{consumer.display_name}' "
                f"({_describe(consumer.key)}), reported by {edge.provenance}.",
            )
        if blocker:
            for gate in ready_gates | gates:
                issues[gate].add(blocker)
        else:
            for gate in gates:
                prerequisites[gate].add(milestone(edge.prerequisite, edge.requires))

    cycles = []
    for component in _components(prerequisites):
        if len(component) == 1 and component[0] not in prerequisites.get(component[0], ()):
            continue
        cycle_items = tuple(
            sorted(
                {
                    subjects[key]
                    for key in component
                    if not isinstance(subjects[key], ResourceKey) and len(subjects[key]) == 3
                }
            )
        )
        message = (
            "Resolve dependency cycle "
            + " -> ".join("/".join(key) for key in component)
            + "; qualify safe shell-then-bind for identity-only bindings, or supply independent replacements."
        )
        cycles.append(Cycle(component, cycle_items, message))
        for key in component:
            issues[key].add(Blocker("dependency_cycle", subjects[key], message))

    # Propagate specific root causes rather than replacing them with generic dependency errors.
    downstream: dict[OperationKey, set[OperationKey]] = defaultdict(set)
    for operation, required in prerequisites.items():
        for prerequisite in required:
            if prerequisite not in prerequisites:
                raise PlanConfigurationError(
                    f"Missing operation {prerequisite!r}; reconcile captured prerequisites."
                )
            downstream[prerequisite].add(operation)
    pending_ops = list(issues)
    while pending_ops:
        operation = pending_ops.pop()
        for consumer in downstream[operation]:
            new = issues[operation] - issues[consumer]
            if new:
                issues[consumer].update(new)
                pending_ops.append(consumer)
    executable = {key for key in prerequisites if not issues[key]}
    indegree = {key: len(prerequisites[key]) for key in executable}
    queue = [key for key, degree in indegree.items() if degree == 0]
    heapq.heapify(queue)
    ordered = []
    while queue:
        operation = heapq.heappop(queue)
        ordered.append(operation)
        for consumer in sorted(downstream[operation] & executable):
            indegree[consumer] -= 1
            if indegree[consumer] == 0:
                heapq.heappush(queue, consumer)
    if len(ordered) != len(executable):
        raise PlanConfigurationError(
            "Unresolved operation cycle; do not execute consumers in inventory order."
        )

    bindings = []
    for node in sorted(active, key=_sort_node):
        replacement = replacement_by_source.get(node)
        mapping = replacement.mapping if replacement else mappings.get(node)
        bindings.append(
            Binding(
                node,
                mapping.target if mapping else None,
                mapping.incarnation if mapping else None,
                replacement is not None,
            )
        )
    return OperationPlan(
        selected_workspaces=tuple(sorted(initial)),
        required_additions=tuple(sorted(additions)),
        placements=tuple(
            WorkspacePlacement(key, capacities[workspaces[key].capacity].target, workspace_targets.get(key))
            for key in sorted(workspace_keys)
        ),
        bindings=tuple(bindings),
        evidence=tuple((key, items[key].evidence) for key in sorted(planned_items)),
        dependencies=tuple(
            edge for edge in edges if edge.consumer in reached and edge.consumer not in replacement_by_source
        ),
        operations=tuple(
            Operation(
                key, subjects[key], kinds[key], tuple(sorted(prerequisites[key])), _blockers(issues[key])
            )
            for key in sorted(prerequisites)
        ),
        executable_order=tuple(ordered),
        consumers=tuple(
            ConsumerStatus(key, _blockers(issues[_op("bind", key)]), _blockers(issues[_op("ready", key)]))
            for key in sorted(planned_items)
        ),
        cycles=tuple(cycles),
        reference_cycles=reference_cycles,
        invalidated=(
            mapping_invalidation(inventory, previous_mappings, combined_mappings)
            if previous_mappings is not None
            else ()
        ),
    )


def _validate_target(
    mapping: TargetMapping,
    source_workspaces: set[WorkspaceKey],
    control_workspace: WorkspaceKey,
) -> None:
    source, target = mapping.source, mapping.target
    source_tenant = source.tenant_id if isinstance(source, ResourceKey) else source[0]
    target_tenant = target.tenant_id if isinstance(target, ResourceKey) else target[0]
    if source_tenant != target_tenant:
        raise PlanConfigurationError("Cross-tenant BCDR replacements/mappings are not qualified.")
    if isinstance(target, ResourceKey):
        target_workspace = (target.tenant_id, target.workspace_id) if target.kind == "endpoint" else None
    else:
        target_workspace = target[:2]
    if target_workspace == control_workspace or target_workspace in source_workspaces:
        raise PlanConfigurationError(
            f"Target {_describe(target)} is a captured or referenced source/control workspace; "
            "supply an independent destination."
        )
    if source == target and not (isinstance(source, ResourceKey) and source.kind == "connection"):
        raise PlanConfigurationError("A replacement must not point back to the original source identity.")
