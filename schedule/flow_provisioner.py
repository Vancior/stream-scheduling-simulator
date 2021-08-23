import heapq
import typing
from collections import defaultdict

import numpy as np
from graph import ExecutionGraph
from topo import Domain, Host, Node, Topology
from utils import gen_uuid

from schedule import SchedulingResult


class ProvisionerNode:
    unscheduled_graphs: typing.List[ExecutionGraph]

    def __init__(
        self, name: str, type: str, node: Node, parent: typing.Optional[Node]
    ) -> None:
        self.name = name
        self.type = type
        self.node = node
        self.local_slots = node.slots
        self.slot_diff = 0
        self.parent = parent
        self.children = []
        self.children_slots = []
        self.scheduled_vertexs = []
        self.unscheduled_graphs = []

    def add_child(self, child) -> None:
        self.children.append(child)
        self.children_slots.append(0)

    def add_unscheduled_graph(self, g: ExecutionGraph) -> None:
        self.unscheduled_graphs.append(g)

    def child_slots_change(self, child_name: str, slot_diff: int):
        for idx, child in enumerate(self.children):
            if child.name == child_name:
                self.children_slots[idx] += slot_diff
                break
        self.slot_diff += slot_diff

    def step(self) -> bool:
        if len(self.unscheduled_graphs) == 0:
            return self.slot_diff != 0

        if self.local_slots - self.node.occupied > 0:
            self.schedule_graph_with_limit(self.local_slots - self.node.occupied)

        print("after local scheduling", self.unscheduled_graphs)
        if len(self.unscheduled_graphs) > 0:
            graph_passed_to_children = self.pass_graph_to_children()

        # TODO: build scatter
        # A. pass graphs to children
        # B. pass residual unscheduled graphs to parent & clear local
        # C. update slot_diff to parent
        return True

    def schedule_graph_with_limit(self, n_slot: int):
        for g in self.unscheduled_graphs:
            for s in g.get_sources():
                if s.domain_constraint.get("host") != self.name:
                    continue
                assert n_slot > 0  # this has been checked in FlowScheduler
                self.scheduled_vertexs.append(s)
                assert self.node.occupy(1)
                self.slot_diff -= 1
                n_slot -= 1
                g.remove_vertex(s.uuid)

        topological_sorted_graphs = [
            g.topological_order_with_upstream_bd() for g in self.unscheduled_graphs
        ]
        groups = [
            [(v_count + 1, v.upstream_bd) for v_count, v in enumerate(vs)]
            for vs in topological_sorted_graphs
        ]
        solution = self.dp(n_slot, groups)
        for gid, eid in solution:
            v_count = groups[gid][eid][0]
            for vidx in range(v_count):
                v = topological_sorted_graphs[gid][vidx]
                self.scheduled_vertexs.append(v)
                assert self.node.occupy(1)
                self.slot_diff -= 1
                self.unscheduled_graphs[gid].remove_vertex(v.uuid)

        self.unscheduled_graphs = [
            g for g in self.unscheduled_graphs if g.number_of_vertexs() > 0
        ]

    def pass_graph_to_children(self) -> typing.List[typing.List[ExecutionGraph]]:
        # A. fill slots if a whole graph could be fitted
        heap_children_slots: typing.List[int, int, int] = []
        for idx, slots in enumerate(self.children_slots):
            heapq.heappush(heap_children_slots, (-slots, idx, slots))
        # REVIEW: could be sorted by input bd
        sorted_unscheduled_graphs = sorted(
            [(g.number_of_vertexs(), g) for g in self.unscheduled_graphs],
            key=lambda i: i[0],
            reverse=True,
        )

        graph_passed_to_children: typing.List[typing.List[ExecutionGraph]] = [
            list() for _ in self.children_slots
        ]
        while len(heap_children_slots) > 0:
            _, child_idx, child_slots = heapq.heappop(heap_children_slots)
            g_idx = 0
            for n_vertex, g in sorted_unscheduled_graphs:
                if n_vertex > child_slots:
                    g_idx += 1
                    continue
                graph_passed_to_children[child_idx].append(g)
                self.children_slots[child_idx] -= n_vertex
                if n_vertex < child_slots:
                    heapq.heappush(
                        heap_children_slots,
                        (n_vertex - child_slots, child_idx, child_slots - n_vertex),
                    )
                break
            if g_idx >= len(sorted_unscheduled_graphs):
                continue
            sorted_unscheduled_graphs.pop(g_idx)
            sorted_unscheduled_graphs = sorted(
                sorted_unscheduled_graphs, key=lambda i: i[0], reverse=True
            )

        for child_idx, child_slots in enumerate(self.children_slots):
            if child_slots == 0:
                continue
            topological_sorted_graphs = [
                g.topological_order_with_upstream_bd() for g in self.unscheduled_graphs
            ]
            groups = [
                [(v_count + 1, v.upstream_bd) for v_count, v in enumerate(vs)]
                for vs in topological_sorted_graphs
            ]
            solution = self.dp(child_slots, groups)
            for gid, eid in solution:
                v_count = groups[gid][eid][0]
                vertex_cut = set(
                    [topological_sorted_graphs[gid][i].uuid for i in range(v_count)]
                )
                graph_passed_to_children[child_idx].append(
                    self.unscheduled_graphs[gid].sub_graph(vertex_cut, gen_uuid())
                )
                for v in vertex_cut:
                    self.unscheduled_graphs[gid].remove_vertex(v)

        return graph_passed_to_children

    def dp(
        self, n_slot: int, groups: typing.List[typing.List[typing.Tuple[int, int]]]
    ) -> typing.List[typing.Tuple[int, int]]:
        dp = [0 for _ in range(n_slot + 1)]
        selected = [[] for _ in range(n_slot + 1)]
        for gid, group in enumerate(groups):
            for capacity in range(n_slot, -1, -1):
                for eid, ele in enumerate(group):
                    volume, value = ele
                    if capacity < volume:
                        continue
                    if dp[capacity - volume] + value > dp[capacity]:
                        dp[capacity] = dp[capacity - volume] + value
                        selected[capacity] = selected[capacity - volume] + [(gid, eid)]
        return selected[n_slot]


class ProvisionerTree:
    name_lookup_map: typing.Dict[str, ProvisionerNode]

    def __init__(self, root: ProvisionerNode) -> None:
        self.name_lookup_map = {}
        self.root = root
        self.add_node(root)

    def add_node(self, node: ProvisionerNode) -> None:
        self.name_lookup_map[node.name] = node

    def get_node(self, name: str) -> typing.Optional[ProvisionerNode]:
        return self.name_lookup_map.get(name)


class TopologicalResourceProvisioner:
    domain: Domain
    topo: Topology
    tree: ProvisionerTree

    def __init__(self, domain: Domain) -> None:
        self.domain = domain
        self.topo = domain.topo
        self.tree = self.build_provisioner_tree()

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> SchedulingResult:
        pass

    def initial_graph_placement(self, g: ExecutionGraph) -> None:
        host_set: typing.Set[Host] = set()
        for s in g.get_sources():
            assert s.domain_constraint.get("host") is not None
            host_set.add(self.domain.find_host(s.domain_constraint["host"]))
        assert len(host_set) == 1
        host = list(host_set)[0]
        node = self.tree.get_node(host.name)
        node.add_unscheduled_graph(g)

    def build_provisioner_tree(self) -> ProvisionerTree:
        router_node = ProvisionerNode(
            self.domain.router.name, "router", self.domain.router.node, None
        )
        tree = ProvisionerTree(router_node)
        for hrg in self.domain.hrgs:
            switch_node = ProvisionerNode(
                hrg.switch.name, "switch", hrg.switch.node, router_node
            )
            tree.add_node(switch_node)
            for host in hrg.hosts:
                host_node = ProvisionerNode(host.name, "host", host.node, switch_node)
                tree.add_node(host_node)
                switch_node.add_child(host_node)
            router_node.add_child(switch_node)
