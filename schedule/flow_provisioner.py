import heapq
import logging
import time
import typing
from collections import defaultdict
from functools import reduce

import numpy as np
from graph import ExecutionGraph
from topo import Domain, Host, Node, Topology
from utils import gen_uuid, get_logger

from schedule import SchedulingResult
from schedule.flow_scheduler import MAX_FLOW

MAX_FLOW = int(1e18)


class ProvisionerScatter:
    unscheduled_graphs: typing.List[ExecutionGraph]
    slot_diff: int

    def __init__(
        self,
        unscheduled_graphs: typing.List[ExecutionGraph] = None,
        slot_diff: int = None,
    ) -> None:
        self.unscheduled_graphs = unscheduled_graphs
        self.slot_diff = slot_diff

    def __str__(self) -> str:
        return "slot: {}, graphs: {}".format(self.slot_diff, self.unscheduled_graphs)

    def __repr__(self) -> str:
        return self.__str__()

    def empty(self) -> bool:
        return self.unscheduled_graphs is None and self.slot_diff is None


class ProvisionerNode:
    unscheduled_graphs: typing.List[ExecutionGraph]
    logger: logging.Logger

    def __init__(
        self, name: str, type: str, node: Node, parent: typing.Optional[Node]
    ) -> None:
        self.name = name
        self.type = type
        self.node = node
        self.local_slots = node.slots
        self.slot_diff = self.local_slots
        self.parent: ProvisionerNode = parent
        self.children: typing.List[ProvisionerNode] = []
        self.children_slots = []
        self.scheduled_vertexs = []
        self.unscheduled_graphs = []
        self.logger = get_logger(self.__class__.__name__ + "[{}]".format(self.name))

    def add_child(self, child) -> None:
        self.children.append(child)
        self.children_slots.append(0)

    def add_unscheduled_graph(self, g: ExecutionGraph) -> None:
        self.unscheduled_graphs.append(g)

    def gather_from_parent(self, scatter: ProvisionerScatter):
        if scatter.unscheduled_graphs is not None:
            for g in scatter.unscheduled_graphs:
                self.add_unscheduled_graph(g)

    def gather_from_child(self, child_name: str, scatter: ProvisionerScatter):
        for child_idx, child in enumerate(self.children):
            if child.name != child_name:
                continue
            if scatter.slot_diff is not None:
                self.children_slots[child_idx] += scatter.slot_diff
                self.slot_diff += scatter.slot_diff
            if scatter.unscheduled_graphs is not None:
                for g in scatter.unscheduled_graphs:
                    self.add_unscheduled_graph(g)

    def step(
        self,
    ) -> typing.Tuple[bool, ProvisionerScatter, typing.List[ProvisionerScatter]]:
        if len(self.unscheduled_graphs) == 0 and self.slot_diff == 0:
            return False, None, None

        if self.local_slots - self.node.occupied > 0:
            self.schedule_graph_with_limit(self.local_slots - self.node.occupied)
            self.clear_empty_graphs()
        self.logger.debug("after local scheduling: %s", self.unscheduled_graphs)

        if len(self.unscheduled_graphs) > 0:
            graph_passed_to_children = self.pass_graph_to_children()
            self.clear_empty_graphs()
        else:
            graph_passed_to_children = [None for _ in self.children]
        self.logger.debug("children graph: %s", graph_passed_to_children)
        self.logger.debug("unscheduled graph: %s", self.unscheduled_graphs)
        self.logger.debug("children slots: %s", self.children_slots)
        self.logger.debug("slot diff: %s", self.slot_diff)

        children_scatters = [ProvisionerScatter() for _ in self.children]
        for scatter, graphs in zip(children_scatters, graph_passed_to_children):
            scatter.unscheduled_graphs = graphs
        parent_scatter = ProvisionerScatter()
        if len(self.unscheduled_graphs) > 0:
            parent_scatter.unscheduled_graphs = self.unscheduled_graphs
            self.unscheduled_graphs = []
        if self.slot_diff != 0:
            parent_scatter.slot_diff = self.slot_diff
            self.slot_diff = 0

        return True, parent_scatter, children_scatters

    def schedule_graph_with_limit(self, n_slot: int):
        # NOTE: A. schedule sources first
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
        # REVIEW upstream_bd could be replaced with exact cross-cut bd
        groups = [
            [(v_count, v.upstream_bd) for v_count, v in enumerate(vs)]
            + [(len(vs), vs[len(vs) - 1].downstream_bd)]
            for vs in topological_sorted_graphs
        ]
        solution = self.grouped_exactly_one_binpack(n_slot, groups)
        for gid, s_idx in enumerate(solution):
            v_count = groups[gid][s_idx][0]
            for vidx in range(v_count):
                v = topological_sorted_graphs[gid][vidx]
                self.scheduled_vertexs.append(v)
                assert self.node.occupy(1)
                self.slot_diff -= 1
                self.unscheduled_graphs[gid].remove_vertex(v.uuid)

    def pass_graph_to_children(self) -> typing.List[typing.List[ExecutionGraph]]:
        # SECTION A. schedule whole graphs
        heap_children_slots: typing.List[int, int, int] = []
        for idx, slots in enumerate(self.children_slots):
            heapq.heappush(heap_children_slots, (-slots, idx, slots))
        # REVIEW could be sorted by (number of vertex, input bd)
        sorted_unscheduled_graphs = sorted(
            [(g.number_of_vertexs(), g) for g in self.unscheduled_graphs],
            key=lambda i: i[0],
            reverse=True,
        )

        graph_passed_to_children: typing.List[typing.List[ExecutionGraph]] = [
            list() for _ in self.children_slots
        ]
        # NOTE pick the largest children slot everytime
        while len(heap_children_slots) > 0:
            _, child_idx, child_slots = heapq.heappop(heap_children_slots)
            g_idx = 0
            # NOTE find the largest graph that can fit into the slot as a whole
            for n_vertex, g in sorted_unscheduled_graphs:
                if n_vertex > child_slots:
                    g_idx += 1
                    continue
                # NOTE copy graph
                graph_passed_to_children[child_idx].append(g.copy(g.uuid))
                sorted_unscheduled_graphs.pop(g_idx)
                # NOTE remove all vertexs in original graph, so that it will be filtered out
                for v in g.get_vertexs():
                    g.remove_vertex(v.uuid)
                self.children_slots[child_idx] -= n_vertex
                self.slot_diff -= n_vertex
                # NOTE if capacity is not 0, push into heap
                if n_vertex < child_slots:
                    heapq.heappush(
                        heap_children_slots,
                        (n_vertex - child_slots, child_idx, child_slots - n_vertex),
                    )
                break
        # !SECTION
        # SECTION B. split graphs to remaining children slots (from large to small)
        for child_idx, child_slots in sorted(
            enumerate(self.children_slots), key=lambda e: e[1], reverse=True
        ):
            if child_slots == 0:
                continue
            topological_sorted_graphs = [
                g.topological_order_with_upstream_bd() for g in self.unscheduled_graphs
            ]
            # REVIEW upstream_bd could be replaced with exact cross-cut bd
            groups = [
                [(v_count, v.upstream_bd) for v_count, v in enumerate(vs)]
                + [(len(vs), vs[len(vs) - 1].downstream_bd)]
                for vs in topological_sorted_graphs
            ]
            solution = self.grouped_exactly_one_binpack(child_slots, groups)
            for gid, eid in enumerate(solution):
                v_count = groups[gid][eid][0]
                if v_count == 0:
                    continue
                vertex_cut = set(
                    [topological_sorted_graphs[gid][i].uuid for i in range(v_count)]
                )
                graph_passed_to_children[child_idx].append(
                    self.unscheduled_graphs[gid].sub_graph(vertex_cut, gen_uuid())
                )
                for v in vertex_cut:
                    self.unscheduled_graphs[gid].remove_vertex(v)
                self.children_slots[child_idx] -= v_count
                self.slot_diff -= v_count
        # !SECTION
        return graph_passed_to_children

    def grouped_exactly_one_binpack(
        self, n_slot: int, groups: typing.List[typing.List[typing.Tuple[int, int]]]
    ) -> typing.List[int]:
        dp = np.full((n_slot + 1,), MAX_FLOW, dtype=np.int64)
        selected = np.full((n_slot + 1,), -1, dtype=np.int32)
        selected[0] = 0
        choices = np.full((len(groups), n_slot + 1), -1, dtype=np.int32)
        dp[0] = 0
        for gid, group in enumerate(groups):
            for capacity in range(n_slot, -1, -1):
                for eid, ele in enumerate(group):
                    volume, value = ele
                    if capacity < volume:
                        continue
                    if selected[capacity - volume] == gid and (
                        dp[capacity - volume] + value < dp[capacity]
                        or selected[capacity] <= gid
                    ):
                        dp[capacity] = dp[capacity - volume] + value
                        selected[capacity] = gid + 1
                        choices[gid, capacity] = eid
        valid_idx = np.where(selected == len(groups))[0]
        backtrace = valid_idx[-1]
        solution: typing.List[int] = [None for _ in range(len(groups))]
        for gid in range(len(groups) - 1, -1, -1):
            assert choices[gid, backtrace] >= 0
            solution[gid] = choices[gid, backtrace]
            backtrace -= groups[gid][solution[gid]][0]
        return solution

    def clear_empty_graphs(self) -> None:
        self.unscheduled_graphs = [
            g for g in self.unscheduled_graphs if g.number_of_vertexs() > 0
        ]

    def traversal(self, f) -> None:
        f(self)
        for child in self.children:
            child.traversal(f)


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

    def step(self) -> bool:
        node_step_map = {k: v.step() for k, v in self.name_lookup_map.items()}
        updated = reduce(
            lambda i, j: i or j, [i[0] for i in node_step_map.values()], False
        )
        if not updated:
            return updated
        for node_name, step_result in node_step_map.items():
            node = self.name_lookup_map[node_name]
            if not step_result[0]:
                continue
            if step_result[1] is not None and node.parent is not None:
                node.parent.gather_from_child(node.name, step_result[1])
            if step_result[2] is not None:
                for child, scatter in zip(node.children, step_result[2]):
                    if scatter is not None:
                        child.gather_from_parent(scatter)
        return True

    def traversal(self, f) -> None:
        self.root.traversal(f)


class TopologicalResourceProvisioner:
    domain: Domain
    topo: Topology
    tree: ProvisionerTree

    def __init__(self, domain: Domain) -> None:
        self.domain = domain
        self.topo = domain.topo
        self.tree = self.build_provisioner_tree()
        # NOTE initial propagation for slots
        self.rebalance()

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> SchedulingResult:
        pass

    def initial_graph_placement(self, g: ExecutionGraph) -> None:
        host_set: typing.Set[Host] = set()
        for s in g.get_sources():
            assert s.domain_constraint.get("host") is not None
            host = self.domain.find_host(s.domain_constraint["host"])
            assert host is not None
            host_set.add(host)
        assert len(host_set) == 1
        host = list(host_set)[0]
        node = self.tree.get_node(host.name)
        node.add_unscheduled_graph(g)

    def rebalance(self) -> None:
        while self.tree.step():
            pass

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
        return tree
