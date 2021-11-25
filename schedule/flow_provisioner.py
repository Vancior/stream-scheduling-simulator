import time
import heapq
import logging
import random
import typing
from functools import reduce

from graph import ExecutionGraph, Vertex
from topo import Domain, Host, Node, Topology
from utils import gen_uuid, get_logger, grouped_exactly_one_full_binpack

from .provision import Provisioner
from .result import SchedulingResult


class ProvisionScatter:
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


class ProvisionNode:
    logger: logging.Logger
    scheduled_vertices: typing.List[Vertex]
    unscheduled_graphs: typing.List[ExecutionGraph]

    def __init__(
        self, name: str, type: str, node: Node, parent: typing.Optional[Node]
    ) -> None:
        self.name = name
        self.type = type
        self.node = node
        self.local_slots = node.slots
        self.slot_diff = self.local_slots
        self.parent: ProvisionNode = parent
        self.children: typing.List[ProvisionNode] = []
        self.children_slots = []
        self.scheduled_vertices = []
        self.unscheduled_graphs = []
        self.logger = get_logger(self.__class__.__name__ + "[{}]".format(self.name))

    def add_child(self, child) -> None:
        self.children.append(child)
        self.children_slots.append(0)

    def add_unscheduled_graph(self, g: ExecutionGraph) -> None:
        self.unscheduled_graphs.append(g)

    def gather_from_parent(self, scatter: ProvisionScatter):
        if scatter.unscheduled_graphs is not None:
            for g in scatter.unscheduled_graphs:
                self.add_unscheduled_graph(g)
        # NOTE parent already decrease child's slots, this is for not reporting decreasing again
        if scatter.slot_diff is not None:
            self.slot_diff += scatter.slot_diff

    def gather_from_child(self, child_name: str, scatter: ProvisionScatter):
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
    ) -> typing.Tuple[bool, ProvisionScatter, typing.List[ProvisionScatter]]:
        if len(self.unscheduled_graphs) == 0 and self.slot_diff == 0:
            return False, None, None

        if self.local_slots - self.node.occupied > 0:
            self.schedule_graph_with_limit(self.local_slots - self.node.occupied)
            self.rearrange_graphs()
        # self.logger.debug("after local scheduling: %s", self.unscheduled_graphs)

        if len(self.unscheduled_graphs) > 0:
            graph_passed_to_children = self.pass_graph_to_children()
            self.rearrange_graphs()
        else:
            graph_passed_to_children = [None for _ in self.children]
        # self.logger.debug("children graph: %s", graph_passed_to_children)
        # self.logger.debug("unscheduled graph: %s", self.unscheduled_graphs)
        # self.logger.debug("children slots: %s", self.children_slots)
        # self.logger.debug("slot diff: %s", self.slot_diff)

        children_scatters = [ProvisionScatter() for _ in self.children]
        for scatter, graphs in zip(children_scatters, graph_passed_to_children):
            scatter.unscheduled_graphs = graphs
            if graphs is not None:
                scatter.slot_diff = sum([len(g) for g in graphs])
        parent_scatter = ProvisionScatter()
        if len(self.unscheduled_graphs) > 0:
            parent_scatter.unscheduled_graphs = self.unscheduled_graphs
            self.unscheduled_graphs = []
        if self.slot_diff != 0:
            parent_scatter.slot_diff = self.slot_diff
            self.slot_diff = 0

        # print(
        #     "node[{}]: local {}, occupy {}".format(
        #         self.name, self.local_slots, self.node.occupied
        #     )
        # )
        return True, parent_scatter, children_scatters

    def schedule_graph_with_limit(self, n_slot: int):
        # NOTE: A. schedule sources first
        for g in self.unscheduled_graphs:
            for s in g.get_sources():
                if s.domain_constraint.get("host") != self.name:
                    continue
                assert n_slot > 0  # this has been checked in FlowScheduler
                self.scheduled_vertices.append(s)
                assert self.node.occupy(1)
                self.slot_diff -= 1
                n_slot -= 1
                g.remove_vertex(s.uuid)
        self.rearrange_graphs()

        vertices_num = sum([g.number_of_vertices() for g in self.unscheduled_graphs])
        if vertices_num <= n_slot:
            for g in self.unscheduled_graphs:
                for v in g.get_vertices():
                    self.scheduled_vertices.append(v)
                    assert self.node.occupy(1)
                    self.slot_diff -= 1
            self.unscheduled_graphs = []

        topological_sorted_graphs = [
            g.topological_order_with_upstream_bd() for g in self.unscheduled_graphs
        ]
        # self.logger.info([[v.uuid for v in vs] for vs in topological_sorted_graphs])
        # REVIEW upstream_bd could be replaced with exact cross-cut bd
        groups = [
            [(v_count, v.upstream_bd) for v_count, v in enumerate(vs)]
            + [(len(vs), vs[len(vs) - 1].downstream_bd)]
            for vs in topological_sorted_graphs
        ]
        solution = grouped_exactly_one_full_binpack(n_slot, groups)
        for g_idx, s_idx in enumerate(solution):
            v_count = groups[g_idx][s_idx][0]
            for vidx in range(v_count):
                v = topological_sorted_graphs[g_idx][vidx]
                self.scheduled_vertices.append(v)
                # self.logger.info("schedule %s to %s", v.uuid, self.name)
                assert self.node.occupy(1)
                self.slot_diff -= 1
                self.unscheduled_graphs[g_idx].remove_vertex(v.uuid)

    def pass_graph_to_children(self) -> typing.List[typing.List[ExecutionGraph]]:
        # SECTION A. schedule whole graphs
        heap_children_slots: typing.List[int, int, int] = []
        for idx, slots in enumerate(self.children_slots):
            heapq.heappush(heap_children_slots, (-slots, idx, slots))
        # REVIEW could be sorted by (number of vertex, input bd)
        sorted_unscheduled_graphs = sorted(
            [(g.number_of_vertices(), g) for g in self.unscheduled_graphs],
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
                # NOTE remove all vertices in original graph, so that it will be filtered out
                for v in g.get_vertices():
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
        self.rearrange_graphs()
        # self.logger.info("graphs: %s", self.unscheduled_graphs)
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
            solution = grouped_exactly_one_full_binpack(child_slots, groups)
            for graph, vertices, group, s_idx in zip(
                self.unscheduled_graphs, topological_sorted_graphs, groups, solution
            ):
                v_count = group[s_idx][0]
                if v_count == 0:
                    continue
                vertex_cut = set([vertices[i].uuid for i in range(v_count)])
                graph_passed_to_children[child_idx].append(
                    graph.sub_graph(vertex_cut, gen_uuid())
                )
                for v in vertex_cut:
                    graph.remove_vertex(v)
                self.children_slots[child_idx] -= v_count
                self.slot_diff -= v_count
            self.rearrange_graphs()
        # !SECTION
        return graph_passed_to_children

    def rearrange_graphs(self) -> None:
        self.unscheduled_graphs = reduce(
            lambda a, b: a + b,
            [
                g.connected_subgraphs()
                for g in self.unscheduled_graphs
                if g.number_of_vertices() > 0
            ],
            [],
        )

    def traversal(self, f) -> None:
        f(self)
        for child in self.children:
            child.traversal(f)


class ProvisionTree:
    name_lookup_map: typing.Dict[str, ProvisionNode]

    def __init__(self, root: ProvisionNode) -> None:
        self.name_lookup_map = {}
        self.root = root
        self.add_node(root)
        self.step_count = 0
        self.debug = False

    def add_node(self, node: ProvisionNode) -> None:
        self.name_lookup_map[node.name] = node

    def get_node(self, name: str) -> typing.Optional[ProvisionNode]:
        return self.name_lookup_map.get(name)

    def step(self) -> bool:
        if self.debug:
            time.sleep(1)
            print("=== new round {} ===".format(self.step_count))
        self.step_count += 1
        node_step_map = {k: v.step() for k, v in self.name_lookup_map.items()}
        if self.debug:
            for k, v in node_step_map.items():
                if v[0]:
                    node = self.name_lookup_map[k]
                    print(k, node.local_slots, node.node.occupied)
        updated = reduce(
            lambda i, j: i or j, [i[0] for i in node_step_map.values()], False
        )
        if not updated:
            return updated
        for node_name, step_result in node_step_map.items():
            node = self.name_lookup_map[node_name]
            if not step_result[0]:
                continue
            if step_result[1].unscheduled_graphs is not None:
                for g in step_result[1].unscheduled_graphs:
                    for v in g.get_vertices():
                        if v.uuid == "g407-v21":
                            print("from {} to parent", node_name)
            for scatter in step_result[2]:
                if scatter.unscheduled_graphs is not None:
                    for g in scatter.unscheduled_graphs:
                        for v in g.get_vertices():
                            if v.uuid == "g407-v21":
                                print("from {} to child", node_name)
            if step_result[1] is not None and node.parent is not None:
                node.parent.gather_from_child(node.name, step_result[1])
            if step_result[2] is not None:
                for child, scatter in zip(node.children, step_result[2]):
                    if scatter is not None:
                        child.gather_from_parent(scatter)
        return True

    def traversal(self, f) -> None:
        self.root.traversal(f)


class TopologicalProvisioner(Provisioner):
    domain: Domain
    topo: Topology
    tree: ProvisionTree

    def __init__(self, domain: Domain) -> None:
        super().__init__(domain)
        self.topo = domain.topo
        self.tree = self.build_provisioner_tree()
        # NOTE initial propagation for slots
        self.rebalance()

    def schedule(self, graph: ExecutionGraph) -> SchedulingResult:
        self.initial_graph_placement(graph)
        self.rebalance()
        return self.gather_scheduling_result(graph)

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> typing.List[SchedulingResult]:
        # print(self.domain.name, "run provisioning")
        for g in graph_list:
            self.initial_graph_placement(g)
        self.rebalance()
        return [self.gather_scheduling_result(g) for g in graph_list]

    def initial_graph_placement(self, g: ExecutionGraph) -> None:
        host_set: typing.Set[Host] = set()
        for s in g.get_sources():
            assert s.domain_constraint.get("host") is not None
            host = self.domain.find_host(s.domain_constraint["host"])
            assert host is not None
            host_set.add(host)
        assert len(host_set) <= 1
        if len(host_set) == 0:
            node = random.choice(list(self.tree.name_lookup_map.values()))
        else:
            host = list(host_set)[0]
            node = self.tree.get_node(host.name)
        node.add_unscheduled_graph(g.copy(g.uuid))

    def rebalance(self) -> None:
        count = 0
        while True:
            # print("rebalance round", count)
            if not self.tree.step():
                break
            count += 1
            if count > 20:
                self.tree.debug = True

    def gather_scheduling_result(self, graph: ExecutionGraph) -> SchedulingResult:
        def gather_callback(vid: str, holder: typing.List):
            def _f(node: ProvisionNode):
                for v in node.scheduled_vertices:
                    if v.uuid == vid:
                        holder[0] = node.node.uuid
                        # node.logger.info("gather %s hit %s", v.uuid, node.name)

            return _f

        result = SchedulingResult()
        for v in graph.get_vertices():
            holder = [None]
            self.tree.traversal(gather_callback(v.uuid, holder))
            if holder[0] is None:
                print(graph.uuid, v.uuid)
                for k, node in self.tree.name_lookup_map.items():
                    print(k, node.local_slots, node.node.occupied)
            assert holder[0] is not None
            result.assign(holder[0], v.uuid)
        return result

    def delete_graph(self, graph: ExecutionGraph):
        vertices_set = set([v.uuid for v in graph.get_vertices()])

        def delete_callback(node: ProvisionNode):
            pre_len = len(node.scheduled_vertices)
            node.scheduled_vertices = [
                v for v in node.scheduled_vertices if v.uuid not in vertices_set
            ]
            post_len = len(node.scheduled_vertices)
            node.slot_diff += pre_len - post_len

        self.tree.traversal(delete_callback)

    def build_provisioner_tree(self) -> ProvisionTree:
        router_node = ProvisionNode(
            self.domain.router.name, "router", self.domain.router.node, None
        )
        tree = ProvisionTree(router_node)
        for hrg in self.domain.hrgs:
            switch_node = ProvisionNode(
                hrg.switch.name, "switch", hrg.switch.node, router_node
            )
            tree.add_node(switch_node)
            for host in hrg.hosts:
                host_node = ProvisionNode(host.name, "host", host.node, switch_node)
                tree.add_node(host_node)
                switch_node.add_child(host_node)
            router_node.add_child(switch_node)
        return tree
