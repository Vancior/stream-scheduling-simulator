import typing
from collections import defaultdict, namedtuple
from typing import NamedTuple

import IPython
from graph import ExecutionGraph
from utils import gen_uuid

MAX_EDGE_CAPACITY = int(1e20)


class FlowGraphNode(NamedTuple):
    out_edges: typing.List[int]


class FlowGraphEdge:
    from_node: str
    to_node: str
    cap: int  # in bps
    flow: int
    disabled: bool

    def __init__(self, from_node: str, to_node: str, cap: int, flow: int):
        self.from_node = from_node
        self.to_node = to_node
        self.cap = cap
        self.flow = flow
        self.disabled = False


class FlowGraph:
    nodes: typing.Dict[str, FlowGraphNode]
    edges: typing.List[FlowGraphEdge]

    def __init__(
        self, nodes: typing.Dict[str, FlowGraphNode], edges: typing.List[FlowGraphEdge]
    ) -> None:
        self.nodes = nodes
        self.edges = edges

    def shortest_path(self, s: str, t: str) -> typing.List[int]:
        QueueItem = namedtuple("QueueItem", ["node", "edges"])
        queue: typing.List[QueueItem] = []
        queue.append(QueueItem(s, []))
        visited: typing.Dict[str, bool] = defaultdict(bool)
        visited[s] = True
        while len(queue) > 0:
            current = queue.pop(0)
            if current.node == t:
                return current.edges
            for e_idx in self.nodes[current.node].out_edges:
                edge = self.edges[e_idx]
                if (
                    (not edge.disabled)
                    and edge.flow < edge.cap
                    and (not visited[e_idx])
                ):
                    visited[e_idx] = True
                    queue.append(QueueItem(edge.to_node, current.edges + [e_idx]))
        return []

    def reachable(self, s: str) -> typing.Set[str]:
        reachable_set = set()
        queue = []
        queue.append(s)
        while len(queue) > 0:
            current = queue.pop(0)
            reachable_set.add(current)
            for to_node in [
                self.edges[i].to_node
                for i in self.nodes[current].out_edges
                if not self.edges[i].disabled and self.edges[i].cap > 0
            ]:
                if to_node not in reachable_set:
                    queue.append(to_node)
        return reachable_set


def min_cut(g: ExecutionGraph):
    nodes: typing.Dict[str, FlowGraphNode] = {
        v.uuid: FlowGraphNode([]) for v in g.get_vertices()
    }
    edges: typing.List[FlowGraphEdge] = []
    index = 0
    for u, v, d in g.get_edges():
        # output_node = g.get_vertex(e[0])
        # bd = int(output_node.out_unit_rate * output_node.out_unit_size)
        bd = d["unit_size"] * d["per_second"]
        edges.append(FlowGraphEdge(u, v, bd, 0))
        nodes[u].out_edges.append(index)
        index += 1
        edges.append(FlowGraphEdge(v, u, 0, 0))
        nodes[v].out_edges.append(index)
        index += 1

    fake_source = gen_uuid()
    fake_sink = gen_uuid()
    nodes[fake_source] = FlowGraphNode([])
    nodes[fake_sink] = FlowGraphNode([])
    for s in g.get_in_vertices():
        edges.append(FlowGraphEdge(fake_source, s.uuid, MAX_EDGE_CAPACITY, 0))
        nodes[fake_source].out_edges.append(index)
        index += 1
        edges.append(FlowGraphEdge(s.uuid, fake_source, 0, 0))
        nodes[s.uuid].out_edges.append(index)
        index += 1
    for s in g.get_out_vertices():
        edges.append(FlowGraphEdge(s.uuid, fake_sink, MAX_EDGE_CAPACITY, 0))
        nodes[s.uuid].out_edges.append(index)
        index += 1
        edges.append(FlowGraphEdge(fake_sink, s.uuid, 0, 0))
        nodes[fake_sink].out_edges.append(index)
        index += 1

    flow_graph = FlowGraph(nodes, edges)
    while True:
        edge_idxes = flow_graph.shortest_path(fake_source, fake_sink)
        if len(edge_idxes) == 0:
            break
        min_incr = min([edges[idx].cap - edges[idx].flow for idx in edge_idxes])
        for edge_idx in edge_idxes:
            edge = edges[edge_idx]
            oppo_edge = edges[edge_idx ^ 1]
            edge.flow += min_incr
            oppo_edge.flow -= min_incr
            if edge.cap - edge.flow == 0:
                edge.disabled = True

    s_cut = flow_graph.reachable(fake_source)
    t_cut = set(flow_graph.nodes.keys()) - s_cut
    s_cut.remove(fake_source)
    t_cut.remove(fake_sink)

    total_flow = 0
    for e in edges:
        if e.from_node in s_cut and e.to_node in t_cut:
            total_flow += e.flow

    return s_cut, t_cut, total_flow
