import typing
from collections import namedtuple
from typing import NamedTuple
from utils import gen_uuid

from graph import ExecutionGraph

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

    def shortest_path(self, s: str, t: str) -> typing.List[FlowGraphEdge]:
        QueueItem = namedtuple("QueueItem", ["node", "edges"])
        queue: typing.List[QueueItem] = []
        queue.append(QueueItem(s, []))
        while len(queue) > 0:
            current = queue.pop(0)
            if current.node == t:
                return current.edges
            for i in self.nodes[current.node].out_edges:
                e = self.edges[i]
                if not e.disabled and e.flow < e.cap:
                    queue.append(QueueItem(e.to_node, current.edges + [e]))
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
                if not self.edges[i].disabled
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
        path = flow_graph.shortest_path(fake_source, fake_sink)
        if len(path) == 0:
            break
        min_incr = min([p.cap - p.flow for p in path])
        for p in path:
            p.flow += min_incr
            if p.cap - p.flow == 0:
                p.disabled = True

    s_cut = flow_graph.reachable(fake_source)
    t_cut = set(flow_graph.nodes.keys()) - s_cut
    s_cut.remove(fake_source)
    t_cut.remove(fake_sink)

    total_flow = 0
    for e in edges:
        if e.from_node in s_cut and e.to_node in t_cut:
            total_flow += e.flow

    return s_cut, t_cut, total_flow
