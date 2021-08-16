import typing
from collections import namedtuple
from typing import NamedTuple

from graph import ExecutionGraph


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
    nodes = {v.uuid: FlowGraphNode([]) for v in g.get_vertexs()}
    edges = []
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

    flow_graph = FlowGraph(nodes, edges)
    source = g.get_sources()[0].uuid  # TODO
    sink = g.get_sinks()[0].uuid  # TODO
    while True:
        path = flow_graph.shortest_path(source, sink)
        if len(path) == 0:
            break
        min_incr = min([p.cap - p.flow for p in path])
        for p in path:
            p.flow += min_incr
            if p.cap - p.flow == 0:
                p.disabled = True

    s_cut = flow_graph.reachable(source)
    t_cut = set(flow_graph.nodes.keys()) - s_cut
    return s_cut, t_cut
