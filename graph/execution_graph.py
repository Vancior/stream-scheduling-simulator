import typing
from typing import NamedTuple

import networkx as nx
from networkx.algorithms.dag import topological_sort


class Vertex(NamedTuple):
    uuid: str
    type: str
    domain_constraint: dict
    out_unit_size: int  # in bytes
    out_unit_rate: float  # per second
    mi: int
    memory: int


class ExecutionGraph:
    g: nx.DiGraph
    uuid: str

    def __init__(self, uuid: str) -> None:
        self.g = nx.DiGraph()
        self.uuid = uuid

    def add_vertex(self, v: Vertex) -> None:
        self.g.add_node(
            v.uuid,
            type=v.type,
            domain_constraint=v.domain_constraint,
            out_unit_size=v.out_unit_size,
            mi=v.mi,
            memory=v.memory,
            vertex=v,
        )

    def connect(
        self, v_from: Vertex, v_to: Vertex, unit_size: int, per_second: int
    ) -> None:
        self.g.add_edge(
            v_from.uuid, v_to.uuid, unit_size=unit_size, per_second=per_second
        )

    def get_vertex(self, vid: str) -> Vertex:
        return self.g.nodes[vid]["vertex"]

    def get_vertexs(self) -> typing.List[Vertex]:
        return [self.get_vertex(vid) for vid in self.g.nodes()]

    def get_up_vertexs(self, vid: str) -> typing.List[Vertex]:
        return [self.get_vertex(e[0]) for e in self.g.in_edges(vid)]

    def get_down_vertexs(self, vid: str) -> typing.List[Vertex]:
        return [self.get_vertex(e[1]) for e in self.g.out_edges(vid)]

    def get_edge(self, v_from: str, v_to: str):
        return self.g.edges[v_from, v_to]

    def get_edges(self):
        return self.g.edges(data=True)

    def get_sources(self):
        return [v for v in self.get_vertexs() if v.type == "source"]

    def get_sinks(self):
        return [v for v in self.get_vertexs() if v.type == "sink"]

    def get_operators(self):
        return [v for v in self.get_vertexs() if v.type == "operator"]

    def get_in_vertexs(self):
        return [v for v in self.get_vertexs() if self.g.in_degree(v.uuid) == 0]

    def get_out_vertexs(self):
        return [v for v in self.get_vertexs() if self.g.out_degree(v.uuid) == 0]

    def topological_order(self) -> typing.Generator[Vertex, None, None]:
        return [self.g.nodes[vid]["vertex"] for vid in topological_sort(self.g)]

    def sub_graph(self, nids: typing.Set[str], uuid: str):
        g = ExecutionGraph(uuid)
        for nid in nids:
            g.add_vertex(self.get_vertex(nid))
        for e in self.get_edges():
            if e[0] in nids and e[1] in nids:
                g.connect(
                    self.get_vertex(e[0]),
                    self.get_vertex(e[1]),
                    e[2]["unit_size"],
                    e[2]["per_second"],
                )
        return g

    @classmethod
    def merge(cls, graph_list, uuid: str):
        g = ExecutionGraph(uuid)
        g.g = nx.compose_all([i.g for i in graph_list])
        return g
