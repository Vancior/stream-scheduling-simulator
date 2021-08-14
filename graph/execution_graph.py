import typing
from typing import NamedTuple

import networkx as nx
from networkx.algorithms.dag import topological_sort


class Vertex(NamedTuple):
    uuid: str
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
            domain_constraint=v.domain_constraint,
            out_unit_size=v.out_unit_size,
            mi=v.mi,
            memory=v.memory,
            vertex=v,
        )

    def connect(self, v_from: Vertex, v_to: Vertex, bd: int) -> None:
        self.g.add_edge(v_from.uuid, v_to.uuid, bd=bd)

    def get_vertex(self, vid: str) -> Vertex:
        return self.g.nodes[vid]["vertex"]

    def get_vertexs(self) -> typing.List[Vertex]:
        return [self.get_vertex(vid) for vid in self.g.nodes()]

    def get_edge(self, v_from: str, v_to: str):
        return self.g.edges[v_from, v_to]

    def get_edges(self):
        return list(self.g.edges())

    def get_sources(self):
        return [v for v in self.get_vertexs() if self.g.in_degree(v.uuid) == 0]

    def get_sinks(self):
        return [v for v in self.get_vertexs() if self.g.out_degree(v.uuid) == 0]

    def topological_order(self) -> typing.Generator[Vertex, None, None]:
        return [self.g.nodes[vid]["vertex"] for vid in topological_sort(self.g)]
