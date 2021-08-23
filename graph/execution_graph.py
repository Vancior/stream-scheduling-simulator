import typing
from typing import NamedTuple

import networkx as nx
from networkx.algorithms.dag import topological_sort

from .vertex import Vertex


class ExecutionGraph:
    g: nx.DiGraph
    uuid: str

    def __init__(self, uuid: str) -> None:
        self.g = nx.DiGraph()
        self.uuid = uuid

    def __str__(self) -> str:
        return "Graph[{}][{}]".format(
            self.uuid, ", ".join([v.uuid for v in self.get_vertexs()])
        )

    def __repr__(self) -> str:
        return self.__str__()

    def number_of_vertexs(self) -> int:
        return self.g.number_of_nodes()

    def add_vertex(self, v: Vertex) -> None:
        self.g.add_node(
            v.uuid,
            type=v.type,
            domain_constraint=v.domain_constraint,
            out_unit_size=v.out_unit_size,
            mi=v.mi,
            memory=v.memory,
            upstream_bd=v.upstream_bd,
        )

    def connect(
        self, v_from: Vertex, v_to: Vertex, unit_size: int, per_second: int
    ) -> None:
        self.g.add_edge(
            v_from.uuid, v_to.uuid, unit_size=unit_size, per_second=per_second
        )
        self.g.nodes[v_to.uuid]["upstream_bd"] += unit_size * per_second

    def remove_vertex(self, vid: str) -> None:
        self.g.remove_node(vid)

    def get_vertex(self, vid: str) -> Vertex:
        return Vertex.from_networkx(vid, self.g.nodes[vid])

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

    def topological_order(self) -> typing.List[Vertex]:
        return [self.get_vertex(vid) for vid in topological_sort(self.g)]

    def topological_order_with_upstream_bd(self) -> typing.List[Vertex]:
        new_g = nx.DiGraph()
        for v in self.get_vertexs():
            new_g.add_node(v.uuid, upstream_bd=v.upstream_bd)
        for v_from, v_to, data in self.get_edges():
            new_g.add_edge(v_from, v_to, bd=data["unit_size"] * data["per_second"])

        v_seq = []
        while new_g.number_of_nodes() != 0:
            upstream_bds = sorted(
                [
                    (vid, new_g.nodes[vid]["upstream_bd"])
                    for vid in new_g.nodes()
                    if new_g.in_degree(vid) == 0
                ],
                key=lambda i: i[1],
                reverse=True,
            )
            vid = upstream_bds[0][0]
            v_seq.append(vid)
            new_g.remove_node(vid)

        return [self.get_vertex(vid) for vid in v_seq]

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
