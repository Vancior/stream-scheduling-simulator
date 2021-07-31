import logging
import typing

from execution_graph import ExecutionGraph
from scheduler import SchedulingResult
from topology import Topology
from utils import get_logger


class ScheduledGraph(typing.NamedTuple):
    graph: ExecutionGraph
    result: SchedulingResult


class LatencyCalculator:
    graph_list: typing.List[ScheduledGraph]

    def __init__(self, topo: Topology) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.topo = topo
        self.graph_list = []

    def add_scheduled_graph(
        self, graph: ExecutionGraph, result: SchedulingResult
    ) -> None:
        self.graph_list.append(ScheduledGraph(graph, result))
        for u, v in graph.get_edges():
            self.topo.occupy_link(
                result.get_scheduled_node(u), result.get_scheduled_node(v)
            )

    def compute_latency(self) -> typing.Dict[str, int]:
        result = dict()
        for g in self.graph_list:
            computation_lat = 0
            for vertex in g.graph.get_vertexs():
                nid = g.result.get_scheduled_node(vertex.uuid)
                computation_lat += int(vertex.mi / self.topo.get_node(nid).mips * 1000)

            # FIXME: wrong logic when parallel streams exist
            intrinsic_lat = 0
            for u, v in g.graph.get_edges():
                intrinsic_lat += self.topo.get_n2n_intrinsic_latency(
                    g.result.get_scheduled_node(u), g.result.get_scheduled_node(v)
                )
            transmission_lat = 0
            for u, v in g.graph.get_edges():
                transmission_lat += self.topo.get_n2n_transmission_latency(
                    g.result.get_scheduled_node(u),
                    g.result.get_scheduled_node(v),
                    g.graph.get_vertex(u).out_unit_size,
                )
            result[g.graph.uuid] = computation_lat + intrinsic_lat + transmission_lat
        return result
