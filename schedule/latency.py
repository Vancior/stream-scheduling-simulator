from collections import defaultdict
import typing

from graph import ExecutionGraph
from topo import Topology
from utils import avg, get_logger

from .result import SchedulingResult


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
        if not result.check_complete(graph):
            print("incomplete scheduling")
            return
        self.graph_list.append(ScheduledGraph(graph, result))
        for u, v, d in graph.get_edges():
            self.topo.occupy_link(
                result.get_scheduled_node(u),
                result.get_scheduled_node(v),
                d["unit_size"] * d["per_second"],
            )
        # this has been executed in scheduler
        # for v in graph.get_vertices():
        #     self.topo.occupy_node(result.get_scheduled_node(v.uuid))

    def compute_latency(
        self,
    ) -> typing.Tuple[typing.Dict[str, int], typing.Dict[str, int]]:
        latency = dict()
        bp_rate = dict()
        for g in self.graph_list:
            lat, bp = self.topological_graph_latency(g)
            latency[g.graph.uuid] = lat
            bp_rate[g.graph.uuid] = bp / len(g.graph.get_edges())
        return latency, bp_rate

    def topological_graph_latency(self, g: ScheduledGraph) -> typing.Tuple[int, int]:
        back_pressure_counter = 0
        latency_dict = {}
        last_vid = None
        for v in g.graph.topological_order():
            up_vertices = g.graph.get_up_vertices(v.uuid)
            node_lat = [latency_dict[u.uuid] for u in up_vertices]
            intri_lat = [
                self.topo.get_n2n_intrinsic_latency(
                    g.result.get_scheduled_node(u.uuid),
                    g.result.get_scheduled_node(v.uuid),
                )
                for u in up_vertices
            ]
            trans_lat = [
                self.topo.get_n2n_transmission_latency(
                    g.result.get_scheduled_node(u.uuid),
                    g.result.get_scheduled_node(v.uuid),
                    g.graph.get_edge(u.uuid, v.uuid)["unit_size"],
                    g.graph.get_edge(u.uuid, v.uuid)["unit_size"]
                    * g.graph.get_edge(u.uuid, v.uuid)["per_second"],
                )
                for u in up_vertices
            ]
            # NOTE check if back-pressure exist
            for u, lat in zip(up_vertices, trans_lat):
                if (1000 / g.graph.get_edge(u.uuid, v.uuid)["per_second"]) < lat:
                    self.logger.info(
                        "bp from %s to %s: expected %f, got %f",
                        u.uuid,
                        v.uuid,
                        1000 / g.graph.get_edge(u.uuid, v.uuid)["per_second"],
                        lat,
                    )
                    back_pressure_counter += 1

            # TODO: configurable latency aggregation
            up_latency = avg(*[sum(i) for i in zip(node_lat, intri_lat, trans_lat)])
            latency_dict[v.uuid] = up_latency + self.topo.get_computation_latency(
                g.result.get_scheduled_node(v.uuid), g.graph.get_vertex(v.uuid).mi
            )
            # self.logger.info(
            #     "vertex %s: %s, com %d",
            #     v.uuid,
            #     str(
            #         list(
            #             zip(
            #                 [i.uuid for i in up_vertices],
            #                 node_lat,
            #                 intri_lat,
            #                 trans_lat,
            #             )
            #         )
            #     ),
            #     latency_dict[v.uuid] - up_latency,
            # )
            last_vid = v.uuid

        if last_vid is None:
            return 0
        return latency_dict[last_vid], back_pressure_counter
