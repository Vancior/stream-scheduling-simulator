import logging
import random
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial

from graph import ExecutionGraph
from topo import Domain, Scenario, Topology
from utils import gen_uuid, get_logger

from .result import SchedulingResult, SchedulingResultStatus


class Scheduler(ABC):
    logger: logging.Logger

    def __init__(self, scenario: Scenario) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.scenario = scenario

    @abstractmethod
    def schedule(self, g: ExecutionGraph) -> SchedulingResult:
        raise NotImplementedError()

    @abstractmethod
    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> typing.List[SchedulingResult]:
        raise NotImplementedError()

    def if_source_in_single_domain(self, g: ExecutionGraph) -> typing.Optional[Domain]:
        domain_set = set()
        for s in g.get_sources():
            for d in self.scenario.get_edge_domains():
                if d.find_host(s.domain_constraint["host"]) is not None:
                    domain_set.add(d.name)
        if len(domain_set) == 1:
            return self.scenario.find_domain(list(domain_set)[0])
        return None

    def if_source_fit(
        self, graph_list: typing.List[ExecutionGraph], domain: Domain
    ) -> bool:
        host_vertex_count = defaultdict(int)
        for g in graph_list:
            for s in g.get_sources():
                host_vertex_count[s.domain_constraint["host"]] += 1

        for hostname, count in host_vertex_count.items():
            host = domain.find_host(hostname)
            if host is None:
                return False
            self.logger.info(
                "required: %d; remaining: %d",
                count,
                host.node.slots - host.node.occupied,
            )
            if not domain.topo.slot_filter(count, host.node.uuid):
                return False
        return True


class RandomScheduler(Scheduler):
    def schedule(self, graph: ExecutionGraph, topo: Topology) -> SchedulingResult:
        """schedule vertex in topological order (source would be scheduled first)"""

        if len(graph.get_sources()) > 0:
            edge_domain = self.if_source_in_single_domain(graph)
            if edge_domain is None:
                return SchedulingResult.failed("sources not in single domain")
            if not self.if_source_fit([graph], edge_domain):
                return SchedulingResult.failed("insufficient resource for sources")

        result = SchedulingResult()
        ordered_vertices = (
            graph.get_sources() + graph.get_operators() + graph.get_sinks()
        )
        for v in ordered_vertices:
            nid_list = list(
                filter(
                    partial(topo.slot_filter, 1),
                    filter(
                        partial(topo.label_filter, v.domain_constraint),
                        [h.uuid for h in topo.get_hosts()],
                    ),
                )
            )
            if len(nid_list) == 0:
                return SchedulingResult.failed(
                    "no available host for {}".format(v.uuid)
                )
            nid = random.choice(nid_list)
            self.logger.debug("Select node %s for vertex %s", nid, v.uuid)
            result.assign(nid, v.uuid)
            topo.occupy_node(nid, 1)

        return result

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph], topo: Topology
    ) -> typing.List[SchedulingResult]:
        big_graph = ExecutionGraph.merge(graph_list, gen_uuid())
        big_result = self.schedule(big_graph, topo)
        if big_result.status == SchedulingResultStatus.FAILED:
            return [
                SchedulingResult(big_result.status, big_result.reason)
                for _ in graph_list
            ]
        return [
            big_result.extract(set([v.uuid for v in g.get_vertices()]))
            for g in graph_list
        ]
