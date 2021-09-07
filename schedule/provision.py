import random
import typing
from abc import ABC, abstractmethod
from functools import partial

from graph import ExecutionGraph
from topo import Domain
from utils import gen_uuid

from .result import SchedulingResult, SchedulingResultStatus


class Provisioner(ABC):
    domain: Domain

    def __init__(self, domain: Domain) -> None:
        self.domain = domain

    @abstractmethod
    def schedule(self, graph: ExecutionGraph) -> SchedulingResult:
        raise NotImplementedError()

    @abstractmethod
    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> typing.List[SchedulingResult]:
        raise NotImplementedError()

    def check_graph_domain(self, graph: ExecutionGraph) -> bool:
        for v in graph.get_vertices():
            if (
                v.domain_constraint.get("host") is not None
                and self.domain.find_host(v.domain_constraint.get("host")) is None
            ):
                return False
        return True


class RandomProvisioner(Provisioner):
    def schedule(self, graph: ExecutionGraph) -> SchedulingResult:
        if not self.check_graph_domain(graph):
            return SchedulingResult.failed("domain constraint violation")

        result = SchedulingResult()
        for v in graph.topological_order():
            nid_list = list(
                filter(
                    partial(self.domain.topo.slot_filter, 1),
                    filter(
                        partial(self.domain.topo.label_filter, v.domain_constraint),
                        [h.uuid for h in self.domain.get_hosts()],
                    ),
                )
            )
            if len(nid_list) == 0:
                return SchedulingResult.failed("no available host")
            nid = random.choice(nid_list)
            result.assign(nid, v.uuid)
            self.domain.topo.occupy_node(nid, 1)

        return result

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> typing.List[SchedulingResult]:
        big_graph = ExecutionGraph.merge(graph_list, gen_uuid())
        big_result = self.schedule(big_graph)
        if big_result.status == SchedulingResultStatus.FAILED:
            return [SchedulingResult.failed(big_result.reason) for _ in graph_list]
        return [
            big_result.extract(set([v.uuid for v in g.get_vertices()]))
            for g in graph_list
        ]
