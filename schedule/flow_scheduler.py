import random
from topo.topology import Topology
import typing

from algo import min_cut
from graph import ExecutionGraph

from .result import SchedulingResult, SchedulingResultStatus
from .scheduler import RandomScheduler, Scheduler
from utils import gen_uuid


class FlowScheduler(Scheduler):
    def schedule(self, g: ExecutionGraph, topo: Topology) -> SchedulingResult:
        # A. if sources not fit into hosts, reject
        # B. do min-cut on g into S & T
        # C. if |S| > edge slots, goto D, else goto E
        # D1. incrementally do min-cut on S to until it fits
        # E. random schedule S & T

        if len(g.get_sources()) == 0:
            return RandomScheduler(self.scenario).schedule(g, topo)

        edge_domain = self.if_source_in_single_domain(g)
        if edge_domain is None:
            return SchedulingResult.failed("sources not in single domain")
        if not self.if_source_fit(g, edge_domain):
            return SchedulingResult.failed("insufficient resource for sources")

        s_cut, t_cut = min_cut(g)
        free_slots = sum([n.slots - n.occupied for n in edge_domain.topo.get_nodes()])
        if len(s_cut) <= free_slots:
            self.logger.debug("enough free slots")
            s_random_scheduler = RandomScheduler(self.scenario)
            s_random_scheduler.logger.setLevel(self.logger.getEffectiveLevel())
            s_result = s_random_scheduler.schedule(
                g.sub_graph(s_cut, gen_uuid()), edge_domain.topo
            )
            if s_result.status == SchedulingResultStatus.FAILED:
                return SchedulingResult.failed(s_result.reason)
            t_random_scheduler = RandomScheduler(self.scenario)
            t_random_scheduler.logger.setLevel(self.logger.getEffectiveLevel())
            t_result = t_random_scheduler.schedule(
                g.sub_graph(t_cut, gen_uuid()),
                random.choice(self.scenario.get_cloud_domains()).topo,
            )
            if t_result.status == SchedulingResultStatus.FAILED:
                return SchedulingResult.failed(t_result.reason)
            return SchedulingResult.merge(s_result, t_result)

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> typing.List[SchedulingResult]:
        pass
