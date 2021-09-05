import random
import typing
from collections import defaultdict, namedtuple

import numpy as np
from algo import min_cut
from graph import ExecutionGraph
from topo import Domain, Topology
from utils import gen_uuid

from .result import SchedulingResult, SchedulingResultStatus
from .scheduler import RandomScheduler, Scheduler

MAX_FLOW = int(1e18)

SourcedGraph = namedtuple("SourcedGraph", ["idx", "g"])


class FlowScheduler(Scheduler):
    def schedule(self, g: ExecutionGraph, topo: Topology = None) -> SchedulingResult:
        # A. if sources not fit into hosts, reject
        # B. do min-cut on g into S & T
        # C. if |S| > edge slots, goto D, else goto E
        # D1. incrementally do min-cut on S to until it fits
        # E. random schedule S & T

        random_scheduler = RandomScheduler(self.scenario)

        # NOTE: operators should not have domain constraint
        if len(g.get_sources()) == 0:
            return random_scheduler.schedule(
                g, random.choice(self.scenario.get_cloud_domains()).topo
            )

        edge_domain = self.if_source_in_single_domain(g)
        if edge_domain is None:
            return SchedulingResult.failed("sources not in single domain")
        if not self.if_source_fit(g, edge_domain):
            return SchedulingResult.failed("insufficient resource for sources")

        free_slots = sum([n.slots - n.occupied for n in edge_domain.topo.get_nodes()])

        cut_options = sorted(gen_cut_options(g), key=lambda o: o.flow)
        cut_choice: CutOption = None
        for option in cut_options:
            if len(option.s_cut) <= free_slots:
                cut_choice = option
                break
        if cut_choice is None:
            return SchedulingResult.failed("slots not enough")
        s_cut, t_cut = cut_choice.s_cut, cut_choice.t_cut

        result = self.random_schedule(g, s_cut, t_cut, edge_domain)
        if result.status != SchedulingResultStatus.FAILED:
            self.logger.info(
                "free slots: %d; newly occupied: %d", free_slots, len(s_cut)
            )
        return result

    def schedule_multiple(
        self, graph_list: typing.List[ExecutionGraph]
    ) -> typing.List[SchedulingResult]:
        results = [None for _ in graph_list]
        # result_s = [0 for _ in graph_list]

        # NOTE: schedule non-contraint graph to cloud
        for idx, g in enumerate(graph_list):
            if len(g.get_sources()) == 0:
                results[idx] = RandomScheduler(self.scenario).schedule(
                    g, random.choice(self.scenario.get_cloud_domains()).topo
                )

        sourced_graphs: typing.List[SourcedGraph] = [
            SourcedGraph(idx, g)
            for idx, g in enumerate(graph_list)
            if g.get_sources() != 0
        ]

        # NOTE: group graphs by edge domains
        edge_domain_map: typing.Dict[str, typing.List[SourcedGraph]] = defaultdict(list)
        for sg in sourced_graphs:
            edge_domain = self.if_source_in_single_domain(sg.g)
            if edge_domain is None:
                results[sg.idx] = SchedulingResult.failed(
                    "sources not in single domain"
                )
            # REVIEW: should be checked combined
            if not self.if_source_fit(sg.g, edge_domain):
                results[sg.idx] = SchedulingResult.failed(
                    "insufficient resource for sources"
                )
            edge_domain_map[edge_domain.name].append(sg)

        # NOTE: for each edge domain
        for domain_name, sg_list in edge_domain_map.items():
            edge_domain = self.scenario.find_domain(domain_name)
            if edge_domain is None:
                continue

            # NOTE: generate cut options, if no option provided, skip this edge domain
            graph_cut_options: typing.List[typing.List[CutOption]] = [
                sorted(gen_cut_options(sg.g), key=lambda o: o.flow) for sg in sg_list
            ]
            if len([None for options in graph_cut_options if len(options) == 0]) > 0:
                self.logger.error("no option provided")
                continue

            free_slots = sum(
                [n.slots - n.occupied for n in edge_domain.topo.get_nodes()]
            )
            if (
                sum(
                    [
                        len(options[0].s_cut)
                        for options in graph_cut_options
                        if len(options) > 0
                    ]
                )
                <= free_slots
            ):
                # NOTE: if slots are enough for min-cut
                s_graph_list = []
                for sg, options in zip(sg_list, graph_cut_options):
                    s_graph_list.append(sg.g.sub_graph(options[0].s_cut, gen_uuid()))
                big_s_graph = ExecutionGraph.merge(s_graph_list, gen_uuid())
                big_result = RandomScheduler(self.scenario).schedule(
                    big_s_graph, edge_domain.topo
                )
                for sg, options in zip(sg_list, graph_cut_options):
                    s_result = big_result.extract(options[0].s_cut)
                    t_result = RandomScheduler(self.scenario).schedule(
                        sg.g.sub_graph(options[0].t_cut),
                        random.choice(self.scenario.get_cloud_domains()).topo,
                    )
                    results[sg.idx] = SchedulingResult.merge(s_result, t_result)
            else:
                dp = np.full((free_slots + 1,), MAX_FLOW, dtype=np.int64)
                selected = np.full((free_slots + 1,), -1, dtype=np.int32)
                selected[0] = 0
                choices = np.full((len(sg_list), free_slots + 1), -1, dtype=np.int32)
                dp[0] = 0
                for idx, sg in enumerate(sg_list):
                    options = graph_cut_options[idx]
                    for capacity in range(free_slots, -1, -1):
                        for o_idx, option in enumerate(options):
                            volume = len(option.s_cut)
                            if volume > capacity:
                                continue
                            # NOTE: if previous groups are selected
                            # NOTE: if selected[capacity] not be overwrited at this round, it cannot be used at next round
                            if selected[capacity - volume] == idx and (
                                dp[capacity - volume] + option.flow < dp[capacity]
                                or selected[capacity] <= idx
                            ):
                                dp[capacity] = dp[capacity - volume] + option.flow
                                selected[capacity] = idx + 1
                                choices[idx, capacity] = o_idx
                # print(dp)
                # print(selected)
                # print(choices)
                valid_idx = np.where(selected == len(sg_list))[0]
                backtrace = valid_idx[dp[valid_idx].argmin()]
                # print(backtrace)
                option_choice: typing.List[CutOption] = [
                    None for _ in range(len(sg_list))
                ]
                for i in range(len(sg_list) - 1, -1, -1):
                    if choices[i, backtrace] < 0:
                        self.logger.error("malicious result")
                        continue
                    option_choice[i] = graph_cut_options[i][choices[i, backtrace]]
                    backtrace -= len(option_choice[i].s_cut)

                s_graph_list = []
                for sg, option in zip(sg_list, option_choice):
                    if option is None:
                        self.logger.error("malicious option")
                        continue
                    s_graph_list.append(sg.g.sub_graph(option.s_cut, gen_uuid()))
                big_s_graph = ExecutionGraph.merge(s_graph_list, gen_uuid())
                big_result = RandomScheduler(self.scenario).schedule(
                    big_s_graph, edge_domain.topo
                )
                for sg, option in zip(sg_list, option_choice):
                    if option is None:
                        continue
                    s_result = big_result.extract(option.s_cut)
                    t_result = RandomScheduler(self.scenario).schedule(
                        sg.g.sub_graph(option.t_cut, gen_uuid()),
                        random.choice(self.scenario.get_cloud_domains()).topo,
                    )
                    results[sg.idx] = SchedulingResult.merge(s_result, t_result)
                    # result_s[sg.idx] = len(option.s_cut)
        # print(result_s)
        return results

    def random_schedule(
        self,
        g: ExecutionGraph,
        s_cut: typing.Set[str],
        t_cut: typing.Set[str],
        edge_domain: Domain,
    ) -> SchedulingResult:
        random_scheduler = RandomScheduler(self.scenario)
        random_scheduler.logger.setLevel(self.logger.getEffectiveLevel())
        s_result = random_scheduler.schedule(
            g.sub_graph(s_cut, gen_uuid()), edge_domain.topo
        )
        if s_result.status == SchedulingResultStatus.FAILED:
            return SchedulingResult.failed(s_result.reason)
        random_scheduler.logger.setLevel(self.logger.getEffectiveLevel())
        t_result = random_scheduler.schedule(
            g.sub_graph(t_cut, gen_uuid()),
            random.choice(self.scenario.get_cloud_domains()).topo,
        )
        if t_result.status == SchedulingResultStatus.FAILED:
            return SchedulingResult.failed(t_result.reason)

        return SchedulingResult.merge(s_result, t_result)


class CutOption(typing.NamedTuple):
    s_cut: typing.Set[str]
    t_cut: typing.Set[str]
    flow: int


def gen_cut_options(g: ExecutionGraph) -> typing.List[CutOption]:
    options: typing.List[CutOption] = []
    s_cut, t_cut, flow = min_cut(g)
    options.append(CutOption(s_cut, t_cut, flow))

    while len(s_cut) > 1:
        sub_graph = g.sub_graph(s_cut, gen_uuid())
        s_cut, _, flow = min_cut(sub_graph)
        options.append(
            CutOption(s_cut, set([v.uuid for v in g.get_vertices()]) - s_cut, flow)
        )

    return options
