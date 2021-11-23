import logging
import os
import sys
import typing
from collections import defaultdict

import coloredlogs
from algo import min_cut, min_cut2
from graph import ExecutionGraph
from topo import Domain, Scenario
from utils import gen_uuid, grouped_exactly_one_binpack

logger = logging.getLogger(__name__)
coloredlogs.install(level="debug", logger=logger)


class SourcedGraph(typing.NamedTuple):
    idx: int
    g: ExecutionGraph


def flow_cut(
    scenario: Scenario, graph_list: typing.List[ExecutionGraph]
) -> typing.List[typing.Tuple[typing.Set[str], typing.Set[str]]]:
    graph_cut_results = [None for _ in graph_list]

    sourced_graphs: typing.List[SourcedGraph] = [
        SourcedGraph(idx, g) for idx, g in enumerate(graph_list)
    ]

    edge_domain_map: typing.Dict[str, typing.List[SourcedGraph]] = defaultdict(list)
    for sg in sourced_graphs:
        edge_domain = extract_edge_domain(scenario, sg.g)
        assert edge_domain is not None
        edge_domain_map[edge_domain.name].append(sg)

    for domain_name, sg_list in edge_domain_map.items():
        edge_domain = scenario.find_domain(domain_name)
        assert edge_domain is not None

        graph_cut_options: typing.List[typing.List[CutOption]] = [
            sorted(gen_cut_options(sg.g), key=lambda o: o.flow, reverse=False)
            for sg in sg_list
        ]
        # for sg, options in zip(sg_list, graph_cut_options):
        #     print("graph", sg.g.uuid)
        #     for op in options:
        #         print(str(op))
        if len([None for options in graph_cut_options if len(options) == 0]) > 0:
            logger.error("no option provided")
            continue

        free_slots = sum([n.slots - n.occupied for n in edge_domain.topo.get_nodes()])
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
            # logger.info("graph_cutting: best")
            s_cut_list: typing.List[typing.Set[str]] = [
                options[0].s_cut for options in graph_cut_options
            ]
            t_cut_list: typing.List[typing.Set[str]] = [
                options[0].t_cut for options in graph_cut_options
            ]
        else:
            # logger.info("graph_cutting: binpack")
            groups = [
                [(len(option.s_cut), option.flow) for option in options]
                for options in graph_cut_options
            ]
            solution = grouped_exactly_one_binpack(free_slots, groups)
            s_cut_list: typing.List[typing.Set[str]] = [
                options[s_idx].s_cut
                for options, s_idx in zip(graph_cut_options, solution)
            ]
            t_cut_list: typing.List[typing.Set[str]] = [
                options[s_idx].t_cut
                for options, s_idx in zip(graph_cut_options, solution)
            ]

        for sg, s_cut, t_cut in zip(sg_list, s_cut_list, t_cut_list):
            graph_cut_results[sg.idx] = (s_cut, t_cut)

    return graph_cut_results


def extract_edge_domain(
    scenario: Scenario, g: ExecutionGraph
) -> typing.Optional[Domain]:
    domain_set = set()
    for s in g.get_sources():
        for d in scenario.get_edge_domains():
            if d.find_host(s.domain_constraint["host"]) is not None:
                domain_set.add(d.name)
    if len(domain_set) == 1:
        return scenario.find_domain(list(domain_set)[0])
    return None


class CutOption(typing.NamedTuple):
    s_cut: typing.Set[str]
    t_cut: typing.Set[str]
    flow: int

    def __str__(self) -> str:
        return "s: {}, f: {}".format(str(self.s_cut), self.flow)


def gen_cut_options(g: ExecutionGraph) -> typing.List[CutOption]:
    options: typing.List[CutOption] = []
    try:
        # s_cut, t_cut = min_cut(g)
        s_cut, t_cut = min_cut2(g)
        flow = cross_bd(g, s_cut, t_cut)
        options.append(CutOption(s_cut, t_cut, flow))
        # print(s_cut, t_cut)
        round = 0
        while len(s_cut) > 1:
            sub_graph = g.sub_graph(s_cut, gen_uuid())
            # s_cut, _ = min_cut(sub_graph)
            s_cut, _ = min_cut2(sub_graph)
            t_cut = set([v.uuid for v in g.get_vertices()]) - s_cut
            flow = cross_bd(g, s_cut, t_cut)
            options.append(CutOption(s_cut, t_cut, flow))
            # print(s_cut, t_cut)
            round += 1
            if round > 100:
                # with open("debug/g1.yaml", "w") as f:
                #     ExecutionGraph.save_all([g], f)
                raise RuntimeError("too many rounds")
                sys.exit()
    except Exception as e:
        with open("debug/g3.yaml", "w") as f:
            ExecutionGraph.save_all([g], f)
        raise e

    return options


def cross_bd(g: ExecutionGraph, s_cut: typing.Set[str], t_cut: typing.Set[str]) -> int:
    total_bd = 0
    for u, v, data in g.get_edges():
        if u in s_cut and v in t_cut:
            total_bd += data["unit_size"] * data["per_second"]
        if u in t_cut and v in s_cut:
            total_bd += data["unit_size"] * data["per_second"]
    return total_bd
