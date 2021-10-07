import random
import typing
from collections import defaultdict, namedtuple

from algo import min_cut
from graph import ExecutionGraph
from topo import Domain, Scenario
from utils import gen_uuid, grouped_exactly_one_binpack

from .flow_provisioner import TopologicalProvisioner
from .provision import Provisioner
from .result import SchedulingResult, SchedulingResultStatus
from .scheduler import RandomScheduler, Scheduler

SourcedGraph = namedtuple("SourcedGraph", ["idx", "g"])


class FlowScheduler(Scheduler):
    provisioner_map: typing.Dict[str, Provisioner]

    def __init__(self, scenario: Scenario, provision_type: str = "topo") -> None:
        super().__init__(scenario)
        self.init_provisioner(provision_type)

    def init_provisioner(self, provision_type: str = "topo") -> None:
        def provisioner_creator(domain: Domain):
            if provision_type == "topo":
                return TopologicalProvisioner(domain)
            raise ValueError("unknown provision type")

        self.provisioner_map = {
            d.name: provisioner_creator(d) for d in self.scenario.domains
        }

    def get_provisioner(self, domain_name: str) -> Provisioner:
        assert self.provisioner_map.get(domain_name) is not None
        return self.provisioner_map.get(domain_name)

    def schedule(self, graph: ExecutionGraph) -> SchedulingResult:
        # A. if sources not fit into hosts, reject
        # B. do min-cut on g into S & T
        # C. find the smallest flow that s-cut can fit into edge slots
        # D. random schedule S & T

        # NOTE operators should not have domain constraint
        if len(graph.get_sources()) == 0:
            return self.get_provisioner(
                random.choice(self.scenario.get_cloud_domains()).name
            ).schedule(graph)

        edge_domain = self.if_source_in_single_domain(graph)
        if edge_domain is None:
            return SchedulingResult.failed("sources not in single domain")
        if not self.if_source_fit(graph, edge_domain):
            return SchedulingResult.failed("insufficient resource for sources")

        free_slots = sum([n.slots - n.occupied for n in edge_domain.topo.get_nodes()])

        cut_options = sorted(gen_cut_options(graph), key=lambda o: o.flow)
        cut_choice: CutOption = None
        for option in cut_options:
            if len(option.s_cut) <= free_slots:
                cut_choice = option
                break
        if cut_choice is None:
            return SchedulingResult.failed("slots not enough")
        s_cut, t_cut = cut_choice.s_cut, cut_choice.t_cut

        s_result = self.get_provisioner(edge_domain.name).schedule(
            graph.sub_graph(s_cut, gen_uuid())
        )
        t_result = self.get_provisioner(
            random.choice(self.scenario.get_cloud_domains()).name
        ).schedule(graph.sub_graph(t_cut, gen_uuid()))
        result = SchedulingResult.merge(s_result, t_result)
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

        # NOTE schedule non-contrained graphs to cloud
        for idx, g in enumerate(graph_list):
            if len(g.get_sources()) == 0:
                results[idx] = RandomScheduler(self.scenario).schedule(
                    g, random.choice(self.scenario.get_cloud_domains()).topo
                )

        # NOTE continue algorithm for contrained graphs
        sourced_graphs: typing.List[SourcedGraph] = [
            SourcedGraph(idx, g)
            for idx, g in enumerate(graph_list)
            if g.get_sources() != 0
        ]

        # NOTE group graphs by edge domains
        edge_domain_map: typing.Dict[str, typing.List[SourcedGraph]] = defaultdict(list)
        for sg in sourced_graphs:
            edge_domain = self.if_source_in_single_domain(sg.g)
            if edge_domain is None:
                results[sg.idx] = SchedulingResult.failed(
                    "sources not in single domain"
                )
            edge_domain_map[edge_domain.name].append(sg)

        # NOTE for each edge domain
        for domain_name, sg_list in edge_domain_map.items():
            edge_domain = self.scenario.find_domain(domain_name)
            assert edge_domain is not None
            if not self.if_source_fit([sg.g for sg in sg_list], edge_domain):
                for sg in sg_list:
                    results[sg.idx] = SchedulingResult.failed(
                        "insufficient resource for sources"
                    )
                continue

            # NOTE generate cut options, if no option provided, skip this edge domain
            graph_cut_options: typing.List[typing.List[CutOption]] = [
                sorted(gen_cut_options(sg.g), key=lambda o: o.flow, reverse=True)
                for sg in sg_list
            ]
            # for option in graph_cut_options[0]:
            #     print(option.s_cut, option.t_cut, option.flow)
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
                # NOTE if slots are enough for min-cut
                s_graph_list: typing.List[ExecutionGraph] = [
                    sg.g.sub_graph(options[0].s_cut, gen_uuid())
                    for sg, options in zip(sg_list, graph_cut_options)
                ]
                t_graph_list: typing.List[ExecutionGraph] = [
                    sg.g.sub_graph(options[0].t_cut, gen_uuid())
                    for sg, options in zip(sg_list, graph_cut_options)
                ]
                self.logger.info("graph_cutting: best")
            else:
                groups = [
                    [(len(option.s_cut), option.flow) for option in options]
                    for options in graph_cut_options
                ]
                solution = grouped_exactly_one_binpack(free_slots, groups)
                s_graph_list: typing.List[ExecutionGraph] = [
                    sg.g.sub_graph(options[s_idx].s_cut, gen_uuid())
                    for sg, options, s_idx in zip(sg_list, graph_cut_options, solution)
                ]
                t_graph_list: typing.List[ExecutionGraph] = [
                    sg.g.sub_graph(options[s_idx].t_cut, gen_uuid())
                    for sg, options, s_idx in zip(sg_list, graph_cut_options, solution)
                ]
                self.logger.info("graph_cutting: binpack")

            self.logger.info(
                "s_graph_list: %s", [g.number_of_vertices() for g in s_graph_list]
            )
            self.logger.info(
                "t_graph_list: %s", [g.number_of_vertices() for g in t_graph_list]
            )
            s_result_list = self.get_provisioner(domain_name).schedule_multiple(
                s_graph_list
            )
            # self.logger.info("s_result_list: %s", s_result_list)
            t_result_list = [
                self.get_provisioner(
                    random.choice(self.scenario.get_cloud_domains()).name
                ).schedule(g)
                for g in t_graph_list
            ]
            # self.logger.info("t_result_list: %s", t_result_list)

            for sg, s_result, t_result in zip(sg_list, s_result_list, t_result_list):
                results[sg.idx] = SchedulingResult.merge(s_result, t_result)
        # print(result_s)
        return results


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
