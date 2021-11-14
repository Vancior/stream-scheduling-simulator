import logging
import subprocess
import typing
from collections import defaultdict

import coloredlogs
from graph import ExecutionGraph
from topo import Scenario

from flow_cut import SourcedGraph, extract_edge_domain

logger = logging.getLogger(__name__)
coloredlogs.install(level="debug", logger=logger)


def best_cut(
    scenario: Scenario, graph_list: typing.List[ExecutionGraph]
) -> typing.List[typing.Tuple[typing.Set[str], typing.Set[str]]]:
    graph_cut_results = [(set(), set()) for _ in graph_list]

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
        free_slots = sum([n.slots - n.occupied for n in edge_domain.topo.get_nodes()])

        with open("glpk/data", "w") as f:
            index_op_map = gen_data_file(f, sg_list, free_slots)

        result = subprocess.run(
            ["glpsol", "--model", "glpk/cut.mod", "--data", "glpk/data"],
            capture_output=True,
        )
        output = result.stdout.decode("ASCII")
        solution = output.split("\n")[-3].strip().split(" ")
        assert len(solution) == sum([sg.g.number_of_vertices() for sg in sg_list])
        for idx, s in enumerate(solution):
            sg_idx = index_op_map[idx][1]
            vid = index_op_map[idx][0]
            if s == "0":
                graph_cut_results[sg_idx][1].add(vid)
            elif s == "1":
                graph_cut_results[sg_idx][0].add(vid)
            else:
                assert False
    return graph_cut_results


def gen_data_file(
    f: typing.TextIO, graph_list: typing.List[SourcedGraph], n_slots: int
) -> typing.Dict[int, typing.Tuple[str, int]]:
    num_op = sum([sg.g.number_of_vertices() for sg in graph_list])
    num_source = sum([len(sg.g.get_sources()) for sg in graph_list])
    num_sink = sum([len(sg.g.get_sinks()) for sg in graph_list])
    op_index_map = dict()
    index_op_map = dict()
    index_counter = 0
    for sg in graph_list:
        for op in sg.g.get_sources():
            op_index_map[op.uuid] = index_counter
            index_op_map[index_counter] = (op.uuid, sg.idx)
            index_counter += 1
    for sg in graph_list:
        for op in sg.g.get_operators():
            op_index_map[op.uuid] = index_counter
            index_op_map[index_counter] = (op.uuid, sg.idx)
            index_counter += 1
    for sg in graph_list:
        for op in sg.g.get_sinks():
            op_index_map[op.uuid] = index_counter
            index_op_map[index_counter] = (op.uuid, sg.idx)
            index_counter += 1
    flow_matrix = [[0 for _ in range(num_op)] for _ in range(num_op)]
    for sg in graph_list:
        for u, v, data in sg.g.get_edges():
            u_idx = op_index_map[u]
            v_idx = op_index_map[v]
            bd = data["unit_size"] * data["per_second"]
            flow_matrix[u_idx][v_idx] = bd
            # flow_matrix[u_idx][v_idx] = bd

    f.write("data;\n")
    f.write("param n := {} ;\n".format(num_op))
    f.write("param s := {} ;\n".format(n_slots))
    f.write("param source := {} ;\n".format(num_source))
    f.write("param sink := {} ;\n".format(num_sink))
    f.write("param f :")
    for i in range(1, num_op + 1):
        f.write(" {}".format(i))
    f.write(" :=\n")
    for i in range(1, num_op + 1):
        f.write("\t{}".format(i))
        for j in range(num_op):
            f.write(" {}".format(flow_matrix[i - 1][j]))
        f.write("\n")
    f.write(";\n")
    f.write("end;\n")
    return index_op_map
