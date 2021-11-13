import sys

sys.path.insert(0, "../..")

import graph
import logging
import math
import networkx as nx
import schedule as sch
import random
import topo
import uuid
import yaml


def gen_uuid():
    return str(uuid.uuid4())[:8]


def run():
    sc = topo.Scenario.from_dict(
        yaml.load(open("../../samples/1e3h.yaml", "r").read(), Loader=yaml.Loader)
    )
    source_selector = graph.SourceSelector({"rasp1": 4, "rasp2": 4, "rasp3": 4})
    gen_args_list = [
        {
            "total_rank": random.randint(7, 7),
            "max_node_per_rank": random.randint(2, 2),
            "max_predecessors": random.randint(2, 2),
            # "mi_cb": lambda: int(math.pow(10, (random.random() * 1) + 0)),
            "mi_cb": lambda: 1,
            "memory_cb": lambda: int(2e8),
            # "unit_size_cb": lambda: int(math.pow(10, (random.random() * 1) + 4)),
            # "unit_rate_cb": lambda: int(math.pow(10, (random.random() * 1) + 1)),
            "unit_size_cb": lambda r: random.randint(20000, 50000)
            / (math.pow(2, r - 1)),
            "unit_rate_cb": lambda: random.randint(10, 20),
            "source_hosts": source_selector,
            "sink_hosts": ["cloud1"],
        }
        for _ in range(8)
    ]
    graph_list = [
        graph.GraphGenerator("g" + str(idx), **gen_args).gen_dag_graph()
        for idx, gen_args in enumerate(gen_args_list)
    ]
    # nx.draw(graph_list[0].g)
    # f = open("../../cases/a.yaml")
    # graph_list = graph.ExecutionGraph.load_all(f)
    # f.close()

    with open("../../cases/a.yaml", "w") as f:
        graph.ExecutionGraph.save_all(graph_list, f)

    sc.topo.clear_occupied()
    flow_scheduler = sch.FlowScheduler(sc)
    flow_scheduler.logger.setLevel(logging.INFO)
    flow_calculator = sch.LatencyCalculator(sc.topo)
    flow_calculator.logger.setLevel(logging.INFO)
    flow_result_list = flow_scheduler.schedule_multiple(graph_list)
    for g, result in zip(graph_list, flow_result_list):
        if result is None:
            print("none")
            continue
        flow_calculator.add_scheduled_graph(g, result)
    flow_latency, flow_bp = flow_calculator.compute_latency()
    print(flow_latency)
    print(flow_bp)
    print(sum(flow_latency.values()))
    print(sum(flow_latency.values()) / len(flow_latency))
    print(sum(flow_bp.values()) / len(flow_bp))

    sc.topo.clear_occupied()
    all_cloud_scheduler = sch.RandomScheduler(sc)
    all_cloud_scheduler.logger.setLevel(logging.INFO)
    all_cloud_calculator = sch.LatencyCalculator(sc.topo)
    all_cloud_calculator.logger.setLevel(logging.INFO)
    all_cloud_result_list = []
    s_graph_list = []
    t_graph_list = []
    for g in graph_list:
        s_cut = set([v.uuid for v in g.get_sources()])
        t_cut = set([v.uuid for v in g.get_sinks()]).union(
            set([v.uuid for v in g.get_operators()])
        )
        s_graph_list.append(g.sub_graph(s_cut, gen_uuid()))
        t_graph_list.append(g.sub_graph(t_cut, gen_uuid()))
    s_result_list = all_cloud_scheduler.schedule_multiple(
        s_graph_list, sc.get_edge_domains()[0].topo
    )
    t_result_list = all_cloud_scheduler.schedule_multiple(
        t_graph_list, sc.get_cloud_domains()[0].topo
    )
    for g, s_result, t_result in zip(graph_list, s_result_list, t_result_list):
        if s_result.status == sch.SchedulingResultStatus.FAILED:
            print("s_graph {} failed: {}".format(g.uuid, s_result.reason))
            continue
        if t_result.status == sch.SchedulingResultStatus.FAILED:
            print("t_graph {} failed: {}".format(g.uuid, t_result.reason))
            continue
        result = sch.SchedulingResult.merge(s_result, t_result)
        all_cloud_calculator.add_scheduled_graph(g, result)
    all_cloud_latency, all_cloud_bp = all_cloud_calculator.compute_latency()
    print(all_cloud_latency)
    print(all_cloud_bp)
    print(sum(all_cloud_latency.values()))
    print(sum(all_cloud_latency.values()) / len(all_cloud_latency))
    print(sum(all_cloud_bp.values()) / len(all_cloud_bp))


if __name__ == "__main__":
    run()
