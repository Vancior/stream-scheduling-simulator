import math
import random
import sys

sys.path.insert(0, "../..")

import graph
import topo
import yaml

import best_cut


def unit_size_cb(r: int):
    return 10000 * math.pow(10, random.randint(0, 1))


def run(topo_file, source_selector_dict, graph_count):
    sc = topo.Scenario.from_dict(
        yaml.load(open(topo_file, "r").read(), Loader=yaml.Loader)
    )
    source_selector = graph.SourceSelector(source_selector_dict)
    gen_args_list = [
        {
            "total_rank": random.randint(3, 7),
            "max_node_per_rank": random.randint(1, 2),
            "max_predecessors": random.randint(1, 2),
            "mi_cb": lambda: 1,
            "memory_cb": lambda: int(2e8),
            "unit_size_cb": unit_size_cb,
            "unit_rate_cb": lambda: random.randint(10, 20),
            "source_hosts": source_selector,
            "sink_hosts": ["cloud1"],
        }
        for _ in range(graph_count)
    ]
    graph_list = [
        graph.GraphGenerator("g" + str(idx), **gen_args).gen_dag_graph()
        for idx, gen_args in enumerate(gen_args_list)
    ]
    results = best_cut.best_cut(sc, graph_list)
    for s_cut, t_cut in results:
        print(s_cut, t_cut)


if __name__ == "__main__":
    source_dict = {"rasp1": 8, "rasp2": 8, "rasp3": 8}
    run("../../samples/1e3h.yaml", source_dict, 4)
