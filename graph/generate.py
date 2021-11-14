from collections import defaultdict
import random
import math
import typing

from .execution_graph import ExecutionGraph, Vertex

default_parameter_range = {
    "graph_length_range": (3, 11),  # equal
    "mi_range": (10, 10),  # equal
    "initial_unit_size_range": (1000, 1000),
    "initial_unit_rate_range": (100, 100),
    "unit_size_range": (100, 10000),
    "unit_rate_range": (1, 10),
    "data_scale_ratio_cb": lambda c, t: math.pow(10, random.random() - 0.5),
}


class SourceSelector:
    def __init__(self, sources: typing.Dict[str, int]):
        self.sources = dict(sources)
        self.total_slots = sum(sources.values())

    def select(self) -> str:
        idx = random.randint(0, self.total_slots - 1)
        for k in random.sample(self.sources.keys(), len(self.sources)):
            if idx < self.sources[k]:
                self.sources[k] -= 1
                self.total_slots -= 1
                return k
            idx -= self.sources[k]
        assert False


class GraphGenerator:
    def __init__(self, name: str, **kwargs) -> None:
        self.name = name
        self.gen_args = kwargs
        random.seed()

    def gen_chain_graph(self) -> ExecutionGraph:
        total_level = self.gen_args["graph_length"]

        g = ExecutionGraph(self.name)
        v_source = Vertex(
            "{}-v{}".format(self.name, 0),
            "source",
            {"host": random.choice(self.gen_args["source_hosts"])},
            self.gen_args["initial_unit_size"],
            self.gen_args["initial_unit_rate"],
            10,
            int(1e8),
        )
        g.add_vertex(v_source)
        current_level = 1
        last_vertex_uuid = v_source.uuid
        last_unit_size = v_source.out_unit_rate
        while current_level < total_level:
            v = Vertex(
                "{}-v{}".format(self.name, current_level),
                {},
                last_unit_size * self.gen_args["data_scale_ratio_cb"],
                self.gen_args["initial_unit_rate"],
                10,
                int(1e8),
            )
            current_level += 1

    def gen_random_chain_graph(self) -> ExecutionGraph:
        total_level = self.gen_args["graph_length"]
        g = ExecutionGraph(self.name)

        v_source = Vertex.from_spec(
            "{}-v{}".format(self.name, 1),
            "source",
            {"host": random.choice(self.gen_args["source_hosts"])},
            0,
            0,
            self.gen_args["mi_cb"](),
            self.gen_args["memory_cb"](),
        )
        g.add_vertex(v_source)
        v_sink = Vertex.from_spec(
            "{}-v{}".format(self.name, total_level),
            "sink",
            {"host": random.choice(self.gen_args["sink_hosts"])},
            0,
            0,
            self.gen_args["mi_cb"](),
            self.gen_args["memory_cb"](),
        )
        g.add_vertex(v_sink)

        last_vertex = v_source
        current_v_idx = 2
        while current_v_idx < total_level:
            v = Vertex.from_spec(
                "{}-v{}".format(self.name, current_v_idx),
                "operator",
                {},
                0,
                0,
                self.gen_args["mi_cb"](),
                self.gen_args["memory_cb"](),
            )
            g.add_vertex(v)
            if last_vertex == v_source:
                size = self.gen_args["unit_size_cb"]()
                rate = self.gen_args["unit_rate_cb"]()
                print("graph {} source bd {} mbps".format(g.uuid, size * rate / 1e6))
                g.connect(
                    last_vertex,
                    v,
                    size,
                    rate
                    # self.gen_args["unit_size_cb"](),
                    # NOTE 5x data density for source
                    # self.gen_args["unit_rate_cb"]() * 100,
                )
            else:
                g.connect(
                    last_vertex,
                    v,
                    self.gen_args["unit_size_cb"](),
                    self.gen_args["unit_rate_cb"](),
                )
            last_vertex = v
            current_v_idx += 1

        g.connect(
            last_vertex,
            v_sink,
            self.gen_args["unit_size_cb"](),
            self.gen_args["unit_rate_cb"](),
        )
        return g

    def gen_dag_graph(self) -> ExecutionGraph:
        total_rank = self.gen_args["total_rank"]
        max_node_per_rank = self.gen_args["max_node_per_rank"]
        max_predecessors = self.gen_args["max_predecessors"]

        g = ExecutionGraph(self.name)

        node_rank_map: typing.Dict[int, int] = dict()
        ranked_nodes = [[0]]
        node_seq_num = 1
        node_rank_map[0] = 0
        edges = []
        node_successor_cnt = defaultdict(int)
        for rank in range(1, total_rank):
            cur_node_cnt = random.randint(1, max_node_per_rank)
            cur_nodes = [node_seq_num + i for i in range(cur_node_cnt)]
            node_seq_num += cur_node_cnt
            for node in cur_nodes:
                node_rank_map[node] = rank
                pre_cnt = random.randint(1, max_predecessors)
                for pre_node in self.dag_select_predecessors(
                    ranked_nodes, pre_cnt, node_successor_cnt
                ):
                    edges.append((pre_node, node))
                    node_successor_cnt[pre_node] += 1
                    # print("{} ---> {}".format(pre_node, node))
            ranked_nodes.append(cur_nodes)

        node_cnt = node_seq_num
        node_out_degree: typing.Dict[int, int] = defaultdict(int)
        for e in edges:
            node_out_degree[e[0]] += 1

        node_vertex_map: typing.Dict[int, Vertex] = dict()
        node_vertex_map[0] = Vertex.from_spec(
            "{}-v{}".format(self.name, 0),
            "source",
            {"host": self.gen_args["source_hosts"].select()},
            0,
            0,
            self.gen_args["mi_cb"](),
            self.gen_args["memory_cb"](),
        )
        for node in range(1, node_cnt):
            if node_out_degree[node] == 0:
                node_type = "sink"
                labels = {"host": random.choice(self.gen_args["sink_hosts"])}
            else:
                node_type = "operator"
                labels = {}
            node_vertex_map[node] = Vertex.from_spec(
                "{}-v{}".format(self.name, node),
                node_type,
                labels,
                0,
                0,
                self.gen_args["mi_cb"](),
                self.gen_args["memory_cb"](),
            )
        for v in node_vertex_map.values():
            g.add_vertex(v)

        for e in edges:
            g.connect(
                node_vertex_map[e[0]],
                node_vertex_map[e[1]],
                self.gen_args["unit_size_cb"](node_rank_map[e[1]]),
                self.gen_args["unit_rate_cb"](),
            )

        return g

    @classmethod
    def dag_select_predecessors(
        cls,
        nodes: typing.List[typing.List[int]],
        cnt: int,
        node_successor_cnt: typing.Dict[int, int],
    ) -> typing.List[int]:
        selected = []
        quota = cnt
        lv = len(nodes) - 1
        while lv >= 0:
            sample_set = [n for n in nodes[lv] if node_successor_cnt[n] == 0]
            sample_cnt = min(random.randint(0, quota), len(sample_set))
            quota -= sample_cnt
            for sample in random.sample(sample_set, sample_cnt):
                selected.append(sample)
            if quota == 0:
                break
            lv -= 1
        lv = len(nodes) - 1
        while lv >= 0:
            sample_set = [n for n in nodes[lv] if node_successor_cnt[n] > 0]
            sample_cnt = min(random.randint(0, quota), len(sample_set))
            quota -= sample_cnt
            for sample in random.sample(sample_set, sample_cnt):
                selected.append(sample)
            if quota == 0:
                break
            lv -= 1
        if len(selected) == 0:
            selected.append(random.choice(nodes[0]))
        return selected


class ParameterGenerator:
    def __init__(self, **kwargs) -> None:
        self.gen_args = kwargs
        random.seed()

    def __call__(self) -> typing.Dict:
        param = {}
        for f in dir(self):
            if not f.startswith("_") and callable(getattr(self, f)):
                param[f] = getattr(self, f)()
        return param

    def graph_length(self) -> int:
        length_range = self.gen_args.get(
            "graph_length_range", default_parameter_range["graph_length_range"]
        )
        return random.randint(length_range[0], length_range[1])

    def initial_unit_size(self) -> int:
        size_range = self.gen_args.get(
            "initial_unit_size_range",
            default_parameter_range["initial_unit_size_range"],
        )
        return random.randint(size_range[0], size_range[1])

    def unit_size_range(self) -> typing.Tuple[int]:
        return self.gen_args.get(
            "unit_size_range", default_parameter_range["unit_size_range"]
        )

    def unit_rate_range(self) -> typing.Tuple[int]:
        return self.gen_args.get(
            "unit_rate_range", default_parameter_range["unit_rate_range"]
        )

    def mi(self) -> int:
        mi_range = self.gen_args.get("mi_range", default_parameter_range["mi_range"])
        return random.randint(mi_range[0], mi_range[1])

    def data_scale_ratio_cb(self) -> int:
        return self.gen_args.get(
            "data_scale_ratio_cb", default_parameter_range["data_scale_ratio_cb"]
        )
