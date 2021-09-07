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
                rate = self.gen_args["unit_rate_cb"]() * 100
                print("graph {} source bd {}".format(g.uuid, size * rate / 1e6))
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
