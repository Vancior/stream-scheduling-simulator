import typing

from graph import ExecutionGraph
from pymprog import *
from topo import Scenario


def best(
    scenario: Scenario, graph_list: typing.List[ExecutionGraph]
) -> typing.List[typing.Tuple[typing.Set[str], typing.Set[str]]]:
    num_sources = 0
    var("op_edge")
