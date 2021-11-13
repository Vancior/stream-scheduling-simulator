import typing
from graph import ExecutionGraph
from topo import Scenario


def all_cloud_cut(
    scenario: Scenario, graph_list: typing.List[ExecutionGraph]
) -> typing.List[typing.Tuple[typing.Set[str], typing.Set[str]]]:
    source_sets = [set([v.uuid for v in g.get_sources()]) for g in graph_list]
    total_sets = [set([v.uuid for v in g.get_vertices()]) for g in graph_list]
    return [(s_cut, t_set - s_cut) for s_cut, t_set in zip(source_sets, total_sets)]
