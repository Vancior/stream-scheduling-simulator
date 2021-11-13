import typing
from graph import ExecutionGraph


def bd_calc(g: ExecutionGraph, s_cut: typing.Set[str], t_cut: typing.Set[str]) -> int:
    total_bd = 0
    for u, v, data in g.get_edges():
        if u in s_cut and v in t_cut:
            total_bd += data["unit_size"] * data["per_second"]
    return total_bd
