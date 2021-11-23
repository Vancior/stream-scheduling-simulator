import typing
from graph import ExecutionGraph


def bd_calc(g: ExecutionGraph, s_cut: typing.Set[str], t_cut: typing.Set[str]) -> int:
    total_bd = 0
    if len(s_cut.intersection(t_cut)):
        raise RuntimeError("intersecting cutting")
    if g.number_of_vertices() != (len(s_cut) + len(t_cut)):
        raise RuntimeError("incomplete cutting")
    for u, v, data in g.get_edges():
        if u in s_cut and v in t_cut:
            total_bd += data["unit_size"] * data["per_second"]
        if u in t_cut and v in s_cut:
            total_bd += data["unit_size"] * data["per_second"]
    return total_bd
