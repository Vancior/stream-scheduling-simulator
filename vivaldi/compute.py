from collections import defaultdict, namedtuple
import logging
import math
import random
import typing

import coloredlogs
import graph
from topo import Node, Topology
from tqdm import trange

from vivaldi.coordinate import Coordinate

STEP_WEIGHT = 0.05
STEP_SCALE = 0.05

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)


def get_latency_matrix(
    topo: Topology, nids: typing.List[str]
) -> typing.Dict[str, typing.Dict[str, float]]:
    latency_matrix = {n: dict() for n in nids}
    for i in nids:
        for j in nids:
            latency_matrix[i][j] = topo.get_n2n_intrinsic_latency(i, j)
    return latency_matrix


def matrix_error(
    matrix: typing.Dict[str, typing.Dict[str, float]],
    coords: typing.Dict[str, Coordinate],
) -> float:
    total = 0
    keys = list(coords.keys())
    cnt = len(keys) * (len(keys) - 1) / 2
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            err = matrix[keys[i]][keys[j]] - abs(coords[keys[i]] - coords[keys[j]])
            total += err * err
    return total / cnt


def vivaldi_compute(
    topo: Topology,
    coords: typing.Dict[str, Coordinate],
    max_tolerance: float,
    max_iteration: int,
) -> typing.Dict[str, Coordinate]:
    if len(coords) == 0:
        return coords

    lat_matrix = get_latency_matrix(topo, list(coords.keys()))
    err = matrix_error(lat_matrix, coords) / len(coords)
    # logger.debug("initial error: {:.3f}".format(err))

    coords = {k: v for k, v in coords.items()}
    keys = list(coords.keys())
    it = 0

    t_range = trange(max_iteration, disable=True)
    for _ in t_range:
        if err <= max_tolerance:
            break
        for i in range(len(keys)):
            ki = keys[i]
            force = coords[ki].zero()
            for j in range(len(keys)):
                if j == i:
                    continue
                kj = keys[j]
                u = abs(coords[ki] - coords[kj])
                e = (lat_matrix[ki][kj] - u) / len(keys)
                if abs(u) == 0:
                    force += coords[ki].random_unit_vector() * e
                else:
                    force += (
                        (coords[ki] - coords[kj]) / abs(coords[ki] - coords[kj]) * e
                    )
            coords[ki] += force * (STEP_WEIGHT + STEP_SCALE * random.random())
        # logger.debug({k: str(v) for k, v in coords.items()})
        err = matrix_error(lat_matrix, coords)
        t_range.set_description("error: {:.3f}".format(err))

    return coords


class ParentChildTuple(typing.NamedTuple):
    parents: typing.List[typing.Tuple[str, float]]
    children: typing.List[typing.Tuple[str, float]]


def constrained_balance(
    graph: graph.ExecutionGraph,
    coords: typing.Dict[str, Coordinate],
    movable: typing.Dict[str, bool],
    max_tolerance: float,
    max_iteration: int,
) -> typing.Dict[str, Coordinate]:
    if len(coords) == 0:
        return coords

    max_bd = 0
    for _, _, e in graph.get_edges():
        max_bd = max(max_bd, e["unit_size"] * e["per_second"])

    parent_child_map = defaultdict(lambda: ParentChildTuple([], []))
    for k, m in movable.items():
        if not m:
            continue
        t = parent_child_map[k]
        for v in graph.get_up_vertices(k):
            e = graph.get_edge(v.uuid, k)
            t.parents.append((v.uuid, e["unit_size"] * e["per_second"] / max_bd))
            # t.parents.append(
            #     (v.uuid, math.sqrt(e["unit_size"] * e["per_second"] / max_bd))
            # )
        for v in graph.get_down_vertices(k):
            e = graph.get_edge(k, v.uuid)
            t.children.append((v.uuid, e["unit_size"] * e["per_second"] / max_bd))
            # t.children.append(
            #     (v.uuid, math.sqrt(e["unit_size"] * e["per_second"] / max_bd))
            # )

    coords = {k: v for k, v in coords.items()}
    keys = list(coords.keys())

    t_range = trange(max_iteration, disable=True)
    for _ in t_range:
        vote_to_hault = 0
        force_sum = 0
        for k in keys:
            if not movable[k]:
                vote_to_hault += 1
                continue
            force = coords[k].zero()
            for p, bd in parent_child_map[k].parents + parent_child_map[k].children:
                u = abs(coords[p] - coords[k])
                if u == 0:
                    force += coords[k].random_unit_vector() * bd
                    # force += coords[k].random_unit_vector()
                else:
                    force += (coords[p] - coords[k]) * bd
                    # force += coords[p] - coords[k]
            if abs(force) < max_tolerance:
                vote_to_hault += 1
            else:
                # print(k, force)
                coords[k] += force * (STEP_WEIGHT + STEP_SCALE * random.random())
                force_sum += abs(force)
        if vote_to_hault == len(keys):
            break
        t_range.set_description("avg force: {:.3f}".format(force_sum))

    return coords
