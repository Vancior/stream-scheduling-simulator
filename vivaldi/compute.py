import logging
import typing

import coloredlogs
from topology import Topology

from vivaldi.coordinate import Coordinate

STEP_WEIGHT = 0.1

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)


def get_latency_matrix(topo: Topology) -> typing.Dict[str, typing.Dict[str, float]]:
    node_list = topo.get_hosts()
    latency_matrix = {n.uuid: dict() for n in node_list}
    for i in node_list:
        for j in node_list:
            latency_matrix[i.uuid][j.uuid] = topo.get_n2n_intrinsic_latency(
                i.uuid, j.uuid
            )
    return latency_matrix


def matrix_error(
    matrix: typing.Dict[str, typing.Dict[str, float]],
    coords: typing.Dict[str, Coordinate],
) -> float:
    total = 0
    keys = list(coords.keys())
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            err = matrix[keys[i]][keys[j]] - abs(coords[keys[i]] - coords[keys[j]])
            total += err * err
    return total


def vivaldi_compute(
    topo: Topology,
    coords: typing.Dict[str, Coordinate],
    max_tolerance: float,
    max_iteration: int,
) -> typing.Dict[str, Coordinate]:
    if len(coords) == 0:
        return coords

    lat_matrix = get_latency_matrix(topo)
    err = matrix_error(lat_matrix, coords)
    logger.debug("initial error: {}".format(err))

    coords = {k: v for k, v in coords.items()}
    keys = list(coords.keys())
    it = 0
    while it < max_iteration and err > max_tolerance:
        for i in range(len(keys)):
            ki = keys[i]
            force = coords[ki].zero()
            for j in range(len(keys)):
                if j == i:
                    continue
                kj = keys[j]
                u = abs(coords[ki] - coords[kj])
                e = lat_matrix[ki][kj] - u
                if abs(u) == 0:
                    force += coords[ki].random_unit_vector() * e
                else:
                    force += (
                        (coords[ki] - coords[kj]) / abs(coords[ki] - coords[kj]) * e
                    )
            coords[ki] += force * STEP_WEIGHT
        # logger.debug({k: str(v) for k, v in coords.items()})
        err = matrix_error(lat_matrix, coords)
        logger.debug("iteration #{} error: {}".format(it, err))
        it += 1

    return coords
