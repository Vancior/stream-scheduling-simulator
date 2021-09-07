import logging
import typing
import uuid
from typing import NamedTuple

import coloredlogs
import numpy as np

LOG_LEVEL = "debug"
MAX = int(1e18)


def namedtuple_to_dict(t: NamedTuple):
    d = {}
    for k in t._fields:
        d[k] = getattr(t, k)
    return d


def gen_uuid():
    return str(uuid.uuid4())[:8]


def get_logger(name: str):
    logger = logging.getLogger(name)
    coloredlogs.install(level=LOG_LEVEL, logger=logger)
    return logger


def avg(*args):
    if len(args) == 0:
        return 0
    return sum(args) / len(args)


class DisjointSet:
    def __init__(self, size: int) -> None:
        self.size = size
        self.roots = [i for i in range(size)]

    def union(self, a: int, b: int) -> None:
        if a > b:
            tmp = a
            a = b
            b = tmp
        self.roots[b] = self.find(a)

    def find(self, a: int) -> int:
        if self.roots[a] == a:
            return a
        self.roots[a] = self.find(self.roots[a])
        return self.roots[a]

    def unique_roots(self) -> typing.Set[int]:
        for i in range(self.size):
            self.find(i)
        return set(self.roots)


def grouped_exactly_one_binpack(
    n_slot: int, groups: typing.List[typing.List[typing.Tuple[int, int]]]
) -> typing.List[int]:
    """arguments:
    n_slot -- binpack capacity
    groups -- list of groups, each contains a list of (volume, value) pairs
    """
    dp = np.full((n_slot + 1,), MAX, dtype=np.int64)
    selected = np.full((n_slot + 1,), -1, dtype=np.int32)
    selected[0] = 0
    choices = np.full((len(groups), n_slot + 1), -1, dtype=np.int32)
    dp[0] = 0
    for gid, group in enumerate(groups):
        for capacity in range(n_slot, -1, -1):
            for eid, ele in enumerate(group):
                volume, value = ele
                if capacity < volume:
                    continue
                # NOTE only when previous groups are selected
                # NOTE if selected[capacity] not be overwrited at this round, it cannot be used at next round
                if selected[capacity - volume] == gid and (
                    dp[capacity - volume] + value < dp[capacity]
                    or selected[capacity] <= gid
                ):
                    dp[capacity] = dp[capacity - volume] + value
                    selected[capacity] = gid + 1
                    choices[gid, capacity] = eid
    valid_idx = np.where(selected == len(groups))[0]
    backtrace = valid_idx[-1]
    solution: typing.List[int] = [None for _ in range(len(groups))]
    for gid in range(len(groups) - 1, -1, -1):
        assert choices[gid, backtrace] >= 0
        solution[gid] = choices[gid, backtrace]
        backtrace -= groups[gid][solution[gid]][0]
    return solution
