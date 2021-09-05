import logging
from typing import NamedTuple
import typing
import uuid

import coloredlogs

LOG_LEVEL = "debug"


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
