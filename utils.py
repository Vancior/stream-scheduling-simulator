import logging
from typing import NamedTuple
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
