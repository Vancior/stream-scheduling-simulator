import logging
from typing import NamedTuple

import coloredlogs

LOG_LEVEL = "debug"


def namedtuple_to_dict(t: NamedTuple):
    d = {}
    for k in t._fields:
        d[k] = getattr(t, k)
    return d


def get_logger(name: str):
    logger = logging.getLogger(name)
    coloredlogs.install(level=LOG_LEVEL, logger=logger)
    return logger
