import random
from abc import ABC, abstractmethod
from functools import partial

from graph import ExecutionGraph
from topo import Topology
from utils import get_logger

from .result import SchedulingResult


class Scheduler(ABC):
    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def schedule(
        self, execution_graph: ExecutionGraph, topology: Topology
    ) -> SchedulingResult:
        raise NotImplemented()


class RandomScheduler(Scheduler):
    def __init__(self) -> None:
        super().__init__()

    def schedule(
        self, execution_graph: ExecutionGraph, topology: Topology
    ) -> SchedulingResult:
        result = SchedulingResult()
        for v in execution_graph.topological_order():
            nid_list = list(
                filter(
                    partial(topology.memory_filter, v.memory),
                    filter(
                        partial(topology.label_filter, v.domain_constraint),
                        [h.uuid for h in topology.get_hosts()],
                    ),
                )
            )
            if len(nid_list) == 0:
                return SchedulingResult.failed("no available host")
            nid = random.choice(nid_list)
            self.logger.debug("Select node %s for vertex %s", nid, v.uuid)
            result.assign(nid, v.uuid)
        return result
