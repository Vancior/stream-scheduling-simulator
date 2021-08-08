import enum
from functools import partial
import random
from abc import ABC, abstractmethod

from execution_graph import ExecutionGraph
from topo import Topology
from utils import get_logger


class SchedulingResultStatus(enum.Enum):
    SUCCEED = 1
    FAILED = 2

    def __str__(self) -> str:
        return self.name


class SchedulingResult:
    status: SchedulingResultStatus
    assign_map: dict
    reason: str

    def __init__(self, status=SchedulingResultStatus.SUCCEED, reason="") -> None:
        self.status = status
        self.assign_map = {}
        self.reason = reason

    def failed(cls, reason: str):
        return SchedulingResult(status=SchedulingResultStatus.FAILED, reason=reason)

    def __str__(self) -> str:
        if self.status == SchedulingResultStatus.SUCCEED:
            return self.assign_map.__str__()
        return {"status": self.status.__str__(), "reason": self.reason}

    def assign(self, nid: str, vid: str):
        self.assign_map[vid] = nid

    def get_scheduled_node(self, vid: str):
        return self.assign_map.get(vid)


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
                return SchedulingResult.failed()
            nid = random.choice(nid_list)
            self.logger.debug("Select node %s for vertex %s", nid, v.uuid)
            result.assign(nid, v.uuid)
        return result
