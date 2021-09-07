import enum
from graph.execution_graph import ExecutionGraph
import typing


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

    @classmethod
    def failed(cls, reason: str):
        return SchedulingResult(status=SchedulingResultStatus.FAILED, reason=reason)

    def __str__(self) -> str:
        if self.status == SchedulingResultStatus.SUCCEED:
            return self.assign_map.__str__()
        return {"status": self.status.__str__(), "reason": self.reason}

    def __repr__(self) -> str:
        return self.__str__()

    def assign(self, nid: str, vid: str):
        self.assign_map[vid] = nid

    def get_scheduled_node(self, vid: str):
        return self.assign_map.get(vid)

    def get_assignments(self) -> typing.ItemsView:
        return self.assign_map.items()

    def check_complete(self, g: ExecutionGraph) -> bool:
        for v in g.get_vertices():
            if self.assign_map.get(v.uuid) is None:
                return False
        return True

    def extract(self, vid_set: typing.Set[str]):
        result = SchedulingResult(self.status, self.reason)
        for vid in list(vid_set):
            if self.get_scheduled_node(vid) is not None:
                result.assign(self.get_scheduled_node(vid), vid)
        return result

    @classmethod
    def merge(cls, *results):
        merged_result = SchedulingResult()
        for r in results:
            if r.status == SchedulingResultStatus.FAILED:
                return SchedulingResult.failed(r.reason)
            for k, v in r.get_assignments():
                merged_result.assign(v, k)
        return merged_result
