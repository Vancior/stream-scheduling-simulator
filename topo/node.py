import threading
import typing

SLOT_MEMORY_SIZE = int(5e8)


class Node:
    uuid: str

    def __init__(self, uuid, data):
        self.uuid = uuid
        self.data = data

    def __str__(self) -> str:
        return "Node[{}] {}".format(self.uuid, str(self.data))

    def __repr__(self) -> str:
        return self.__str__()

    @classmethod
    def from_spec(
        cls,
        uuid: str,
        type: str,
        mips: int,
        cores: int,
        memory_total: int,
        memory_assigned: int,
        memory_used: int,
        labels: typing.Dict[str, str],
    ):
        data = {
            "type": type,
            "mips": mips,
            "cores": cores,
            "slots": memory_total // SLOT_MEMORY_SIZE,
            "memory_total": memory_total,
            "memory_assigned": memory_assigned,
            "memory_used": memory_used,
            "memory_lock": threading.Lock(),
            "labels": labels,
            "occupied": 0,
        }
        return cls(uuid, data)

    @classmethod
    def from_networkx(cls, uuid: str, data: dict):
        return cls(uuid, data)

    @property
    def type(self) -> str:
        return self.data["type"]

    @property
    def mips(self) -> int:
        return self.data["mips"]

    @property
    def cores(self) -> int:
        return self.data["cores"]

    @property
    def slots(self) -> int:
        return self.data["slots"]

    @property
    def memory_total(self) -> int:
        return self.data["memory_total"]

    @property
    def memory_assigned(self) -> int:
        return self.data["memory_assigned"]

    @property
    def memory_used(self) -> int:
        return self.data["memory_used"]

    @property
    def memory_lock(self) -> threading.Lock:
        return self.data["memory_lock"]

    @property
    def labels(self) -> typing.Dict[str, str]:
        return self.data["labels"]

    @property
    def occupied(self) -> int:
        self.data["memory_lock"].acquire()
        occupied = self.data["occupied"]
        self.data["memory_lock"].release()
        return occupied

    def occupy(self, n: int) -> bool:
        succeed = False
        self.data["memory_lock"].acquire()
        if self.data["slots"] - self.data["occupied"] >= n:
            succeed = True
            self.data["occupied"] += n
        self.data["memory_lock"].release()
        return succeed
