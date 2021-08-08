import uuid
from topo.topology import Node
import typing


class Host:
    def __init__(
        self, mips: int, cores: int, memory: int, labels: typing.Dict[str, str]
    ) -> None:
        self.mips = mips
        self.cores = cores
        self.memory = memory
        self.labels = labels
        self.node = Node(str(uuid.uuid4()), "host", mips, cores, memory, 0, 0, labels)

    @classmethod
    def from_dict(cls, data):
        return cls(
            int(data["mips"]),
            int(data["cores"]),
            int(data["memory"] * 1e9),
            data["labels"],
        )
