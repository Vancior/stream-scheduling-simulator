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
        self.node = Node.from_spec(
            str(uuid.uuid4())[:8], "host", mips, cores, memory, 0, 0, labels
        )

    def replace_node(self, node: Node) -> None:
        self.node = node

    @classmethod
    def from_dict(cls, data: typing.Dict, host_seq: int):
        labels = {"host": "{}{}".format(data["prefix"], host_seq)}
        labels.update(data["labels"])
        return cls(
            int(data["mips"]), int(data["cores"]), int(data["memory"] * 1e9), labels
        )
