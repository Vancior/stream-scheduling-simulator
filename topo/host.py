import typing
import uuid

from .node import Node


class Host:
    def __init__(
        self,
        name: str,
        mips: int,
        cores: int,
        memory: int,
        labels: typing.Dict[str, str],
    ) -> None:
        self.name = name
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
    def from_dict(cls, name: str, data: typing.Dict):
        labels = {"host": name}
        labels.update(data["labels"])
        return cls(
            name,
            int(data["mips"]),
            int(data["cores"]),
            int(data["memory"] * 1e9),
            labels,
        )
