from topo.switch import Switch
import uuid

from topo.topology import Node, Topology


class Router:
    def __init__(self, name: str, bd: int, delay: int) -> None:
        self.name = name
        self.bd = bd
        self.delay = delay
        self.node = Node.from_spec(str(uuid.uuid4())[:8], "router", 0, 0, 0, 0, 0, {})

    def connect_switch(self, topo: Topology, switch: Switch) -> None:
        topo.connect(self.node, switch.node, str(uuid.uuid4())[:8], self.bd, self.delay)

    def replace_node(self, node: Node) -> None:
        self.node = node

    @classmethod
    def from_dict(cls, name, data):
        return cls(name, int(data["bd"] * 1e6), int(data["delay"]))
