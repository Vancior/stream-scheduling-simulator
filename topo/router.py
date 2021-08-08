from topo.switch import Switch
import uuid

from topo.topology import Node, Topology


class Router:
    def __init__(self, bd: int, delay: int) -> None:
        self.bd = bd
        self.delay = delay
        self.node = Node(str(uuid.uuid4()), "router", 0, 0, 0, 0, 0, {})

    def connect_switch(self, topo: Topology, switch: Switch) -> None:
        topo.connect(self.node, switch.node, uuid.uuid4(), self.bd, self.delay)

    @classmethod
    def from_dict(cls, data):
        return cls(int(data["bd"] * 1e6), int(data["delay"] * 1000))
