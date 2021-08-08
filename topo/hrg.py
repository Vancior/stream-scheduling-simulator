import typing

from topo.host import Host
from topo.switch import Switch
from topo.topology import Topology


class HRG:
    def __init__(self, switch: Switch, hosts: typing.List[Host]) -> None:
        self.switch = switch
        self.hosts = hosts
        self.topo = Topology()
        self.link_topo()

    def link_topo(self):
        self.topo.add_node(self.switch.node)
        for h in self.hosts:
            self.topo.add_node(h.node)
            self.switch.connect_host(self.topo, h)

    @classmethod
    def from_dict(cls, data):
        hosts = [Host.from_dict(data["spec"]) for _ in range(int(data["replica"]))]
        return cls(Switch.from_dict(data["switch"]), hosts)
