import typing

from topo.hrg import HRG
from topo.router import Router
from topo.topology import Topology


class Domain:
    def __init__(
        self, type: str, name: str, router: Router, hrgs: typing.List[HRG]
    ) -> None:
        self.type = type
        self.name = name
        self.router = router
        self.hrgs = hrgs
        self.topo = Topology()
        self.link_topo()

    def link_topo(self):
        self.topo.add_node(self.router.node)
        for hrg in self.hrgs:
            self.topo.add_nodes_from(hrg.topo.get_nodes())
            self.topo.add_links_from(hrg.topo.get_links())
            self.router.connect_switch(self.topo, hrg.switch)

    @classmethod
    def from_dict(cls, data):
        router = Router.from_dict(data["router"])
        hrgs = [HRG.from_dict(d) for d in data["hrgs"]]
        return cls(data["type"], data["name"], router, hrgs)
