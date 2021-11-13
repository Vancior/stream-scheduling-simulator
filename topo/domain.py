import typing

import networkx as nx

from topo.host import Host
from topo.hrg import HRG
from topo.router import Router
from topo.topology import Topology


class Domain:
    type: str
    name: str
    topo: Topology
    host_lookup_table: typing.Dict[str, Host]

    def __init__(
        self, type: str, name: str, router: Router, hrgs: typing.List[HRG]
    ) -> None:
        self.type = type
        self.name = name
        self.router = router
        self.hrgs = hrgs
        self.topo = Topology()
        self.link_topo()
        self.host_lookup_table = {}
        for hrg in self.hrgs:
            for host in hrg.hosts:
                self.host_lookup_table[host.labels["host"]] = host

    def link_topo(self):
        self.topo.add_node(self.router.node)
        for hrg in self.hrgs:
            self.topo.add_nodes_from(hrg.topo.get_nodes())
            self.topo.add_links_from(hrg.topo.get_links())
            self.router.connect_switch(self.topo, hrg.switch)

    def find_host(self, hostname: str) -> typing.Optional[Host]:
        return self.host_lookup_table.get(hostname, None)

    def replace_graph(self, g: nx.Graph):
        self.topo.replace_graph(g.subgraph([n.uuid for n in self.topo.get_nodes()]))
        self.router.replace_node(self.topo.get_node(self.router.node.uuid))
        for hrg in self.hrgs:
            hrg.replace_graph(self.topo.g)

    @classmethod
    def from_dict(cls, data):
        router = Router.from_dict(data["name"] + "_router", data["router"])
        hrgs = [HRG.from_dict(d) for d in data["hrgs"]]
        return cls(data["type"], data["name"], router, hrgs)
