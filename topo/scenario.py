import typing
import uuid

from topo.domain import Domain
from topo.topology import Topology


class Scenario:
    def __init__(self, domains: typing.List[Domain], bd: int, delay: int) -> None:
        self.domains = domains
        self.bd = bd
        self.delay = delay
        self.topo = Topology()
        self.link_topo()
        self.domain_lookup_table = {}
        for d in self.domains:
            self.domain_lookup_table[d.name] = d

    def link_topo(self):
        for d in self.domains:
            self.topo.add_nodes_from(d.topo.get_nodes())
            self.topo.add_links_from(d.topo.get_links())
        length = len(self.domains)
        for i in range(length):
            for j in range(i + 1, length):
                self.topo.connect(
                    self.domains[i].router.node,
                    self.domains[j].router.node,
                    str(uuid.uuid4())[:8],
                    self.bd,
                    self.delay,
                )
        for d in self.domains:
            d.replace_graph(self.topo.g)

    def get_edge_domains(self) -> typing.List[Domain]:
        return [d for d in self.domains if d.type == "edge"]

    def get_cloud_domains(self) -> typing.List[Domain]:
        return [d for d in self.domains if d.type == "cloud"]

    def find_domain(self, domain_name: str) -> typing.Optional[Domain]:
        return self.domain_lookup_table.get(domain_name, None)

    @classmethod
    def from_dict(cls, data):
        domains = []
        for d in data["domains"]:
            domains.append(Domain.from_dict(d))
        return cls(
            domains,
            int(data["interdomain"]["bd"] * 1e6),
            int(data["interdomain"]["delay"]),
        )
