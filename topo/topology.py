from typing import NamedTuple
import typing

import networkx as nx
import threading

LOCAL_BANDWIDTH = int(1e8)


class Node(NamedTuple):
    uuid: str
    type: str
    mips: int
    cores: int
    memory_total: int
    memory_assigned: int
    memory_used: int
    labels: typing.Dict[str, str]


class Link(NamedTuple):
    uuid: str
    bd: int
    delay: int


class Topology:
    def __init__(self) -> None:
        self.g = nx.Graph()

    def add_node(self, n: Node) -> None:
        self.g.add_node(
            n.uuid,
            type=n.type,
            mips=n.mips,
            cores=n.cores,
            memory_total=n.memory_total,
            memory_assigned=n.memory_assigned,
            memory_used=n.memory_used,
            memory_lock=threading.Lock(),
            labels=n.labels,
            occupied=0,
            node=n,
        )

    def add_nodes_from(self, nodes: typing.Iterable[Node]) -> None:
        for n in nodes:
            self.add_node(n)

    def connect(self, n1: Node, n2: Node, uuid: str, bd: int, delay: int) -> None:
        self.g.add_edge(
            n1.uuid,
            n2.uuid,
            uuid=uuid,
            bd=bd,
            delay=delay,
            occupied=0,
            link=Link(uuid, bd, delay),
        )

    def add_link(self, n1: Node, n2: Node, link: Link) -> None:
        self.g.add_edge(
            n1.uuid,
            n2.uuid,
            uuid=link.uuid,
            bd=link.bd,
            delay=link.delay,
            occupied=0,
            link=link,
        )

    def add_links_from(
        self, links: typing.Iterable[typing.Tuple[Node, Node, Link]]
    ) -> None:
        for l in links:
            self.add_link(l[0], l[1], l[2])

    def get_graph(self):
        return self.g

    def get_node(self, nid: str) -> Node:
        """return a read-only proxy"""
        return self.g.nodes[nid]["node"]

    def get_nodes(self) -> typing.List[Node]:
        return [self.get_node(nid) for nid in self.g.nodes()]

    def get_link(self, e: str) -> Link:
        return self.g.edges[e]["link"]

    def get_links(self) -> typing.List[typing.Tuple[Node, Node, Link]]:
        return [
            (self.get_node(e[0]), self.get_node(e[1]), self.get_link(e))
            for e in self.g.edges()
        ]

    def get_hosts(self) -> typing.List[Node]:
        return [
            self.g.nodes[nid]["node"]
            for nid in self.g.nodes()
            if self.g.nodes[nid]["type"] == "host"
        ]

    def get_n2n_intrinsic_latency(self, n1: str, n2: str) -> int:
        """NOTE: shortest path is used"""
        if n1 == n2:
            return 0
        path = nx.shortest_path(self.g, n1, n2)
        total = 0
        i = 0
        while i < len(path) - 1:
            total += self.g.edges[(path[i], path[i + 1])]["delay"]
            i += 1
        return total

    def get_n2n_transmission_latency(
        self, n1: str, n2: str, unit_size: int, bd: int
    ) -> int:
        if n1 == n2:
            return int(unit_size / LOCAL_BANDWIDTH * 1000)
        path = nx.shortest_path(self.g, n1, n2)
        total = 0
        i = 0
        while i < len(path) - 1:
            e = self.g.edges[(path[i], path[i + 1])]
            total += int((unit_size / (e["bd"] / e["occupied"] * bd)) * 1000)
            i += 1
        return total

    def get_computation_latency(self, nid: str, mi: int) -> int:
        """
        return value in ms
        assume all tasks are executed in single thread
        """
        node = self.g.nodes[nid]
        return int(
            mi / (min(node["cores"] / node["occupied"], 1) * node["mips"]) * 1000
        )

    def occupy_node(self, nid: str):
        self.g.nodes[nid]["occupied"] += 1

    def occupy_link(self, n1: str, n2: str, bd: int):
        """NOTE: shortest path is used"""
        if n1 == n2:
            return
        path = nx.shortest_path(self.g, n1, n2)
        i = 0
        while i < len(path) - 1:
            self.g.edges[(path[i], path[i + 1])]["occupied"] += bd
            i += 1

    def clear_occupied(self):
        for _, d in self.g.nodes(data=True):
            d["occupied"] = 0
        for _, _, d in self.g.edges(data=True):
            d["occupied"] = 0

    def filter_node_by_memory(self, memory_required: int) -> typing.List[Node]:
        ns = []
        for nid in self.g.nodes():
            n = self.g.nodes[nid]
            n["memory_lock"].acquire()
            if n["memory_total"] - n["memory_assigned"] >= memory_required:
                ns.append(n["node"])
            n["memory_lock"].release()
        return ns

    def memory_filter(self, memory_required: int, nid: str) -> bool:
        valid = False
        n = self.g.nodes[nid]
        n["memory_lock"].acquire()
        if n["memory_total"] - n["memory_assigned"] >= memory_required:
            valid = True
        n["memory_lock"].release()
        return valid

    def label_filter(self, required_labels: typing.Dict[str, str], nid: str) -> bool:
        valid = True
        n = self.g.nodes[nid]
        for rk, rv in required_labels.items():
            if n["labels"].get(rk) is None or n["labels"].get(rk) != rv:
                valid = False
        return valid
