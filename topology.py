from typing import NamedTuple
import typing

import networkx as nx
import threading

LOCAL_BANDWIDTH = int(1e8)


class Node(NamedTuple):
    uuid: str
    type: str
    mips: int
    memory_total: int
    memory_assigned: int
    memory_used: int


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
            memory_total=n.memory_total,
            memory_assigned=n.memory_assigned,
            memory_used=n.memory_used,
            memory_lock=threading.Lock(),
            node=n,
        )

    def connect(self, n1: Node, n2: Node, uuid: str, bd: int, delay: int) -> None:
        self.g.add_edge(n1.uuid, n2.uuid, uuid=uuid, bd=bd, delay=delay, occupied=0)

    def get_node(self, nid: str) -> Node:
        return self.g.nodes[nid]["node"]

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

    def get_n2n_transmission_latency(self, n1: str, n2: str, unit_size: int) -> int:
        if n1 == n2:
            return int(unit_size / LOCAL_BANDWIDTH * 1000)
        path = nx.shortest_path(self.g, n1, n2)
        total = 0
        i = 0
        while i < len(path) - 1:
            e = self.g.edges[(path[i], path[i + 1])]
            total += int((unit_size / (e["bd"] / e["occupied"])) * 1000)
            i += 1
        return total

    def filter_node_by_memory(self, memory_required: int) -> typing.List[Node]:
        ns = []
        for nid in self.g.nodes():
            n = self.g.nodes[nid]
            n["memory_lock"].acquire()
            if n["memory_total"] - n["memory_assigned"] >= memory_required:
                ns.append(n["node"])
            n["memory_lock"].release()
        return ns

    def occupy_link(self, n1: str, n2: str):
        """NOTE: shortest path is used"""
        if n1 == n2:
            return
        path = nx.shortest_path(self.g, n1, n2)
        i = 0
        while i < len(path) - 1:
            self.g.edges[(path[i], path[i + 1])]["occupied"] += 1
            i += 1
