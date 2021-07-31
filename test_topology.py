import uuid

from topology import Node, Topology


def test_create_topology():
    topo = Topology()
    n1 = Node(str(uuid.uuid4()), "host", int(1e8), int(1e8))
    n2 = Node(str(uuid.uuid4()), "host", int(1e8), int(1e8))
    topo.add_node(n1)
    topo.add_node(n2)
    topo.connect(n1, n2, str(uuid.uuid4()), int(1e8), 10)
