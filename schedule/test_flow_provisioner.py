import logging
from graph.vertex import Vertex
from graph.execution_graph import ExecutionGraph
from topo import Node
from utils import gen_uuid, get_logger
from .flow_provisioner import ProvisionerNode

logger = get_logger("Provisioner Test")


def test_single_node_provisioning():
    print("")
    n_slot = 5
    node = Node.from_spec(
        gen_uuid(), "host", 1000, 4, n_slot * int(2e8), 0, 0, {"host": "mock"}
    )
    p_node = ProvisionerNode("mock", "host", node, None)
    p_node.logger.setLevel(logging.DEBUG)
    graph1 = ExecutionGraph("g1")
    v1_1 = Vertex.from_spec("v1_1", "source", {"host": "mock"}, 0, 0, 0, 0)
    v1_21 = Vertex.from_spec("v1_21", "operator", {}, 0, 0, 0, 0)
    v1_22 = Vertex.from_spec("v1_22", "operator", {}, 0, 0, 0, 0)
    v1_31 = Vertex.from_spec("v1_31", "operator", {}, 0, 0, 0, 0)
    v1_32 = Vertex.from_spec("v1_32", "operator", {}, 0, 0, 0, 0)
    v1_4 = Vertex.from_spec("v1_4", "sink", {}, 0, 0, 0, 0)
    graph1.add_vertex(v1_1)
    graph1.add_vertex(v1_21)
    graph1.add_vertex(v1_22)
    graph1.add_vertex(v1_31)
    graph1.add_vertex(v1_32)
    graph1.add_vertex(v1_4)
    graph1.connect(v1_1, v1_21, 1, 1100)
    graph1.connect(v1_1, v1_22, 1, 1900)
    graph1.connect(v1_21, v1_31, 1, 1100)
    graph1.connect(v1_22, v1_32, 1, 2000)
    graph1.connect(v1_31, v1_4, 1, 100)
    graph1.connect(v1_32, v1_4, 1, 100)
    logger.info(graph1)

    graph2 = ExecutionGraph("g2")
    v2_1 = Vertex.from_spec("v2_1", "source", {"host": "mock"}, 0, 0, 0, 0)
    v2_2 = Vertex.from_spec("v2_2", "operator", {}, 0, 0, 0, 0)
    v2_3 = Vertex.from_spec("v2_3", "sink", {}, 0, 0, 0, 0)
    graph2.add_vertex(v2_1)
    graph2.add_vertex(v2_2)
    graph2.add_vertex(v2_3)
    graph2.connect(v2_1, v2_2, 1, 1000)
    graph2.connect(v2_2, v2_3, 1, 1000)

    p_node.add_unscheduled_graph(graph1)
    p_node.add_unscheduled_graph(graph2)
    # p_node.add_child(None)
    # p_node.children_slots[0] = 2
    p_node.step()
    logger.info([v.uuid for v in p_node.scheduled_vertexs])
    for g in p_node.unscheduled_graphs:
        logger.info([v.uuid for v in g.get_vertexs()])
