from graph.vertex import Vertex
from graph.execution_graph import ExecutionGraph
from topo import Node
from utils import gen_uuid
from .flow_provisioner import ProvisionerNode


def test_single_node_provisioning():
    n_slot = 2
    node = Node.from_spec(
        gen_uuid(), "host", 1000, 4, n_slot * int(2e8), 0, 0, {"host": "mock"}
    )
    p_node = ProvisionerNode("mock", "host", node, None)
    graph = ExecutionGraph(gen_uuid())
    v1 = Vertex.from_spec(gen_uuid(), "source", {"host": "mock"}, 0, 0, 0, 0)
    v21 = Vertex.from_spec(gen_uuid(), "operator", {}, 0, 0, 0, 0)
    v22 = Vertex.from_spec(gen_uuid(), "operator", {}, 0, 0, 0, 0)
    v31 = Vertex.from_spec(gen_uuid(), "operator", {}, 0, 0, 0, 0)
    v32 = Vertex.from_spec(gen_uuid(), "operator", {}, 0, 0, 0, 0)
    v4 = Vertex.from_spec(gen_uuid(), "sink", {}, 0, 0, 0, 0)
    graph.add_vertex(v1)
    graph.add_vertex(v21)
    graph.add_vertex(v22)
    graph.add_vertex(v31)
    graph.add_vertex(v32)
    graph.add_vertex(v4)
    graph.connect(v1, v21, 1, 1000)
    graph.connect(v1, v22, 1, 2000)
    graph.connect(v21, v31, 1, 1000)
    graph.connect(v22, v32, 1, 2000)
    graph.connect(v31, v4, 1, 100)
    graph.connect(v32, v4, 1, 100)
    print(graph)

    p_node.add_unscheduled_graph(graph)
    p_node.add_child(None)
    p_node.children_slots[0] = 2
    p_node.step()
    print(p_node.scheduled_vertexs)
    for g in p_node.unscheduled_graphs:
        print(g.get_vertexs())
