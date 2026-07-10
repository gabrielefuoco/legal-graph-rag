from src.parsing.models import GraphNodeDTO, GraphEdgeDTO, NodeType, EdgeType

def test_graph_node_dto_extended():
    node = GraphNodeDTO(
        id="test_id",
        type=NodeType.EXPRESSION,
        tag_name="comma",
        embedding=[0.1, 0.2, 0.3]
    )
    assert node.embedding == [0.1, 0.2, 0.3]
    assert node.type == NodeType.EXPRESSION

def test_graph_edge_dto_extended():
    edge = GraphEdgeDTO(
        source_id="src",
        target_id="tgt",
        type=EdgeType.HAS_TOPIC,
        score=0.95,
        modification_type="SUBSTITUTION",
        quoted_text="Nuovo testo"
    )
    assert edge.type == EdgeType.HAS_TOPIC
    assert edge.score == 0.95
    assert edge.modification_type == "SUBSTITUTION"
    assert edge.quoted_text == "Nuovo testo"
