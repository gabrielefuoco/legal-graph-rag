import pytest
from lxml import etree
from src.parsing.namespaces import detect_namespace
from src.parsing.body_parser import parse_body
from src.parsing.models import NodeType, EdgeType


def test_parse_table_basic():
    """Test that a <table> inside an article is parsed as an EXPRESSION node."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <rubrica>Tabella dei contributi</rubrica>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>I contributi sono determinati nella seguente tabella:</corpo>
                    </comma>
                    <table id="art1-tab1">
                        <tr>
                            <th>Anno</th>
                            <th>Importo</th>
                            <th>Note</th>
                        </tr>
                        <tr>
                            <td>2024</td>
                            <td>1.000 euro</td>
                            <td>Base</td>
                        </tr>
                        <tr>
                            <td>2025</td>
                            <td>1.200 euro</td>
                            <td>Aggiornato</td>
                        </tr>
                    </table>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:table"

    nodes, edges = parse_body(root, ns_map, urn)

    # Find the table node
    table_nodes = [n for n in nodes if n.metadata.get("is_table")]
    assert len(table_nodes) == 1

    table_node = table_nodes[0]
    assert table_node.type == NodeType.EXPRESSION
    assert table_node.tag_name == "table"

    # Check text_vector contains all cell values with context
    assert "Anno" in table_node.text_vector
    assert "1.000 euro" in table_node.text_vector
    assert "Art. 1." in table_node.text_vector  # Context injection

    # Check text_display is Markdown format
    assert "| Anno |" in table_node.text_display
    assert "| 2024 |" in table_node.text_display

    # Check PART_OF edge exists
    part_of = [e for e in edges if e.source_id == table_node.id and e.type == EdgeType.PART_OF]
    assert len(part_of) == 1


def test_parse_table_with_refs():
    """Test that <ref> elements inside table cells are extracted as CITES edges."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <table id="art1-tab1">
                        <tr>
                            <th>Norma</th>
                            <th>Modifica</th>
                        </tr>
                        <tr>
                            <td><ref href="urn:nir:stato:legge:2000;1">Legge 1/2000</ref></td>
                            <td>Invariata</td>
                        </tr>
                    </table>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:table-refs"

    nodes, edges = parse_body(root, ns_map, urn)

    cites_edges = [e for e in edges if e.type == EdgeType.CITES]
    assert len(cites_edges) >= 1
    assert any(e.target_id == "urn:nir:stato:legge:2000;1" for e in cites_edges)


def test_parse_table_no_headers():
    """Test table parsing when there are no <th> elements, only <td>."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso>
        <act>
            <body>
                <articolo id="art1">
                    <table id="tab1">
                        <tr>
                            <td>A</td>
                            <td>B</td>
                        </tr>
                        <tr>
                            <td>1</td>
                            <td>2</td>
                        </tr>
                    </table>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:table-noheaders"

    nodes, edges = parse_body(root, ns_map, urn)

    table_nodes = [n for n in nodes if n.metadata.get("is_table")]
    assert len(table_nodes) == 1
    # Without headers, text_vector should still contain all values
    assert "A" in table_nodes[0].text_vector
    assert "B" in table_nodes[0].text_vector
