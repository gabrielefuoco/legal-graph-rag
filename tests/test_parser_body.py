import pytest
from lxml import etree
from src.parsing.namespaces import detect_namespace
from src.parsing.body_parser import parse_body
from src.parsing.models import NodeType

def test_parse_body_semantic_chunking():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="urn:oasis:names:tc:legalxml-exec:schema:xsd:Common-0.2">
        <act>
            <body>
                <libro id="lib1">
                    <num>Libro Primo</num>
                    <titolo id="lib1-tit1">
                        <num>Titolo I</num>
                        <rubrica>Delle Persone</rubrica>
                        <articolo id="art1">
                            <num>Art. 1.</num>
                            <rubrica>(Capacita' giuridica)</rubrica>
                            <comma id="art1-com1">
                                <num>1.</num>
                                <corpo>La capacita' giuridica si acquista dal momento della nascita.</corpo>
                            </comma>
                            <comma id="art1-com2">
                                <num>2.</num>
                                <corpo>I diritti che la legge riconosce a favore del concepito sono subordinati all'evento della nascita.</corpo>
                            </comma>
                        </articolo>
                    </titolo>
                </libro>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    # the test urn
    urn = "urn:nir:stato:codice:1942-03-16;262"
    
    nodes, edges = parse_body(root, ns_map, urn)
    
    # Assert structural nodes are created
    structural_nodes = [n for n in nodes if n.type == NodeType.STRUCTURAL]
    assert len(structural_nodes) == 4 # libro, titolo, articolo, rubrica
    
    # Assert leaf nodes (expressions) are created
    expression_nodes = [n for n in nodes if n.type == NodeType.EXPRESSION]
    assert len(expression_nodes) == 2 # 2 commi
    
    # Assert Context Injection
    com1 = next(n for n in expression_nodes if n.eid == "art1-com1")
    assert "Libro Primo" in com1.text_vector
    assert "Titolo I" in com1.text_vector
    assert "Delle Persone" in com1.text_vector
    assert "Art. 1." in com1.text_vector
    assert "(Capacita' giuridica)" in com1.text_vector
    assert "La capacita' giuridica si acquista" in com1.text_vector
    assert com1.text_display == "La capacita' giuridica si acquista dal momento della nascita."
    assert com1.tag_name == "comma"

def test_body_robustness_missing_ids():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso>
        <act>
            <body>
                <articolo>
                    <num>Art. 2.</num>
                    <comma>
                        <corpo>Testo senza ID nativo</corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    urn = "urn:test"
    
    nodes, edges = parse_body(root, ns_map, urn)
    
    expressions = [n for n in nodes if n.type == NodeType.EXPRESSION]
    assert len(expressions) == 1
    # Check that surrogate IDs have been generated deterministically
    assert expressions[0].id is not None
    assert expressions[0].id != ""
