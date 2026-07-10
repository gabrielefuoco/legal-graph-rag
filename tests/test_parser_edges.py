import pytest
from lxml import etree
from src.parsing.namespaces import detect_namespace
from src.parsing.body_parser import parse_body
from src.parsing.models import EdgeType, generate_id

def test_parse_edges_citations():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0" xmlns:xlink="http://www.w3.org/1999/xlink">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>In deroga all'<rif xlink:href="urn:nir:stato:legge:1990;241#art2">articolo 2 della legge 241</rif>, si dispone quanto segue.</corpo>
                    </comma>
                    <comma id="art1-com2">
                        <num>2.</num>
                        <corpo>Ai sensi dell'<ref href="urn:nir:ministero:decreto:2000;1">art 1</ref> e del <rif xlink:href="#art3">presente decreto</rif>.</corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    urn = "urn:nir:stato:legge:2024-01-01;1"
    
    nodes, edges = parse_body(root, ns_map, urn)
    
    cites_edges = [e for e in edges if e.type == EdgeType.CITES]
    assert len(cites_edges) == 3
    
    # Check absolute reference extraction (using xlink:href)
    assert any(e.target_id == "urn:nir:stato:legge:1990;241#art2" for e in cites_edges)
    # Check absolute reference extraction (using href fallback)
    assert any(e.target_id == "urn:nir:ministero:decreto:2000;1" for e in cites_edges)
    
    # Check relative reference normalization (hashed via generate_id)
    expected_rel_id = generate_id(urn, "art3")
    assert any(e.target_id == expected_rel_id for e in cites_edges)

def test_parse_edges_modifies():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0" xmlns:xlink="http://www.w3.org/1999/xlink">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>
                            All'articolo 1 della legge numero 2 sono apportate le seguenti modificazioni:
                            <mod id="mod1">
                                <rif xlink:href="urn:nir:stato:legge:2000;2#art1">a)</rif> il comma 1 e' sostituito dal seguente:
                                <virgolette id="mod1-vir1" tipo="struttura">
                                    <comma id="mod1-vir1-com1">
                                        <num>1.</num><corpo>Nuovo testo.</corpo>
                                    </comma>
                                </virgolette>
                            </mod>
                        </corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    urn = "urn:nir:stato:legge:2024-01-01;1"
    
    nodes, edges = parse_body(root, ns_map, urn)
    
    modifies_edges = [e for e in edges if e.type == EdgeType.MODIFIES]
    assert len(modifies_edges) == 1
    assert modifies_edges[0].target_id == "urn:nir:stato:legge:2000;2#art1"
    
    # Check that quoted content was extracted
    assert "Nuovo testo" in modifies_edges[0].properties["quoted_text"]
