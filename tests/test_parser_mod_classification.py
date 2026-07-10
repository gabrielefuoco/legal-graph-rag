import pytest
from lxml import etree
from src.parsing.namespaces import detect_namespace
from src.parsing.body_parser import parse_body
from src.parsing.models import EdgeType, ModificationType


def test_mod_substitution():
    """Test that a substitution modification is correctly classified."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0" xmlns:xlink="http://www.w3.org/1999/xlink">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>
                            Il comma 3 dell'articolo 5 della legge è sostituito dal seguente:
                            <mod id="mod1">
                                <ref href="urn:nir:stato:legge:2000;1#art5-com3">art. 5, comma 3</ref>
                                è sostituito dal seguente:
                                <quotedStructure>
                                    <comma><num>3.</num><corpo>Nuovo testo sostitutivo.</corpo></comma>
                                </quotedStructure>
                            </mod>
                        </corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:mod-substitution"

    nodes, edges = parse_body(root, ns_map, urn)

    mod_edges = [e for e in edges if e.type == EdgeType.MODIFIES]
    assert len(mod_edges) >= 1
    assert mod_edges[0].properties["modification_type"] == ModificationType.SUBSTITUTION.value


def test_mod_insertion():
    """Test that an insertion modification is correctly classified."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>
                            Dopo il comma 2 dell'articolo 3 è inserito il seguente:
                            <mod id="mod1">
                                <ref href="urn:nir:stato:legge:2000;1#art3">art. 3</ref>
                                dopo il comma 2 è inserito il seguente:
                                <quotedText>2-bis. Testo aggiunto.</quotedText>
                            </mod>
                        </corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:mod-insertion"

    nodes, edges = parse_body(root, ns_map, urn)

    mod_edges = [e for e in edges if e.type == EdgeType.MODIFIES]
    assert len(mod_edges) >= 1
    assert mod_edges[0].properties["modification_type"] == ModificationType.INSERTION.value


def test_mod_repeal():
    """Test that a repeal modification is correctly classified."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>
                            <mod id="mod1">
                                L'articolo 7 della legge è abrogato.
                                <ref href="urn:nir:stato:legge:2000;1#art7">articolo 7</ref>
                            </mod>
                        </corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:mod-repeal"

    nodes, edges = parse_body(root, ns_map, urn)

    mod_edges = [e for e in edges if e.type == EdgeType.MODIFIES]
    assert len(mod_edges) >= 1
    assert mod_edges[0].properties["modification_type"] == ModificationType.REPEAL.value


def test_mod_generic_amendment():
    """Test that a generic modification without clear keywords falls back to AMENDMENT."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>
                            <mod id="mod1">
                                Sono apportate le seguenti modificazioni alla legge:
                                <ref href="urn:nir:stato:legge:2000;1">legge 1/2000</ref>
                            </mod>
                        </corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode("utf-8"))
    ns_map = detect_namespace(root)
    urn = "urn:test:mod-generic"

    nodes, edges = parse_body(root, ns_map, urn)

    mod_edges = [e for e in edges if e.type == EdgeType.MODIFIES]
    assert len(mod_edges) >= 1
    assert mod_edges[0].properties["modification_type"] == ModificationType.AMENDMENT.value
