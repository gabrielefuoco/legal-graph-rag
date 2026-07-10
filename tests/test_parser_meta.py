import pytest
from datetime import date
from lxml import etree
from src.parsing.namespaces import detect_namespace
from src.parsing.meta_parser import parse_meta

def test_parse_meta_standard():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <meta>
                <identification source="#somebody">
                    <FRBRWork>
                        <FRBRthis value="/akn/it/act/legge/stato/2024-01-01/1/main"/>
                        <FRBRuri value="urn:nir:stato:legge:2024-01-01;1"/>
                        <FRBRdate date="2024-01-01" name="promulgation"/>
                    </FRBRWork>
                    <FRBRExpression>
                        <FRBRthis value="/akn/it/act/legge/2024-01-01/1/ita@/main"/>
                        <FRBRuri value="urn:nir:stato:legge:2024-01-01;1@"/>
                        <FRBRdate date="2024-01-02" name="publication"/>
                    </FRBRExpression>
                </identification>
                <lifecycle source="#somebody">
                    <eventRef date="2024-01-15" id="e1" type="generation"/>
                </lifecycle>
            </meta>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    
    frbr = parse_meta(root, ns_map)
    
    # Meta_parser tries to derive urn from FRBRthis if no alias exists.
    assert frbr.urn == "urn:nir:stato:legge:2024-01-01;1"
    assert frbr.date_promulgation == date(2024, 1, 1)
    assert frbr.doc_type == "atto"  # The parser falls back to "act"->"atto" since no FRBRname is provided
    assert frbr.vigenza_start == date(2024, 1, 15)

def test_parse_meta_missing_vigenza():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <meta>
                <identification source="#somebody">
                    <FRBRWork>
                        <FRBRthis value="urn:nir:stato:decreto.legge:2023-05-10;45"/>
                        <FRBRdate date="2023-05-10" name="promulgation"/>
                        <FRBRname value="decreto.legge"/>
                    </FRBRWork>
                </identification>
            </meta>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    
    frbr = parse_meta(root, ns_map)
    
    assert frbr.urn == "urn:nir:stato:decreto.legge:2023-05-10;45"
    assert frbr.doc_type == "decreto.legge"
    assert frbr.vigenza_start is None
