import pytest
from src.parsing.parser import AknParser
from lxml import etree
import tempfile
import json
import os

def test_akn_parser_malformed_xml():
    # Tag XML non chiuso correttamente
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <meta>
                <identification source="#somebody">
                    <FRBRWork>
                        <FRBRthis value="urn:test:1/main"/>
                        <FRBRdate date="2024-01-01" name="promulgation"/>
                        <FRBRname value="atto"/>
                    </FRBRWork>
                </identification>
            </meta>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>Tag rotto
                    <!-- </comma> is missing -->
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        f.write(xml_content.encode('utf-8'))
        temp_path = f.name
        
    try:
        # Should succeed because recover=True by default in AknParser
        parser = AknParser()
        doc = parser.parse_file(temp_path)
        
        assert doc.frbr.urn == "urn:test:1"
        assert len(doc.nodes) > 0 # At least some nodes were salvaged
    finally:
        os.unlink(temp_path)

def test_akn_parser_unrecoverable_xml():
    # Completely broken string
    xml_content = "NOT AN XML AT ALL"
    
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        f.write(xml_content.encode('utf-8'))
        temp_path = f.name
        
    try:
        parser = AknParser()
        with pytest.raises((ValueError, AttributeError, etree.XMLSyntaxError)):
            parser.parse_file(temp_path)
    finally:
        os.unlink(temp_path)
