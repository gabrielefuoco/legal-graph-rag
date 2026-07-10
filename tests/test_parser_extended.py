import pytest
from lxml import etree
from src.parsing.namespaces import detect_namespace
from src.parsing.body_parser import parse_body
from src.parsing.models import NodeType, EdgeType

def test_parse_body_attachments():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <corpo>Testo principale.</corpo>
                </articolo>
            </body>
            <attachments>
                <attachment name="Allegato A" eId="annex1">
                    <mainBody>
                        <articolo id="annex1-art1">
                            <num>Art. 1.</num>
                            <corpo>Testo allegato.</corpo>
                        </articolo>
                    </mainBody>
                </attachment>
            </attachments>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    urn = "urn:test:attachments"
    
    nodes, edges = parse_body(root, ns_map, urn)
    
    # Check that attachment node exists
    att_node = next((n for n in nodes if n.eid == "annex1"), None)
    assert att_node is not None
    assert att_node.heading == "Allegato A"
    
    # Check internal attachment node
    inner_node = next((n for n in nodes if n.eid == "annex1-art1"), None)
    assert inner_node is not None
    # ID is a 16-char hash (deterministic)
    assert len(inner_node.id) == 16
    
    # Check tree structure (edge exists)
    parent_edge = next((e for e in edges if e.source_id == inner_node.id), None)
    assert parent_edge is not None
    assert parent_edge.type == EdgeType.PART_OF
    # annex1-art1 is structural, let's check its child (comma/content). 
    # In my body_parser, <articolo> is structural, its children are processed. 
    # Wait, in the XML above, <articolo> has <corpo> directly. <corpo> is not in ATOMIC_TAGS.
    # Ah, if a tag is not in lists, it recurse. <corpo> text will be picked up by... 
    # Actually <corpo> is usually handled by _handle_atomic if it's the leaf or if it contains text?
    # No, ATOMIC_TAGS contains "paragraph", "comma", "content". 
    # Let's check body_parser.py again to see how it handles unknown tags with text.
    
def test_parse_edges_rref():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
        <act>
            <body>
                <articolo id="art1">
                    <num>Art. 1.</num>
                    <comma id="art1-com1">
                        <num>1.</num>
                        <corpo>Vedi <rref from="#art2" upTo="#art5">articoli da 2 a 5</rref>.</corpo>
                    </comma>
                </articolo>
            </body>
        </act>
    </akomaNtoso>
    """
    root = etree.fromstring(xml_content.encode('utf-8'))
    ns_map = detect_namespace(root)
    urn = "urn:test:rref"
    
    nodes, edges = parse_body(root, ns_map, urn)
    
    rref_edges = [e for e in edges if e.type == EdgeType.CITES and e.properties.get("range")]
    assert len(rref_edges) >= 1
    assert rref_edges[0].properties["range_from"] == "#art2"
    assert rref_edges[0].properties["range_to"] == "#art5"

def test_parse_complex_doc_types():
    # Test doc_type detection for different root tags
    doc_types = ["bill", "amendment", "judgment", "doc"]
    for dt in doc_types:
        xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
        <akomaNtoso xmlns="http://docs.oasis-open.org/legaldocml/ns/akn/3.0">
            <{dt}>
                <meta>
                    <identification source="#me">
                        <FRBRWork>
                            <FRBRthis value="urn:test:{dt}/main"/>
                            <FRBRdate date="2024-01-01" name="promulgation"/>
                        </FRBRWork>
                    </identification>
                </meta>
                <body><articolo id="a1"><corpo>text</corpo></articolo></body>
            </{dt}>
        </akomaNtoso>
        """
        root = etree.fromstring(xml_content.encode('utf-8'))
        from src.parsing.meta_parser import parse_meta
        ns_map = detect_namespace(root)
        frbr = parse_meta(root, ns_map)
        
        expected_map = {
            "bill": "disegno di legge",
            "amendment": "emendamento",
            "judgment": "sentenza",
            "doc": "documento"
        }
        assert frbr.doc_type == expected_map[dt]
