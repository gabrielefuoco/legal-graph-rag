"""
Integration test: Parse real XML files from Normattiva and Senato.

This test is conditional: it is skipped if the XML files are not present
on disk. When executed, it validates that the parser produces structurally
valid output on real-world data.
"""
import os
import pytest
from src.parsing.parser import AknParser
from src.parsing.models import NodeType


REAL_FILES = [
    r"c:\Users\gabri\APP\Università\Tesi\data\raw\normattiva\normattiva_export_13d1e3b1-9d45-4f41-b36e-b6bb8c5692c8\DECRETO LEGISLATIVO_20240108_1\2024-01-12_24G00007_VIGENZA_2026-01-25_V0.xml",
    r"c:\Users\gabri\APP\Università\Tesi\data\raw\senato\AkomaNtosoBulkData\Leg19\Atto00055177\ddlpres\01360967-ft.akn.xml",
]


@pytest.mark.parametrize("filepath", REAL_FILES, ids=lambda p: os.path.basename(p))
def test_real_file_parsing(filepath):
    """Parse a real XML file and check structural validity."""
    if not os.path.exists(filepath):
        pytest.skip(f"File not found: {filepath}")

    parser = AknParser()
    doc = parser.parse_file(filepath)

    # Assert basic validity
    assert doc.frbr.urn != "urn:unknown", f"URN should be resolved, got {doc.frbr.urn}"
    assert len(doc.nodes) > 0, "Document should have at least one node"

    # Assert structural integrity
    structural = [n for n in doc.nodes if n.type == NodeType.STRUCTURAL]
    expressions = [n for n in doc.nodes if n.type == NodeType.EXPRESSION]
    assert len(structural) >= 1, "Must have at least one STRUCTURAL node"
    assert len(expressions) >= 1, "Must have at least one EXPRESSION node"

    # Assert no duplicate IDs
    ids = [n.id for n in doc.nodes]
    assert len(ids) == len(set(ids)), f"Found duplicate node IDs: {len(ids)} total, {len(set(ids))} unique"

    # Assert edges reference valid node IDs or external URNs
    node_ids = doc.node_ids()
    for edge in doc.edges:
        # source_id must be internal
        assert edge.source_id in node_ids, f"Edge source {edge.source_id} not found in nodes"
        # target_id can be external (URN) for CITES/MODIFIES
