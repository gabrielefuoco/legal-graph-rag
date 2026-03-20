"""
Edge Extractor — Estrazione di relazioni inline.

Scansiona i nodi atomici (EXPRESSION) per elementi inline come <ref>, <mod>,
<quotedStructure>, <quotedText> e produce archi CITES/MODIFIES.
"""
import logging
import re
from typing import Optional

from lxml import etree

from src.parsing.namespaces import local_name
from src.parsing.models import GraphEdgeDTO, EdgeType, ModificationType, generate_id

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_edges(
    element: etree._Element,
    ns_map: dict,
    source_node_id: str,
    doc_urn: str,
) -> list[GraphEdgeDTO]:
    """
    Scan an element (typically a content/paragraph node) for inline
    references and modifications, producing graph edges.

    Args:
        element: The XML element to scan (e.g. <content>, <paragraph>).
        ns_map: Namespace map.
        source_node_id: ID of the source GraphNodeDTO.
        doc_urn: URN of the parent document (for resolving relative refs).

    Returns:
        List of GraphEdgeDTO (CITES, MODIFIES).
    """
    edges: list[GraphEdgeDTO] = []

    for desc in element.iter():
        ln = local_name(desc)

        if ln in ("ref", "rif"):
            edge = _parse_ref(desc, source_node_id, doc_urn)
            if edge:
                edges.append(edge)

        elif ln == "rref":
            range_edges = _parse_rref(desc, source_node_id, doc_urn)
            edges.extend(range_edges)

        elif ln == "mref":
            # Multiple references: iterate over child <ref> elements
            for child in desc:
                if local_name(child) in ("ref", "rif"):
                    edge = _parse_ref(child, source_node_id, doc_urn)
                    if edge:
                        edges.append(edge)

        elif ln == "mod":
            mod_edges = _parse_mod(desc, source_node_id, doc_urn)
            edges.extend(mod_edges)

    return edges


# ---------------------------------------------------------------------------
# Internal: <ref> → CITES
# ---------------------------------------------------------------------------

def _parse_ref(
    ref_el: etree._Element,
    source_node_id: str,
    doc_urn: str,
) -> Optional[GraphEdgeDTO]:
    """Parse a <ref> element and produce a CITES edge."""
    # Lookup standard href or xlink:href
    href = ref_el.get("href")
    if not href:
        href = ref_el.get("{http://www.w3.org/1999/xlink}href", "")
    if not href:
        return None

    target_id = _resolve_href(href, doc_urn)
    ref_text = "".join(ref_el.itertext()).strip()

    return GraphEdgeDTO(
        source_id=source_node_id,
        target_id=target_id,
        type=EdgeType.CITES,
        properties={
            "href": href,
            "text": ref_text,
            "relation": "EXPLICIT",
        },
    )


# ---------------------------------------------------------------------------
# Internal: <rref> → CITES (range)
# ---------------------------------------------------------------------------

def _parse_rref(
    rref_el: etree._Element,
    source_node_id: str,
    doc_urn: str,
) -> list[GraphEdgeDTO]:
    """
    Parse a <rref> (range reference) element.

    Generates a single CITES edge with range=True property,
    since the exact range boundaries are often not machine-parsable.
    """
    href_from = rref_el.get("from", "")
    href_to = rref_el.get("upTo", rref_el.get("to", ""))
    ref_text = "".join(rref_el.itertext()).strip()

    edges = []

    if href_from:
        target_from = _resolve_href(href_from, doc_urn)
        edges.append(GraphEdgeDTO(
            source_id=source_node_id,
            target_id=target_from,
            type=EdgeType.CITES,
            properties={
                "href": href_from,
                "text": ref_text,
                "relation": "EXPLICIT",
                "range": True,
                "range_from": href_from,
                "range_to": href_to,
            },
        ))
    elif href_to:
        target_to = _resolve_href(href_to, doc_urn)
        edges.append(GraphEdgeDTO(
            source_id=source_node_id,
            target_id=target_to,
            type=EdgeType.CITES,
            properties={
                "href": href_to,
                "text": ref_text,
                "relation": "EXPLICIT",
                "range": True,
            },
        ))

    return edges


# ---------------------------------------------------------------------------
# Internal: <mod> → MODIFIES
# ---------------------------------------------------------------------------

def _parse_mod(
    mod_el: etree._Element,
    source_node_id: str,
    doc_urn: str,
) -> list[GraphEdgeDTO]:
    """
    Parse a <mod> element (normative modification).

    Finds the <ref> inside <mod> to identify the target,
    extracts <quotedText>/<quotedStructure> as the modification content,
    and classifies the modification type (substitution, insertion, repeal).
    """
    edges = []

    # Find target from inner <ref> elements
    target_refs = []
    for child in mod_el.iter():
        ln = local_name(child)
        if ln in ("ref", "rif"):
            href = child.get("href")
            if not href:
                href = child.get("{http://www.w3.org/1999/xlink}href", "")
            if href:
                target_refs.append(href)

    # Extract quoted content
    quoted_text = _extract_quoted(mod_el)

    # Classify the modification type
    mod_type = _classify_modification(mod_el)

    # If no <ref> found inside <mod>, create an edge with unknown target
    if not target_refs:
        target_refs = ["urn:unknown:modification"]

    for href in target_refs:
        target_id = _resolve_href(href, doc_urn)
        edges.append(GraphEdgeDTO(
            source_id=source_node_id,
            target_id=target_id,
            type=EdgeType.MODIFIES,
            properties={
                "href": href,
                "quoted_text": quoted_text,
                "relation": "MODIFICATION",
                "modification_type": mod_type.value,
            },
        ))

    return edges


# Regex patterns for Italian legal modification keywords
_SUBSTITUTION_PATTERN = re.compile(
    r"sostitu[it]|rimpiazzat|è\s+così\s+modifica",
    re.IGNORECASE,
)
_INSERTION_PATTERN = re.compile(
    r"inserit|aggiunt|dopo\s+(il|l[ae']|i|gli|le)\s+(comma|articol|letter|parol)",
    re.IGNORECASE,
)
_REPEAL_PATTERN = re.compile(
    r"abrogat|soppress|eliminat|è\s+soppresso|sono\s+soppress",
    re.IGNORECASE,
)


def _classify_modification(mod_el: etree._Element) -> ModificationType:
    """
    Classify the type of normative modification from the text of a <mod> element.

    Uses regex pattern matching on Italian legal keywords:
    - "sostituito" → SUBSTITUTION
    - "inserito" / "aggiunto" → INSERTION
    - "abrogato" / "soppresso" → REPEAL
    - fallback → AMENDMENT
    """
    text = "".join(mod_el.itertext()).strip()

    if _REPEAL_PATTERN.search(text):
        return ModificationType.REPEAL
    if _SUBSTITUTION_PATTERN.search(text):
        return ModificationType.SUBSTITUTION
    if _INSERTION_PATTERN.search(text):
        return ModificationType.INSERTION

    return ModificationType.AMENDMENT


def _extract_quoted(mod_el: etree._Element) -> str:
    """Extract the text of <quotedText> or <quotedStructure> inside a <mod>."""
    for desc in mod_el.iter():
        ln = local_name(desc)
        if ln in ("quotedText", "quotedStructure", "virgolette", "strutturaCitata"):
            return "".join(desc.itertext()).strip()
    return ""


# ---------------------------------------------------------------------------
# Internal: Href Resolution
# ---------------------------------------------------------------------------

def _resolve_href(href: str, doc_urn: str) -> str:
    """
    Resolve an href attribute to a target node ID or external URN.

    Cases:
    1. href starts with '#' → internal reference → generate_id(doc_urn, fragment)
    2. href starts with '/akn/' → absolute AKN URI → use as-is
    3. href starts with 'urn:' → URN → use as-is
    4. href is relative → resolve against doc_urn
    """
    if href.startswith("#"):
        # Internal reference within the same document
        fragment = href.lstrip("#")
        return generate_id(doc_urn, fragment)

    if href.startswith("/akn/") or href.startswith("urn:"):
        # Absolute reference
        return href

    if href.startswith("http"):
        # Full URL (e.g. Senato URIs)
        return href

    # Relative reference — resolve against doc URN
    return f"{doc_urn}#{href}"
