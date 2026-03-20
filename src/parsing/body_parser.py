"""
Body Parser — Attraversamento Ricorsivo DFS & Semantic Chunking.

Attraversa l'elemento <body> dei documenti Akoma Ntoso usando DFS,
producendo nodi STRUCTURAL e EXPRESSION con archi PART_OF/NEXT.
Implementa l'iniezione di contesto per i nodi EXPRESSION.
"""
import logging
import re
from typing import Optional

from lxml import etree

from src.parsing.namespaces import find, local_name, tag
from src.parsing.models import (
    GraphNodeDTO, GraphEdgeDTO, NodeType, EdgeType, generate_id,
)
from src.parsing.edge_extractor import extract_edges

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tag Classification Sets
# ---------------------------------------------------------------------------

# Tags that create STRUCTURAL nodes (containers in the hierarchy)
STRUCTURAL_TAGS: set[str] = {
    # AKN 3.0 official names
    "book", "title", "part", "chapter", "section", "article",
    "subchapter", "subsection",
    # Italian-specific / NIR names (for robustness)
    "libro", "titolo", "capo", "sezione", "articolo",
    # Attachments
    "attachment", "annex", "doc",
    # Headings become part of their parent structural node
}

# Tags that create EXPRESSION nodes (atomic content for embeddings)
ATOMIC_TAGS: set[str] = {
    # AKN 3.0 official names
    "paragraph", "subparagraph", "content", "list", "point",
    "indent", "alinea", "item",
    # Italian-specific / NIR names
    "comma", "el", "lettera",
}

# Tags that are scanned for edges but don't create new nodes
INLINE_TAGS: set[str] = {
    "ref", "rref", "mref", "mod", "quotedStructure", "quotedText",
    "noteRef", "authorialNote",
}

# Tags that create atomic TABLE nodes (treated as single EXPRESSION)
TABLE_TAGS: set[str] = {
    "table",
}

# Tags to skip entirely (not meaningful for the graph)
SKIP_TAGS: set[str] = {
    "meta", "identification", "lifecycle", "workflow", "references",
    "analysis", "temporalData", "presentation", "components",
    "num", "heading",  # Handled inline by the parent node
}

# Tags whose text contributes to display but shouldn't create separate nodes
FORMATTING_TAGS: set[str] = {
    "i", "b", "u", "sub", "sup", "span", "br", "eol",
    "p",  # <p> inside <content> is just a text wrapper
    "docTitle", "docType", "docNumber", "docDate", "docProponent",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_body(
    root: etree._Element,
    ns_map: dict,
    urn: str,
) -> tuple[list[GraphNodeDTO], list[GraphEdgeDTO]]:
    """Phase B+C: Scan the document body and extract edges."""
    # 1. Resolve the main document element (act, bill, doc, etc.)
    doc_element = root
    if local_name(root) in ("akomaNtoso", "NIR"):
        for child in root:
            if local_name(child) not in ("meta", "coverPage"):
                doc_element = child
                break

    # 2. Find <body> or NIR-equivalent containers
    body = find(ns_map, doc_element, "body")
    if body is None:
        body = find(ns_map, doc_element, "articolato")
    
    if body is None:
        # Fallback manual loop (for namespace-agnostic search in tests)
        for child in doc_element:
            if local_name(child) in ("body", "articolato"):
                body = child
                break
    
    if body is None:
        logger.warning(f"No body container found in {local_name(doc_element)}")
        return [], []

    all_nodes: list[GraphNodeDTO] = []
    all_edges: list[GraphEdgeDTO] = []

    # Create a root-level structural node for the body itself
    body_eid = body.get("eId", body.get("id", "body"))
    body_id = generate_id(urn, body_eid)
    body_title = body.get("title", "")

    body_node = GraphNodeDTO(
        id=body_id,
        type=NodeType.STRUCTURAL,
        eid=body_eid,
        heading=body_title if body_title else None,
        level=0,
        tag_name=local_name(body),
    )
    all_nodes.append(body_node)

    # Traverse main body
    _traverse(
        element=body,
        ns_map=ns_map,
        urn=urn,
        parent_id=body_id,
        parent_eid=body_eid,
        context_chain=[],
        depth=1,
        counters={"gen": 0},
        all_nodes=all_nodes,
        all_edges=all_edges,
        prev_sibling_ids={},
    )

    # --- NIR/AKN Attachments (ROB-10) ---
    attachments_el = find(ns_map, doc_element, "attachments")
    if attachments_el is None:
        attachments_el = find(ns_map, doc_element, "annessi")
    
    if attachments_el is not None:
        logger.info("Found attachments section, parsing...")
        # Create a structural node for the attachments section itself
        attachments_eid = attachments_el.get("eId", attachments_el.get("id", "attachments"))
        attachments_id = generate_id(urn, attachments_eid)
        
        attachments_node = GraphNodeDTO(
            id=attachments_id,
            type=NodeType.STRUCTURAL,
            eid=attachments_eid,
            heading="Allegati",
            level=1,
            tag_name=local_name(attachments_el),
        )
        all_nodes.append(attachments_node)

        # Edge: attachments PART_OF body (root of the graph)
        all_edges.append(GraphEdgeDTO(
            source_id=attachments_id,
            target_id=body_id,
            type=EdgeType.PART_OF,
        ))

        # Traverse children of attachments
        _traverse(
            element=attachments_el,
            ns_map=ns_map,
            urn=urn,
            parent_id=attachments_id,
            parent_eid=attachments_eid,
            context_chain=["Allegato"],
            depth=2,
            counters={"gen": 0},
            all_nodes=all_nodes,
            all_edges=all_edges,
            prev_sibling_ids={},
        )

    logger.info(
        f"Body parsed: {len(all_nodes)} nodes, {len(all_edges)} edges"
    )
    return all_nodes, all_edges


# ---------------------------------------------------------------------------
# Internal: DFS Traversal
# ---------------------------------------------------------------------------

def _traverse(
    element: etree._Element,
    ns_map: dict,
    urn: str,
    parent_id: str,
    parent_eid: str,
    context_chain: list[str],
    depth: int,
    counters: dict,
    all_nodes: list[GraphNodeDTO],
    all_edges: list[GraphEdgeDTO],
    prev_sibling_ids: dict[int, str],
):
    """
    Recursive DFS traversal of the document body.

    For each child element:
    1. Classify as STRUCTURAL, ATOMIC, or SKIP
    2. Create the appropriate node
    3. Generate PART_OF edge to parent
    4. Generate NEXT edge to previous sibling at same depth
    5. For ATOMIC nodes: extract inline edges and apply context injection
    6. Recurse into children (for STRUCTURAL nodes)
    """
    for child in element:
        if not isinstance(child.tag, str):
            continue  # Skip comments, PIs

        ln = local_name(child)

        if ln in SKIP_TAGS or ln in FORMATTING_TAGS or ln in INLINE_TAGS:
            continue

        if ln in STRUCTURAL_TAGS:
            _handle_structural(
                child, ln, ns_map, urn, parent_id, parent_eid,
                context_chain, depth, counters,
                all_nodes, all_edges, prev_sibling_ids,
            )
        elif ln in ATOMIC_TAGS:
            _handle_atomic(
                child, ln, ns_map, urn, parent_id, parent_eid,
                context_chain, depth, counters,
                all_nodes, all_edges, prev_sibling_ids,
            )
        elif ln in TABLE_TAGS:
            _handle_table(
                child, ln, ns_map, urn, parent_id, parent_eid,
                context_chain, depth, counters,
                all_nodes, all_edges, prev_sibling_ids,
            )
        else:
            # Unknown tag — recurse into it transparently
            _traverse(
                child, ns_map, urn, parent_id, parent_eid,
                context_chain, depth, counters,
                all_nodes, all_edges, prev_sibling_ids,
            )


def _handle_structural(
    element: etree._Element,
    ln: str,
    ns_map: dict,
    urn: str,
    parent_id: str,
    parent_eid: str,
    context_chain: list[str],
    depth: int,
    counters: dict,
    all_nodes: list[GraphNodeDTO],
    all_edges: list[GraphEdgeDTO],
    prev_sibling_ids: dict[int, str],
):
    """Create a STRUCTURAL node and recurse into its children."""
    eid = _get_eid(element, ln, counters, parent_eid)
    node_id = generate_id(urn, eid)

    # Extract num and heading
    num_text = _extract_child_text(element, ns_map, "num")
    heading_text = _extract_child_text(element, ns_map, "heading")
    if not heading_text:
        heading_text = _extract_child_text(element, ns_map, "rubrica")
    
    # Fallback to attributes (common in attachments or NIR)
    if not heading_text:
        heading_text = element.get("name") or element.get("title")
    
    if ln == "attachment":
        logger.debug(f"Attachment element name={element.get('name')}, heading_text={heading_text}")

    # Build context label
    label_parts = []
    if num_text:
        label_parts.append(str(num_text).strip())
    if heading_text:
        label_parts.append(str(heading_text).strip())
    context_label = " - ".join(label_parts) if label_parts else str(ln).capitalize()

    node = GraphNodeDTO(
        id=node_id,
        type=NodeType.STRUCTURAL,
        eid=eid,
        num=num_text,
        heading=heading_text,
        level=depth,
        tag_name=ln,
    )
    all_nodes.append(node)

    # Edge: PART_OF → parent
    all_edges.append(GraphEdgeDTO(
        source_id=node_id,
        target_id=parent_id,
        type=EdgeType.PART_OF,
    ))

    # Edge: NEXT → previous sibling at same depth
    if depth in prev_sibling_ids:
        all_edges.append(GraphEdgeDTO(
            source_id=prev_sibling_ids[depth],
            target_id=node_id,
            type=EdgeType.NEXT,
        ))
    prev_sibling_ids[depth] = node_id

    # Recurse into children with extended context
    new_context = context_chain + [context_label]
    _traverse(
        element, ns_map, urn, node_id, eid,
        new_context, depth + 1, counters,
        all_nodes, all_edges, prev_sibling_ids,
    )


def _handle_atomic(
    element: etree._Element,
    ln: str,
    ns_map: dict,
    urn: str,
    parent_id: str,
    parent_eid: str,
    context_chain: list[str],
    depth: int,
    counters: dict,
    all_nodes: list[GraphNodeDTO],
    all_edges: list[GraphEdgeDTO],
    prev_sibling_ids: dict[int, str],
):
    """
    Create an EXPRESSION node with context injection.

    Checks if the atomic element contains sub-structures (e.g. <list> inside
    <paragraph>). If so, treats it as STRUCTURAL and recurses. If not, creates
    an atomic EXPRESSION node with context-injected text.
    """
    # Check for nested structural/atomic children (e.g. <paragraph> containing <list>)
    has_sub_structure = False
    for child in element:
        if isinstance(child.tag, str):
            cln = local_name(child)
            if cln in STRUCTURAL_TAGS or cln in ATOMIC_TAGS:
                has_sub_structure = True
                break

    if has_sub_structure:
        # Treat as structural container and recurse
        _handle_structural(
            element, ln, ns_map, urn, parent_id, parent_eid,
            context_chain, depth, counters,
            all_nodes, all_edges, prev_sibling_ids,
        )
        return

    eid = _get_eid(element, ln, counters, parent_eid)
    node_id = generate_id(urn, eid)

    # Extract text content
    num_text = _extract_child_text(element, ns_map, "num")
    raw_text = _extract_full_text(element)
    display_text = _extract_display_text(element)

    # Context Injection: build text_vector
    context_label = ""
    if num_text:
        # Add the comma/paragraph number to the context
        num_clean = num_text.strip().rstrip(".")
        context_parts = context_chain + [num_clean]
        context_label = " > ".join(context_parts)
    else:
        context_label = " > ".join(context_chain) if context_chain else ""

    text_vector = f"{context_label}: {raw_text}" if context_label else raw_text

    node = GraphNodeDTO(
        id=node_id,
        type=NodeType.EXPRESSION,
        eid=eid,
        num=num_text,
        text_content=raw_text,
        text_vector=text_vector,
        text_display=display_text,
        level=depth,
        tag_name=ln,
    )
    all_nodes.append(node)

    # Edge: PART_OF → parent
    all_edges.append(GraphEdgeDTO(
        source_id=node_id,
        target_id=parent_id,
        type=EdgeType.PART_OF,
    ))

    # Edge: NEXT → previous sibling at same depth
    if depth in prev_sibling_ids:
        all_edges.append(GraphEdgeDTO(
            source_id=prev_sibling_ids[depth],
            target_id=node_id,
            type=EdgeType.NEXT,
        ))
    prev_sibling_ids[depth] = node_id

    # Extract inline edges (CITES, MODIFIES) from this node's content
    inline_edges = extract_edges(element, ns_map, node_id, urn)
    all_edges.extend(inline_edges)

    # Mark modifier status in metadata if MODIFIES edges found
    if any(e.type == EdgeType.MODIFIES for e in inline_edges):
        node.metadata["is_modifier"] = True


def _handle_table(
    element: etree._Element,
    ln: str,
    ns_map: dict,
    urn: str,
    parent_id: str,
    parent_eid: str,
    context_chain: list[str],
    depth: int,
    counters: dict,
    all_nodes: list[GraphNodeDTO],
    all_edges: list[GraphEdgeDTO],
    prev_sibling_ids: dict[int, str],
):
    """
    Create an atomic EXPRESSION node from a <table> element.

    The entire table is treated as a single semantic unit.
    - text_display: Markdown table format
    - text_vector: linearized semantic text with context injection
    """
    eid = _get_eid(element, ln, counters)
    node_id = generate_id(urn, eid)

    # Extract table content
    display_text = _table_to_markdown(element)
    vector_text = _table_to_linear(element)

    # Context Injection
    context_label = " > ".join(context_chain) if context_chain else ""
    text_vector = f"{context_label} > Tabella: {vector_text}" if context_label else f"Tabella: {vector_text}"

    node = GraphNodeDTO(
        id=node_id,
        type=NodeType.EXPRESSION,
        eid=eid,
        text_content=vector_text,
        text_vector=text_vector,
        text_display=display_text,
        level=depth,
        tag_name=ln,
        metadata={"is_table": True},
    )
    all_nodes.append(node)

    # Edge: PART_OF → parent
    all_edges.append(GraphEdgeDTO(
        source_id=node_id,
        target_id=parent_id,
        type=EdgeType.PART_OF,
    ))

    # Edge: NEXT → previous sibling at same depth
    if depth in prev_sibling_ids:
        all_edges.append(GraphEdgeDTO(
            source_id=prev_sibling_ids[depth],
            target_id=node_id,
            type=EdgeType.NEXT,
        ))
    prev_sibling_ids[depth] = node_id

    # Extract inline edges from cell contents
    inline_edges = extract_edges(element, ns_map, node_id, urn)
    all_edges.extend(inline_edges)


def _table_to_markdown(table_el: etree._Element) -> str:
    """Convert a <table> XML element to Markdown table format."""
    rows = []
    for child in table_el.iter():
        cln = local_name(child)
        if cln in ("tr", "row"):
            cells = []
            for cell in child:
                cell_ln = local_name(cell)
                if cell_ln in ("th", "td", "cell"):
                    cell_text = "".join(cell.itertext()).strip()
                    cell_text = re.sub(r"\s+", " ", cell_text)
                    cells.append(cell_text)
            if cells:
                rows.append(cells)

    if not rows:
        return "".join(table_el.itertext()).strip()

    lines = []
    # First row as header
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join(["---"] * len(rows[0])) + " |")
    # Remaining rows
    for row in rows[1:]:
        # Pad or truncate to match header column count
        while len(row) < len(rows[0]):
            row.append("")
        lines.append("| " + " | ".join(row[:len(rows[0])]) + " |")

    return "\n".join(lines)


def _table_to_linear(table_el: etree._Element) -> str:
    """
    Convert a <table> XML element to linearized semantic text for embedding.

    Format: "Riga 1: Col1=val1, Col2=val2. Riga 2: ..."
    """
    rows = []
    headers = []

    for child in table_el.iter():
        cln = local_name(child)
        if cln in ("tr", "row"):
            cells = []
            is_header = False
            for cell in child:
                cell_ln = local_name(cell)
                if cell_ln in ("th", "td", "cell"):
                    cell_text = "".join(cell.itertext()).strip()
                    cell_text = re.sub(r"\s+", " ", cell_text)
                    cells.append(cell_text)
                    if cell_ln == "th":
                        is_header = True
            if cells:
                if is_header and not headers:
                    headers = cells
                else:
                    rows.append(cells)

    if not rows and not headers:
        return re.sub(r"\s+", " ", "".join(table_el.itertext())).strip()

    parts = []
    for idx, row in enumerate(rows, 1):
        if headers:
            pairs = [f"{headers[i]}={row[i]}" for i in range(min(len(headers), len(row)))]
            parts.append(f"Riga {idx}: {', '.join(pairs)}")
        else:
            parts.append(f"Riga {idx}: {', '.join(row)}")

    return ". ".join(parts)


# ---------------------------------------------------------------------------
# Internal: Attachments
# ---------------------------------------------------------------------------

def _parse_attachments(
    attachments_el: etree._Element,
    ns_map: dict,
    urn: str,
    body_id: str,
    all_nodes: list[GraphNodeDTO],
    all_edges: list[GraphEdgeDTO],
):
    """Parse <attachments> section, treating each child as a sub-document."""
    for idx, child in enumerate(attachments_el):
        if not isinstance(child.tag, str):
            continue

        ln = local_name(child)
        eid = child.get("eId", child.get("id", f"attachment_{idx}"))
        att_id = generate_id(urn, eid)
        att_name = child.get("name", ln)

        att_node = GraphNodeDTO(
            id=att_id,
            type=NodeType.STRUCTURAL,
            eid=eid,
            heading=att_name,
            level=1,
            tag_name=ln,
            metadata={"relation": "attachment"},
        )
        all_nodes.append(att_node)

        all_edges.append(GraphEdgeDTO(
            source_id=att_id,
            target_id=body_id,
            type=EdgeType.PART_OF,
            properties={"relation": "attachment"},
        ))

        # If the attachment has a <mainBody> or <body>, recurse into it
        inner_body = find(ns_map, child, "mainBody")
        if inner_body is None:
            inner_body = find(ns_map, child, "body")
        if inner_body is None:
            for sub in child:
                if local_name(sub) in ("mainBody", "body"):
                    inner_body = sub
                    break

        if inner_body is not None:
            _traverse(
                element=inner_body,
                ns_map=ns_map,
                urn=urn,
                parent_id=att_id,
                parent_eid=eid,
                context_chain=[f"Allegato: {att_name}"],
                depth=2,
                counters={"gen": 0},
                all_nodes=all_nodes,
                all_edges=all_edges,
                prev_sibling_ids={},
            )


# ---------------------------------------------------------------------------
# Internal: Helpers
# ---------------------------------------------------------------------------

def _get_eid(element: etree._Element, ln: str, counters: dict, parent_eid: str = "") -> str:
    """
    Get the eId of an element, generating a surrogate if missing.
    Ensures hierarchical uniqueness by prepending parent_eid and qualifying numeric IDs.
    Handles sibling deduplication if the same EID is generated twice.
    """
    raw_eid = element.get("eId") or element.get("id")
    
    if raw_eid:
        base = raw_eid
        # Always prepend tag name if ID is purely numeric or very short
        if base.isdigit() or len(base) <= 2:
            base = f"{ln}{base}"
    else:
        # Generate surrogate ID
        counters["gen"] += 1
        base = f"gen_{ln}_{counters['gen']}"

    # Prepend parent_eid for hierarchical uniqueness (skipping root 'body')
    if parent_eid and parent_eid != "body":
        # Avoid redundant prefixing
        if not base.startswith(parent_eid + "__"):
            full_eid = f"{parent_eid}__{base}"
        else:
            full_eid = base
    else:
        full_eid = base
    
    # Sibling deduplication: check if this full_eid has been used before
    if "eid_seen" not in counters:
        counters["eid_seen"] = {}
    
    count = counters["eid_seen"].get(full_eid, 0)
    if count > 0:
        # We've seen this EID before, apply a suffix (-n2, -n3, etc.)
        final_eid = f"{full_eid}-n{count + 1}"
    else:
        final_eid = full_eid
    
    # Update tracker
    counters["eid_seen"][full_eid] = count + 1
    
    return final_eid



def _extract_child_text(
    element: etree._Element,
    ns_map: dict,
    child_tag: str,
) -> Optional[str]:
    """Extract the text content of a direct child element."""
    child = find(ns_map, element, child_tag)
    if child is None:
        for c in element:
            if local_name(c) == child_tag:
                child = c
                break
    if child is not None:
        text = "".join(child.itertext()).strip()
        return text if text else None
    return None


def _extract_full_text(element: etree._Element) -> str:
    """
    Extract all text content from an element, recursively.
    Strips excess whitespace and normalizes line breaks.
    """
    raw = "".join(element.itertext())
    # Normalize whitespace
    text = re.sub(r"\s+", " ", raw).strip()
    return text


def _extract_display_text(element: etree._Element) -> str:
    """
    Extract text preserving some formatting for display.

    Converts:
    - <i> → *italic*
    - <b> → **bold**
    - <ref> → [text](href)
    """
    parts = []
    _walk_display(element, parts)
    text = "".join(parts)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _walk_display(element: etree._Element, parts: list[str]):
    """Recursive helper for display text extraction."""
    if element.text:
        parts.append(element.text)

    for child in element:
        if not isinstance(child.tag, str):
            if child.tail:
                parts.append(child.tail)
            continue

        ln = local_name(child)

        if ln == "i":
            parts.append("*")
            _walk_display(child, parts)
            parts.append("*")
        elif ln == "b":
            parts.append("**")
            _walk_display(child, parts)
            parts.append("**")
        elif ln == "ref":
            href = child.get("href", "")
            ref_text = "".join(child.itertext())
            parts.append(f"[{ref_text}]({href})")
        elif ln in ("br", "eol"):
            parts.append("\n")
        elif ln == "num":
            pass  # Skip <num> inside content, already handled
        else:
            _walk_display(child, parts)

        if child.tail:
            parts.append(child.tail)
