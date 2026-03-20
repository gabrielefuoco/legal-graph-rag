"""
Header Parser — FRBR Metadata Extraction.

Estrae i metadati FRBR (Work, Expression, Manifestation) dal blocco <meta>
degli documenti Akoma Ntoso, producendo un FRBRMetadata DTO.
"""
import logging
import re
from datetime import date
from typing import Optional

from lxml import etree

from src.parsing.namespaces import (
    detect_namespace, find, find_recursive, local_name, tag, xpath,
)
from src.parsing.models import FRBRMetadata

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_meta(root: etree._Element, ns_map: dict) -> FRBRMetadata:
    """
    Parse the <meta> block and produce FRBRMetadata.

    Args:
        root: The root <akomaNtoso> element.
        ns_map: Namespace map from detect_namespace().

    Returns:
        FRBRMetadata with all extracted fields.
    """
    # The document element is the first child of <akomaNtoso> (e.g. <act>, <bill>, <doc>)
    doc_element = _get_document_element(root, ns_map)
    if doc_element is None:
        raise ValueError("Cannot find document element (act/bill/doc) under <akomaNtoso>")

    doc_type = _detect_doc_type(doc_element, ns_map)
    meta_el = find(ns_map, doc_element, "meta")
    if meta_el is None:
        # NIR 2.2 robustness: meta might be a direct child of document element
        meta_el = find(ns_map, doc_element, "meta")
    
    if meta_el is None:
        raise ValueError("Cannot find <meta> element in document")

    # Detect if we are in NIR territory (Normattiva legacy)
    is_nir = "normeinrete.it" in (ns_map.get("akn", ""))

    if is_nir:
        return _parse_meta_nir(doc_element, meta_el, ns_map, doc_type)

    # --- Standard AKN Path ---
    # --- Extract FRBR layers ---
    frbr_work = _extract_frbr_work(meta_el, ns_map)
    frbr_expr = _extract_frbr_expression(meta_el, ns_map)
    frbr_manif = _extract_frbr_manifestation(meta_el, ns_map)

    # --- Extract identifiers ---
    urn = _extract_urn(meta_el, ns_map, frbr_work)
    eli = _extract_eli(meta_el, ns_map)

    # --- Extract title from preface/coverPage ---
    title = _extract_title(doc_element, ns_map)

    # --- Extract dates ---
    date_promulgation = _extract_date(frbr_work, ns_map)
    vigenza_start, vigenza_end = _extract_vigenza(meta_el, ns_map)

    # --- Extract doc number ---
    doc_number = _extract_attrib(frbr_work, ns_map, "FRBRnumber", "value", "")

    return FRBRMetadata(
        urn=urn,
        eli=eli,
        date_promulgation=date_promulgation,
        vigenza_start=vigenza_start,
        vigenza_end=vigenza_end,
        country=_extract_attrib(frbr_work, ns_map, "FRBRcountry", "value", "it"),
        doc_type=doc_type,
        doc_number=doc_number,
        title=title,
        frbr_work_uri=_extract_attrib(frbr_work, ns_map, "FRBRthis", "value", ""),
        frbr_expression_uri=_extract_attrib(frbr_expr, ns_map, "FRBRthis", "value", None)
        if frbr_expr is not None else None,
        frbr_manifestation_uri=_extract_attrib(frbr_manif, ns_map, "FRBRthis", "value", None)
        if frbr_manif is not None else None,
    )


# ---------------------------------------------------------------------------
# Internal Helpers
# ---------------------------------------------------------------------------

def _get_document_element(root: etree._Element, ns_map: dict) -> Optional[etree._Element]:
    """Find the main document element (act, bill, doc, amendment, etc.)."""
    doc_tag_names = [
        "act", "bill", "doc", "amendment", "judgment", "debate",
        # NIR-style names (should not appear in AKN, but just in case)
        "DecretoLegge", "Legge", "DecretoLegislativo",
    ]
    for child in root:
        ln = local_name(child)
        if ln in doc_tag_names:
            return child
    # Fallback: return first element child
    for child in root:
        if isinstance(child.tag, str):
            return child
    return None


def _detect_doc_type(doc_element: etree._Element, ns_map: dict) -> str:
    """Detect the document type from FRBRname or the document element tag."""
    # Try FRBRname first (more precise, e.g. "disegno di legge" from Senato)
    meta_el = find(ns_map, doc_element, "meta")
    if meta_el is None:
        for ch in doc_element:
            if local_name(ch) == "meta":
                meta_el = ch
                break
    if meta_el is not None:
        frbr_work = _extract_frbr_work(meta_el, ns_map)
        frbr_name = _extract_attrib(frbr_work, ns_map, "FRBRname", "value", None)
        if frbr_name:
            return frbr_name

    # Fallback to element tag mapping
    ln = local_name(doc_element)
    type_map = {
        "act": "atto",
        "bill": "disegno di legge",
        "doc": "documento",
        "amendment": "emendamento",
        "judgment": "sentenza",
    }
    return type_map.get(ln, ln)


def _extract_frbr_work(meta_el: etree._Element, ns_map: dict) -> Optional[etree._Element]:
    """Find the FRBRWork element inside <identification>."""
    identification = find(ns_map, meta_el, "identification")
    if identification is None:
        # Try without namespace (some documents nest differently)
        for child in meta_el:
            if local_name(child) == "identification":
                identification = child
                break
    if identification is not None:
        result = find(ns_map, identification, "FRBRWork")
        if result is None:
            for child in identification:
                if local_name(child) == "FRBRWork":
                    return child
        return result
    return None


def _extract_frbr_expression(meta_el: etree._Element, ns_map: dict) -> Optional[etree._Element]:
    """Find the FRBRExpression element."""
    identification = find(ns_map, meta_el, "identification")
    if identification is None:
        for child in meta_el:
            if local_name(child) == "identification":
                identification = child
                break
    if identification is not None:
        result = find(ns_map, identification, "FRBRExpression")
        if result is None:
            for child in identification:
                if local_name(child) == "FRBRExpression":
                    return child
        return result
    return None


def _extract_frbr_manifestation(meta_el: etree._Element, ns_map: dict) -> Optional[etree._Element]:
    """Find the FRBRManifestation element."""
    identification = find(ns_map, meta_el, "identification")
    if identification is None:
        for child in meta_el:
            if local_name(child) == "identification":
                identification = child
                break
    if identification is not None:
        result = find(ns_map, identification, "FRBRManifestation")
        if result is None:
            for child in identification:
                if local_name(child) == "FRBRManifestation":
                    return child
        return result
    return None


def _extract_attrib(
    parent: Optional[etree._Element],
    ns_map: dict,
    child_local: str,
    attrib_name: str,
    default=None,
):
    """Extract an attribute from a child element."""
    if parent is None:
        return default
    child = find(ns_map, parent, child_local)
    if child is None:
        # Fallback: search without namespace
        for c in parent:
            if local_name(c) == child_local:
                child = c
                break
    if child is not None:
        return child.get(attrib_name, default)
    return default


def _extract_urn(meta_el: etree._Element, ns_map: dict, frbr_work: Optional[etree._Element]) -> str:
    """
    Extract the URN identifier.
    
    Tries:
    1. FRBRalias[@name="urn"] or FRBRalias[@name="urn:nir"]
    2. Derive from FRBRthis value
    """
    if frbr_work is not None:
        # Look for FRBRalias with urn
        for alias in frbr_work:
            if local_name(alias) == "FRBRalias":
                name = alias.get("name", "")
                if name in ("urn", "urn:nir"):
                    return alias.get("value", "")

        # Derive from FRBRthis
        frbr_this = _extract_attrib(frbr_work, ns_map, "FRBRthis", "value", "")
        if frbr_this:
            return _uri_to_urn(frbr_this)

    return "urn:unknown"


def _uri_to_urn(uri: str) -> str:
    """
    Convert an AKN/FRBR URI to a URN-like identifier.

    Handles both:
    - AKN paths: /akn/it/act/legge/stato/2024-01-11/2/!main
    - Senato HTTP URIs: http://dati.senato.it/osr/Ddl/2022-10-13/1/main
    """
    # Remove trailing /main, /!main, etc.
    uri = re.sub(r"/?!?main.*$", "", uri)

    # --- Case 1: AKN path ---
    if uri.startswith("/akn/"):
        uri = re.sub(r"^/akn/it/act/", "", uri)
        parts = uri.strip("/").split("/")
        if len(parts) >= 4:
            doc_type, authority, doc_date, number = parts[0], parts[1], parts[2], parts[3]
            return f"urn:nir:{authority}:{doc_type}:{doc_date};{number}"
        return f"urn:akn:{uri}"

    # --- Case 2: Senato HTTP URI ---
    if "dati.senato.it" in uri:
        # http://dati.senato.it/osr/Ddl/2022-10-13/1 → urn:senato:Ddl:2022-10-13;1
        try:
            from urllib.parse import urlparse
            path = urlparse(uri).path.strip("/")
            parts = path.split("/")
            # Usually: osr/Ddl/date/number
            if len(parts) >= 4:
                doc_type, doc_date, number = parts[1], parts[2], parts[3]
                return f"urn:senato:{doc_type}:{doc_date};{number}"
            elif len(parts) >= 2:
                return f"urn:senato:{':'.join(parts[1:])}"
        except Exception:
            pass
        return f"urn:senato:{uri}"

    # --- Case 3: Already a URN ---
    if uri.startswith("urn:"):
        return uri

    # Fallback
    return f"urn:akn:{uri}"


def _extract_eli(meta_el: etree._Element, ns_map: dict) -> Optional[str]:
    """Extract ELI (European Legislation Identifier) from FRBRalias."""
    identification = find(ns_map, meta_el, "identification")
    if identification is None:
        for child in meta_el:
            if local_name(child) == "identification":
                identification = child
                break
    if identification is None:
        return None

    # Search in FRBRWork for alias with name="eli"
    frbr_work = find(ns_map, identification, "FRBRWork")
    if frbr_work is None:
        for child in identification:
            if local_name(child) == "FRBRWork":
                frbr_work = child
                break
    if frbr_work is not None:
        for alias in frbr_work:
            if local_name(alias) == "FRBRalias" and alias.get("name") == "eli":
                return alias.get("value")
    return None


def _extract_title(doc_element: etree._Element, ns_map: dict) -> Optional[str]:
    """Extract the document title from <preface>/<coverPage>/<docTitle>."""
    # Try <preface> first (Normattiva)
    for section_name in ("preface", "coverPage"):
        section = find(ns_map, doc_element, section_name)
        if section is None:
            for child in doc_element:
                if local_name(child) == section_name:
                    section = child
                    break
        if section is not None:
            # Look for <docTitle> anywhere in the section
            for desc in section.iter():
                if local_name(desc) == "docTitle":
                    text = "".join(desc.itertext()).strip()
                    if text:
                        return text
    return None


def _extract_date(frbr_work: Optional[etree._Element], ns_map: dict) -> date:
    """Extract the promulgation date from FRBRdate."""
    if frbr_work is not None:
        date_str = _extract_attrib(frbr_work, ns_map, "FRBRdate", "date", None)
        if date_str:
            try:
                return date.fromisoformat(date_str)
            except ValueError:
                logger.warning(f"Invalid date format: {date_str}")

    logger.warning("No promulgation date found, using date.min")
    return date.min


def _extract_vigenza(meta_el: etree._Element, ns_map: dict) -> tuple[Optional[date], Optional[date]]:
    """
    Extract validity period (vigenza) from lifecycle events.
    Returns (vigenza_start, vigenza_end).
    """
    lifecycle = find(ns_map, meta_el, "lifecycle")
    if lifecycle is None:
        for child in meta_el:
            if local_name(child) == "lifecycle":
                lifecycle = child
                break

    if lifecycle is None:
        return None, None

    dates = []
    for event in lifecycle:
        if local_name(event) == "eventRef":
            d = event.get("date")
            if d:
                try:
                    dates.append(date.fromisoformat(d))
                except ValueError:
                    pass

    if not dates:
        return None, None
    if len(dates) == 1:
        return dates[0], None
    dates.sort()
    return dates[0], dates[-1] if dates[-1] != dates[0] else None

def _parse_meta_nir(doc_element: etree._Element, meta_el: etree._Element, ns_map: dict, doc_type: str) -> FRBRMetadata:
    """Specialized parser for NIR 2.2 metadata."""
    descrittori = find(ns_map, meta_el, "descrittori")
    
    # 1. URN
    urn = "urn:unknown"
    if descrittori is not None:
        urn_el = find(ns_map, descrittori, "urn")
        if urn_el is not None:
            urn = urn_el.get("valore", "urn:unknown")
            if urn == "urn:": urn = "urn:unknown"

    # 2. Dates
    promulgation_date = date.min
    intestazione = find(ns_map, doc_element, "intestazione")
    if intestazione is not None:
        data_doc = find(ns_map, intestazione, "dataDoc")
        if data_doc is not None:
            norm_date = data_doc.get("norm", "")
            if re.match(r"^\d{8}$", norm_date):
                try:
                    promulgation_date = date(int(norm_date[:4]), int(norm_date[4:6]), int(norm_date[6:8]))
                except ValueError:
                    pass

    # 3. Title
    title = ""
    if intestazione is not None:
        titolo_doc = find(ns_map, intestazione, "titoloDoc")
        if titolo_doc is not None:
            title = "".join(titolo_doc.itertext()).strip()

    # 4. Doc Number
    doc_number = ""
    if intestazione is not None:
        num_doc = find(ns_map, intestazione, "numDoc")
        if num_doc is not None:
            doc_number = "".join(num_doc.itertext()).strip()

    return FRBRMetadata(
        urn=urn,
        eli=None,
        date_promulgation=promulgation_date,
        vigenza_start=promulgation_date,
        vigenza_end=None,
        country="it",
        doc_type=doc_type,
        doc_number=doc_number,
        title=title,
        frbr_work_uri=urn,
        frbr_expression_uri=None,
        frbr_manifestation_uri=None,
    )
