"""
Namespace Manager for Akoma Ntoso XML parsing.

Handles the two namespace conventions used by Italian legal data sources:
- Normattiva: default namespace (no prefix) — AKN 3.0
- Senato: prefixed namespace (an:) — AKN 3.0 CSD03
"""
from lxml import etree
from typing import Dict, Optional

# ---------------------------------------------------------------------------
# Namespace Dictionaries
# ---------------------------------------------------------------------------

# Normattiva uses AKN 3.0 as default namespace (no prefix in XML)
NS_NORMATTIVA: Dict[str, str] = {
    "akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0",
}

# Senato uses AKN 3.0 CSD03 with an: prefix
NS_SENATO: Dict[str, str] = {
    "an": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD03",
}

# Known AKN/NIR namespace URIs (used for detection)
_KNOWN_AKN_URIS = {
    "http://docs.oasis-open.org/legaldocml/ns/akn/3.0",
    "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD03",
    "http://www.normeinrete.it/nir/2.2/",
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_namespace(root: etree._Element) -> Dict[str, str]:
    """
    Detect which namespace convention is used by inspecting the root element.

    Returns:
        A namespace map suitable for lxml xpath/find operations.
        The key is always "akn" for uniform access.
    """
    # Strategy 1: Check the root tag for a namespace URI
    root_tag = root.tag
    if root_tag.startswith("{"):
        uri = root_tag.split("}")[0].lstrip("{")
        if uri in _KNOWN_AKN_URIS:
            return {"akn": uri}

    # Strategy 2: Check nsmap on the root element
    for prefix, uri in root.nsmap.items():
        if uri in _KNOWN_AKN_URIS:
            return {"akn": uri}

    # Strategy 3: Walk first-level children to find an AKN namespace
    for child in root:
        if child.tag.startswith("{"):
            uri = child.tag.split("}")[0].lstrip("{")
            if uri in _KNOWN_AKN_URIS:
                return {"akn": uri}

    # Fallback: try Normattiva default
    return NS_NORMATTIVA.copy()


def tag(ns_map: Dict[str, str], local_name: str) -> str:
    """
    Build a fully-qualified tag string for use with lxml find/findall.

    Example:
        tag(ns_map, "article") → "{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}article"
    """
    uri = ns_map.get("akn", "")
    if uri:
        return f"{{{uri}}}{local_name}"
    return local_name


def local_name(element: etree._Element) -> str:
    """
    Extract the local name of an element, stripping the namespace URI.

    Example:
        "{http://...}article" → "article"
    """
    t = element.tag
    if isinstance(t, str) and t.startswith("{"):
        return t.split("}", 1)[1]
    return t if isinstance(t, str) else ""


def xpath(ns_map: Dict[str, str], element: etree._Element, expr: str) -> list:
    """
    Execute an XPath expression with namespace awareness.

    The expression should use the 'akn:' prefix for AKN elements.
    Example:
        xpath(ns_map, root, ".//akn:article")
    """
    try:
        return element.xpath(expr, namespaces=ns_map)
    except etree.XPathError:
        return []


def find(ns_map: Dict[str, str], element: etree._Element, local: str) -> Optional[etree._Element]:
    """
    Find a direct child element by local name, namespace-aware.

    Example:
        find(ns_map, article_el, "num") → <num> element or None
    """
    return element.find(tag(ns_map, local))


def findall(ns_map: Dict[str, str], element: etree._Element, local: str) -> list:
    """
    Find all direct child elements by local name, namespace-aware.
    """
    return element.findall(tag(ns_map, local))


def find_recursive(ns_map: Dict[str, str], element: etree._Element, local: str) -> list:
    """
    Find all descendant elements by local name, namespace-aware.
    """
    return element.findall(f".//{tag(ns_map, local)}")
