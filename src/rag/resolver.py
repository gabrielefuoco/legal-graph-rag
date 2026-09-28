"""Resolve the cross-reference that the question itself names.

A question like "che rinvia all'articolo 17 del Codice dell'amministrazione digitale" contains
everything needed to reach the answer, but the retrieval channels cannot use it: dense and BM25
search the *text* of provisions, and the citation hop only expands the edges of the chunks that
were already retrieved (a law cites dozens of acts, so the referenced one is rarely among them).

This node closes that gap with information only the graph holds: it recognises the act and the
article mentioned in the question, walks to that exact provision and puts it in the context.
"""
import logging
import re

from neo4j import AsyncDriver

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)

# "art. 17", "articolo 213", "art. 5-bis"
ARTICLE_RE = re.compile(r"\bart(?:icolo)?\.?\s*(\d+(?:-[a-z]+)?)", re.IGNORECASE)

# Generic legal vocabulary: useless for telling one act from another
STOPWORDS = {
    "articolo", "articoli", "comma", "commi", "legge", "leggi", "decreto", "decreti",
    "disciplina", "disposizioni", "prevede", "prevedono", "vigente", "vigenti", "norma",
    "norme", "rinvia", "richiama", "richiamato", "richiamata", "riferimento", "secondo",
    "quale", "quali", "della", "delle", "dello", "degli", "dalla", "dalle", "nella", "nelle",
    "sono", "essere", "viene", "vengono", "come", "cosa", "quando", "dove", "anche", "sulla",
    "sulle", "dagli", "presso", "nonché", "ovvero", "oppure", "relativo", "relativa",
    "introdotta", "introdotto", "modifica", "modificato", "abrogato", "abrogata",
    "applicazione", "attuazione", "materia", "settore", "ambito", "ipotesi", "caso", "casi",
    "effetti", "termine", "termini", "soggetti", "soggetto", "presente", "seguente",
    "specifico", "specifica", "generale", "generali", "particolare", "particolari",
}

# Match an act by the distinctive words of its title
ACT_MATCH_QUERY = """
MATCH (w:Work)
WITH w, toLower(coalesce(w.title, '')) AS t
WITH w, t, [tok IN $tokens WHERE t CONTAINS tok] AS hits
WHERE size(hits) >= 2
RETURN w.urn AS urn, w.title AS title, size(hits) AS score, hits
ORDER BY score DESC, size(w.title) ASC
LIMIT 3
"""

# Fetch the provision whose text starts with "Art. N" inside a given act
ARTICLE_QUERY = """
MATCH (w:Work {urn: $urn})<-[:PART_OF*1..2]-(e:Expression)
WHERE e.text_display =~ ('(?i)^art\\.?\\s*' + $number + '([^0-9].*)?')
  AND e.text_display IS NOT NULL
OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
RETURN e.id AS expression_id,
       e.text_display AS text,
       u.tag_name AS structural_tag,
       w.urn AS work_urn,
       w.title AS work_title,
       e.vigenza_start AS vigenza_start,
       e.vigenza_end AS vigenza_end
LIMIT $limit
"""


def _query_tokens(query: str) -> list[str]:
    """Distinctive lowercase words of the question (what the act title should match)."""
    words = re.findall(r"[a-zà-ù]{5,}", (query or "").lower())
    return [w for w in dict.fromkeys(words) if w not in STOPWORDS][:20]


def _reference_window(query: str) -> str:
    """Text that follows the article number, i.e. where the referenced act is named.

    "... che rinvia all'articolo 17 del Codice dell'amministrazione digitale" -> the act is
    named right after the article number. Questions often mention two acts (the citing one and
    the cited one), so matching on the whole question picks the wrong one.
    """
    match = ARTICLE_RE.search(query or "")
    if not match:
        return ""
    return (query or "")[match.end(): match.end() + 140]


async def resolve_query_citations(state: RagState) -> dict:
    """Add the provisions explicitly referenced by the question to the graph results.

    Every exit path returns a non-empty update: LangGraph turns an empty dict into a None
    update, which then breaks the caller's state merge.
    """
    unchanged = {"graph_results": state.get("graph_results") or []}

    if not getattr(settings, "RESOLVE_QUERY_CITATIONS", True):
        return unchanged

    query = state.get("query") or ""
    numbers = list(dict.fromkeys(ARTICLE_RE.findall(query)))[:3]
    if not numbers:
        return unchanged

    tokens = _query_tokens(_reference_window(query))
    if len(tokens) < 2:
        return unchanged

    driver: AsyncDriver = state["_driver"]
    existing_ids = {c.expression_id for c in (state.get("fused_chunks") or [])}
    resolved: list[RetrievedChunk] = []

    try:
        async with driver.session() as session:
            result = await session.run(ACT_MATCH_QUERY, tokens=tokens)
            acts = [record async for record in result]

            for act in acts:
                for number in numbers:
                    article_result = await session.run(
                        ARTICLE_QUERY, urn=act["urn"], number=number, limit=4
                    )
                    async for record in article_result:
                        expr_id = record.get("expression_id") or ""
                        if not expr_id or expr_id in existing_ids:
                            continue
                        existing_ids.add(expr_id)
                        resolved.append(RetrievedChunk(
                            text=record.get("text") or "",
                            expression_id=expr_id,
                            work_urn=record.get("work_urn"),
                            structural_context=record.get("structural_tag") or "",
                            score=0.0,
                            source="citation_resolve",
                            expansion_trace={
                                "relation": "RINVIO",
                                "citing_title": record.get("work_title"),
                                "citation_text": f"articolo {number} richiamato nella domanda",
                            },
                        ))
                if resolved:
                    logger.info(
                        f"Rinvio risolto: {len(resolved)} articoli da '{str(act['title'])[:60]}' "
                        f"(match {act['score']}: {', '.join(act['hits'][:5])})"
                    )
                    break
    except Exception as e:
        logger.error(f"Errore nella risoluzione dei rinvii: {e}")

    if not resolved:
        logger.info("Rinvio nella domanda: nessuna corrispondenza trovata nel grafo")
        return unchanged

    return {"graph_results": (state.get("graph_results") or []) + resolved}
