"""Resolve the act (and article) that the question names, using the graph.

Why this node exists: a question like

    "Secondo l'articolo 125 del decreto legislativo 18 aprile 2016, n. 50, che modifica
     l'articolo 213 dello stesso decreto, quali misure sono previste?"

contains an exact, machine-readable reference, but no text-retrieval channel can use it: dense
and BM25 search the *words* of provisions, and the citation hop can only expand the edges of
chunks that were already retrieved.

Two ways to resolve a reference, both graph-native:
  * numeric form ("decreto legislativo 18 aprile 2016, n. 50", "legge 137 del 2018",
    "d.lgs. 50/2016") -> the act URN is `urn:nir:stato:<type>:<YYYY-MM-DD>;<number>`;
  * name form ("Codice dell'amministrazione digitale") -> match on the act title.
The article number is taken from the text *before* the act reference, which is how Italian
legislation is quoted.

The resolved provision is pinned (the reranker cannot cut it, the judge cannot drop it) and
doubles as a seed for the citation hop when the question names the *citing* act.
"""
import logging
import re

from neo4j import AsyncDriver

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)

# "art. 17", "articolo 213", "art. 5-bis"
ARTICLE_RE = re.compile(r"\bart(?:icolo)?\.?\s*(\d+(?:-[a-z]+)?)", re.IGNORECASE)

MONTHS = {
    "gen": "01", "feb": "02", "mar": "03", "apr": "04", "mag": "05", "giu": "06",
    "lug": "07", "ago": "08", "set": "09", "ott": "10", "nov": "11", "dic": "12",
}

TYPE_TO_URN = {
    "legge": "legge",
    "decreto legislativo": "decreto.legislativo",
    "decreto-legislativo": "decreto.legislativo",
    "decreto-legge": "decreto.legge",
    "decreto legge": "decreto.legge",
    "d.lgs.": "decreto.legislativo",
    "d.lgs": "decreto.legislativo",
    "d.l.": "decreto.legge",
    "d.l": "decreto.legge",
    "d.p.r.": "decreto.del.presidente.della.repubblica",
    "d.p.r": "decreto.del.presidente.della.repubblica",
}

ACT_PATTERNS = [
    # decreto legislativo 18 aprile 2016, n. 50
    re.compile(
        r"\b(?P<type>legge|decreto[\s-]legislativo|decreto[\s-]legge|d\.?\s?lgs\.?|d\.?\s?l\.?|d\.?\s?p\.?\s?r\.?)"
        r"\s+(?P<day>\d{1,2})\s+(?P<month>gen|feb|mar|apr|mag|giu|lug|ago|set|ott|nov|dic)[a-z]*"
        r"\s+(?P<year>\d{4})\s*,?\s*n?\.?\s*(?P<number>\d{1,4})",
        re.IGNORECASE,
    ),
    # legge 137 del 2018 / decreto legislativo 50 del 2016
    re.compile(
        r"\b(?P<type>legge|decreto[\s-]legislativo|decreto[\s-]legge|d\.?\s?lgs\.?|d\.?\s?l\.?)"
        r"\s+(?P<number>\d{1,4})\s+del\s+(?P<year>\d{4})",
        re.IGNORECASE,
    ),
    # d.lgs. 50/2016 / legge 137/2018
    re.compile(
        r"\b(?P<type>legge|decreto[\s-]legislativo|decreto[\s-]legge|d\.?\s?lgs\.?|d\.?\s?l\.?)"
        r"\s+(?P<number>\d{1,4})/(?P<year>\d{4})",
        re.IGNORECASE,
    ),
]

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

WORK_BY_URN = "MATCH (w:Work {urn: $urn}) RETURN w.urn AS urn, w.title AS title LIMIT 1"

WORK_BY_NUMBER_YEAR = """
MATCH (w:Work)
WHERE w.urn ENDS WITH (';' + $number) AND w.urn CONTAINS (':' + $year + '-')
RETURN w.urn AS urn, w.title AS title
LIMIT 5
"""

WORK_BY_TITLE_TOKENS = """
MATCH (w:Work)
WITH w, toLower(coalesce(w.title, '')) AS t
WITH w, t, [tok IN $tokens WHERE t CONTAINS tok] AS hits
WHERE size(hits) >= 2
RETURN w.urn AS urn, w.title AS title, size(hits) AS score, hits
ORDER BY score DESC, size(w.title) ASC
LIMIT 3
"""

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

# Every article of an act, embeddings only: the article number quoted in a question often
# belongs to the *citing* act, so the provision actually wanted has to be picked by relevance.
ACT_ARTICLES_QUERY = """
MATCH (w:Work {urn: $urn})<-[:PART_OF*1..2]-(e:Expression)
WHERE e.text_display IS NOT NULL
RETURN e.id AS expression_id, e.embedding AS embedding
"""

ARTICLE_DETAILS_QUERY = """
UNWIND $ids AS eid
MATCH (e:Expression {id: eid})
OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
RETURN e.id AS expression_id,
       e.text_display AS text,
       u.tag_name AS structural_tag,
       w.urn AS work_urn,
       w.title AS work_title,
       e.vigenza_start AS vigenza_start,
       e.vigenza_end AS vigenza_end
"""


def _query_tokens(text: str) -> list[str]:
    """Distinctive lowercase words of a piece of text."""
    words = re.findall(r"[a-zà-ù]{5,}", (text or "").lower())
    return [w for w in dict.fromkeys(words) if w not in STOPWORDS][:20]


def _reference_window(query: str) -> str:
    """Text after the article number: where the referenced act is usually named."""
    match = ARTICLE_RE.search(query or "")
    if not match:
        return ""
    return (query or "")[match.end(): match.end() + 140]


def parse_act_references(query: str) -> list[dict]:
    """Every act quoted in the question, with the article number quoted right before it."""
    references = []
    for pattern in ACT_PATTERNS:
        for match in pattern.finditer(query or ""):
            groups = match.groupdict()
            act_type = re.sub(r"\s+", " ", groups["type"].strip().lower())
            urn_type = TYPE_TO_URN.get(act_type)
            if not urn_type:
                continue
            day, month, year = groups.get("day"), groups.get("month"), groups.get("year")
            iso_date = f"{year}-{MONTHS[month[:3]]}-{int(day):02d}" if day and month and year else None
            prefix = (query or "")[: match.start()]
            numbers = ARTICLE_RE.findall(prefix)
            references.append({
                "urn": f"urn:nir:stato:{urn_type}:{iso_date};{groups['number']}" if iso_date else None,
                "number": groups["number"],
                "year": year,
                "article": numbers[-1] if numbers else None,
                "text": match.group(0),
            })
    # De-duplicate on (urn, article)
    seen, unique = set(), []
    for ref in references:
        key = (ref["urn"], ref["number"], ref["year"], ref["article"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(ref)
    return unique


async def _resolve_work(session, ref: dict) -> dict | None:
    """Act URN first (exact, then number+year), then the act named in prose."""
    if ref.get("urn"):
        result = await session.run(WORK_BY_URN, urn=ref["urn"])
        record = await result.single()
        if record:
            return {"urn": record["urn"], "title": record["title"], "how": "urn esatto"}

    if ref.get("number") and ref.get("year"):
        result = await session.run(WORK_BY_NUMBER_YEAR, number=ref["number"], year=ref["year"])
        records = [record async for record in result]
        if records:
            best = max(records, key=lambda r: len(r.get("title") or ""))
            return {"urn": best["urn"], "title": best["title"], "how": "urn numero+anno"}

    tokens = _query_tokens(ref.get("window") or "")
    if len(tokens) >= 2:
        result = await session.run(WORK_BY_TITLE_TOKENS, tokens=tokens)
        records = [record async for record in result]
        if records:
            return {"urn": records[0]["urn"], "title": records[0]["title"], "how": "titolo"}
    return None


async def _rank_act_articles(session, urn: str, query_embedding, number: str | None, limit: int):
    """Best articles of an act: the quoted number first, then the ones closest to the question."""
    result = await session.run(ACT_ARTICLES_QUERY, urn=urn)
    candidates = [record async for record in result]
    if not candidates:
        return []

    def _score(record) -> float:
        """Cosine similarity between the query embedding and an article's embedding."""
        embedding = record.get("embedding")
        if not embedding or query_embedding is None:
            return -1.0
        dot = sum(a * b for a, b in zip(query_embedding, embedding))
        na = sum(a * a for a in query_embedding) ** 0.5
        nb = sum(b * b for b in embedding) ** 0.5
        return dot / (na * nb) if na and nb else -1.0

    candidates.sort(key=_score, reverse=True)
    selected = [c["expression_id"] for c in candidates[:limit]]

    # The number quoted in the question is still worth adding when the act really has it
    if number:
        quoted = await session.run(ARTICLE_QUERY, urn=urn, number=number, limit=2)
        for record in [r async for r in quoted]:
            expr_id = record.get("expression_id")
            if expr_id and expr_id not in selected:
                selected.insert(0, expr_id)
    return selected[: max(limit, 2)]


async def resolve_query_citations(state: RagState) -> dict:
    """Inject the provisions the question explicitly refers to.

    Every exit path returns a non-empty update: LangGraph turns an empty dict into a None
    update, which then breaks the caller's state merge.
    """
    unchanged = {"graph_results": state.get("graph_results") or []}
    if not getattr(settings, "RESOLVE_QUERY_CITATIONS", True):
        return unchanged

    query = state.get("query") or ""
    references = parse_act_references(query)
    window = _reference_window(query)
    if not references and not window:
        return unchanged

    # A prose-only reference ("che rinvia al Codice dell'amministrazione digitale") has no URN:
    # resolve it by title, taking the article number quoted before the act name.
    if not references:
        references = [{"urn": None, "number": None, "year": None,
                       "article": (ARTICLE_RE.findall(query) or [None])[0],
                       "window": window, "text": window[:60]}]

    driver: AsyncDriver = state["_driver"]
    existing_ids = {c.expression_id for c in (state.get("fused_chunks") or [])}
    query_embedding = state.get("query_embedding")
    per_act = max(1, getattr(settings, "RESOLVED_ACT_ARTICLES", 3))
    resolved: list[RetrievedChunk] = []

    try:
        async with driver.session() as session:
            for ref in references[:3]:
                work = await _resolve_work(session, ref)
                if not work:
                    continue
                number = ref.get("article") or (ARTICLE_RE.findall(query) or [None])[0]
                selected = await _rank_act_articles(session, work["urn"], query_embedding, number, per_act)
                if not selected:
                    continue
                detail_result = await session.run(ARTICLE_DETAILS_QUERY, ids=selected)
                async for record in detail_result:
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
                            "citation_text": f"{ref['text'][:80]}",
                        },
                    ))
                logger.info(
                    f"Rinvio risolto ({work['how']}): {len(selected)} articoli di "
                    f"'{str(work['title'])[:55]}'"
                )
    except Exception as e:
        logger.error(f"Errore nella risoluzione dei rinvii: {e}")

    if not resolved:
        logger.info("Rinvio nella domanda: nessuna corrispondenza trovata nel grafo")
        return unchanged

    return {"graph_results": (state.get("graph_results") or []) + resolved}
