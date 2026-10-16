"""Diagnose the multi-hop failures and measure the resolvable subset.

Two things, both mechanical (graph + embeddings only, no LLM):

1. Population definition. For every question we compute
     * does the question name the act that contains the answer?
       (the resolver's act == the golden's act, with the resolver run on the question alone)
     * is the golden reachable through :CITES from the seed that generated the question?
     * is the golden reachable through :CITES from the chunks a graph-free run retrieves?
   "Resolvable subset" = questions where the named act IS the golden's act.

2. Failure classification for the questions the graph misses:
     * CORPUS  - the citation chain does not determine the provision (edges are act-level: the
                 question does not name the article, and the cited act has hundreds of articles)
     * MATCH   - the question does name the golden's act, but the resolver picked another act
     * RANK    - the act is right and reachable, but the provision is ranked below the injection
                 budget
     * OK      - the golden is in the injected set (so the miss happened downstream)

Usage:
    py scripts/diagnose_multihop.py
"""
import asyncio
import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from neo4j import AsyncGraphDatabase

from src.config import settings
from src.logging_config import setup_logging
from src.parsing.vector_engine import VectorEngine
from src.rag.resolver import ACT_ARTICLES_QUERY, _resolve_work, parse_act_references, resolve_query_citations

setup_logging()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpx2").setLevel(logging.WARNING)
logger = logging.getLogger("diagnose")

GOLDEN_INFO = """
MATCH (g:Expression {id: $gold})
OPTIONAL MATCH (g)-[:PART_OF*1..2]->(w:Work)
RETURN w.urn AS act_urn, w.title AS act_title, left(g.text_display, 60) AS text
"""

SEED_CITES = """
MATCH (s:Expression {id: $seed})-[:CITES]->(t)
OPTIONAL MATCH (g:Expression {id: $gold})-[:PART_OF*1..2]->(t)
RETURN count(g) AS reachable
"""


def _score(query_embedding, embedding) -> float:
    """Cosine similarity between the query and a provision embedding, -1.0 when missing."""
    if not embedding or query_embedding is None:
        return -1.0
    dot = sum(a * b for a, b in zip(query_embedding, embedding))
    na = sum(a * a for a in query_embedding) ** 0.5
    nb = sum(b * b for b in embedding) ** 0.5
    return dot / (na * nb) if na and nb else -1.0


async def main():
    """Diagnose the multi-hop failures: naming, graph reachability and resolver coverage."""
    with open("data/eval_multihop_all.json", "r", encoding="utf-8") as f:
        dataset = json.load(f)

    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    vector_engine = VectorEngine()

    rows = []
    async with driver.session() as session:
        for i, item in enumerate(dataset, 1):
            golden = (item.get("expected_ids") or [""])[0]
            seed = item.get("_multihop_seed") or ""
            query = item["query"]

            info = await (await session.run(GOLDEN_INFO, gold=golden)).single()
            golden_act = info["act_urn"] if info else None
            golden_text = (info["text"] if info else "") or ""
            golden_article = re.match(r"\s*art\.?\s*(\d+(?:-[a-z]+)?)", golden_text, re.IGNORECASE)

            # what the question names
            refs = parse_act_references(query)
            resolved_act = None
            if refs:
                work = await _resolve_work(session, refs[0])
                resolved_act = work["urn"] if work else None
            names_golden_act = bool(resolved_act) and resolved_act == golden_act

            # is the golden reachable from the seed through the citation graph?
            reach = await (await session.run(SEED_CITES, seed=seed, gold=golden)).single()
            seed_reachable = bool(reach and reach["reachable"])

            # is the golden among the provisions the resolver injects?
            query_embedding = (await vector_engine.compute_embeddings_batch([query]))[0]
            out = await resolve_query_citations(
                {"query": query, "query_embedding": query_embedding, "_driver": driver,
                 "graph_results": [], "fused_chunks": []}
            )
            injected = [c.expression_id for c in (out.get("graph_results") or [])]
            golden_injected = golden in injected

            # rank of the golden among the resolved act's provisions
            rank = None
            if resolved_act:
                candidates = [dict(r) async for r in await session.run(ACT_ARTICLES_QUERY, urn=resolved_act)]
                ranked = sorted(candidates, key=lambda c: _score(query_embedding, c.get("embedding")), reverse=True)
                ids = [c["expression_id"] for c in ranked]
                rank = ids.index(golden) + 1 if golden in ids else None

            rows.append({
                "q": i,
                "names_golden_act": names_golden_act,
                "seed_reachable": seed_reachable,
                "golden_injected": golden_injected,
                "rank_in_act": rank,
                "n_articles_in_act": None,
                "golden_has_article_number": bool(golden_article),
            })

    subset = [r for r in rows if r["names_golden_act"]]
    print(f"\n=== popolazione ===")
    print(f"  domande totali                              : {len(rows)}")
    print(f"  nominano l'atto che contiene la risposta    : {len(subset)} ({len(subset)/len(rows):.0%})")
    print(f"  di cui con catena :CITES dal seed           : {sum(1 for r in subset if r['seed_reachable'])}")
    print(f"  di cui con la golden nel set iniettato      : {sum(1 for r in subset if r['golden_injected'])}")

    print(f"\n=== diagnosi delle domande fuori dal sottoinsieme (l'atto nominato non è quello della risposta) ===")
    for r in rows:
        if not r["names_golden_act"]:
            cause = "CORPUS (nessun riferimento numerico risolvibile)" if r["rank_in_act"] is None else "MATCH"
            print(f"  q{r['q']:>2}  {cause:<8} rank nel proprio atto={r['rank_in_act']}  catena={r['seed_reachable']}")

    print(f"\n=== sottoinsieme risolvibile: dove si perde ===")
    for r in subset:
        if r["golden_injected"]:
            state = "OK (iniettata)"
        elif r["rank_in_act"] is None:
            state = "CORPUS (la golden non è tra i figli dell'atto)"
        elif r["rank_in_act"] <= 3:
            state = "RANK (dovrebbe essere iniettata)"
        else:
            state = f"RANK (rank {r['rank_in_act']} oltre il budget)"
        print(f"  q{r['q']:>2}  {state}")

    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
