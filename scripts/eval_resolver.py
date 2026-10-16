"""Score the cross-reference resolver alone, without running the LLM pipeline.

The resolver is pure graph work (no LLM), so its accuracy can be measured on the whole
benchmark in seconds instead of ~50s per question. For every question we report
  * the article number and the act name it mentions,
  * the act the resolver matched,
  * whether the expected provision was resolved (and, when it was not, whether the act was right
    and only the article wrong).

Usage:
    py scripts/eval_resolver.py [--dataset data/eval_multihop_all.json] [--verbose]
"""
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from neo4j import AsyncGraphDatabase

from src.config import settings
from src.logging_config import setup_logging
from src.rag.resolver import ARTICLE_RE, _query_tokens, _reference_window

setup_logging()
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("eval_resolver")

GOLDEN_ACT_QUERY = """
MATCH (e:Expression {id: $id})
OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
RETURN w.urn AS urn, w.title AS title
LIMIT 1
"""


async def main():
    """Evaluate the citation resolver alone, without the LLM stages."""
    parser = argparse.ArgumentParser(description="Valuta solo il risolutore di rinvii (senza LLM)")
    parser.add_argument("--dataset", default="data/eval_multihop_all.json")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    with open(args.dataset, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    from src.rag.resolver import resolve_query_citations

    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )

    hits = resolved_something = act_right = 0
    rows = []
    async with driver.session() as session:
        for i, item in enumerate(dataset, 1):
            golden = (item.get("expected_ids") or [""])[0]
            query = item["query"]
            numbers = ARTICLE_RE.findall(query)
            window = _reference_window(query).strip()[:70]
            tokens = _query_tokens(_reference_window(query))

            state = {"query": query, "_driver": driver, "graph_results": [], "fused_chunks": []}
            out = await resolve_query_citations(state)
            chunks = out.get("graph_results") or []
            ids = [c.expression_id for c in chunks]
            hit = golden in ids
            hits += hit
            resolved_something += bool(ids)

            golden_act = ""
            result = await session.run(GOLDEN_ACT_QUERY, id=golden)
            record = await result.single()
            if record:
                golden_act = record.get("title") or ""
            resolved_act = chunks[0].expansion_trace.get("citing_title") if chunks else ""
            same_act = bool(resolved_act) and _same_act(resolved_act, golden_act)
            act_right += same_act

            rows.append((i, numbers, window, resolved_act, golden_act, hit, same_act))
            if args.verbose:
                print(f"[{i:>2}] art={numbers} finestra='{window}'")
                print(f"     risolto : {str(resolved_act)[:70]}")
                print(f"     golden  : {str(golden_act)[:70]}")
                print(f"     esito   : {'GIUSTO' if hit else ('atto giusto, articolo sbagliato' if same_act else 'atto sbagliato')}")

    n = len(dataset)
    print(f"\n=== risolutore su {n} domande ===")
    print(f"  ha risolto qualcosa        : {resolved_something}/{n} ({resolved_something / n:.0%})")
    print(f"  atto corretto              : {act_right}/{n} ({act_right / n:.0%})")
    print(f"  norma attesa risolta       : {hits}/{n} ({hits / n:.0%})")
    print("\n  dettaglio (atto risolto | atto della golden):")
    for i, numbers, window, resolved_act, golden_act, hit, same_act in rows:
        mark = "OK " if hit else ("~  " if same_act else "X  ")
        print(f"  {mark}[{i:>2}] art={','.join(numbers) or '-':<8} | {str(resolved_act)[:42]:<42} | {str(golden_act)[:42]}")

    await driver.close()


def _same_act(a: str, b: str) -> bool:
    """Loose title comparison: same act if they share a long distinctive token."""
    import re

    ta = set(re.findall(r"[a-zà-ù]{6,}", (a or "").lower()))
    tb = set(re.findall(r"[a-zà-ù]{6,}", (b or "").lower()))
    return len(ta & tb) >= 2


if __name__ == "__main__":
    asyncio.run(main())
