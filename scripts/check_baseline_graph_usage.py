"""Inspect what the "graph-free" baseline still borrows from the knowledge graph.

`enable_graph_search=False` disables the TESEO channel/expansion, citation hops and topological
expansion, but the chunks themselves are graph nodes: they carry a structural breadcrumb
(PART_OF hierarchy), a vigenza window, and they are fetched through Neo4j indexes. This script
prints, per retrieved chunk, which graph-derived fields are still populated, so the thesis can
state exactly what "baseline" means.

Usage:
    py scripts/check_baseline_graph_usage.py
"""
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.logging_config import setup_logging
from src.rag.engine import RagEngine

setup_logging()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("src").setLevel(logging.WARNING)
logger = logging.getLogger("check_baseline")


async def main():
    """Report how many graph-derived fields the baseline actually sees with the graph off."""
    with open("data/eval_dataset.json", "r", encoding="utf-8") as f:
        dataset = json.load(f)

    engine = RagEngine()

    queries = dataset[:3]
    totals = {"chunks": 0, "breadcrumb": 0, "vigenza": 0, "teseo": 0, "citation": 0, "topology": 0}
    for item in queries:
        chunks, _trace, _ = await engine.retrieve_with_trace(
            query=item["query"],
            top_k=settings.RAG_TOP_K,
            final_k=10,
            enable_graph_search=False,
            enable_multi_hop=False,
            max_citation_hops=1,
            enable_topological_expansion=False,
            skip_generation=True,
        )
        print(f"\n--- query: {item['query'][:70]}... ({len(chunks)} chunk finali)")
        for c in chunks:
            totals["chunks"] += 1
            has_bc = bool(c.structural_context)
            has_vig = bool(c.vigenza_start or c.vigenza_end)
            has_tes = bool(c.metadata.get("matched_concepts") or c.metadata.get("matched_concept_labels"))
            has_cit = bool((c.expansion_trace or {}).get("citing_title"))
            has_topo = bool(c.expanded_text)
            totals["breadcrumb"] += has_bc
            totals["vigenza"] += has_vig
            totals["teseo"] += has_tes
            totals["citation"] += has_cit
            totals["topology"] += has_topo
            flags = [
                f"source={c.source}",
                f"breadcrumb={'SÌ' if has_bc else 'no'}",
                f"vigenza={'SÌ' if has_vig else 'no'}",
                f"teseo={'SÌ' if has_tes else 'no'}",
                f"citazione={'SÌ' if has_cit else 'no'}",
                f"topologia={'SÌ' if has_topo else 'no'}",
            ]
            print(f"   [{c.expression_id[:8]}] " + " | ".join(flags))

    n = max(1, totals["chunks"])
    print("\n=== riepilogo (baseline: grafo disattivato) ===")
    print(f"chunk analizzati: {totals['chunks']}")
    for key, label in [
        ("breadcrumb", "breadcrumb strutturale (PART_OF)"),
        ("vigenza", "finestra di vigenza (proprietà del grafo)"),
        ("teseo", "match TESEO"),
        ("citation", "citazione (multi-hop)"),
        ("topology", "espansione topologica"),
    ]:
        print(f"  {label:<42} {totals[key]}/{n} ({totals[key] / n:.0%})")

    await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
