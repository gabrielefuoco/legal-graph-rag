"""Build a multi-hop evaluation set where the graph is the ONLY path to the answer.

Eligibility is mechanical, not hand-picked. For every candidate we require:

  1. a SEED expression `S` that cites a work `W` which is now relinked to a real ingested act
     (`S-[:CITES]->W`, `W` has child expressions);
  2. a GOLDEN expression `G` inside that cited work (`G-[:PART_OF*1..2]->W`);
  3. a generated question whose answer lives in `G`, verified so that the graph-free pipeline
     (dense + BM25 with the raw query) does NOT retrieve `G` in its top-k, while it DOES
     retrieve the seed `S` (so the citation hop is available to the graph).

Queries that fail condition 3 are discarded automatically: if the base channels already find
the answer, the graph cannot add anything and the sample is not informative.

Usage:
    py scripts/build_multihop_benchmark.py --samples 40
    py scripts/build_multihop_benchmark.py --samples 40 --output data/eval_multihop.json
"""
import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from neo4j import AsyncGraphDatabase
from src.config import settings
from src.logging_config import setup_logging
from src.rag.llm_factory import get_llm
from src.rag.query_analyzer import analyze_query
from src.rag.retriever import bm25_search, vector_search
from src.rag.fusion import fuse_and_filter

setup_logging()
logger = logging.getLogger("build_multihop")

# Sample one random citing expression per cited work, then one golden article inside that work.
# Randomised on purpose: the first rows of the graph are dominated by a few acts.
CANDIDATE_QUERY = """
MATCH (w:Work)<-[:PART_OF*1..2]-(gold:Expression)
WHERE EXISTS { MATCH (:Expression)-[:CITES]->(w) }
  AND gold.text_display IS NOT NULL
  AND size(gold.text_display) > 400
WITH DISTINCT w, gold
WITH w, gold, rand() AS r
ORDER BY r
LIMIT $limit
MATCH (src:Expression)-[c:CITES]->(w)
WHERE src.text_display IS NOT NULL AND size(src.text_display) > 300
WITH w, gold, src, c, rand() AS r2
ORDER BY r2
WITH w, collect(src)[0] AS src, collect(gold)[0] AS gold, collect(c.text)[0] AS citation_text
RETURN DISTINCT
       src.id AS seed_id,
       left(src.text_display, 1800) AS seed_text,
       w.urn AS cited_urn,
       w.title AS cited_title,
       gold.id AS golden_id,
       left(gold.text_display, 1800) AS golden_text,
       left(coalesce(citation_text, ''), 400) AS citation_text
"""

QUESTION_PROMPT = (
    "Sei un redattore di quesiti per la valutazione di un sistema RAG giuridico italiano. "
    "Devi scrivere una domanda MULTI-HOP: la risposta si trova in una norma RICHIAMATA da un'altra "
    "norma, e la domanda deve far capire quale norma è richiamata.\n\n"
    "Ti fornisco:\n"
    "(A) l'ARTICOLO DELLA NORMA CHE RINVIA (contiene il rinvio);\n"
    "(B) il testo del RINVIO contenuto in (A), se disponibile;\n"
    "(C) il TESTO DELLA NORMA RICHIAMATA, che contiene la risposta.\n\n"
    "Scrivi UNA domanda in italiano che:\n"
    "(i) riguardi l'istituto e i fatti della norma richiamata (C), usando la sua terminologia "
    "tecnica (deve essere possibile riconoscere la disposizione dall'argomento);\n"
    "(ii) dica esplicitamente che la risposta va cercata nella norma richiamata da (A) "
    "(es. 'nella disciplina introdotta da ... che rinvia a ...'), così che si capisca che serve "
    "seguire il rinvio;\n"
    "(iii) NON copi frasi intere di (C) e non contenga la risposta, ma ne descriva l'argomento.\n\n"
    "Restituisci SOLO un oggetto JSON con la chiave \"question\"."
)

ANSWER_PROMPT = (
    "Leggi il seguente articolo normativo e rispondi in 1-2 frasi, in italiano, alla domanda "
    "dell'utente usando SOLO le informazioni contenute nell'articolo. Restituisci SOLO un oggetto "
    "JSON con la chiave \"answer\"."
)


async def generate_question(llm, seed_text: str, golden_text: str, cited_title: str, citation_text: str = "") -> dict:
    """Ask the LLM for a question built ONLY from the citing article (never from the answer)."""
    from langchain_core.messages import HumanMessage, SystemMessage

    messages = [
        SystemMessage(content=QUESTION_PROMPT),
        HumanMessage(content=(
            f"(A) ARTICOLO DELLA NORMA CHE RINVIA:\n{seed_text}\n\n"
            f"(B) TESTO DEL RINVIO:\n{citation_text or 'non disponibile'}\n\n"
            f"(C) TESTO DELLA NORMA RICHIAMATA:\n{golden_text}"
        )),
    ]
    try:
        response = await llm.ainvoke(messages)
        content = response.content if hasattr(response, "content") else str(response)
        if isinstance(content, list):
            content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        content = content.strip()
        if "{" in content and "}" in content:
            content = content[content.index("{"): content.rindex("}") + 1]
        data = json.loads(content)
        question = (data.get("question") or "").strip()
        if len(question) < 20:
            return {}
        return {"question": question}
    except Exception as e:
        logger.warning(f"Generazione domanda fallita: {e}")
        return {}


async def generate_answer(llm, question: str, golden_text: str) -> str:
    """Produce the golden answer for the generated question from the cited article only."""
    from langchain_core.messages import HumanMessage, SystemMessage

    messages = [
        SystemMessage(content=ANSWER_PROMPT),
        HumanMessage(content=f"ARTICOLO:\n{golden_text}\n\nDOMANDA: {question}"),
    ]
    try:
        response = await llm.ainvoke(messages)
        content = response.content if hasattr(response, "content") else str(response)
        if isinstance(content, list):
            content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        content = content.strip()
        if "{" in content and "}" in content:
            content = content[content.index("{"): content.rindex("}") + 1]
        data = json.loads(content)
        return (data.get("answer") or "").strip()
    except Exception as e:
        logger.warning(f"Generazione risposta fallita: {e}")
        return ""


async def main():
    """Sample citation triples, verify their eligibility and write the multi-hop benchmark."""
    parser = argparse.ArgumentParser(description="Costruisce il benchmark multi-hop (regola meccanica)")
    parser.add_argument("--samples", type=int, default=40, help="Quante triple candidato provare")
    parser.add_argument("--output", default="data/eval_multihop.json")
    parser.add_argument("--report", default="multihop_benchmark_report.json")
    parser.add_argument("--eligibility", choices=["cheap", "full"], default="cheap",
                        help="cheap = solo canali base (veloce), full = pipeline completa con LLM")
    parser.add_argument("--queries-only", action="store_true", help="Salva solo le query ammissibili già trovate")
    args = parser.parse_args()

    if not args.queries_only:
        async with AsyncGraphDatabase.driver(
            settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
        ) as driver:
            async with driver.session() as session:
                result = await session.run(CANDIDATE_QUERY, limit=args.samples)
                candidates = [dict(r) async for r in result]
        # The query already samples one random article per cited work: just drop duplicates
        seen_golden, triples = set(), []
        for c in candidates:
            if c["golden_id"] in seen_golden:
                continue
            seen_golden.add(c["golden_id"])
            triples.append(c)
        triples = triples[: args.samples]
        logger.info(f"Triple candidate (seed -> atto citato -> articolo): {len(triples)} su {len(candidates)} righe")

        llm = get_llm(temperature=0.2, reasoning=False)
        generated = []
        for idx, t in enumerate(triples, 1):
            qa = await generate_question(llm, t["seed_text"], t["golden_text"], t["cited_title"], t.get("citation_text", ""))
            if not qa:
                logger.warning(f"[{idx}/{len(triples)}] scartato (generazione vuota)")
                continue
            answer = await generate_answer(llm, qa["question"], t["golden_text"])
            if not answer:
                logger.warning(f"[{idx}/{len(triples)}] scartato (risposta vuota)")
                continue
            generated.append({**t, **qa, "answer": answer})
            logger.info(f"[{idx}/{len(triples)}] Q: {qa['question'][:80]}...")
        with open(".multihop_generated.json", "w", encoding="utf-8") as f:
            json.dump(generated, f, ensure_ascii=False, indent=2)
        logger.info(f"Domande generate: {len(generated)} (salvate in .multihop_generated.json)")
    else:
        with open(".multihop_generated.json", "r", encoding="utf-8") as f:
            generated = json.load(f)

    # ---- Mechanical verification: the graph-free pipeline must MISS the golden but FIND the seed
    previous = {k: getattr(settings, k) for k in ("RAG_TOP_K",)}
    settings.RAG_TOP_K = 30
    from src.rag.engine import RagEngine

    engine = RagEngine()
    eligible, discarded = [], []
    try:
        for idx, item in enumerate(generated, 1):
            if args.eligibility == "cheap":
                # Base channels only (no LLM reranker/grader): the golden must not be reachable
                # by plain retrieval, the seed must be. ~5s instead of ~40s per question.
                state = {
                    "query": item["question"],
                    "top_k": settings.RAG_TOP_K,
                    "final_k": 10,
                    "_driver": driver,
                    "_analyzer": engine.analyzer,
                    "enable_graph_search": False,
                }
                state.update(await analyze_query(state))
                for fn in (vector_search, bm25_search):
                    res = fn(state)
                    state.update(res if isinstance(res, dict) else await res)
                chunks = fuse_and_filter(state)["fused_chunks"][:10]
            else:
                chunks, _, _ = await engine.retrieve_with_trace(
                    query=item["question"],
                    top_k=settings.RAG_TOP_K,
                    final_k=10,
                    enable_graph_search=False,      # no TESEO concepts, no TESEO query expansion
                    enable_multi_hop=False,          # no citation hops
                    enable_topological_expansion=False,
                    skip_generation=True,
                )
            ids = set()
            for c in chunks:
                ids.add(c.expression_id)
                ids.add(c.expression_id.replace("merged_", ""))
                ids.update(c.metadata.get("merged_ids") or [])
            golden_found = item["golden_id"] in ids
            seed_found = item["seed_id"] in ids
            verdict = {
                "question": item["question"],
                "golden_answer": item["answer"],
                "expected_ids": [item["golden_id"]],
                "seed_id": item["seed_id"],
                "cited_urn": item["cited_urn"],
                "cited_title": item["cited_title"],
                "baseline_found_golden": golden_found,
                "baseline_found_seed": seed_found,
            }
            if not golden_found and seed_found:
                eligible.append(verdict)
                logger.info(f"[{idx}/{len(generated)}] AMMISSIBILE (golden assente, seed presente)")
            else:
                discarded.append(verdict)
                reason = "golden già trovata" if golden_found else "seed non trovato"
                logger.info(f"[{idx}/{len(generated)}] scartata: {reason}")
    finally:
        await engine.close()
        for k, v in previous.items():
            setattr(settings, k, v)

    dataset = [
        {"query": v["question"], "golden_answer": v["golden_answer"], "expected_ids": v["expected_ids"],
         "_multihop_seed": v["seed_id"], "_cited_urn": v["cited_urn"], "_cited_title": v["cited_title"]}
        for v in eligible
    ]
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump({"eligible": eligible, "discarded": discarded}, f, ensure_ascii=False, indent=2)
    logger.info(
        f"Benchmark multi-hop: {len(eligible)} query ammissibili su {len(generated)} generate "
        f"({len(discarded)} scartate) -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
