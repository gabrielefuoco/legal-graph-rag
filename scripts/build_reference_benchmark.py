"""Build questions that name a provision, to test the graph's reference-resolution capability.

A question such as

    "Quali disposizioni prevede l'articolo 18 della legge 30 dicembre 2018, n. 145?"

cannot be answered by dense or BM25 search: the text of article 18 does not contain the name or
the number of its own act, so nothing in the provision matches the question's words. The
knowledge graph resolves the reference exactly (act URN -> article).

The questions are generated mechanically from the act URN, so no LLM is involved in the
construction and the wording is always the official one. A question is kept only if the
graph-free pipeline does NOT retrieve the provision in its top-10 (otherwise it tests nothing).

Usage:
    py scripts/build_reference_benchmark.py --samples 15
"""
import argparse
import asyncio
import json
import logging
import random
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from neo4j import AsyncGraphDatabase

from src.config import settings
from src.logging_config import setup_logging

setup_logging()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpx2").setLevel(logging.WARNING)
logger = logging.getLogger("build_reference")

MONTHS = [
    "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
    "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre",
]

TYPE_LABELS = {
    "legge": "legge",
    "decreto.legislativo": "decreto legislativo",
    "decreto.legge": "decreto-legge",
    "decreto.del.presidente.della.repubblica": "decreto del Presidente della Repubblica",
    "decreto.del.presidente.del.consiglio.dei.ministri": "decreto del Presidente del Consiglio dei ministri",
}

# One random article per random act, with a usable text and a parseable article number
CANDIDATE_QUERY = """
MATCH (w:Work)<-[:PART_OF*1..2]-(e:Expression)
WHERE e.text_display =~ '(?i)^art\\\\.?\\\\s*\\\\d+.*'
  AND size(e.text_display) > 400
  AND w.urn STARTS WITH 'urn:nir:stato:'
WITH w, e, rand() AS r
ORDER BY r
LIMIT $limit
RETURN w.urn AS urn, w.title AS work_title, e.id AS expression_id, e.text_display AS text
"""


def format_reference(urn: str) -> str | None:
    """urn:nir:stato:legge:2018-12-30;145 -> 'legge 30 dicembre 2018, n. 145'"""
    match = re.match(r"urn:nir:stato:([a-z.]+):(\d{4})-(\d{2})-(\d{2});(\d+)", urn or "")
    if not match:
        return None
    act_type, year, month, day, number = match.groups()
    label = TYPE_LABELS.get(act_type)
    if not label:
        return None
    return f"{label} {int(day)} {MONTHS[int(month) - 1]} {year}, n. {number}"


def article_number(text: str) -> str | None:
    """Extract the article number from a label such as 'art. 5-bis'."""
    match = re.match(r"\s*art\.?\s*(\d+(?:-[a-z]+)?)", text or "", re.IGNORECASE)
    return match.group(1) if match else None


TEMPLATES = [
    "Quali disposizioni prevede l'articolo {art} della {ref}?",
    "Cosa stabilisce l'articolo {art} della {ref}?",
    "Qual è il contenuto dell'articolo {art} della {ref}?",
    "Riassumi quanto disposto dall'articolo {art} della {ref}.",
]


async def main():
    """Build the named-reference benchmark from random (act, article) pairs of the graph."""
    parser = argparse.ArgumentParser(description="Costruisce il benchmark sui rinvii nominati")
    parser.add_argument("--samples", type=int, default=15, help="Quante domande costruire")
    parser.add_argument("--output", default="data/eval_reference.json")
    parser.add_argument("--report", default="reference_benchmark_report.json")
    args = parser.parse_args()

    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )

    async with driver.session() as session:
        result = await session.run(CANDIDATE_QUERY, limit=args.samples * 6)
        candidates = [dict(record) async for record in result]

    questions, seen = [], set()
    for candidate in candidates:
        if len(questions) >= args.samples:
            break
        reference = format_reference(candidate["urn"])
        number = article_number(candidate["text"])
        if not reference or not number:
            continue
        key = (candidate["urn"], number)
        if key in seen:
            continue
        seen.add(key)
        template = random.choice(TEMPLATES)
        question = template.format(art=number, ref=reference)
        questions.append({
            "query": question,
            "expected_ids": [candidate["expression_id"]],
            "golden_answer": " ".join((candidate["text"] or "").split())[:1200],
            "_reference": reference,
            "_article": number,
            "_act_urn": candidate["urn"],
            "_act_title": candidate["work_title"],
        })

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(questions, f, ensure_ascii=False, indent=2)

    logger.info(f"Benchmark rinvii: {len(questions)} domande -> {args.output}")
    for q in questions[:5]:
        logger.info(f"  {q['query']}")

    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
