"""Relink citation edges from external-reference stubs to the real ingested Works.

The ingestion creates a stub `Work` node for every citation target ("Riferimento Esterno")
using a year-only URN (e.g. `urn:nir:stato:legge:1988;400`), while ingested acts carry the
dated URN (e.g. `urn:nir:stato:legge:2024-03-04;40`). Stubs have no child Expressions, so
`expand_citations` can never walk from a retrieved chunk to the cited provisions: the
multi-hop expansion silently returns nothing (measured: 0 chunks added on 45/45 retrievals).

This script redirects the incoming `CITES` edges of the relinkable stubs to the real Work,
preserving the edge properties (`text`, `extracted_by`) and marking the stub for traceability.
It is read-only unless `--apply` is passed.

Usage:
    py scripts/relink_citations.py                 # dry-run: report candidates and counts
    py scripts/relink_citations.py --apply         # migrate the edges in batches
    py scripts/relink_citations.py --apply --limit 50
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

setup_logging()
logger = logging.getLogger("relink_citations")

# Real Works that actually contain provisions (the relink targets)
REAL_WORKS_QUERY = """
MATCH (real:Work)<-[:PART_OF]-(:Expression)
RETURN DISTINCT real.id AS id, real.urn AS urn
"""

# External-reference stubs: no child Expressions, but may carry incoming CITES edges
STUB_WORKS_QUERY = """
MATCH (stub:Work)
WHERE NOT (stub)<-[:PART_OF]-(:Expression)
OPTIONAL MATCH (:Expression)-[c:CITES]->(stub)
RETURN stub.id AS id, stub.urn AS urn, stub.title AS title, count(c) AS citations
"""

MIGRATE_QUERY = """
MATCH (stub:Work {id: $stub_id})
WHERE stub.relinked_to IS NULL
MATCH (real:Work {id: $real_id})
MATCH (src:Expression)-[rel:CITES]->(stub)
MERGE (src)-[new:CITES]->(real)
SET new.text = coalesce(new.text, rel.text),
    new.extracted_by = coalesce(new.extracted_by, rel.extracted_by)
DELETE rel
RETURN count(*) AS migrated
"""

# Marker is applied separately: it must also run for stubs whose edges were already migrated
MARK_QUERY = """
MATCH (stub:Work {id: $stub_id})
SET stub.relinked_to = $real_urn,
    stub.relinked_at = coalesce(stub.relinked_at, datetime())
RETURN stub.id AS id
"""

STUB_EDGE_COUNT = """
MATCH (:Expression)-[c:CITES]->(:Work {id: $stub_id}) RETURN count(c) AS c
"""


def urn_key(urn: str):
    """Derive the (type segments, year, number) key of a NIR URN, ignoring the publication date.

    Stub URNs carry the year only (`urn:nir:stato:legge:1988;400`) while ingested Works carry
    the full date (`urn:nir:stato:legge:2024-03-04;40`): comparing the segments before the date
    plus year and number identifies the same act without false positives on the number alone.
    """
    if not urn:
        return None
    segments = urn.split(":")
    if len(segments) < 2:
        return None
    tail = segments[-1].split(";")
    if len(tail) != 2:
        return None
    date_or_year, number = tail
    year = date_or_year[:4]
    prefix = tuple(segments[:-1])
    return prefix, year, number


async def collect_candidates(driver) -> list:
    """Return the relinkable (stub -> real Work) pairs with their citation counts."""
    async with driver.session() as session:
        real_result = await session.run(REAL_WORKS_QUERY)
        reals = [dict(r) async for r in real_result]
        stub_result = await session.run(STUB_WORKS_QUERY)
        stubs = [dict(r) async for r in stub_result]

    lookup = {}
    for real in reals:
        key = urn_key(real["urn"])
        if key:
            lookup.setdefault(key, []).append(real)

    pairs = []
    for stub in stubs:
        key = urn_key(stub["urn"])
        matches = lookup.get(key) if key else None
        if not matches:
            continue
        # Deterministic pick when several Works share the same act identifiers
        target = sorted(matches, key=lambda r: r["id"])[0]
        pairs.append({
            "stub_id": stub["id"],
            "stub_urn": stub["urn"],
            "stub_title": stub["title"],
            "real_id": target["id"],
            "real_urn": target["urn"],
            "citations": stub["citations"],
            "ambiguous": len(matches) > 1,
        })
    pairs.sort(key=lambda p: p["citations"], reverse=True)
    return pairs


async def migrate_pair(driver, pair) -> int:
    """Redirect every incoming CITES edge of one stub to the real Work; return edges moved."""
    async with driver.session() as session:
        result = await session.run(
            MIGRATE_QUERY, stub_id=pair["stub_id"], real_id=pair["real_id"]
        )
        record = await result.single()
        moved = record["migrated"] if record else 0
        # Mark the stub (separately, so already-migrated stubs stay traceable too)
        await session.run(MARK_QUERY, stub_id=pair["stub_id"], real_urn=pair["real_urn"])
        return moved


async def main():
    """Relink CITES edges from external-reference stubs to the real Works (dry-run by default)."""
    parser = argparse.ArgumentParser(description="Relink CITES edges from stubs to real Works")
    parser.add_argument("--apply", action="store_true", help="Esegue la migrazione (default: dry-run)")
    parser.add_argument("--clean-markers", action="store_true", help="Rimuove i marker relinked_* da tutti i Work e termina")
    parser.add_argument("--limit", type=int, default=None, help="Numero massimo di stub da migrare")
    parser.add_argument("--report", default="relink_citations_report.json", help="Report JSON dei candidati")
    args = parser.parse_args()

    async with AsyncGraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    ) as driver:
        if args.clean_markers:
            async with driver.session() as session:
                await session.run("MATCH (w:Work) REMOVE w.relinked_to, w.relinked_at")
            logger.info("Marker relinked_* rimossi da tutti i Work.")
            return 0

        # Verify how many citation edges are currently walkable (stub with children) for context
        async with driver.session() as session:
            walkable = await session.run(
                "MATCH (:Expression)-[:CITES]->(w:Work)<-[:PART_OF]-(:Expression) RETURN count(DISTINCT w) AS c"
            )
            logger.info(
                f"Work citati con articoli (percorribili OGGI): {(await walkable.single())['c']}"
            )

        pairs = await collect_candidates(driver)
        total_edges = sum(p["citations"] for p in pairs)
        distinct_stubs = len({p["stub_id"] for p in pairs})
        logger.info(
            f"Candidati relink: {len(pairs)} coppie | {distinct_stubs} stub distinti | "
            f"{total_edges} archi CITES da redirigere"
        )

        ambiguous = [p for p in pairs if p["ambiguous"]]
        if ambiguous:
            logger.warning(
                f"{len(ambiguous)} stub corrispondono a più Work reali: scelto quello con id minore."
            )

        # Deduplicate: one migration per stub, targeting its best-matching real Work
        chosen = {}
        for p in pairs:
            chosen.setdefault(p["stub_id"], p)
        chosen_pairs = sorted(chosen.values(), key=lambda p: p["citations"], reverse=True)
        if args.limit:
            chosen_pairs = chosen_pairs[: args.limit]

        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "dry_run": not args.apply,
                    "pairs": chosen_pairs,
                    "total_pairs": len(chosen_pairs),
                    "total_edges": sum(p["citations"] for p in chosen_pairs),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info(f"Report scritto in {args.report}")

        for p in chosen_pairs[:10]:
            logger.info(
                f"  {p['stub_urn']}  ->  {p['real_urn']}  ({p['citations']} citazioni) | {str(p['stub_title'])[:44]}"
            )

        if not args.apply:
            logger.info("DRY-RUN: nessuna modifica applicata. Usa --apply per eseguire la migrazione.")
            return 0

        moved_total = 0
        for idx, p in enumerate(chosen_pairs, 1):
            moved = await migrate_pair(driver, p)
            moved_total += moved
            if idx % 25 == 0 or idx == len(chosen_pairs):
                logger.info(f"[{idx}/{len(chosen_pairs)}] archi migrati finora: {moved_total}")

        logger.info(f"Migrazione completata: {moved_total} archi CITES redirigiti su Work reali.")
        async with driver.session() as session:
            walkable = await session.run(
                "MATCH (:Expression)-[:CITES]->(w:Work)<-[:PART_OF]-(:Expression) RETURN count(DISTINCT w) AS c"
            )
            logger.info(f"Work citati con articoli (percorribili DOPO): {(await walkable.single())['c']}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
