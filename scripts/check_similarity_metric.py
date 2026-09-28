"""Sanity check of the answer-similarity metric used by the grid search / ablations.

The metric averages, over the golden answer's sentences, the best cosine similarity with any
sentence of the generated answer (sentence-recall style). Before trusting deltas of ~0.01 it
must be shown that the metric *discriminates*: unrelated legal answers must score clearly lower
than the correct one.

Reported comparisons (per dataset):
  * golden_i vs golden_j (i != j)          -> unrelated legal answers
  * golden   vs fallback phrase            -> the model's "I have no information" reply
  * golden   vs off-topic sentence         -> unrelated everyday text
  * golden   vs its own answer (identity)  -> ceiling, must be ~1.0

Usage:
    py scripts/check_similarity_metric.py
"""
import asyncio
import json
import logging
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.logging_config import setup_logging
from src.parsing.vector_engine import VectorEngine
from scripts.grid_search_stage2 import compute_similarity

setup_logging()
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("check_similarity")

FALLBACK = "Non dispongo di informazioni sufficienti per rispondere a questa domanda."
OFF_TOPIC = "Il cielo è azzurro, i gatti dormono sul divano e il treno per Milano parte alle otto."


def describe(label: str, values: list[float]):
    if not values:
        print(f"{label:<44} nessun dato")
        return
    print(
        f"{label:<44} media {statistics.mean(values):.4f} | mediana {statistics.median(values):.4f} "
        f"| min {min(values):.4f} | max {max(values):.4f}"
    )


async def check(dataset_path: str, engine: VectorEngine, limit: int = 15):
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)[:limit]
    goldens = [d.get("golden_answer", "") for d in dataset]
    goldens = [g for g in goldens if g]
    if len(goldens) < 2:
        return

    print(f"\n=== {dataset_path} ({len(goldens)} risposte attese) ===")

    identity = [await compute_similarity(g, g, engine) for g in goldens]
    describe("stessa risposta (tetto, atteso ~1.0)", identity)

    cross = []
    for i, g in enumerate(goldens):
        for j, other in enumerate(goldens):
            if i == j:
                continue
            cross.append(await compute_similarity(g, other, engine))
    describe("risposte diverse dello stesso dominio", cross)

    fallback = [await compute_similarity(g, FALLBACK, engine) for g in goldens]
    describe("risposta di fallback ('non so')", fallback)

    off_topic = [await compute_similarity(g, OFF_TOPIC, engine) for g in goldens]
    describe("testo fuori tema (vita quotidiana)", off_topic)


async def main():
    engine = VectorEngine()
    await check("data/eval_dataset.json", engine)
    await check("data/eval_multihop_all.json", engine, limit=15)


if __name__ == "__main__":
    asyncio.run(main())
