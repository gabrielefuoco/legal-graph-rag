"""Merge the multi-hop evaluation sets built by build_multihop_benchmark.py.

Each builder run samples different (citing act -> cited act) triples, so the eligible questions
are pooled here (deduplicated by question text) into one benchmark file.

Usage:
    py scripts/merge_multihop_sets.py --inputs data/eval_multihop.json data/eval_multihop_v2.json \
        --output data/eval_multihop_all.json
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.logging_config import setup_logging

setup_logging()
logger = logging.getLogger("merge_multihop")


def main():
    """Merge the input multi-hop sets into one deduplicated benchmark file."""
    parser = argparse.ArgumentParser(description="Unisce i set di valutazione multi-hop")
    parser.add_argument("--inputs", nargs="+", required=True)
    parser.add_argument("--output", default="data/eval_multihop_all.json")
    args = parser.parse_args()

    merged, seen = [], set()
    for path in args.inputs:
        if not Path(path).exists():
            logger.warning(f"File assente, salto: {path}")
            continue
        with open(path, "r", encoding="utf-8") as f:
            items = json.load(f)
        added = 0
        for item in items:
            key = " ".join(item["query"].split()).lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            added += 1
        logger.info(f"{path}: {added}/{len(items)} domande aggiunte (totale {len(merged)})")

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    logger.info(f"Set multi-hop unito: {len(merged)} domande -> {args.output}")


if __name__ == "__main__":
    main()
