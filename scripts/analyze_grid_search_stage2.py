"""Summarize Stage 2 grid search results (marginal effects and winning configuration).

Reads the Stage 2 CSV produced by scripts/grid_search_stage2.py and prints:
  - number of evaluated configurations and their global score range
  - the winning configuration
  - marginal effects of each Stage 2 dimension (num_ctx, stuff threshold, topological chars)
  - marginal effect of the Stage 1 rank carried into Stage 2

Usage:
    py scripts/analyze_grid_search_stage2.py
    py scripts/analyze_grid_search_stage2.py --csv grid_search_stage2_results.csv
"""
import argparse
import csv
from collections import defaultdict


def load_rows(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def marginal(rows, key, metric="global_score"):
    groups = defaultdict(list)
    for r in rows:
        groups[r[key]].append(float(r[metric]))
    return {k: sum(v) / len(v) for k, v in sorted(groups.items(), key=lambda x: float(x[0]))}


def main():
    parser = argparse.ArgumentParser(description="Analisi risultati Stadio 2")
    parser.add_argument("--csv", default="grid_search_stage2_results.csv")
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    rows = load_rows(args.csv)
    scores = [float(r["global_score"]) for r in rows]
    print("=" * 78)
    print(f"CONFIGURAZIONI VALUTATE: {len(rows)} | score {min(scores):.4f} - {max(scores):.4f}")
    print("=" * 78)

    best = rows[0]
    print("\nCONFIGURAZIONE VINCENTE (miglior global score)")
    for k, v in best.items():
        print(f"  {k:<28} {v}")

    print(f"\nTOP {args.top}")
    print(f"{'#':<3}{'S1':<4}{'ctx':<7}{'stuff':<8}{'topo':<7}{'topk':<6}{'vmin':<6}{'recall':<9}{'sim':<9}{'gen_s':<7}{'score':<8}")
    for i, r in enumerate(rows[: args.top], 1):
        print(
            f"{i:<3}{r['stage1_rank']:<4}{r['generator_num_ctx']:<7}{r['generator_stuff_threshold']:<8}"
            f"{r['topological_max_chars']:<7}{r.get('rag_top_k', '-'):<6}{r.get('vector_min_score', '-'):<6}"
            f"{float(r['avg_recall']):<9.4f}{float(r['avg_similarity']):<9.4f}"
            f"{float(r['avg_gen_time_s']):<7.2f}{float(r['global_score']):<8.4f}"
        )

    print("\nEFFETTI MARGINALI (medie)")
    for key, metric in [
        ("generator_num_ctx", "global_score"),
        ("generator_stuff_threshold", "global_score"),
        ("topological_max_chars", "global_score"),
        ("final_k", "global_score"),
        ("stage1_rank", "global_score"),
        ("rag_top_k", "global_score"),
        ("vector_min_score", "global_score"),
        ("generator_num_ctx", "avg_similarity"),
        ("generator_stuff_threshold", "avg_similarity"),
        ("topological_max_chars", "avg_similarity"),
        ("generator_num_ctx", "avg_gen_time_s"),
    ]:
        if key not in rows[0]:
            continue
        vals = marginal(rows, key, metric)
        pretty = " | ".join(f"{k}: {v:.4f}" for k, v in vals.items())
        print(f"  {metric:<16} per {key:<26} {pretty}")

    print("\nMIGLIORI PER DIMENSIONE")
    for key in [
        "generator_num_ctx", "generator_stuff_threshold", "topological_max_chars",
        "stage1_rank", "rag_top_k", "vector_min_score",
    ]:
        if key not in rows[0]:
            continue
        vals = marginal(rows, key, "global_score")
        winner = max(vals.items(), key=lambda x: x[1])
        print(f"  {key:<26} -> {winner[0]} (score medio {winner[1]:.4f})")


if __name__ == "__main__":
    main()
