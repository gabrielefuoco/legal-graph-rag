"""Per-query analysis of the multi-hop ablation: where exactly does the graph earn its keep?

Reads the per-query CSV produced by ablation_graph.py and prints
  * the summary per configuration,
  * the recovery matrix (queries found by the graph but missed by the baseline),
  * the list of questions the graph unlocks (the "dove serve" panel for the thesis).

Usage:
    py scripts/analyze_multihop_ablation.py --csv ablation_multihop_per_query.csv
"""
import argparse
import statistics
from collections import defaultdict

import pandas as pd


def main():
    """Compare the per-query metrics of the multi-hop ablation arms and write a summary."""
    parser = argparse.ArgumentParser(description="Analisi per-query dell'ablation multi-hop")
    parser.add_argument("--csv", default="ablation_multihop_per_query.csv")
    parser.add_argument("--summary-csv", default="ablation_multihop_results.csv")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    configs = list(dict.fromkeys(df["config"]))

    print("=== riepilogo per config ===")
    for cfg in configs:
        sub = df[df["config"] == cfg]
        sims = [s for s in sub["similarity"].tolist() if isinstance(s, (int, float))]
        print(
            f"{cfg:<50} recall {sub['recall'].mean():.2%} | MRR {sub['mrr'].mean():.4f} "
            f"| chunk {sub['n_chunks'].mean():.2f} | hops {sub['hops'].mean():.2f}"
            + (f" | sim {statistics.mean(sims):.4f}" if sims else "")
        )

    if len(configs) < 2:
        return

    base_cfg = configs[0]
    by_query = defaultdict(dict)
    for _, row in df.iterrows():
        by_query[row["query_index"]][row["config"]] = row

    print(f"\n=== matrice di recupero (righe = query, colonne = config; 1 = golden trovata) ===")
    header = "  #  " + " ".join(f"{c.split('.')[0]:>3}" for c in configs)
    print(header)
    recovered_by_graph = []
    lost_by_graph = []
    for q_idx in sorted(by_query):
        cells = []
        for cfg in configs:
            r = by_query[q_idx].get(cfg)
            cells.append("  1" if r is not None and r["recall"] > 0 else "  0")
        print(f"{q_idx:>3}  " + " ".join(cells))
        base_hit = by_query[q_idx].get(base_cfg) is not None and by_query[q_idx][base_cfg]["recall"] > 0
        best_others = [cfg for cfg in configs[1:] if by_query[q_idx].get(cfg) is not None and by_query[q_idx][cfg]["recall"] > 0]
        if not base_hit and best_others:
            recovered_by_graph.append((q_idx, by_query[q_idx][configs[0]]["query"], best_others))
        if base_hit and not best_others:
            lost_by_graph.append((q_idx, by_query[q_idx][configs[0]]["query"]))

    print(f"\n=== domande che il GRAFO sblocca ({len(recovered_by_graph)} su {len(by_query)}) ===")
    for q_idx, query, cfgs in recovered_by_graph:
        print(f"  [{q_idx}] {query[:110]}...  -> recuperata da: {', '.join(c.split('.')[0] for c in cfgs)}")

    if lost_by_graph:
        print(f"\n=== domande perse dal grafo ({len(lost_by_graph)}) ===")
        for q_idx, query in lost_by_graph:
            print(f"  [{q_idx}] {query[:110]}...")


if __name__ == "__main__":
    main()
