"""Build the final results table (CSV + Markdown) from the per-query ablation outputs.

One row per (question class, configuration), with every computed value:
recall@10, MRR, Hit@1, chunks in context, context characters, citation hops, answer similarity.

Usage:
    py scripts/analyze_final_results.py
"""
import argparse
import statistics
from pathlib import Path

import pandas as pd

# (class label, baseline per-query CSV, GraphRAG per-query CSV, baseline config prefix, graph config prefix)
SETS = [
    ("A. Testo sufficiente (15)", "ablation_base_main_per_query.csv", "ablation_prod_main_per_query.csv", None, None),
    ("B. Rinvio nominato (12)", "ablation_base_ref_per_query.csv", "ablation_final_ref_per_query.csv", None, None),
    ("C. Multi-hop (20)", "ablation_base_mh_per_query.csv", "ablation_final_mh_per_query.csv", None, None),
]

# Optional extra rows appended when the files exist (class label, baseline CSV, graph CSV, prefixes)
OPTIONAL_SETS = [
    ("C2. Multi-hop rigenerato (tutte)", "ablation_base_mh3_per_query.csv", "ablation_graph_mh3_per_query.csv", None, None),
    ("C3. Multi-hop rigenerato (rinvio risolvibile)", "ablation_base_mh3_subset_per_query.csv", "ablation_graph_mh3_subset_per_query.csv", None, None),
]

CONFIGS = [("Baseline (denso+BM25)", 0), ("GraphRAG (config unica)", 1)]


def CONFIGS_WITH_PREFIX(base_prefix, graph_prefix):
    """Config rows to read from each CSV, with the config prefix to select inside it."""
    return [
        ("Baseline (denso+BM25)", 0, base_prefix),
        ("GraphRAG (config unica)", 1, graph_prefix),
    ]


def _row(label_class: str, label_config: str, df: pd.DataFrame) -> dict:
    sims = [s for s in df["similarity"].tolist() if isinstance(s, (int, float))]
    return {
        "classe": label_class,
        "configurazione": label_config,
        "domande": len(df),
        "recall@10": round(df["recall"].mean(), 4),
        "MRR": round(df["mrr"].mean(), 4),
        "Hit@1": round(df["hit_at_1"].mean(), 4),
        "chunk_medi": round(df["n_chunks"].mean(), 2),
        "caratteri_contesto": round(df["context_chars"].mean()),
        "salti_citazione": round(df["hops"].mean(), 2),
        "similarita": round(statistics.mean(sims), 4) if sims else "",
    }


def main():
    parser = argparse.ArgumentParser(description="Tabella finale dei risultati")
    parser.add_argument("--output", default="final_results_table.csv")
    parser.add_argument("--markdown", default="final_results_table.md",
                        help="tabella Markdown (NON sovrascrive FINAL_RESULTS.md, che è il documento)")
    args = parser.parse_args()

    rows = []
    for entry in SETS + OPTIONAL_SETS:
        label_class, base_csv, graph_csv = entry[0], entry[1], entry[2]
        base_prefix = entry[3] if len(entry) > 3 else None
        graph_prefix = entry[4] if len(entry) > 4 else None
        for label_config, index, prefix in CONFIGS_WITH_PREFIX(base_prefix, graph_prefix):
            path = Path(base_csv if index == 0 else graph_csv)
            if not path.exists():
                print(f"MANCA| {path} (salto {label_class} / {label_config})")
                continue
            df = pd.read_csv(path)
            if prefix:
                df = df[df["config"].astype(str).str.startswith(prefix)]
            rows.append(_row(label_class, label_config, df))

    table = pd.DataFrame(rows)
    table.to_csv(args.output, index=False, encoding="utf-8")
    print(f"OK| scritto {args.output} ({len(table)} righe)")

    # Markdown
    lines = [
        "| Classe di domande | Configurazione | Domande | Recall@10 | MRR | Hit@1 | Chunk | Car. contesto | Salti | Similarità |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for _, r in table.iterrows():
        sim = f"{r['similarita']:.4f}" if isinstance(r["similarita"], float) else "—"
        lines.append(
            f"| {r['classe']} | {r['configurazione']} | {r['domande']} | {r['recall@10']:.2%} | "
            f"{r['MRR']:.4f} | {r['Hit@1']:.2%} | {r['chunk_medi']} | {r['caratteri_contesto']} | "
            f"{r['salti_citazione']} | {sim} |"
        )
    markdown = "\n".join(lines) + "\n"
    Path(args.markdown).write_text(markdown, encoding="utf-8")
    print(f"OK| scritto {args.markdown}")
    print(markdown)


if __name__ == "__main__":
    main()
