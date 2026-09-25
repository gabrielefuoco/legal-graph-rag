"""One-command runner for the v2 retrieval tuning experiment (Stage 1 + Stage 2).

Chains two isolated subprocesses so each stage runs with clean settings:

  1. Stage 1 — extended grid: per-channel candidate depth (RAG_TOP_K), vector similarity
     floor (RAG_VECTOR_MIN_SCORE), fusion weights (now meaningful after the per-channel
     dedup fix), reranker cutoffs and query-side embedding instruction.
  2. Stage 2 — context/generation tuning on the winning Stage 1 configurations, with the
     chosen reranker backend (local CrossEncoder or OpenRouter listwise LLM).

Nothing runs on import: the experiment is launched explicitly.

Usage:
    py scripts/run_grid_search_v2.py --dry-run                 # quick smoke test, no LLM cost
    py scripts/run_grid_search_v2.py                           # full experiment (defaults)
    py scripts/run_grid_search_v2.py --queries 1 2 3 --top-n 2  # smaller scope
    py scripts/run_grid_search_v2.py --reranker-backend llm     # LLM listwise reranking
    py scripts/run_grid_search_v2.py --skip-stage2              # tuning retrieval only
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Rough wall-clock expectations used for the printed plan (measured on the previous run)
STAGE1_MINUTES = {"default": 12, "extended": 30}
STAGE2_MINUTES_PER_CONFIG = 0.7


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Runner dell'esperimento v2 di grid search")
    parser.add_argument("--dataset", default="data/eval_dataset.json", help="Dataset di valutazione")
    parser.add_argument("--tag", default="v2", help="Etichetta dell'esperimento usata nei nomi dei file")
    parser.add_argument("--grid", choices=["default", "extended"], default="extended", help="Griglia Stadio 1")
    parser.add_argument("--top-n", type=int, default=3, help="Configurazioni Stage-1 esportate allo Stadio 2")
    parser.add_argument("--queries", nargs="+", type=int, default=None, help="Indici query (1-indexed); default: tutte")
    parser.add_argument(
        "--embedding-instruction",
        choices=["on", "off"],
        default="on",
        help="Prefisso di istruzione sulle query di embedding (nessuna re-indicizzazione)",
    )
    parser.add_argument(
        "--reranker-backend",
        choices=["local", "llm"],
        default="local",
        help="Backend di reranking per lo Stadio 2 (lo Stadio 1 richiede sempre i logit locali)",
    )
    parser.add_argument("--stuff-thresholds", nargs="+", type=int, default=None, help="Soglie stuffing dello Stadio 2")
    parser.add_argument("--num-ctx-options", nargs="+", type=int, default=None, help="Valori GENERATOR_NUM_CTX")
    parser.add_argument("--topo-options", nargs="+", type=int, default=None, help="Valori TOPOLOGICAL_MAX_CHARS")
    parser.add_argument("--dry-run", action="store_true", help="Stadio 2 senza chiamate LLM (solo retrieval e similarità)")
    parser.add_argument("--skip-stage1", action="store_true", help="Riusa le configurazioni Stage-1 già prodotte")
    parser.add_argument("--skip-stage2", action="store_true", help="Esegui solo lo Stadio 1")
    parser.add_argument("--log-dir", default=".", help="Cartella dei log")
    return parser


def run_streamed(cmd, log_path: Path, env) -> int:
    """Run a command streaming output to the console and to a log file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n$ {' '.join(cmd)}\n", flush=True)
    with open(log_path, "a", encoding="utf-8", errors="replace") as log:
        log.write(f"\n$ {' '.join(cmd)}\n")
        log.flush()
        process = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        for line in process.stdout:
            print(line, end="", flush=True)
            log.write(line)
            log.flush()
        process.wait()
    return process.returncode


def main():
    args = build_parser().parse_args()
    python = sys.executable
    env = dict(os.environ)
    env["PYTHONPATH"] = f"{ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}"
    # Keep unicode log lines (emoji, accents) from breaking on Windows consoles
    env["PYTHONIOENCODING"] = "utf-8"

    stage1_csv = f"grid_search_stage1_{args.tag}_results.csv"
    stage1_configs = f"top_stage1_configs_{args.tag}.json"
    stage2_csv = f"grid_search_stage2_{args.tag}_results.csv"
    stage2_best = f"best_global_config_{args.tag}.json"
    candidates_cache = f"data/cache_candidates_{args.tag}.pkl"
    generations_cache = f"data/cache_stage2_generations_{args.tag}.json"
    retrievals_cache = f"data/cache_stage2_retrievals_{args.tag}.pkl"

    log_dir = Path(args.log_dir)
    stage1_log = log_dir / f"grid_search_stage1_{args.tag}.log"
    stage2_log = log_dir / f"grid_search_stage2_{args.tag}.log"

    queries = [str(q) for q in args.queries] if args.queries else []

    stage2_configs = None
    if not args.skip_stage2:
        if args.skip_stage1 or os.path.exists(stage1_configs):
            if os.path.exists(stage1_configs):
                with open(stage1_configs, "r", encoding="utf-8") as f:
                    top_configs = json.load(f)
                stage2_configs = len(top_configs) * len(args.stuff_thresholds or [16000, 32000, 64000]) * len(
                    args.num_ctx_options or [8192, 16384, 32768]
                ) * len(args.topo_options or [6000, 12000, 20000])

    print("=" * 78)
    print(f"ESPERIMENTO GRID SEARCH {args.tag.upper()} — Stadio 1{' + Stadio 2' if not args.skip_stage2 else ''}")
    print("=" * 78)
    print(f"  dataset                : {args.dataset}")
    print(f"  query                  : {len(queries) if queries else 'tutte'}")
    print(f"  griglia Stadio 1       : {args.grid}")
    print(f"  istruzione embedding   : {args.embedding_instruction}")
    print(f"  top-N verso Stadio 2   : {args.top_n}")
    print(f"  reranker (Stadio 2)    : {args.reranker_backend}")
    print(f"  stuff thresholds       : {args.stuff_thresholds or 'default (16000 32000 64000)'}")
    print(f"  num_ctx                : {args.num_ctx_options or 'default (8192 16384 32768)'}")
    print(f"  topo max chars         : {args.topo_options or 'default (6000 12000 20000)'}")
    print(f"  dry-run                : {args.dry_run}")
    print(f"  cache candidati        : {candidates_cache}")
    print(f"  cache generazioni      : {generations_cache}")
    if stage2_configs:
        print(f"  config Stadio 2        : {stage2_configs}")
    print("=" * 78)
    print(f"  Durata stimata         : Stadio 1 ~{STAGE1_MINUTES[args.grid]} min"
          + (f" | Stadio 2 ~{stage2_configs * STAGE2_MINUTES_PER_CONFIG:.0f} min" if stage2_configs else ""))
    print("=" * 78, flush=True)

    start = time.perf_counter()
    failed_stage = None

    # ---------------------------------------------------------------- Stage 1
    if not args.skip_stage1:
        env_stage1 = dict(env)
        env_stage1["RERANKER_BACKEND"] = "local"  # Stage 1 needs precomputable CrossEncoder logits
        cmd = [
            python, "scripts/grid_search_stage1.py",
            "--dataset", args.dataset,
            "--output", stage1_csv,
            "--top-configs-output", stage1_configs,
            "--cache-candidates", candidates_cache,
            "--grid", args.grid,
            "--embedding-instruction", args.embedding_instruction,
            "--top-n", str(args.top_n),
        ]
        if queries:
            cmd += ["--queries"] + queries
        print("\n" + "-" * 78)
        print("FASE 1/2 — STADIO 1 (retrieval, ranking e profondità dei canali)")
        print("-" * 78)
        if run_streamed(cmd, stage1_log, env_stage1) != 0:
            failed_stage = "Stadio 1"
    else:
        print("\n[SKIP] Stadio 1 saltato: uso le configurazioni già presenti.")

    # ---------------------------------------------------------------- Stage 2
    if not failed_stage and not args.skip_stage2:
        env_stage2 = dict(env)
        env_stage2["RERANKER_BACKEND"] = args.reranker_backend
        cmd = [
            python, "scripts/grid_search_stage2.py",
            "--stage1-configs", stage1_configs,
            "--dataset", args.dataset,
            "--output", stage2_csv,
            "--best-output", stage2_best,
            "--cache-generations", generations_cache,
            "--retrieval-cache", retrievals_cache,
        ]
        if queries:
            cmd += ["--queries"] + queries
        if args.stuff_thresholds:
            cmd += ["--stuff-thresholds"] + [str(v) for v in args.stuff_thresholds]
        if args.num_ctx_options:
            cmd += ["--num-ctx-options"] + [str(v) for v in args.num_ctx_options]
        if args.topo_options:
            cmd += ["--topo-options"] + [str(v) for v in args.topo_options]
        if args.dry_run:
            cmd.append("--dry-run")

        print("\n" + "-" * 78)
        print(f"FASE 2/2 — STADIO 2 (contesto, generazione, reranker={args.reranker_backend})")
        print("-" * 78)
        if run_streamed(cmd, stage2_log, env_stage2) != 0:
            failed_stage = "Stadio 2"

    elapsed = (time.perf_counter() - start) / 60

    print("\n" + "=" * 78)
    if failed_stage:
        print(f"[ERRORE] {failed_stage} fallito dopo {elapsed:.1f} minuti. Vedi il log per i dettagli.")
        print(f"  log Stadio 1: {stage1_log}")
        print(f"  log Stadio 2: {stage2_log}")
        print("  Le cache su disco permettono di riprendere: rilanciare lo stesso comando riusa il lavoro fatto.")
        print("=" * 78)
        return 1

    print(f"[COMPLETATO] Esperimento '{args.tag}' concluso in {elapsed:.1f} minuti.")
    if os.path.exists(stage2_best):
        with open(stage2_best, "r", encoding="utf-8") as f:
            best = json.load(f)
        print("\n  Configurazione vincente (Stadio 2):")
        for key in [
            "stage1_rank", "weight_vector", "weight_bm25", "weight_graph", "rrf_k",
            "rag_top_k", "vector_min_score", "rerank_top_k", "rerank_min_score", "final_k",
            "generator_num_ctx", "generator_stuff_threshold", "topological_max_chars",
            "avg_recall", "avg_similarity", "global_score",
        ]:
            if key in best:
                print(f"    {key:<28} {best[key]}")
    print("\n  File prodotti:")
    for path in [stage1_csv, stage1_configs, stage2_csv, stage2_best]:
        marker = "OK " if os.path.exists(path) else "MANCANTE "
        print(f"    [{marker}] {path}")
    print(f"\n  Analisi marginale: py scripts/analyze_grid_search_stage2.py --csv {stage2_csv}")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
