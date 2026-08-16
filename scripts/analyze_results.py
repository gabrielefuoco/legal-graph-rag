import pandas as pd
import numpy as np

def analyze_results():
    try:
        df = pd.read_csv('ablation_results.csv')
    except FileNotFoundError:
        print("File ablation_results.csv non trovato!")
        return

    print("="*60)
    print("      RISULTATI ABLATION STUDY (BASELINE vs GRAPHRAG)")
    print("="*60)
    
    print("\n1. TEMPI DI ESECUZIONE (Secondi)")
    print(f"   - Baseline RAG: {df['baseline_time_s'].mean():.2f}s (Mediana: {df['baseline_time_s'].median():.2f}s)")
    print(f"   - GraphRAG:     {df['graphrag_time_s'].mean():.2f}s (Mediana: {df['graphrag_time_s'].median():.2f}s)")
    
    print("\n2. CONTESTO RECUPERATO (Caratteri forniti all'LLM)")
    print(f"   - Baseline RAG: {df['baseline_ctx_chars'].mean():.0f} chars")
    print(f"   - GraphRAG:     {df['graphrag_ctx_chars'].mean():.0f} chars")
    
    print("\n3. CHUNK RECUPERATI DOPO RERANKING")
    print(f"   - Baseline RAG: {df['baseline_chunks'].mean():.2f} chunk")
    print(f"   - GraphRAG:     {df['graphrag_chunks'].mean():.2f} chunk")

    print("\n4. METRICHE DI QUALITÀ (Target Recall)")
    b_rec = df['baseline_recall'].mean() * 100
    g_rec = df['graphrag_recall'].mean() * 100
    print(f"   - Baseline RAG: {b_rec:.2f}%")
    print(f"   - GraphRAG:     {g_rec:.2f}%")
    print(f"   Variazione:     {g_rec - b_rec:+.2f}%")

    print("\n5. METRICHE DI QUALITÀ (Semantic Similarity Score)")
    b_sim = df['baseline_sim_score'].mean()
    g_sim = df['graphrag_sim_score'].mean()
    print(f"   - Baseline RAG: {b_sim:.4f}")
    print(f"   - GraphRAG:     {g_sim:.4f}")
    
    # Calcolo variazione percentuale
    if b_sim > 0:
        var_perc = ((g_sim - b_sim) / b_sim) * 100
        print(f"   Miglioramento:  {var_perc:+.2f}%")
        
    print("\n" + "="*60 + "\n")
    
    # Creazione di una tabellina in Markdown per esportazione rapida
    md_content = f"""# Riassunto Metriche Finali
    
| Metrica | Baseline RAG | GraphRAG | Variazione |
|---------|-------------|----------|------------|
| **Tempo di Esecuzione Medio** | {df['baseline_time_s'].mean():.2f}s | {df['graphrag_time_s'].mean():.2f}s | {df['graphrag_time_s'].mean() - df['baseline_time_s'].mean():+.2f}s |
| **Dimensione Contesto Media** | {df['baseline_ctx_chars'].mean():.0f} char | {df['graphrag_ctx_chars'].mean():.0f} char | +{df['graphrag_ctx_chars'].mean() - df['baseline_ctx_chars'].mean():.0f} char |
| **Target Recall** | {b_rec:.1f}% | {g_rec:.1f}% | {g_rec - b_rec:+.1f}% |
| **Semantic Similarity (Qualità)** | **{b_sim:.4f}** | **{g_sim:.4f}** | **{var_perc:+.1f}%** |
"""
    
    with open("metrics_summary.md", "w", encoding="utf-8") as f:
        f.write(md_content)
        
    print("\n[+] Ho salvato la tabella finale nel file 'metrics_summary.md' presente nella root del progetto.")

if __name__ == "__main__":
    analyze_results()
