# Fase 11: Valutazione Quantitativa (Framework LLM-as-a-judge)

Per la solidità accademica della tesi, è fondamentale misurare oggettivamente le performance del GraphRAG rispetto al RAG tradizionale, passando da uno Studio di Ablazione "visivo" (Fase 10) a uno prettamente analitico e numerico.

---

## Obiettivi
1. Creazione di un dataset di valutazione (Ground Truth).
2. Valutazione tramite framework standard (es. RAGAS o script custom basati su LLM).
3. Estrazione delle metriche chiave per il capitolo dei Risultati della tesi.

## Dettagli Implementativi
- **Dataset**: Costruzione di circa 30-50 casi di test contenenti `[Domanda, Contesto Atteso, Risposta Attesa]` su argomenti legali ben coperti dal database ingerito.
- **Metriche da calcolare**:
  - *Context Precision / Context Recall*: Il grafo migliora la rilevanza dei documenti rispetto al solo approccio vettoriale?
  - *Faithfulness*: Le risposte generate dal `LegalGenerator` sono immuni da allucinazioni?
  - *Answer Relevance*: La risposta prodotta risponde esattamente alla domanda o si perde in digressioni?
- **Script di Automazione**: Un notebook o script Python che esegue automaticamente le query nel dataset, prima con `enable_graph_search=False` e poi `True`, generando un report finale (CSV o grafici) con i confronti.
