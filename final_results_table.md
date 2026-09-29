| Classe di domande | Configurazione | Domande | Recall@10 | MRR | Hit@1 | Chunk | Car. contesto | Salti | Similarità |
|---|---|---|---|---|---|---|---|---|---|
| A. Testo sufficiente (15) | Baseline (denso+BM25) | 15 | 100.00% | 1.0000 | 100.00% | 2.0 | 11494 | 0.0 | 0.7658 |
| A. Testo sufficiente (15) | GraphRAG (config unica) | 15 | 100.00% | 1.0000 | 100.00% | 2.67 | 13988 | 1.0 | 0.7667 |
| B. Rinvio nominato (12) | Baseline (denso+BM25) | 12 | 0.00% | 0.0000 | 0.00% | 1.83 | 4534 | 0.0 | 0.2591 |
| B. Rinvio nominato (12) | GraphRAG (config unica) | 12 | 83.33% | 0.6111 | 41.67% | 3.83 | 7766 | 1.0 | 0.6611 |
| C. Multi-hop (20) | Baseline (denso+BM25) | 20 | 15.00% | 0.1000 | 5.00% | 1.9 | 38051 | 0.0 | 0.5115 |
| C. Multi-hop (20) | GraphRAG (config unica) | 20 | 30.00% | 0.2167 | 15.00% | 3.6 | 59762 | 1.0 | 0.5160 |
