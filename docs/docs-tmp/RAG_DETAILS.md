# 🧠 Documentazione Tecnica Avanzata: Retrieval Engine (RAG)

Il modulo di RAG (Retrieval-Augmented Generation) in **Legal GraphRAG** non è un banale wrapper per query vettoriali. Rappresenta una vera e propria macchina di ragionamento su base grafica costruita con **LangGraph**, capace di implementare logiche cicliche di espansione e combinazione di punteggi non lineari tramite ricerca Ibrida a tre stadi.

---

## 🏗️ Gestione dello Stato: `RagState`

LangGraph orchestra l'esecuzione passando un dizionario tipizzato (lo stato) di nodo in nodo. Questa immutabilità funzionale è fondamentale per evitare conflitti o effetti collaterali durante i rami di esecuzione parallela.

```python
class RagState(TypedDict):
    query: str                       # Domanda dell'utente ("Qual è la pena per il furto?")
    reference_date: str              # Per il retrieval temporale ("2023-01-01")
    top_k: int                       # Cutoff per singolo canale (es. 10)
    final_k: int                     # Cutoff post-fusione RRF (es. 5)
    
    analyzed_query: AnalyzedQuery    # Risultato del QueryAnalyzer (Thesaurus matches)
    query_embedding: list[float]     # Vettore in virgola mobile prodotto da Ollama (Qwen)
    
    # Risultati Parziali (Popolati da retrieve_all in async.gather)
    vector_results: list[RetrievedChunk]
    bm25_results: list[RetrievedChunk]
    graph_results: list[RetrievedChunk]
    
    # Variabili di mutazione e ciclo
    fused_chunks: list[RetrievedChunk] # Il risultato unificato (RRF + Cutoff)
    hop_count: int                   # Contatore per bloccare ricorsioni infinite
    final_chunks: list[RetrievedChunk] # Chunk inviati all'LLM (dopo multi-hop e abrogation)
```

---

## 🔍 Il cuore: L'Analisi della Query (QueryAnalyzer)

Prima di interrogare il DB, la stringa dell'utente viene pre-processata nel nodo iniziale:
1. **Rilevamento Lessicale**: Cerca match con il Thesaurus TESEO (es. "Sostenibilità").
2. **Neo4j Ontology Expansion**: Sfrutta gli archi Semantici `[:BROADER]` o `[:NARROWER]` nel database Neo4j. Se la query parla di "Veicoli", l'algoritmo identifica concetti narrower come "Auto" e "Motocicli", arricchendo i metadati della query passati alla fase successiva, prevenendo l'isolamento causato da termini iperonimi.
3. **Calcolo Vettori**: Converte la query arricchita via `VectorEngine` per le fasi vettoriali successive.

---

## 🔀 Retrieval Ibrido Parallelo (Fan-Out Asincrono)

Il nodo `retrieve_all` esegue `asyncio.gather()` su tre funzioni separate per intercettare il database su piani dimensionali diversi in modo performante.

### 1. Vector Search (Neo4j Vector Index)
Implementazione di ricerca K-Nearest Neighbor (k-NN) basata su Similarità Coseno. Ideale per le ambiguità sintattiche.
```cypher
CALL db.index.vector.queryNodes('expression_embedding_vector', $top_k, $embedding)
YIELD node, score
WHERE node.type = 'EXPRESSION'
RETURN node.text_display AS text, node.id AS id, node.work_urn AS work_urn, 
       node.vigenza_start AS vig_start, node.vigenza_end AS vig_end, score
```

### 2. BM25 Search (Neo4j Full-Text Index)
Ricerca Testuale classica basata su TF-IDF (Term Frequency-Inverse Document Frequency). Critica nel dominio legale per intercettare date precise, acronimi (es. "DDL 123"), o numeri esatti di Articoli.
```cypher
CALL db.index.fulltext.queryNodes('expression_fulltext', $query)
YIELD node, score
RETURN ...
```

### 3. Graph Semantic Search (TESEO Topic Traversal)
Identifica i nodi `EXPRESSION` che sono collegati (`[:HAS_TOPIC]`) ai concetti TESEO estratti durante il `QueryAnalyzer`. 
```cypher
MATCH (concept:TESEO_Concept)<-[rel:HAS_TOPIC]-(node:GraphNode)
WHERE concept.id IN $teseo_concept_ids
RETURN node.text_display, SUM(rel.score) AS total_score
ORDER BY total_score DESC LIMIT $top_k
```

---

## 🧪 Fusione Matematica: Weighted Reciprocal Rank Fusion (RRF)

Poiché il BM25 usa scale logaritmiche, la Cosine Similarity usa [0,1], e la Graph Search usa somme algebriche, i punteggi originali **non sono sommabili**. 

Il `fusion.py` normalizza i tre canali tramite il **Reciprocal Rank Fusion**. Il RRF assegna punteggi basati sulla posizione (Rank) nel risultato originale. Un risultato in posizione 1 prende più punti di un risultato in posizione 2. A questo sommiamo un sistema di pesi configurabile (Weighted RRF).

$$RRF\_Score(d) = \frac{W_{vector}}{k + Rank(d_{vector})} + \frac{W_{bm25}}{k + Rank(d_{bm25})} + \frac{W_{graph}}{k + Rank(d_{graph})}$$

**Perché K=60?** Nella letteratura dell'Information Retrieval, la costante K=60 mitiga il peso dei primissimi risultati, premiando maggiormente i documenti che riescono a comparire in **molteplici** canali di ricerca, il che è sinonimo di estrema affidabilità per il dominio normativo.

---

## ⏳ Temporal RAG: Gestione delle Abrogazioni

La vigenza temporale è il punto più critico del diritto. Il filtro opera in modo "Soft" al nodo di fusione:
1. Valuta il parametro `reference_date` della query (se nullo, usa la data odierna).
2. Confronta con le date di vigenza del nodo testuale in formato ISO `YYYY-MM-DD`.
3. **Abrogation Marking**: Se `vigenza_end < reference_date`, la norma non viene scartata (poiché potrebbe servire come informazione storica), ma il suo testo (`text_display`) subisce una mutazione:
   ```markdown
   [ATTENZIONE: NORMA ABROGATA]
   Originale: Il furto è punito con anni 2...
   ```
Questo trucco di "Prompt Injection" forzata sposta l'onere della contestualizzazione all'LLM.

---

## 🔗 RAG Espansivo (Citations Multi-Hop)

L'RAG cicla finché la condizione `should_expand` è vera.

Se il chunk RRF vincitore è un Articolo che cita una definizione (Es: "Secondo quanto definito dalla Legge 431/98..."), l'LLM non avrà abbastanza dati. Il nodo `expand_citations` esegue questa query:

```cypher
MATCH (source {id: $source_id})-[:CITES]->(target:GraphNode)
RETURN target
```
I nuovi chunk testuali di `target` vengono aggiunti alla lista di `final_chunks`, fornendo all'LLM un "dossier" normativo autocompilato. Il contatore `hop_count` previene cicli infiniti bloccando il grafo al valore di configurazione `MAX_CITATION_HOPS`.

---

## 🧩 Work-level Merging e Prompt Template

Se il sistema restituisce Comma 1, Comma 2, e Comma 4 dell'Articolo X, passare 3 chunk distinti all'LLM spreca Token di contesto per colpa della ripetizione dei metadati. 

Il codice implementa una funzione `_merge_chunks()` in `fusion.py` che raggruppa tutti i chunk basati sulla stessa URN sorgente, concatenandoli con una visualizzazione strutturata:

```markdown
--- TITOLO I > Art. 1 > Comma 1 ---
Testo 1

--- TITOLO I > Art. 1 > Comma 2 ---
Testo 2
```
Ciò fornisce all'LLM una visuale macroscopica e contigua della Legge, abbassando la probabilità di "Hallucination".
