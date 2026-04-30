"""
Modelli di dato per il Retrieval Engine.

Definisce lo State di LangGraph (RagState) e i DTO di output (RetrievedChunk, AnalyzedQuery).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TypedDict, Any


# ---------------------------------------------------------------------------
# DTO di output
# ---------------------------------------------------------------------------

@dataclass
class RetrievedChunk:
    """Singolo frammento recuperato dal knowledge graph."""

    text: str                            # Testo dell'Expression
    expression_id: str                   # ID del nodo Expression in Neo4j
    work_urn: str | None = None          # URN del Work padre
    structural_context: str = ""         # es. "articolo - Art. 5"
    score: float = 0.0                   # Score post-fusione RRF
    source: str = ""                     # "vector" | "bm25" | "graph" | "citation_hop"
    vigenza_start: date | None = None
    vigenza_end: date | None = None
    metadata: dict[str, Any] = field(default_factory=dict)  # Metadati aggiuntivi (es. motivi retrieval)


@dataclass
class AnalyzedQuery:
    """Risultato dell'analisi della query utente."""

    original_query: str                                       # Testo originale
    teseo_concept_ids: list[str] = field(default_factory=list) # TESEO concept IDs trovati
    expanded_labels: list[str] = field(default_factory=list)   # Label narrower/broader espanse
    expanded_query_text: str = ""                              # Query arricchita per BM25


# ---------------------------------------------------------------------------
# LangGraph State
# ---------------------------------------------------------------------------

class RagState(TypedDict):
    """
    Stato condiviso tra tutti i nodi del grafo LangGraph.

    Ogni nodo legge i campi di cui ha bisogno e scrive i propri output.
    I campi sono separati per canale di retrieval per evitare conflitti
    durante il fan-out parallelo.
    """

    # --- Input ---
    query: str
    reference_date: str | None               # ISO format (YYYY-MM-DD), opzionale
    top_k: int                               # Numero di risultati estratti per canale
    final_k: int                             # Numero di risultati finali dopo RRF e filtering

    # --- Step 1: Query Analysis ---
    analyzed_query: AnalyzedQuery | None
    query_embedding: list[float] | None

    # --- Step 2: Retrieval (un campo per canale) ---
    vector_results: list[RetrievedChunk]
    bm25_results: list[RetrievedChunk]
    graph_results: list[RetrievedChunk]

    # --- Step 3: Fusion ---
    fused_chunks: list[RetrievedChunk]

    # --- Step 4: Multi-hop ---
    hop_count: int
    final_chunks: list[RetrievedChunk]

    # --- Dipendenze iniettate (private) ---
    _driver: Any      # AsyncDriver (Neo4j)
    _analyzer: Any    # QueryAnalyzer
