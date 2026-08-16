from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    Uses pydantic-settings for validation and automatic .env loading.
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Neo4j Database
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USERNAME: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    # LLM & Embedding
    QWEN3_ENDPOINT: str = "http://localhost:11434"
    EMBEDDING_MODEL_NAME: str = "qwen3-embedding:0.6b"
    EMBEDDING_DIMENSIONS: int = 1024  # Aggiornato per il nuovo modello
    GENERATIVE_MODEL_NAME: str = "qwen3.5:4b"

    # RAG Retrieval
    TESEO_RDF_PATH: str = "data/external/teseo_full.ttl"
    RAG_TOP_K: int = 15
    RRF_WEIGHT_VECTOR: float = 1.0
    RRF_WEIGHT_BM25: float = 1.0
    RRF_WEIGHT_GRAPH: float = 1.2
    RRF_K: int = 60
    MAX_CITATION_HOPS: int = 1
    RAG_MIN_SCORE: float = 0.01
    RAG_VECTOR_MIN_SCORE: float = 0.3
    MAX_AGENTIC_ITERATIONS: int = 5

    # Reranker
    RERANKER_MODEL_NAME: str = "Qwen/Qwen3-Reranker-0.6B"
    RERANK_TOP_K: int = 20  # Quanti chunk passare al reranker dopo la fusion
    RERANK_MIN_SCORE: float = 0.05 # Soglia minima di pertinenza (post-sigmoid)

    # TESEO Matching
    TESEO_DENSE_THRESHOLD: float = 0.45        # Soglia similarità coseno per Full Semantic Matching
    TESEO_SPARSE_BOOST: float = 1.0            # Score assegnato ai match esatti Aho-Corasick
    TESEO_MAX_CONCEPTS: int = 10               # Numero massimo di concetti TESEO restituiti per chunk/query

    # Topological Expansion
    TOPOLOGICAL_MAX_CHARS: int = 6000           # Limite massimo caratteri per super-chunk espanso
    TOPOLOGICAL_EXPAND_NEXT: bool = True        # Espandi tramite archi :NEXT (±1 comma)
    TOPOLOGICAL_EXPAND_PARENT: bool = True      # Espandi tramite arco :PART_OF (articolo padre)
    TOPOLOGICAL_EXPAND_CITES: bool = True       # Espandi tramite archi :CITES/:MODIFIES

    # Hardware-ready context window (modificabile al cambio hardware)
    GENERATOR_NUM_CTX: int = 4096              # Context window del generatore
    GENERATOR_STUFF_THRESHOLD: int = 8000      # Caratteri max per lo stuffing. Oltre, usa Map-Reduce.
    SUPERVISOR_NUM_CTX: int = 16384            # Context window del supervisor



# Singleton instance for the application
settings = Settings()
