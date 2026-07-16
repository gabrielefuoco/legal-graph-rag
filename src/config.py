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
    RERANK_MIN_SCORE: float = 0.05 # Soglia minima di pertinenza semantica



# Singleton instance for the application
settings = Settings()
