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
    EMBEDDING_MODEL_NAME: str = "nomic-embed-text"
    EMBEDDING_DIMENSIONS: int = 768

    # RAG Retrieval
    TESEO_RDF_PATH: str = "data/external/teseo_sample.rdf"
    RAG_TOP_K: int = 10
    RRF_WEIGHT_VECTOR: float = 0.8
    RRF_WEIGHT_BM25: float = 1.5
    RRF_WEIGHT_GRAPH: float = 1.0
    RRF_K: int = 60
    MAX_CITATION_HOPS: int = 1

# Singleton instance for the application
settings = Settings()
