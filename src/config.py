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

    # LLM & Embedding (Qwen3)
    QWEN3_ENDPOINT: str = "http://localhost:11434"
    EMBEDDING_MODEL_NAME: str = "qwen3-embedding:0.6b"
    EMBEDDING_DIMENSIONS: int = 1024

# Singleton instance for the application
settings = Settings()
