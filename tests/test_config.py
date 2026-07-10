import os
from src.config import Settings

def test_settings_load():
    # Mock environment variables
    os.environ["NEO4J_URI"] = "bolt://test:7687"
    os.environ["NEO4J_USERNAME"] = "testuser"
    os.environ["NEO4J_PASSWORD"] = "testpass"
    
    settings = Settings()
    
    assert settings.NEO4J_URI == "bolt://test:7687"
    assert settings.NEO4J_USERNAME == "testuser"
    assert settings.NEO4J_PASSWORD == "testpass"
    assert settings.EMBEDDING_DIMENSIONS == 1536 # Default
