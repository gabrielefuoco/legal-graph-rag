import pytest
from unittest.mock import patch
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from src.rag.llm_factory import get_llm
from src.config import settings

def test_get_llm_ollama_default():
    with patch.object(settings, "LLM_PROVIDER", "ollama"):
        llm = get_llm(num_ctx=2048, temperature=0.1)
        assert isinstance(llm, ChatOllama)
        assert llm.model == settings.GENERATIVE_MODEL_NAME
        assert llm.temperature == 0.1

def test_get_llm_openrouter():
    with patch.object(settings, "LLM_PROVIDER", "openrouter"), \
         patch.object(settings, "OPENROUTER_API_KEY", "test-key-123"), \
         patch.object(settings, "OPENROUTER_MODEL_NAME", "qwen/qwen3.5-9b"):
        llm = get_llm(temperature=0.2)
        assert isinstance(llm, ChatOpenAI)
        assert llm.model_name == "qwen/qwen3.5-9b"
        assert str(llm.openai_api_base).rstrip("/") == "https://openrouter.ai/api/v1"
        assert llm.openai_api_key.get_secret_value() == "test-key-123"

def test_get_llm_override_provider():
    llm = get_llm(provider="openrouter", model_name="custom/model", temperature=0.5)
    assert isinstance(llm, ChatOpenAI)
    assert llm.model_name == "custom/model"
    assert llm.temperature == 0.5
