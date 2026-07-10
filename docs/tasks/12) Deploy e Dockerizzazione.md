# Fase 12: Deploy Finale e Dockerizzazione

Il confezionamento dell'intero progetto garantisce che l'ecosistema sviluppato sia interamente riproducibile. Questo è un requisito chiave per una tesi informatica di alto livello.

---

## Obiettivi
1. Unificazione dell'architettura in un singolo ambiente eseguibile (Docker Compose).
2. Automazione del setup per chiunque voglia scaricare e valutare il progetto.

## Dettagli Implementativi
- **`docker-compose.yml` Architetturale**: Deve orchestrare l'accensione simultanea di:
  - Neo4j (Knowledge Graph).
  - Weaviate (Vector Database).
  - Ollama (LLM locali, con uno script entrypoint che faccia automaticamente il pull di modelli come `qwen3-embedding` se non presenti).
  - Streamlit Web App.
- **Refactoring Variabili d'Ambiente**: Assicurarsi che tutti i path (es. URI di Weaviate, IP di Ollama) leggano dal file `.env` senza alcun hardcoding (`localhost`), per permettere il networking corretto tra i container Docker.
