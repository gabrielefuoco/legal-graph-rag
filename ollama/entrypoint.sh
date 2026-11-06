#!/bin/bash

# Avvia il server ollama in background
/bin/ollama serve &
# Registra il PID del server per poterlo aspettare alla fine
pid=$!

# Attendi che l'API di ollama sia pronta
echo "Attesa che Ollama si avvii..."
while ! ollama list > /dev/null 2>&1; do
    sleep 2
done
echo "Ollama è pronto!"

# Scarica i modelli necessari in background
echo "Eseguo il pull dei modelli necessari (potrebbe richiedere tempo)..."
ollama pull qwen3-embedding:0.6b
# Il modello generativo serve solo con esecuzione locale: con LLM_PROVIDER=openrouter
# la generazione passa dall'API e l'unico modello necessario è l'embedder.
if [ "${LLM_PROVIDER:-ollama}" = "ollama" ]; then
    ollama pull qwen3.5:4b
else
    echo "LLM_PROVIDER=${LLM_PROVIDER}: salto il pull di qwen3.5:4b."
fi

echo "Modelli scaricati e pronti!"

# Attendi il processo server per mantenere il container in vita
wait $pid
