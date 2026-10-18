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
ollama pull qwen3.5:4b

echo "Modelli scaricati e pronti!"

# Attendi il processo server per mantenere il container in vita
wait $pid
