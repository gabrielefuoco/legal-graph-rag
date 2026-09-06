"""Create a clean migration archive (.zip) of the project excluding caches, raw data, and logs."""

import os
import zipfile
from pathlib import Path
import time

def create_migration_archive():
    """Package project files into a migration zip archive with exclusions."""
    # Identify project root directory
    project_root = Path(__file__).parent.parent.resolve()
    
    # Store zip archive outside project root to avoid self-inclusion
    archive_name = project_root.parent / "Tesi_Migration.zip"
    
    # Directories to exclude
    EXCLUDES = {
        '.git', '.agent', '.gemini', 'venv', '.venv', 'env', 
        '__pycache__', '.pytest_cache', 'scratch', '.vscode', '.idea',
        'models_cache', 'raw'
    }
    
    # File extensions and specific filenames to exclude
    EXCLUDE_EXTENSIONS = {'.csv', '.log', '.ipynb'}
    EXCLUDE_FILES = {
        'scratch.py', 'metrics_summary.md', 'relazione_tecnica_rag.md',
        'tail_ollama.txt', 'tail_stream.txt'
    }
    
    print(f"Preparazione dell'archivio di migrazione...", flush=True)
    print(f"Sorgente: {project_root}", flush=True)
    print(f"Destinazione: {archive_name}", flush=True)
    print(f"Esclusioni dir: {', '.join(EXCLUDES)} + ollama/data", flush=True)
    print(f"Esclusioni estensioni: {', '.join(EXCLUDE_EXTENSIONS)}\n", flush=True)
    print("Inizio compressione (esclusi file raw e cache ollama, includendo entrypoint.sh e tutto il grafo)...", flush=True)
    
    start_time = time.time()
    total_files = 0
    
    # Open zip archive with standard compression
    with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
        for root, dirs, files in os.walk(project_root):
            # Filter directories in-place to avoid traversing excluded paths
            dirs[:] = [
                d for d in dirs 
                if d not in EXCLUDES and not (d == 'data' and 'ollama' in root.replace('\\', '/'))
            ]
            
            for file in files:
                # Skip archive itself and excluded files
                if file == archive_name.name or file in EXCLUDE_FILES:
                    continue
                if any(file.endswith(ext) for ext in EXCLUDE_EXTENSIONS):
                    continue
                
                file_path = Path(root) / file
                # Compute relative archive path
                arcname = "Tesi" / file_path.relative_to(project_root)
                
                zipf.write(file_path, arcname)
                total_files += 1
                
                if total_files % 100 == 0:
                    print(f"Aggiunti {total_files} file...", flush=True)

    elapsed = time.time() - start_time
    size_mb = archive_name.stat().st_size / (1024 * 1024)
    
    print(f"\n[OK] Compressione completata con successo in {elapsed:.1f} secondi!", flush=True)
    print(f"[FILE] File creato: {archive_name}", flush=True)
    print(f"[SIZE] Dimensione finale: {size_mb:.2f} MB", flush=True)
    print(f"[TOTAL] Totale file: {total_files}", flush=True)
    print("\nOra puoi trasferire il file 'Tesi_Migration.zip' sul nuovo PC, estrarlo e lanciare 'docker compose up -d'!", flush=True)

if __name__ == "__main__":
    create_migration_archive()
