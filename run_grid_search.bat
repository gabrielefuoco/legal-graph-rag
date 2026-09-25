@echo off
setlocal EnableDelayedExpansion
set "PYTHONPATH=%~dp0;%PYTHONPATH%"

title Legal GraphRAG - Grid Search Runner

echo ======================================================================
echo             LEGAL GRAPHRAG - PIPELINE DI GRID SEARCH
echo ======================================================================
echo.
echo [INFO] Configurazione Hardware e Modelli:
echo        - Generatore LLM: OpenRouter (Cloud API via qwen/qwen3.5-9b)
echo        - Embedding e CrossEncoder: Automaticamente su CPU (GPU NON necessaria)
echo.
echo ======================================================================
echo  Scegli quale operazione avviare:
echo ======================================================================
echo   [1] Esegui TUTTO (Stadio 1 Retrieval + Stadio 2 Generazione OpenRouter)
echo   [2] Solo Stadio 1 (Tuning Retrieval e Reranker - Zero chiamate LLM)
echo   [3] Solo Stadio 2 (Tuning Finestra Contesto e Generazione OpenRouter)
echo   [4] Quick Mini-Test (Test rapido su 3 query con Dry-Run senza costi API)
echo   [5] Esci
echo   [6] Esperimento v2 (profondita' retrieval + embedding instruction)
echo ======================================================================
echo.

set /p CHOICE="Inserisci il numero dell'opzione [1-5]: "

if "%CHOICE%"=="1" goto RUN_ALL
if "%CHOICE%"=="2" goto RUN_STAGE1
if "%CHOICE%"=="3" goto RUN_STAGE2
if "%CHOICE%"=="4" goto RUN_DRY
if "%CHOICE%"=="5" goto END
if "%CHOICE%"=="6" goto RUN_V2

echo [ERRORE] Scelta non valida.
goto END

:RUN_ALL
echo.
echo ======================================================================
echo [FASE 1/2] Avvio Stadio 1: Retrieval e Reranking Tuning...
echo ======================================================================
py manage.py grid-search-stage1 --dataset data/eval_dataset.json --cache-candidates data/cache_candidates.pkl
if errorlevel 1 (
    echo.
    echo [ERRORE] Lo Stadio 1 ha riscontrato un errore. Operazione interrotta.
    pause
    exit /b 1
)

echo.
echo ======================================================================
echo [FASE 2/2] Avvio Stadio 2: Contesto e Generazione con OpenRouter...
echo ======================================================================
py manage.py grid-search-stage2 --stage1-configs top_stage1_configs.json --dataset data/eval_dataset.json
if errorlevel 1 (
    echo.
    echo [ERRORE] Lo Stadio 2 ha riscontrato un errore.
    pause
    exit /b 1
)

echo.
echo ======================================================================
echo [SUCCESSO] Grid Search completata con successo!
echo Controlla i report generati:
echo   - grid_search_stage1_results.md
echo   - grid_search_stage2_results.md
echo   - best_global_config.json
echo ======================================================================
pause
goto END

:RUN_STAGE1
echo.
echo ======================================================================
echo Avvio Stadio 1: Retrieval e Reranking Tuning (Zero chiamate LLM)...
echo ======================================================================
py manage.py grid-search-stage1 --dataset data/eval_dataset.json --cache-candidates data/cache_candidates.pkl
if errorlevel 1 (
    echo.
    echo [ERRORE] Esecuzione fallita.
    pause
    exit /b 1
)
echo.
echo [SUCCESSO] Stadio 1 completato! Report generato in grid_search_stage1_results.md
pause
goto END

:RUN_STAGE2
echo.
echo ======================================================================
echo Avvio Stadio 2: Contesto e Generazione OpenRouter...
echo ======================================================================
py manage.py grid-search-stage2 --stage1-configs top_stage1_configs.json --dataset data/eval_dataset.json
if errorlevel 1 (
    echo.
    echo [ERRORE] Esecuzione fallita.
    pause
    exit /b 1
)
echo.
echo [SUCCESSO] Stadio 2 completato! Report generato in grid_search_stage2_results.md
pause
goto END

:RUN_DRY
echo.
echo ======================================================================
echo Avvio Mini-Test (3 query, Dry-Run senza costi token)...
echo ======================================================================
py scripts/grid_search_stage1.py --queries 1 2 3 --cache-candidates data/cache_mini.pkl
py scripts/grid_search_stage2.py --queries 1 2 3 --stage1-configs top_stage1_configs.json --dry-run
echo.
echo [SUCCESSO] Mini-Test di prova completato!
pause
goto END

:RUN_V2
echo.
echo ======================================================================
echo  Esperimento v2: griglia estesa (RAG_TOP_K, soglia vettoriale, pesi RRF)
echo  + istruzione query-side sugli embedding + Stadio 2 sul vincitore.
echo  Log in grid_search_stage1_v2.log e grid_search_stage2_v2.log
echo ======================================================================
py scripts/run_grid_search_v2.py
if errorlevel 1 (
    echo.
    echo [ERRORE] Esperimento v2 fallito. Vedi i log nella cartella del progetto.
    pause
    exit /b 1
)
echo.
echo [SUCCESSO] Esperimento v2 completato!
echo Controlla i file:
echo   - grid_search_stage1_v2_results.md
echo   - grid_search_stage2_v2_results.md
echo   - best_global_config_v2.json
pause
goto END

:END
exit /b 0