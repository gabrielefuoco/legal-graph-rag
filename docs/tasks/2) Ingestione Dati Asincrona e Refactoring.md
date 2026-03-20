# Fase 1: Ingestione Dati Asincrona e Refactoring

In questa fase, il focus principale è stato l'evoluzione della pipeline di ingestione da un approccio misto (sincrono/asincrono) a un'architettura **completamente asincrona e non bloccante**. Questo garantisce maggiore efficienza, scalabilità e una gestione unificata delle fonti dati.

## Obiettivi Raggiunti

### 1. Architettura Asincrona Completa
Tutti i client di ingestione sono stati implementati o rifattorizzati per utilizzare `aiohttp` e `asyncio`, eliminando le chiamate bloccanti `requests`.

- **Senato della Repubblica**: Implementato `AsyncSenatoScraper` per navigare e scaricare atti dal sito del Senato, con gestione robusta degli errori di parsing.
- **Camera dei Deputati**: Creato `AsyncCameraClient` per l'interazione con i portali Open Data della Camera.
- **EUR-Lex**: Sviluppato `AsyncEurLexClient` per interrogare l'endpoint SPARQL europeo, filtrando specificamente Regolamenti, Direttive e Decisioni dal 2024 in poi.
- **Corte Costituzionale**: Implementato `AsyncCorteCostClient` per il recupero delle sentenze.
- **Normattiva**: Rifattorizzato il precedente client sincrono in `AsyncNormattivaClient`, mantenendo la logica di polling ma sfruttando `await` per non bloccare il loop principale.
- **TESEO**: Convertito il downloader del thesaurus in `AsyncTeseoClient`.

### 2. Orchestrazione della Pipeline (`manage.py`)
Il file `manage.py` è stato riscritto per fungere da orchestratore asincrono.
- La funzione `run_pipeline()` esegue sequenzialmente (o potenzialmente in parallelo) i vari client all'interno di un unico event loop (`asyncio.run()`).
- Questo garantisce che un errore in una sorgente non blocchi necessariamente l'intero processo, pur mantenendo un flusso ordinato di log.

### 3. Pulizia del Codice Legacy
Sono stati rimossi gli script obsoleti e i file di debug temporanei:
- `normattiva_api.py` (Sostituito da versione async)
- `teseo_downloader.py` (Sostituito da versione async)
- Script di debug (`debug_sparql.py`, `get_predicates.py`, etc.)

## Dettagli Tecnici dei Client

| Fonte | Classe | Tecnologia | Note |
| :--- | :--- | :--- | :--- |
| **Senato** | `AsyncSenatoScraper` | `aiohttp`, `BeautifulSoup` | Scraper su pagine HTML per atti legislativi. |
| **EUR-Lex** | `AsyncEurLexClient` | `aiohttp` (SPARQL) | Query ottimizzata per Regolamenti/Direttive > 2024. |
| **Camera** | `AsyncCameraClient` | `aiohttp` | Client API Open Data. |
| **Corte Cost.** | `AsyncCorteCostClient` | `aiohttp` | Scraping/API endpoint specifico. |
| **Normattiva** | `AsyncNormattivaClient` | `aiohttp` | Polling asincrono su endpoint di ricerca/export. |
| **TESEO** | `AsyncTeseoClient` | `aiohttp` | Download RDF/XML da endpoint SPARQL. |

## Verifica e Utilizzo

Per avviare l'intera pipeline di ingestione:

```bash
python manage.py ingest
```

Il sistema produrrà log dettagliati per ogni fase, salvando i dati grezzi nelle rispettive sottocartelle di `data/`.

