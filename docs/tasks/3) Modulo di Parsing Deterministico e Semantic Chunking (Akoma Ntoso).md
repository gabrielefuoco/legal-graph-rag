# Fase 3: Modulo di Parsing Deterministico e Semantic Chunking (Akoma Ntoso)

In questa fase, l'obiettivo è stato lo sviluppo completo del modulo di parsing per la trasformazione dei documenti XML Akoma Ntoso in strutture dati a grafo (Nodes + Edges), pronte per l'ingestion in Neo4j e per la ricerca vettoriale.

## Attività Svolte

### 1. Architettura e Stack Tecnologico
- **Engine XML:** Utilizzo di `lxml` con modalità `recover=True` per gestire XML malformati o "sporchi" (comuni nei dati reali).
- **Validazione:** Adozione di **Pydantic V2** per definire un DTO rigoroso (`DocumentDTO`, `GraphNodeDTO`, `GraphEdgeDTO`).
- **Namespace Agnostic:** Implementazione di un gestore intelligente (`namespaces.py`) che rileva automaticamente se il documento usa lo standard Akoma Ntoso 3.0 (Normattiva) o la variante CSD03 (Senato).

### 2. Pipeline di Parsing
Il parser (`parser.py`) orchestra tre fasi sequenziali:

- **Fase A: Metadata (Header)**
  - Estrazione deterministica dell'URN NIR (es. `urn:nir:stato:legge:1990;241`).
  - Parsing delle date (Promulgazione, Pubblicazione) e gestione del versioning FRBR.

- **Fase B: Body & Context Injection**
  - Traversamento DFS ricorsivo dell'albero XML.
  - Distinzione tra **Nodi Strutturali** (Libro, Articolo) e **Nodi Atomici** (Comma, Lettera).
  - **Context Injection:** Implementazione della logica che "inietta" la gerarchia dei padri nel testo dei nodi foglia (es. `"Art. 1 > Comma 3: [testo]"`), cruciale per la qualità degli embedding.
	  - Le Rubriche sono trattate come proprietà del nodo padre e iniettate nel contesto, mantenendo il grafo pulito senza nodi "etichetta" superflui.

- **Fase C: Edges (Relazioni)**
  - Estrazione degli archi `:PART_OF` (gerarchia) e `:NEXT` (sequenza).
  - Analisi dei tag inline per creare archi semantici:
    - `:CITES`: da `<ref>` e `<rref>` (con normalizzazione URN e gestione range).
    - `:MODIFIES`: da `<mod>`, con estrazione del testo novellato (`<quotedText>`).

### 3. Gestione Anomalie (Robustness)
Il sistema è progettato per non fallire su dati imperfetti:
- **Fallback Encoding:** Tentativo automatico di rilettura in `latin-1` se `utf-8` fallisce.
- **ID Surrogati:** Generazione deterministica di `eId` (`gen_art1_para2`) quando l'XML ne è sprovvisto.
- **Sanitizzazione:** Normalizzazione automatica di whitespace e caratteri di controllo.

### 4. Parsing Tabelle
I tag `<table>`, `<tr>`, `<th>`, `<td>`, comuni negli allegati tecnici, vengono correttamente riconosciuti.
- **Decisione Architetturale:** Le tabelle sono trattate come **nodi EXPRESSION atomici** (testo linearizzato), non come sotto-grafi. Questa scelta preserva il contesto semantico durante il retrieval vettoriale.
- **Doppia Rappresentazione:**
  - `text_display`: formato Markdown (`| Col1 | Col2 |`), per la visualizzazione.
  - `text_vector`: formato semantico con Context Injection (es. `"Art. 5 > Tabella: Riga 1: Anno=2024, Importo=1.000"`), per gli embedding.
- **Metadati:** `metadata["is_table"] = True` per gestione differenziata nel retriever.
- **Edge Extraction:** I `<ref>` contenuti nelle celle generano archi `:CITES`.

### 5. Classificazione Avanzata delle Modifiche Normative
Il tag `<mod>` identifica una modifica normativa, ma non ne distingue il tipo. Questa informazione è cruciale per il grafo della vigenza.

- **Enum `ModificationType`:** Aggiunto in `models.py` con 4 valori: `SUBSTITUTION`, `INSERTION`, `REPEAL`, `AMENDMENT`.
- **Classificazione via Regex:** Pattern matching su keyword italiane:
  - "sostituito" → `SUBSTITUTION`
  - "inserito" / "aggiunto" → `INSERTION`
  - "abrogato" / "soppresso" → `REPEAL`
  - Fallback → `AMENDMENT`
- **Output:** Il tipo viene aggiunto come proprietà `modification_type` nell'arco `:MODIFIES`.

### 6. Transformer Layer (`transformers.py`)
I client di ingestion (Fase 0.5) scaricano dati grezzi ma non li trasformano nei DTO previsti dal data model. Il Transformer Layer colma questo gap.

- **Camera → `IterLegisStepDTO`** (Implementazione Completa ✅):
  - Legge il JSONL prodotto da `AsyncCameraClient`.
  - Genera ID deterministici, classifica `step_type` via keyword matching, converte URI → URN.
- **Corte Cost. → `JudgementDTO`** (Scaffold con Fallback 🟠):
  - Scansiona XML con `lxml` in `recover=True`.
  - Tenta estrazione di data, tipo pronuncia, URN norme giudicate.
  - **Graceful fallback:** se il formato non è quello atteso, logga un warning e restituisce lista vuota.
- **`enrich_document()`:** Arricchisce un `DocumentDTO` già parsato facendo matching per URN.

---

## Comandi per l'Utilizzo

### 1. Parsing di un singolo file
```bash
python src/parsing/parser.py --input data/raw/leggi/legge_1990_241.xml
python src/parsing/parser.py --input data/raw/leggi/legge_1990_241.xml --output parsed_doc.json
```

### 2. Parsing massivo (Directory)
```bash
python src/parsing/parser.py --input data/raw/ --output all_documents.json
```

### 3. Opzioni utili
- `--no-recover`: Disabilita il tentativo di recupero errori XML.
- `--verbose`: Attiva i log di livello DEBUG.

### 4. Esecuzione Test Suite
```bash
# Suite completa (18 test)
python -m pytest tests/ -v

# Solo tabelle
python -m pytest tests/test_parser_tables.py -v

# Solo classificazione modifiche
python -m pytest tests/test_parser_mod_classification.py -v

# Test su dati reali (skip automatico se file assenti)
python -m pytest test_real_data.py -v
```


