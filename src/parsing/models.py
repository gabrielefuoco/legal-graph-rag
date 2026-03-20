"""
Data Model — Pydantic V2 DTOs for the parsing pipeline output.

Definisce il formato di interscambio tra il parser e il layer di ingestione Neo4j.
Tutti i nodi e gli archi prodotti dal parser sono validati attraverso questi modelli.
"""
import hashlib
from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class NodeType(str, Enum):
    """Type of graph node."""
    STRUCTURAL = "STRUCTURAL"
    EXPRESSION = "EXPRESSION"


class EdgeType(str, Enum):
    """Type of graph edge."""
    PART_OF = "PART_OF"
    NEXT = "NEXT"
    CITES = "CITES"
    MODIFIES = "MODIFIES"
    HAS_TOPIC = "HAS_TOPIC"


class ModificationType(str, Enum):
    """Type of normative modification inside a <mod> element."""
    SUBSTITUTION = "SUBSTITUTION"   # "è sostituito dal seguente"
    INSERTION = "INSERTION"         # "è inserito" / "è aggiunto"
    REPEAL = "REPEAL"              # "è abrogato" / "è soppresso"
    AMENDMENT = "AMENDMENT"         # fallback generico


# ---------------------------------------------------------------------------
# ID Generation
# ---------------------------------------------------------------------------

def generate_id(urn: str, eid: str) -> str:
    """
    Generate a deterministic node ID from URN + eId.

    Uses SHA-256 hash truncated to 16 hex characters.

    Args:
        urn: Document URN (e.g. "urn:nir:stato:legge:2024-01-11;2")
        eid: Element ID within the document (e.g. "art_1__para_1")

    Returns:
        16-character hex string (e.g. "a1b2c3d4e5f6g7h8")
    """
    composite = f"{urn}#{eid}"
    return hashlib.sha256(composite.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# DTO Models
# ---------------------------------------------------------------------------

class FRBRMetadata(BaseModel):
    """FRBR metadata extracted from the <meta> block."""

    urn: str = Field(..., description="URN identifier (e.g. urn:nir:stato:legge:2024-01-11;2)")
    eli: Optional[str] = Field(None, description="ELI identifier if available")
    date_promulgation: date = Field(..., description="Date of promulgation/publication")
    date_publication: Optional[date] = Field(None, description="Date of publication in Gazzetta Ufficiale")
    vigenza_start: Optional[date] = Field(None, description="Start of validity period")
    vigenza_end: Optional[date] = Field(None, description="End of validity period")
    country: str = Field("it", description="Country code")
    doc_type: str = Field(..., description="Document type (legge, decreto-legge, etc.)")
    doc_number: str = Field("", description="Document number")
    title: Optional[str] = Field(None, description="Document title from preface/coverPage")
    frbr_work_uri: str = Field(..., description="FRBR Work URI")
    frbr_expression_uri: Optional[str] = Field(None, description="FRBR Expression URI")
    frbr_manifestation_uri: Optional[str] = Field(None, description="FRBR Manifestation URI")


class GraphNodeDTO(BaseModel):
    """A node in the knowledge graph."""

    id: str = Field(..., description="Deterministic hash ID (URN + eId)")
    type: NodeType = Field(..., description="STRUCTURAL or EXPRESSION")
    eid: Optional[str] = Field(None, description="Original eId/id attribute from XML")
    num: Optional[str] = Field(None, description="Article/comma number (e.g. 'Art. 1')")
    heading: Optional[str] = Field(None, description="Heading/rubrica text")
    text_content: Optional[str] = Field(None, description="Raw text content of the node")
    text_vector: Optional[str] = Field(
        None,
        description="Context-injected text optimized for embedding (EXPRESSION only)"
    )
    embedding: Optional[list[float]] = Field(
        None,
        description="Dense vector calculated by the model (EXPRESSION only)"
    )
    text_display: Optional[str] = Field(
        None,
        description="Display-ready text with preserved formatting (EXPRESSION only)"
    )
    level: int = Field(0, description="Depth in the document hierarchy (0 = root)")
    tag_name: str = Field(..., description="Original XML tag local name (article, paragraph, ...)")
    metadata: dict = Field(default_factory=dict, description="Additional attributes as JSON")


class GraphEdgeDTO(BaseModel):
    """An edge in the knowledge graph."""

    source_id: str = Field(..., description="Source node ID")
    target_id: str = Field(..., description="Target node ID (or external URN)")
    type: EdgeType = Field(..., description="Edge type")
    score: Optional[float] = Field(None, description="Semantic score for HAS_TOPIC")
    modification_type: Optional[str] = Field(None, description="Type of modification for MODIFIES")
    quoted_text: Optional[str] = Field(None, description="Novella text for MODIFIES")
    properties: dict = Field(default_factory=dict, description="Additional edge attributes")


class IterLegisStepDTO(BaseModel):
    """
    Represents a step in the legislative process (e.g., presentation, vote).
    Source: Camera/Senato RDF.
    """
    id: str  # Deterministic ID
    date: str
    description: str
    step_type: str  # e.g., "DDL_PRESENTATION", "VOTE", "ASSIGNMENT"
    authority: str  # e.g., "Senato della Repubblica", "Camera dei Deputati"
    related_work_urn: str  # URN of the Work being modified/discussed


class JudgementDTO(BaseModel):
    """
    Represents a court judgement (Sentenza).
    Source: Corte Costituzionale / EurLex.
    """
    id: str  # E.g., "sentenza_cortecost_2024_1"
    date: str
    description: str  # Massima or Abstract
    court: str # e.g. "Corte Costituzionale"
    judgement_type: str  # "Sentenza", "Ordinanza"
    judgement_result: str | None = None # "Accoglimento", "Rigetto"
    affected_urns: list[str] = Field(default_factory=list) # List of URNs interpreted/judged


class DocumentDTO(BaseModel):
    """
    Root output object for a single parsed document.

    Contains FRBR metadata, all graph nodes, and all graph edges.
    This is the interchange format between the parser and the Neo4j ingestion layer.
    """

    frbr: FRBRMetadata
    nodes: list[GraphNodeDTO] = Field(default_factory=list)
    edges: list[GraphEdgeDTO] = Field(default_factory=list)
    iter_legis: list[IterLegisStepDTO] = Field(default_factory=list)
    judgements: list[JudgementDTO] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_structure(self) -> "DocumentDTO":
        """Validate that the document has at least one STRUCTURAL node."""
        if self.nodes:
            has_structural = any(n.type == NodeType.STRUCTURAL for n in self.nodes)
            if not has_structural:
                raise ValueError("DocumentDTO must contain at least one STRUCTURAL node")
        return self

    def node_ids(self) -> set[str]:
        """Return the set of all node IDs in this document."""
        return {n.id for n in self.nodes}
