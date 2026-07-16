# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CPG domain models — typed representations of Joern code property graph output.

These models accept ALL Joern node/edge types (the full CPG schema).
See schema.py for the complete type enumeration and property specs.

Node types (20): METHOD, CALL, IDENTIFIER, LITERAL, BLOCK, COMMENT, FILE,
    LOCAL, MEMBER, META_DATA, METHOD_REF, METHOD_RETURN, MODIFIER, NAMESPACE,
    NAMESPACE_BLOCK, PARAMETER, RETURN, TAG, TYPE_DECL, TYPE

Edge types (14): AST, CFG, CDG, REACHING_DEF, CALL, ARGUMENT, DOMINATE,
    POST_DOMINATE, CONTAINS, BINDS_TO, REF, INHERITS_FROM, CONDITION, TAGGED_BY

Supported languages: Java, JavaScript/TypeScript, Python, C/C++, Go, PHP,
    Ruby, Kotlin, Swift (via Joern frontends)
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional


@dataclass
class CPGNode:
    """A code property graph node from Joern export.

    Accepts any node_type — the full Joern schema (20 types) flows through.
    See schema.NodeType for the complete enumeration.

    Identity: full_name (stable across line shifts — used for delta comparison)
    Change detection: hash (content fingerprint computed by Joern)

    Key properties by type:
        METHOD: full_name, signature, hash, is_external, line_number
        CALL: full_name (callee), code, line_number
        TYPE_DECL: full_name, name, is_external
        IDENTIFIER: name, code, line_number
        LITERAL: code, line_number
        PARAMETER: name, type_full_name, order
        FILE: name (path)
    """

    id: str
    node_type: str  # Any of the 20 Joern node types (see schema.NodeType)
    full_name: str = ""
    hash: str = ""
    filename: str = ""
    name: str = ""
    code: str = ""
    signature: str = ""
    line_number: Optional[int] = None
    line_number_end: Optional[int] = None
    order: Optional[int] = None
    type_full_name: str = ""
    is_external: bool = False
    properties: Dict[str, Any] = field(default_factory=dict)

    @property
    def stable_id(self) -> str:
        """Content-addressed identity for delta comparison.

        For METHOD nodes: full_name is stable across line shifts.
        For other nodes: falls back to Joern's numeric id.
        """
        return self.full_name or self.id

    @classmethod
    def from_joern(cls, raw: dict) -> "CPGNode":
        """Create a CPGNode from a raw Joern JSON export entry.

        Args:
            raw: Dict from Joern's nodes.json export with 'id', 'label', 'properties' keys.

        Returns:
            CPGNode with Joern properties mapped to typed fields.
        """
        props = raw.get("properties", {})
        return cls(
            id=str(raw.get("id", "")),
            node_type=raw.get("label", "UNKNOWN"),
            full_name=props.get("FULL_NAME", props.get("fullName", "")),
            hash=props.get("HASH", props.get("hash", "")),
            filename=props.get("FILENAME", props.get("filename", "")),
            name=props.get("NAME", props.get("name", "")),
            code=props.get("CODE", props.get("code", "")),
            signature=props.get("SIGNATURE", props.get("signature", "")),
            line_number=props.get("LINE_NUMBER", props.get("lineNumber")),
            line_number_end=props.get("LINE_NUMBER_END", props.get("lineNumberEnd")),
            order=props.get("ORDER", props.get("order")),
            type_full_name=props.get("TYPE_FULL_NAME", props.get("typeFullName", "")),
            is_external=props.get("IS_EXTERNAL", props.get("isExternal", False)),
            properties=props,
        )

    @classmethod
    def from_artifact(cls, raw: dict) -> "CPGNode":
        """Create a CPGNode from a portable CPG artifact schema entry.

        Args:
            raw: Dict with keys: cpg_node_id, node_type, labels, repo_id,
                 commit_sha, file_path, fully_qualified_name, name,
                 line_start, line_end, code_hash, properties.

        Returns:
            CPGNode with artifact fields mapped to typed fields.
        """
        return cls(
            id=raw.get("cpg_node_id", ""),
            node_type=raw.get("node_type", "UNKNOWN"),
            full_name=raw.get("fully_qualified_name", ""),
            hash=raw.get("code_hash", ""),
            filename=raw.get("file_path", ""),
            name=raw.get("name", ""),
            line_number=raw.get("line_start"),
            line_number_end=raw.get("line_end"),
            properties=raw.get("properties", {}),
        )


@dataclass
class CPGEdge:
    """A code property graph edge from Joern export.

    Accepts any edge_type — the full Joern schema (14+ types) flows through.
    See schema.EdgeType for the complete enumeration.

    Edge layers:
        AST:          Syntax tree structure (parent → child)
        CFG:          Control flow (execution order)
        CDG:          Control dependence (branching influence)
        REACHING_DEF: Data flow (definition → use)
        CALL:         Call graph (caller → callee)
        ARGUMENT:     Call → argument mapping
        CONTAINS:     File/Type → contained elements
        INHERITS_FROM: Type hierarchy
    """

    source_id: str
    target_id: str
    edge_type: str  # Any of the 14+ Joern edge types (see schema.EdgeType)
    properties: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        """Unique edge identity for diff."""
        return f"{self.source_id}->{self.edge_type}->{self.target_id}"

    @classmethod
    def from_joern(cls, raw: dict) -> "CPGEdge":
        """Create a CPGEdge from a raw Joern JSON export entry.

        Args:
            raw: Dict from Joern's edges.json export with 'src', 'dst', 'label' keys.

        Returns:
            CPGEdge with Joern properties mapped.
        """
        return cls(
            source_id=str(raw.get("src", raw.get("source_id", ""))),
            target_id=str(raw.get("dst", raw.get("target_id", ""))),
            edge_type=raw.get("label", raw.get("edge_type", "UNKNOWN")),
            properties=raw.get("properties", {}),
        )

    @classmethod
    def from_artifact(cls, raw: dict) -> "CPGEdge":
        """Create a CPGEdge from a portable CPG artifact schema entry.

        Args:
            raw: Dict with keys: edge_id, source_cpg_node_id,
                 target_cpg_node_id, edge_type, properties.

        Returns:
            CPGEdge with artifact fields mapped.
        """
        return cls(
            source_id=raw.get("source_cpg_node_id", ""),
            target_id=raw.get("target_cpg_node_id", ""),
            edge_type=raw.get("edge_type", "UNKNOWN"),
            properties=raw.get("properties", {}),
        )


@dataclass
class Manifest:
    """CPG extraction manifest — tracks the current graph state for a repo.

    The manifest stores method signatures ({full_name: hash}) as the
    fingerprint for delta comparison. Only METHOD nodes participate in
    change detection because method body changes represent meaningful
    code behavior changes. Other changes (comments, formatting, metadata)
    don't affect program behavior.
    """

    repo: str
    signature: str  # sha256 of sorted method full_name:hash pairs
    job_id: str
    tenant_id: str
    exported_at: str
    nodes_path: str
    edges_path: str
    node_count: int = 0
    edge_count: int = 0
    language: str = ""
    method_signatures: Dict[str, str] = field(default_factory=dict)  # full_name → hash


@dataclass
class VectorRecord:
    """An embedding vector associated with a CPG node.

    Used for semantic search over code elements (methods, types, etc.).
    """

    cpg_node_id: str
    embedding_target: str
    embedding_model: str
    embedding_dimensions: int
    similarity_function: str
    embedding_text_hash: str
    vector: List[float] = field(default_factory=list)

    @classmethod
    def from_artifact(cls, raw: dict) -> "VectorRecord":
        """Create a VectorRecord from a portable artifact schema entry.

        Args:
            raw: Dict with keys: cpg_node_id, embedding_target, embedding_model,
                 embedding_dimensions, similarity_function, embedding_text_hash, vector.

        Returns:
            VectorRecord with artifact fields mapped.
        """
        return cls(
            cpg_node_id=raw.get("cpg_node_id", ""),
            embedding_target=raw.get("embedding_target", ""),
            embedding_model=raw.get("embedding_model", ""),
            embedding_dimensions=raw.get("embedding_dimensions", 0),
            similarity_function=raw.get("similarity_function", ""),
            embedding_text_hash=raw.get("embedding_text_hash", ""),
            vector=raw.get("vector", []),
        )


@dataclass
class SummaryRecord:
    """An LLM-generated summary associated with a CPG node.

    Used for natural-language descriptions of code elements.
    """

    cpg_node_id: str
    summary_type: str
    text: str
    generation_model: str
    generation_prompt_version: str

    @classmethod
    def from_artifact(cls, raw: dict) -> "SummaryRecord":
        """Create a SummaryRecord from a portable artifact schema entry.

        Args:
            raw: Dict with keys: cpg_node_id, summary_type, text,
                 generation_model, generation_prompt_version.

        Returns:
            SummaryRecord with artifact fields mapped.
        """
        return cls(
            cpg_node_id=raw.get("cpg_node_id", ""),
            summary_type=raw.get("summary_type", ""),
            text=raw.get("text", ""),
            generation_model=raw.get("generation_model", ""),
            generation_prompt_version=raw.get("generation_prompt_version", ""),
        )


@dataclass
class CodeSliceRecord:
    """A code slice (excerpt) associated with a CPG node.

    Represents extracted code fragments for analysis or display.
    """

    cpg_node_id: str
    slice_type: str
    code: str
    language: str
    line_start: Optional[int] = None
    line_end: Optional[int] = None
    file_path: str = ""

    @classmethod
    def from_artifact(cls, raw: dict) -> "CodeSliceRecord":
        """Create a CodeSliceRecord from a portable artifact schema entry.

        Args:
            raw: Dict with keys: cpg_node_id, slice_type, code, language,
                 line_start, line_end, file_path.

        Returns:
            CodeSliceRecord with artifact fields mapped.
        """
        return cls(
            cpg_node_id=raw.get("cpg_node_id", ""),
            slice_type=raw.get("slice_type", ""),
            code=raw.get("code", ""),
            language=raw.get("language", ""),
            line_start=raw.get("line_start"),
            line_end=raw.get("line_end"),
            file_path=raw.get("file_path", ""),
        )
