# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Node and relation embeddings for a graph, computed once and cached on disk."""

import hashlib
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np

__all__ = ['OPAQUE_ID', 'MAX_EMBED_CHARS', 'is_opaque_id', 'truncate_for_embedding',
           'normalize_rows', 'embed_texts', 'top_k_indices', 'EmbeddingTable', 'load_or_embed',
           'GraphEmbeddings']

logger = logging.getLogger(__name__)

# Freebase machine ids (m.0abc, g.11xyz) are never mentioned by name: embedding them
# costs calls and adds no signal.
OPAQUE_ID = re.compile(r'^[mg]\.[0-9a-z_]+$')

# Texts per batch_embed call, and per checkpoint. The embedding wrapper batches
# further for the API; this bounds memory and what a failed build loses.
EMBED_CHUNK = 2048

# Bedrock's Cohere Embed v3 rejects texts over 2,048 characters, and a few Freebase
# node names (WebQSP, CWQ) are longer. They are cut to this before embedding; the
# full text still keys the row.
MAX_EMBED_CHARS = 2048


def is_opaque_id(text: str) -> bool:
    return bool(OPAQUE_ID.match(text))


def truncate_for_embedding(text: str) -> str:
    return text[:MAX_EMBED_CHARS]


def normalize_rows(vectors: np.ndarray) -> np.ndarray:
    """L2-normalize each row so inner product is cosine similarity."""
    norms = np.linalg.norm(vectors, axis=-1, keepdims=True)
    if np.any(norms == 0) or not np.all(np.isfinite(norms)):
        raise ValueError('embedding contains a zero or non-finite vector; cannot normalize')
    return (vectors / norms).astype(np.float32)


def top_k_indices(scores: np.ndarray, k: int) -> np.ndarray:
    """Indices of the k highest scores, best first; ties go to the lower index."""
    k = min(k, len(scores))
    if k <= 0:
        return np.empty(0, dtype=np.int64)
    # argpartition alone picks arbitrarily among scores tied at the cut-off, so take
    # everything above the k-th score, then fill from the tied ones by index.
    kth = np.partition(scores, len(scores) - k)[len(scores) - k]
    above = np.flatnonzero(scores > kth)
    tied = np.flatnonzero(scores == kth)[:k - len(above)]
    picked = np.concatenate([above, tied])
    return picked[np.lexsort((picked, -scores[picked]))]


def _save_atomic(path: str, array: np.ndarray) -> None:
    tmp = f'{path}.tmp.npy'
    np.save(tmp, array)
    os.replace(tmp, path)


def embed_texts(embedding, texts: Sequence[str], checkpoint_dir: Optional[str] = None,
                model_id: Optional[str] = None) -> np.ndarray:
    """Embed texts as documents: (len(texts), dim) float32, L2-normalized.

    Texts over MAX_EMBED_CHARS are truncated. With checkpoint_dir, each finished
    chunk is saved there and reused by the next call, so a throttle late in a large
    build costs one chunk rather than every call made so far.
    """
    chunks = []
    for index, start in enumerate(range(0, len(texts), EMBED_CHUNK)):
        batch = [truncate_for_embedding(t) for t in texts[start:start + EMBED_CHUNK]]
        path = None
        if checkpoint_dir:
            # Named by model and content, so a checkpoint is only reused for the same
            # texts embedded by the same model.
            digest = hashlib.sha256(json.dumps([model_id, batch]).encode('utf-8')).hexdigest()[:16]
            path = os.path.join(checkpoint_dir, f'{index:06d}-{digest}.npy')
        if path and os.path.exists(path):
            vectors = np.load(path)
        else:
            raw = embedding.batch_embed(batch)
            if len(raw) != len(batch):
                raise ValueError(f'batch_embed returned {len(raw)} vectors for {len(batch)} texts')
            vectors = np.asarray(raw, dtype=np.float32)
            if path:
                os.makedirs(checkpoint_dir, exist_ok=True)
                _save_atomic(path, vectors)
        if vectors.ndim != 2 or vectors.shape[0] != len(batch):
            raise ValueError(f'expected {len(batch)} vectors, got array of shape {vectors.shape}')
        if chunks and vectors.shape[1] != chunks[0].shape[1]:
            raise ValueError(f'embedding dimension changed mid-build: {chunks[0].shape[1]} -> {vectors.shape[1]}')
        chunks.append(vectors)
        if len(texts) > EMBED_CHUNK:
            logger.info('embedded %d/%d', min(start + EMBED_CHUNK, len(texts)), len(texts))
    if not chunks:
        raise ValueError('nothing to embed')
    return normalize_rows(np.concatenate(chunks))


@dataclass(frozen=True)
class EmbeddingTable:
    """Texts and their embeddings, row-aligned."""
    texts: List[str]
    vectors: np.ndarray  # (len(texts), dim), float32, L2-normalized
    _rows: Dict[str, int] = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if self.vectors.shape[0] != len(self.texts):
            raise ValueError(f'{len(self.texts)} texts but {self.vectors.shape[0]} vectors')
        object.__setattr__(self, '_rows', {t: i for i, t in enumerate(self.texts)})

    @property
    def dim(self) -> int:
        return self.vectors.shape[1]

    def row(self, text: str) -> Optional[int]:
        return self._rows.get(text)


def _write_json_atomic(path: str, value) -> None:
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(value, f)
    os.replace(tmp, path)


def load_or_embed(texts: Sequence[str], embedding, cache_dir: Optional[str] = None,
                  model_id: Optional[str] = None) -> EmbeddingTable:
    """Embed texts, reusing cache_dir/{texts.json,meta.json,vectors.npy} when present.

    meta.json records the model and dimension. A cache whose texts, model, dimension
    or row count don't match raises instead of being reused or silently recomputed:
    it means the graph or the model changed under the key. A hit returns exactly the
    vectors the miss computed, so results don't depend on which run built the cache.
    """
    texts = list(texts)
    vectors_path = os.path.join(cache_dir, 'vectors.npy') if cache_dir else None
    if vectors_path and os.path.exists(vectors_path):
        with open(os.path.join(cache_dir, 'texts.json'), encoding='utf-8') as f:
            cached_texts = json.load(f)
        with open(os.path.join(cache_dir, 'meta.json'), encoding='utf-8') as f:
            meta = json.load(f)
        vectors = np.load(vectors_path)
        problems = []
        if cached_texts != texts:
            problems.append(f'{len(cached_texts)} cached texts vs {len(texts)} expected')
        if model_id is not None and meta.get('model_id') != model_id:
            problems.append(f"model {meta.get('model_id')!r} vs {model_id!r}")
        if vectors.dtype != np.float32 or vectors.shape != (len(texts), meta.get('dim')):
            problems.append(f"vectors {vectors.dtype} {vectors.shape} vs float32 "
                            f"({len(texts)}, {meta.get('dim')})")
        elif not np.allclose(np.linalg.norm(vectors, axis=1), 1.0, atol=1e-3):
            problems.append('vectors are not unit length')
        if problems:
            raise ValueError(f"embedding cache {cache_dir} does not match: {'; '.join(problems)}; "
                             f"delete it or use another cache directory")
        logger.info('loaded %d embeddings from %s', len(texts), cache_dir)
        return EmbeddingTable(texts, vectors)

    vectors = embed_texts(embedding, texts, model_id=model_id,
                          checkpoint_dir=os.path.join(cache_dir, 'partial') if cache_dir else None)
    if cache_dir:
        # vectors.npy is written last: its presence is what marks a cache complete.
        _write_json_atomic(os.path.join(cache_dir, 'texts.json'), texts)
        _write_json_atomic(os.path.join(cache_dir, 'meta.json'),
                           {'model_id': model_id, 'dim': int(vectors.shape[1]), 'count': len(texts)})
        _save_atomic(vectors_path, vectors)
        shutil.rmtree(os.path.join(cache_dir, 'partial'), ignore_errors=True)
    return EmbeddingTable(texts, vectors)


@dataclass(frozen=True)
class GraphEmbeddings:
    """Embeddings of a graph's named nodes and relation labels.

    Opaque ids are not embedded; callers treat their similarity as 0.
    """
    nodes: EmbeddingTable
    relations: EmbeddingTable

    def __post_init__(self):
        if self.nodes.dim != self.relations.dim:
            raise ValueError(f'node dim {self.nodes.dim} != relation dim {self.relations.dim}')

    @classmethod
    def build(cls, triplets: Iterable[Sequence[str]], embedding, cache_dir: Optional[str] = None,
              extra_nodes: Iterable[str] = (), model_id: Optional[str] = None) -> 'GraphEmbeddings':
        """Embed every named head/tail (plus extra_nodes) and every relation label.

        Sorted, so the row order — and therefore the cache — is reproducible.
        model_id is recorded in the cache and checked on reuse.
        """
        nodes, relations = set(extra_nodes), set()
        for triplet in triplets:
            if len(triplet) != 3:
                raise ValueError(f'triplet must be (head, relation, tail), got {triplet!r}')
            head, relation, tail = triplet
            nodes.update((head, tail))
            relations.add(relation)
        node_texts = sorted(n for n in nodes if not is_opaque_id(n))
        return cls(
            nodes=load_or_embed(node_texts, embedding,
                                cache_dir and os.path.join(cache_dir, 'nodes'), model_id),
            relations=load_or_embed(sorted(relations), embedding,
                                    cache_dir and os.path.join(cache_dir, 'relations'), model_id),
        )
