# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, Iterable, List, Optional, Tuple

from graphrag_toolkit.lexical_graph.indexing.constants import SOURCE_HASH_PROPERTY
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

# Metadata keys tried, in order, when naming a document in an error. The same list names
# the incoming document and the one the graph holds, so both sides read the same way.
NAME_KEYS = ('file_path', 'file_name', 'url', 'source', 'title')

# Source ids per lookup. One read covers a build batch unless the batch is larger than this.
LOOKUP_BATCH_SIZE = 1000


class SourceIdCollisionError(ValueError):
    """Raised when two different documents resolve to one source id."""


def name_of(metadata:Optional[Dict], default:str='a document') -> str:
    """The first of NAME_KEYS the metadata carries, or the default."""
    for key in NAME_KEYS:
        value = (metadata or {}).get(key)
        if value:
            return str(value)
    return default


class SourceIdClaims:
    """
    The source hashes a build batch claimed, checked against the hashes the graph
    ended up holding.

    Two documents that collide can both be in one batch, so a claim is checked twice.
    Against the other claims in the batch, as it is added: that needs no read, names
    both documents rather than one of them and whatever the graph holds, and is the only
    check that covers the case, since a batched write keeps the last row's hash rather
    than the first. Against the graph, after the write: that is the only way to see a
    document an earlier build wrote.

    The source write MERGEs on the source id and keeps the hash already there, so across
    builds the document that claimed an id owns it and later writers leave that value
    alone. Reading it back after the write detects a collision however those writes
    interleave: two concurrent builds both merge, both read the owner's hash, and the one
    that did not win raises. A read taken before the write cannot do that. The cost is
    that the colliding document's chunks and lower tiers are written before the build
    raises, so it has to be re-run once the collision is resolved.

    Collected from source nodes as they pass through graph construction, so a directly
    wired ``BuildPipeline`` is covered as well as ``LexicalGraphIndex``.
    """

    def __init__(self):
        self._claims:Dict[str, Tuple[str, str]] = {}

    def __len__(self) -> int:
        return len(self._claims)

    def add(self, node:Any) -> None:
        """
        Records the hash a source node claims for its id. Nodes without a hash,
        which is what a build writes for a source whose id was not generated from its
        text, are not recorded and not checked.

        Raises:
            SourceIdCollisionError: Another node in this batch claimed the same id
                with a different hash.
        """
        source_metadata = (node.metadata or {}).get('source', {})
        source_id = source_metadata.get('sourceId')
        source_hash = source_metadata.get(SOURCE_HASH_PROPERTY)
        if not (source_id and source_hash):
            return

        name = name_of(source_metadata.get('metadata'))
        earlier = self._claims.get(source_id)
        if earlier and earlier[0] != source_hash:
            raise SourceIdCollisionError(
                f'Two different documents claim source id {source_id}: '
                f'{earlier[1]} (hash {earlier[0]}) and {name} (hash {source_hash}), '
                f'both in this build. The id is a prefix of the hash, so a build '
                f'cannot tell these two documents apart. Widen SOURCE_ID_WIDTH on a '
                f'new graph, or set DETECT_SOURCE_ID_COLLISIONS=false to accept '
                f'whichever document the graph merges first.'
            )
        self._claims[source_id] = (source_hash, name)

    def verify(self, graph_store:GraphStore) -> None:
        """
        Raises when the graph holds a different hash for an id this batch claimed.

        Args:
            graph_store: The store to read the recorded hashes from.

        Raises:
            SourceIdCollisionError: Two different documents claim one source id.
        """
        source_ids = list(self._claims)
        for start in range(0, len(source_ids), LOOKUP_BATCH_SIZE):
            self._verify_batch(graph_store, source_ids[start:start + LOOKUP_BATCH_SIZE])

    def _verify_batch(self, graph_store:GraphStore, source_ids:List[str]) -> None:
        source_id_field = graph_store.node_id('s.sourceId')
        names = ', '.join(f's.`{key}`' for key in NAME_KEYS)
        rows = graph_store.execute_query_with_retry(
            f'MATCH (s:`__Source__`) WHERE {source_id_field} IN $sourceIds '
            f'RETURN {source_id_field} AS sourceId, '
            f's.{SOURCE_HASH_PROPERTY} AS sourceHash, coalesce({names}) AS name',
            {'sourceIds': source_ids},
        )
        for row in rows or []:
            stored = row.get('sourceHash')
            claimed = self._claims.get(row['sourceId'])
            if stored and claimed and stored != claimed[0]:
                raise SourceIdCollisionError(
                    f'Two different documents claim source id {row["sourceId"]}: '
                    f'{claimed[1]} (hash {claimed[0]}) in this build, and '
                    f'{row.get("name") or "a document"} already in the graph '
                    f'(hash {stored}). The id is a prefix of the hash, so a build '
                    f'cannot tell these two documents apart. Widen SOURCE_ID_WIDTH '
                    f'on a new graph, or set DETECT_SOURCE_ID_COLLISIONS=false to '
                    f'accept the document the graph already holds.'
                )


def check_source_hashes_agree(source_id:str, hashes:Iterable[Optional[str]], name:str) -> None:
    """
    Raises when chunks that share a source id do not share a source hash.

    Two documents whose ids collide share a storage prefix, so a read of that prefix
    returns one document holding both sets of chunks. Each chunk still carries its
    own document's hash, which is the only trace of the two.

    Args:
        source_id: The id the chunks share.
        hashes: The source hash each chunk carries.
        name: How to describe the document in an error.

    Raises:
        SourceIdCollisionError: The chunks carry more than one hash.
    """
    distinct = {h for h in hashes if h}
    if len(distinct) > 1:
        raise SourceIdCollisionError(
            f'Two different documents claim source id {source_id}: chunks read as '
            f'{name} carry hashes {" and ".join(sorted(distinct))}. The two documents '
            f'share a storage prefix, so they were read back as one.'
        )
