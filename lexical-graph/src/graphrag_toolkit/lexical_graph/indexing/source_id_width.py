# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Iterable, Optional

from llama_index.core.schema import NodeRelationship

from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.config import SourceIdWidth
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.model import SourceType, SourceDocument
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

logger = logging.getLogger(__name__)

SOURCE_ID_WIDTH_CONFIG_ID = 'source_id_width'
SOURCE_ID_SAMPLE_SIZE = 100


class SourceIdWidthMismatchError(ValueError):
    """Raised when one collection would hold source ids of two widths."""


def _describe(width:SourceIdWidth) -> str:
    return f'{width.name} ({width.value})'


def resolve_source_id_width(stored_widths:Iterable[Optional[SourceIdWidth]], configured:Optional[SourceIdWidth], default:Optional[SourceIdWidth]) -> Optional[SourceIdWidth]:
    """
    The width a run must use. A collection that already holds ids keeps the width
    they were written at; an empty one takes the explicit setting, or the default
    when there is none. Unknown widths (None) are ignored.

    Raises:
        SourceIdWidthMismatchError: If the stored widths disagree, or an explicit
            setting contradicts them.
    """
    widths = sorted({w for w in stored_widths if w is not None})

    if len(widths) > 1:
        raise SourceIdWidthMismatchError(
            f'This collection would hold source ids at more than one width: '
            f'{" and ".join(_describe(w) for w in widths)}.'
        )

    if not widths:
        return configured or default

    stored = widths[0]

    if configured is not None and configured != stored:
        raise SourceIdWidthMismatchError(
            f'This collection was written at source id width {_describe(stored)}, but '
            f'SOURCE_ID_WIDTH is set to {_describe(configured)}. Unset it, or set it '
            f'to {stored.name}, to keep writing to this collection.'
        )

    return stored


def graph_source_id_width(graph_store:GraphStore) -> Optional[SourceIdWidth]:
    """
    The width a graph was written at: its recorded width, else the width of a
    sample of stored source ids, else None when the graph holds none the
    generator wrote.

    The sample is the first SOURCE_ID_SAMPLE_SIZE source ids the store returns, so
    a graph whose widths diverge beyond that window reads as whichever width the
    sample holds. Every graph written from here on records its width, which is
    read in full, and the guard rejects a document at any other width.
    """
    results = graph_store.execute_query(
        'MATCH (c:`__SYS_Config__`) WHERE c.sourceIdWidth IS NOT NULL '
        'RETURN c.sourceIdWidth AS width LIMIT 1'
    )
    if results:
        return SourceIdWidth.parse(results[0]['width'])

    results = graph_store.execute_query(
        f'MATCH (s:`__Source__`) RETURN {graph_store.node_id("s.sourceId")} AS sourceId '
        f'LIMIT {SOURCE_ID_SAMPLE_SIZE}'
    )
    widths = sorted({IdGenerator.width_of_source_id(r['sourceId']) for r in results} - {None})
    if not widths:
        return None
    logger.debug(f'Read the source id width from stored ids [sampled: {len(results)}]')
    if len(widths) > 1:
        raise SourceIdWidthMismatchError(
            f'This graph already holds source ids at more than one width: '
            f'{" and ".join(_describe(w) for w in widths)}. Nothing can be written '
            f'to it until they agree.'
        )

    return widths[0]


def record_graph_source_id_width(graph_store:GraphStore, tenant_id:TenantId, width:SourceIdWidth) -> None:
    """
    Records the width on the graph. Sets it only when the record is created, so a
    concurrent writer that loses the race reads back the winner's width.

    Raises:
        SourceIdWidthMismatchError: If the graph already records another width.
    """
    results = graph_store.execute_query(
        f'MERGE (c:`__SYS_Config__`{{{graph_store.node_id("sysConfigId")}: $configId}}) '
        'ON CREATE SET c.sourceIdWidth = $width '
        'RETURN c.sourceIdWidth AS width',
        {
            'configId': tenant_id.format_id('sys_config', SOURCE_ID_WIDTH_CONFIG_ID),
            'width': width.value
        }
    )
    if not results:
        # A store that writes but returns nothing, the dummy store among them,
        # leaves nothing to check against.
        logger.debug(f'Graph returned no width after recording [width: {width.name}]')
        return

    recorded = SourceIdWidth.parse(results[0]['width'])
    if recorded != width:
        raise SourceIdWidthMismatchError(
            f'This graph already records source id width {_describe(recorded)}; '
            f'cannot record {_describe(width)}.'
        )


class SourceIdWidthGuard:
    """
    Checks that every document entering a build carries the graph's source id
    width, and records the width on a graph that has none when the first
    document arrives.
    """

    def __init__(self, graph_store:GraphStore, tenant_id:TenantId):
        self.graph_store = graph_store
        self.tenant_id = tenant_id

    @staticmethod
    def _source_id(item:SourceType) -> Optional[str]:
        """The source id an input carries, or None when it carries none: a build
        takes documents and bare nodes, and a node need not have a source."""
        if isinstance(item, SourceDocument):
            if not item.nodes:
                return None
            node = item.nodes[0]
        else:
            node = item

        source = node.relationships.get(NodeRelationship.SOURCE)
        return source.node_id if source else None

    def _check(self, inputs:Iterable[SourceType]):
        graph_width = graph_source_id_width(self.graph_store)

        for item in inputs:
            source_id = self._source_id(item)
            width = IdGenerator.width_of_source_id(source_id) if source_id else None

            if width is not None:
                if graph_width is None:
                    graph_width = width
                    record_graph_source_id_width(self.graph_store, self.tenant_id, width)
                    logger.debug(f'Recorded source id width [width: {width.name}]')
                elif width != graph_width:
                    raise SourceIdWidthMismatchError(
                        f'Document {source_id} has a source id at width {_describe(width)}, '
                        f'but this graph is written at {_describe(graph_width)}. Extract '
                        f'with SOURCE_ID_WIDTH={graph_width.name} to write to this graph.'
                    )
            yield item

    def __call__(self, inputs:Iterable[SourceType]):
        """
        Yields the inputs unchanged. A sized input comes back as a list so the
        build pipeline can still report batch totals.
        """
        checked = self._check(inputs)
        return list(checked) if hasattr(inputs, '__len__') else checked
