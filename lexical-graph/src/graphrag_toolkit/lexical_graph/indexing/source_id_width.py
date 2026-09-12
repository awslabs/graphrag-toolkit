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


def recorded_source_id_width(graph_store:GraphStore) -> Optional[SourceIdWidth]:
    """
    The width recorded on the collection, or None when it holds no record.

    Every record is read rather than the first one. Nothing constrains a store to
    a single __SYS_Config__ node, so two first runs at different widths can each
    create one, and two records that disagree are an error rather than whichever
    the store happens to return first.
    """
    results = graph_store.execute_query(
        'MATCH (c:`__SYS_Config__`) WHERE c.sourceIdWidth IS NOT NULL '
        'RETURN c.sourceIdWidth AS width'
    )
    widths = sorted({SourceIdWidth.parse(r['width']) for r in results} - {None})
    if not widths:
        return None
    if len(widths) > 1:
        raise SourceIdWidthMismatchError(
            f'This collection records a source id width more than once, and the '
            f'records disagree: {" and ".join(_describe(w) for w in widths)}.'
        )

    return widths[0]


def sampled_source_id_width(graph_store:GraphStore) -> Optional[SourceIdWidth]:
    """
    The width of a sample of stored source ids, or None when the sample holds none
    the generator wrote.

    The sample is the first SOURCE_ID_SAMPLE_SIZE source ids the store returns, so
    a collection whose widths already diverge beyond that window reads as whichever
    width the sample holds. The guard records the width it resolves, so a
    collection is sampled once and read from its record on every run after that.
    """
    results = graph_store.execute_query(
        f'MATCH (s:`__Source__`) RETURN {graph_store.node_id("s.sourceId")} AS sourceId '
        f'LIMIT {SOURCE_ID_SAMPLE_SIZE}'
    )
    widths = sorted({
        IdGenerator.width_of_source_id(r['sourceId'])
        for r in results if r['sourceId']
    } - {None})
    if not widths:
        return None
    logger.debug(f'Read the source id width from stored ids [sampled: {len(results)}]')
    if len(widths) > 1:
        raise SourceIdWidthMismatchError(
            f'This collection already holds source ids at more than one width: '
            f'{" and ".join(_describe(w) for w in widths)}. Nothing can be written '
            f'to it until they agree.'
        )

    return widths[0]


def graph_source_id_width(graph_store:GraphStore) -> Optional[SourceIdWidth]:
    """
    The width a collection was written at: its recorded width, else the width of a
    sample of stored source ids, else None when it holds none the generator wrote.
    """
    return recorded_source_id_width(graph_store) or sampled_source_id_width(graph_store)


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
    if recorded is not None and recorded != width:
        raise SourceIdWidthMismatchError(
            f'This collection already records source id width {_describe(recorded)}; '
            f'cannot record {_describe(width)}.'
        )


class SourceIdWidthGuard:
    """
    Checks that every document entering a build carries the collection's source id
    width, and records that width whenever the collection holds no record.

    `configured` is the explicit SOURCE_ID_WIDTH setting, or None when it is
    unset. A build is the one entry point that takes documents it did not extract,
    so an empty collection is stamped with the width of the documents arriving at
    it, and the setting has to be checked here rather than only where extraction
    resolves it.
    """

    def __init__(self, graph_store:GraphStore, tenant_id:TenantId, configured:Optional[SourceIdWidth]=None):
        self.graph_store = graph_store
        self.tenant_id = tenant_id
        self.configured = configured

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
        recorded = recorded_source_id_width(self.graph_store)
        graph_width = recorded or sampled_source_id_width(self.graph_store)

        for item in inputs:
            source_id = self._source_id(item)
            width = IdGenerator.width_of_source_id(source_id) if source_id else None

            if width is not None:
                if graph_width is None:
                    if self.configured is not None and width != self.configured:
                        raise SourceIdWidthMismatchError(
                            f'Document {source_id} has a source id at width {_describe(width)}, '
                            f'but SOURCE_ID_WIDTH is set to {_describe(self.configured)}. Extract '
                            f'these documents at the set width, or unset it, to build them.'
                        )
                    graph_width = width
                elif width != graph_width:
                    raise SourceIdWidthMismatchError(
                        f'Document {source_id} has a source id at width {_describe(width)}, '
                        f'but this collection is written at {_describe(graph_width)}. Extract '
                        f'with SOURCE_ID_WIDTH={graph_width.name} to write to this collection.'
                    )

                if recorded is None:
                    record_graph_source_id_width(self.graph_store, self.tenant_id, graph_width)
                    recorded = graph_width
                    logger.debug(f'Recorded source id width [width: {graph_width.name}]')
            yield item

    def __call__(self, inputs:Iterable[SourceType]):
        """
        Yields the inputs unchanged. A sized input comes back as a list so the
        build pipeline can still report batch totals.
        """
        checked = self._check(inputs)
        return list(checked) if hasattr(inputs, '__len__') else checked
