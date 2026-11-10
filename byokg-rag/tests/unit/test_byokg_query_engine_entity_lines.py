# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ByoKGQueryEngine seeds for an extracted line that lists several entities, with the
real KGLinker, FuzzyStringIndex and EntityLinker."""

import pytest

from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine
from graphrag_toolkit.byokg_rag.graph_connectors import KGLinker
from graphrag_toolkit.byokg_rag.graph_retrievers import EntityLinker
from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex

GRAPH = {
    'Buffyverse': {'country of origin': {'triplets': [['Buffyverse', 'country of origin', 'Australia']]}},
    'Australia': {'official language': {'triplets': [['Australia', 'official language', 'English']]}},
    'English': {},
}


class OneLineLLM:
    """KG-Linker output as recorded on CWQ: three entities on one <entities> line."""

    def generate(self, prompt, system_prompt=None):
        return ('<entities>\nBuffyverse, Australia, language\n</entities>\n'
                '<paths>\n</paths>\n<answers>\n</answers>\n<task-completion>FINISH</task-completion>')


class SeedRecorder:
    def __init__(self):
        self.seeds = []

    def retrieve(self, query, source_entities):
        self.seeds.extend(source_entities)
        return []


def _seeds(single_best_match, keep_verbatim_names=False, split_entity_lines=False):
    store, llm, recorder = LocalKGStore(GRAPH), OneLineLLM(), SeedRecorder()
    index = FuzzyStringIndex(keep_verbatim_names=keep_verbatim_names)
    index.add(store.nodes())
    engine = ByoKGQueryEngine(
        graph_store=store, llm_generator=llm, triplet_retriever=recorder,
        entity_linker=EntityLinker(index.as_entity_matcher()),
        kg_linker=KGLinker(llm_generator=llm, graph_store=store, split_entity_lines=split_entity_lines),
        single_best_match=single_best_match)
    engine.query('What language is spoken where the Buffyverse is from?', iterations=1)
    return set(recorder.seeds)


@pytest.mark.parametrize('single_best_match', [False, True])
def test_by_default_the_line_links_nothing(single_best_match):
    assert _seeds(single_best_match) == set()


@pytest.mark.parametrize('single_best_match', [False, True])
def test_splitting_links_each_listed_name(single_best_match):
    assert {'Buffyverse', 'Australia'} <= _seeds(single_best_match, split_entity_lines=True)


def test_verbatim_names_alone_give_the_line_one_seed_under_single_best_match():
    assert _seeds(False, keep_verbatim_names=True) == {'Buffyverse', 'Australia'}
    assert _seeds(True, keep_verbatim_names=True) == {'Buffyverse'}  # the longer of two one-word names


def test_both_options_seed_every_listed_name_under_single_best_match():
    assert {'Buffyverse', 'Australia'} <= _seeds(True, keep_verbatim_names=True, split_entity_lines=True)
