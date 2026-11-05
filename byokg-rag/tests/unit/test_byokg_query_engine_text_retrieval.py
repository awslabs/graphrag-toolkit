# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ByoKGQueryEngine with and without the optional text_triplet_retriever."""

import pytest

from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine
from graphrag_toolkit.byokg_rag.graph_retrievers import TextTripletRetriever
from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
from tests.unit.hashing_embedding import HashingEmbedding

QUESTION = 'What was the first award Pat LaFontaine received?'


def _graph(*triplets):
    graph = {}
    for h, r, t in triplets:
        graph.setdefault(h, {}).setdefault(r, {'triplets': []})['triplets'].append([h, r, t])
        graph.setdefault(t, {})
    return graph


GRAPH = _graph(('Pat LaFontaine', 'award received (1990)', 'Bill Masterton Memorial Trophy'),
               ('Pat LaFontaine', 'award received (1995)', 'Lester Patrick Trophy'),
               ('Pat LaFontaine', 'member of sports team (1983 - 1991)', 'New York Islanders'),
               ('New York Islanders', 'league', 'National Hockey League'))


class ScriptedLLM:
    """Answers in the tag formats KGLinker and AgenticRetriever parse; records prompts."""

    def __init__(self, entities='Pat LaFontaine', finish=True, answers='Bill Masterton Memorial Trophy'):
        self.entities, self.finish, self.answers, self.prompts = entities, finish, answers, []

    def generate(self, prompt, system_prompt=None):
        self.prompts.append(prompt)
        if '### Task: Entity Extraction' in prompt:
            done = '<task-completion>FINISH</task-completion>' if self.finish else ''
            return (f'<entities>\n{self.entities}\n</entities>\n<paths>\naward received (1990)\n</paths>\n'
                    f'<answers>\n{self.answers}\n</answers>\n{done}')
        if '<selected>' in prompt:
            return '<selected>\naward received (1990)\n</selected>'
        if 'next-entities' in prompt:
            return '<next-entities>\n</next-entities>'
        return ''


class RecordingRetriever:
    def __init__(self, results=('Pat LaFontaine -> award received (1995) -> Lester Patrick Trophy',)):
        self.results, self.calls = list(results), []

    def retrieve(self, query, **kwargs):
        self.calls.append(query)
        return list(self.results)


@pytest.mark.parametrize('single_best_match', [False, True])
def test_default_pipeline_output_is_unchanged(single_best_match):
    # Recorded from main (37ffcc3f) before text_triplet_retriever existed.
    engine = ByoKGQueryEngine(graph_store=LocalKGStore(GRAPH), llm_generator=ScriptedLLM(),
                              single_best_match=single_best_match)
    assert engine.text_triplet_retriever is None
    assert engine.query(QUESTION) == [
        'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy']


class TestTextTripletRetrieval:
    def test_runs_once_in_first_iteration_even_with_nothing_linked(self):
        text, agentic = RecordingRetriever(), RecordingRetriever()
        # No entities and no draft answers (either would link a seed), no FINISH: two
        # full iterations with an empty seed set.
        engine = ByoKGQueryEngine(graph_store=LocalKGStore(GRAPH),
                                  llm_generator=ScriptedLLM(entities='', answers='', finish=False),
                                  triplet_retriever=agentic, text_triplet_retriever=text)
        context = engine.query(QUESTION, iterations=2)
        assert agentic.calls == [], 'precondition: nothing was linked, so agentic never ran'
        assert text.calls == [QUESTION]
        assert context == ['Pat LaFontaine -> award received (1995) -> Lester Patrick Trophy']

    def test_joins_context_before_agentic_triplets(self):
        engine = ByoKGQueryEngine(graph_store=LocalKGStore(GRAPH), llm_generator=ScriptedLLM(),
                                  text_triplet_retriever=RecordingRetriever())
        assert engine.query(QUESTION) == [
            'Pat LaFontaine -> award received (1995) -> Lester Patrick Trophy',
            'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy']

    def test_next_kg_linker_call_sees_text_triplets(self):
        llm = ScriptedLLM(entities='', answers='', finish=False)
        engine = ByoKGQueryEngine(graph_store=LocalKGStore(GRAPH), llm_generator=llm,
                                  text_triplet_retriever=RecordingRetriever())
        engine.query(QUESTION, iterations=2)
        # Both KG-Linker prompts (first and iterative) share this preamble.
        linker_prompts = [p for p in llm.prompts if 'Given a question, schema, and optional graph context' in p]
        assert len(linker_prompts) == 2
        assert 'Lester Patrick Trophy' not in linker_prompts[0]
        assert 'Lester Patrick Trophy' in linker_prompts[1]

    def test_duplicates_with_agentic_results_are_dropped(self):
        same = 'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy'
        engine = ByoKGQueryEngine(graph_store=LocalKGStore(GRAPH), llm_generator=ScriptedLLM(),
                                  text_triplet_retriever=RecordingRetriever([same]))
        assert engine.query(QUESTION) == [same]

    def test_real_retriever_end_to_end(self):
        emb = HashingEmbedding()
        store = LocalKGStore(GRAPH)
        text = TextTripletRetriever.from_graph_store(store, emb, topk=1)
        engine = ByoKGQueryEngine(graph_store=store, llm_generator=ScriptedLLM(entities='', finish=True),
                                  text_triplet_retriever=text)
        assert engine.query('Which award did Pat LaFontaine receive in 1990?') == [
            'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy']
        assert emb.query_calls == ['Which award did Pat LaFontaine receive in 1990?']
