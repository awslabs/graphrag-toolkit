# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end composition: a custom extractor with a fallback to a toolkit
default stage, assembled through ExtractionConfig.from_stages() and run
through PipelineBuilder, exactly as a user would wire it up."""

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig
from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_builder import PipelineBuilder
from graphrag_toolkit.lexical_graph.indexing.extract.stages.custom_extraction_stage import (
    CustomExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.fallback_extraction_stage import (
    FallbackExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.constants import (
    EXTRACTION_STATUS_KEY,
    EXTRACTION_STATUS_QUARANTINED,
)


def in_house_ner(n):
    """Stands in for a customer's own NER/classification service."""
    if 'unsupported' in n.text:
        raise ValueError('document format not supported by in-house NER')
    return {'entities': [f'entity-from:{n.text}']}


def default_fallback(n):
    """Stands in for the toolkit's default extraction stage."""
    return {'entities': [f'default-entity-from:{n.text}']}


class TestCustomExtractionPipelineIntegration:

    def build_pipeline(self):
        custom = CustomExtractionStage(extractor_fn=in_house_ner, output_keys=['entities'])
        default = CustomExtractionStage(extractor_fn=default_fallback, output_keys=['entities'])
        fallback = FallbackExtractionStage(chain=[custom, default])

        config = ExtractionConfig.from_stages([fallback])
        builder = PipelineBuilder()
        for stage in config.stages:
            builder.add(stage)
        return builder.build()

    def test_pipeline_builds_from_custom_and_fallback_stages(self):
        [transform] = self.build_pipeline()
        assert transform is not None

    def test_documents_the_custom_extractor_handles_use_its_output(self):
        [transform] = self.build_pipeline()

        [result] = transform([TextNode(text='a normal document')])

        assert result.metadata['entities'] == ['entity-from:a normal document']

    def test_documents_the_custom_extractor_rejects_fall_back_to_default(self):
        [transform] = self.build_pipeline()

        [result] = transform([TextNode(text='an unsupported document')])

        assert result.metadata['entities'] == ['default-entity-from:an unsupported document']
        assert result.metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_QUARANTINED

    def test_mixed_corpus_resolves_each_document_independently(self):
        [transform] = self.build_pipeline()

        corpus = [
            TextNode(text='doc-a normal document'),
            TextNode(text='doc-b an unsupported document'),
            TextNode(text='doc-c normal again'),
        ]

        results = transform(corpus)
        by_text = {n.text: n for n in results}

        assert by_text['doc-a normal document'].metadata['entities'] == [
            'entity-from:doc-a normal document'
        ]
        assert by_text['doc-b an unsupported document'].metadata['entities'] == [
            'default-entity-from:doc-b an unsupported document'
        ]
        assert by_text['doc-c normal again'].metadata['entities'] == [
            'entity-from:doc-c normal again'
        ]
