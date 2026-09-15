# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for composing an ontology's vocabulary into the prompts.

Covers prompt composition:

* The shipped templates carry no `{ontology_constraints}` placeholder, and
  `with_ontology_constraints` returns its argument *itself* when there is nothing
  to compose - so a no-ontology render cannot move. That matters because the
  `LLMCache` key is a sha256 of the rendered prompt, and one added blank line
  would silently invalidate every existing user's `cache/llm/` directory.
  Otherwise the block is inserted at a placeholder, at the documented anchor, or
  at the end.
* Braces arriving from an ontology survive `str.format`. Nothing asked for this; an `rdfs:comment` containing `{text}` would otherwise either
  raise `KeyError` from inside the prompt renderer or quietly substitute the
  chunk into the middle of the vocabulary block.
* The composition is applied at the render point in all four extractors, and the
  batch pair carries it into `_get_json`'s request body and into the extractor
  built by `_run_non_batch_extractor`.

The extractor tests patch `LLMCache.predict` and read the prompt it was handed,
which is the same string `LLMCache` would have hashed - so they assert against
what the model would actually receive rather than against an intermediate.
"""

import json
from hashlib import sha256
from unittest.mock import Mock, patch

import pytest

from llama_index.core.llms import MockLLM
from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.prompts import (
    EXTRACT_PROPOSITIONS_ANCHOR,
    EXTRACT_PROPOSITIONS_PROMPT,
    EXTRACT_TOPICS_ANCHOR,
    EXTRACT_TOPICS_PROMPT,
    ONTOLOGY_CONSTRAINTS_PLACEHOLDER,
    with_ontology_constraints,
)
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import format_list, format_text
from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.utils.llm_concurrency import shutdown as shutdown_llm_pool

TEXT = 'Amy Bell works for Example Corp. Example Corp was founded in 1998.'

VOCABULARY_HEADER = '# Vocabulary for this extraction'
ENTITY_TYPES_HEADER = '# Entity types for this extraction'

# Stands in for llm.to_json() when comparing one cache key against another. The
# model half of the key is not a property of the prompt.
STUB_LLM_JSON = '{"stub": "llm"}'

def cache_key_of(rendered, llm_json=STUB_LLM_JSON):
    '''
    Reproduce LLMCache.predict's cache key for an already-rendered prompt.
    '''
    return sha256(f'{llm_json},{rendered}'.encode('utf-8')).hexdigest()

def render(template, constraints, **arguments):
    '''
    Compose and format a template the way an extractor does.
    '''
    return PromptTemplate(
        template=with_ontology_constraints(template, constraints)
    ).format(**arguments)

def capture_prompts():
    '''
    Patch LLMCache.predict to record the prompt it was handed, formatted.

    The recorded string is what predict itself would have hashed, so a test can
    read the model's view of the prompt without a model.
    '''
    prompts = []

    def predict(self, prompt, **prompt_args):
        prompts.append(prompt.format(**prompt_args))
        return ''

    return (prompts, patch.object(LLMCache, 'predict', predict))

def mock_llm_cache():
    '''
    An LLMCache over MockLLM: no Bedrock client, no GraphRAGConfig.
    '''
    return LLMCache(llm=MockLLM(max_tokens=16), enable_cache=False)

def batch_config():
    '''
    A BatchConfig with the required fields. Nothing here reaches AWS.
    '''
    return BatchConfig(
        role_arn='arn:aws:iam::123456789012:role/test-role',
        region='us-east-1',
        bucket_name='test-bucket'
    )

@pytest.fixture(autouse=True)
def fresh_pool():
    """
    Driving a real extractor creates the LLM call pool, which is module state - a
    pool left behind decides what a later test sees, including tests in other
    files that read its size. Same guard as `test_llm_concurrency.py`.
    """
    shutdown_llm_pool()
    yield
    shutdown_llm_pool()

@pytest.fixture
def company_topic_constraints(company_ontology):
    '''
    Fixture for the align-level vocabulary block for the topics prompt.
    '''
    return company_ontology.format_as_prompt_constraint('align')

@pytest.fixture
def company_proposition_constraints(company_ontology):
    '''
    Fixture for the align-level entity-type hint for the propositions prompt.
    '''
    return company_ontology.format_as_proposition_constraint('align')


class TestShippedTemplatesAreUntouched:
    """The templates gain no placeholder, and composing nothing changes nothing."""

    @pytest.mark.parametrize('template', [EXTRACT_TOPICS_PROMPT, EXTRACT_PROPOSITIONS_PROMPT])
    def test_no_placeholder_in_the_shipped_templates(self, template):
        """Verify neither shipped template carries an ontology placeholder."""
        assert ONTOLOGY_CONSTRAINTS_PLACEHOLDER not in template
        assert 'ontology_constraints' not in template

    @pytest.mark.parametrize('template', [EXTRACT_TOPICS_PROMPT, EXTRACT_PROPOSITIONS_PROMPT])
    def test_composing_nothing_returns_the_template_itself(self, template):
        """Verify an empty block leaves the shipped template untouched."""
        assert with_ontology_constraints(template, '') is template


class TestEmptyConstraints:
    """An empty block is a no-op, on any template."""

    def test_returns_the_same_object(self):
        """Verify identity, not equality: nothing is rebuilt."""
        template = 'a template'
        assert with_ontology_constraints(template, '') is template

    def test_a_template_with_a_placeholder_is_left_alone(self):
        """Verify an empty block is not substituted into a placeholder.

        Substituting '' would leave the blank line the whole design avoids.
        """
        template = f'before\n\n{ONTOLOGY_CONSTRAINTS_PLACEHOLDER}\n\nafter'
        assert with_ontology_constraints(template, '') is template

    def test_a_template_with_an_anchor_is_left_alone(self):
        """Verify an empty block does not disturb the anchor."""
        template = f'preamble\n\n{EXTRACT_TOPICS_ANCHOR}\n'
        assert with_ontology_constraints(template, '') is template


class TestPlaceholderInsertion:
    """A custom template may choose its own insertion point."""

    def test_substitutes_at_the_placeholder(self):
        """Verify the block replaces the placeholder in place."""
        template = f'before\n\n{ONTOLOGY_CONSTRAINTS_PLACEHOLDER}\n\nafter'
        composed = with_ontology_constraints(template, 'BLOCK')
        assert composed == 'before\n\nBLOCK\n\nafter'
        assert ONTOLOGY_CONSTRAINTS_PLACEHOLDER not in composed

    def test_the_placeholder_wins_over_an_anchor(self):
        """Verify an explicit placeholder takes precedence over the anchor."""
        template = (
            f'{ONTOLOGY_CONSTRAINTS_PLACEHOLDER}\n\nmiddle\n\n{EXTRACT_TOPICS_ANCHOR}'
        )
        composed = with_ontology_constraints(template, 'BLOCK')
        assert composed.startswith('BLOCK\n\nmiddle')
        assert composed.endswith(EXTRACT_TOPICS_ANCHOR)

    def test_every_placeholder_is_substituted(self):
        """Verify a template repeating the placeholder gets the block at each."""
        template = f'{ONTOLOGY_CONSTRAINTS_PLACEHOLDER}|{ONTOLOGY_CONSTRAINTS_PLACEHOLDER}'
        assert with_ontology_constraints(template, 'BLOCK') == 'BLOCK|BLOCK'


class TestAnchorInsertion:
    """The shipped templates are recognized by their documented anchors."""

    @pytest.mark.parametrize('template,anchor', [
        (EXTRACT_TOPICS_PROMPT, EXTRACT_TOPICS_ANCHOR),
        (EXTRACT_PROPOSITIONS_PROMPT, EXTRACT_PROPOSITIONS_ANCHOR),
    ])
    def test_each_shipped_template_contains_its_anchor(self, template, anchor):
        """Verify the anchors are real strings in the templates they name."""
        assert template.count(anchor) == 1

    @pytest.mark.parametrize('template,anchor', [
        (EXTRACT_TOPICS_PROMPT, EXTRACT_TOPICS_ANCHOR),
        (EXTRACT_PROPOSITIONS_PROMPT, EXTRACT_PROPOSITIONS_ANCHOR),
    ])
    def test_the_block_lands_immediately_before_the_anchor(self, template, anchor):
        """Verify the block is inserted ahead of the closing admonition."""
        composed = with_ontology_constraints(template, 'BLOCK')
        assert f'BLOCK\n\n{anchor}' in composed

    @pytest.mark.parametrize('template', [EXTRACT_TOPICS_PROMPT, EXTRACT_PROPOSITIONS_PROMPT])
    def test_the_anchor_survives_the_insertion(self, template):
        """Verify the instruction the block is placed against is still there."""
        composed = with_ontology_constraints(template, 'BLOCK')
        for anchor in [EXTRACT_TOPICS_ANCHOR, EXTRACT_PROPOSITIONS_ANCHOR]:
            assert composed.count(anchor) == template.count(anchor)

    def test_the_block_precedes_the_payload_in_the_topics_prompt(self):
        """Verify the vocabulary is read before the propositions it applies to."""
        composed = with_ontology_constraints(EXTRACT_TOPICS_PROMPT, 'BLOCK')
        assert composed.index('BLOCK') < composed.index('<propositions>')

    def test_the_block_precedes_the_payload_in_the_propositions_prompt(self):
        """Verify the hint is read before the text it applies to."""
        composed = with_ontology_constraints(EXTRACT_PROPOSITIONS_PROMPT, 'BLOCK')
        assert composed.index('BLOCK') < composed.index('<text>')

    def test_only_the_first_occurrence_of_an_anchor_is_used(self):
        """Verify a template repeating an anchor receives the block once."""
        template = f'{EXTRACT_TOPICS_ANCHOR}\n\nand again: {EXTRACT_TOPICS_ANCHOR}'
        composed = with_ontology_constraints(template, 'BLOCK')
        assert composed.count('BLOCK') == 1
        assert composed.startswith(f'BLOCK\n\n{EXTRACT_TOPICS_ANCHOR}')


class TestAppendInsertion:
    """A custom template with neither marker still receives the vocabulary."""

    def test_appends_when_there_is_no_placeholder_and_no_anchor(self):
        """Verify the block is appended rather than dropped."""
        template = 'my own prompt\n\n<text>\n{text}\n</text>\n'
        composed = with_ontology_constraints(template, 'BLOCK')
        assert composed == f'{template}\n\nBLOCK'

    def test_the_custom_template_is_preserved_verbatim(self):
        """Verify nothing in the custom template is rewritten."""
        template = 'my own prompt'
        composed = with_ontology_constraints(template, 'BLOCK')
        assert composed.startswith(template)


class TestBraceHandling:
    """A brace from the ontology must not be read as a prompt argument.

    `PromptTemplate.format` is llama-index's `SafeFormatter`, a regex over
    `{name}`, not `str.format`: an unknown name is left exactly as written and
    nothing is raised. So the case worth guarding is the narrow one - a brace
    group that happens to name a prompt argument, which would otherwise pull the
    chunk into the middle of the vocabulary block silently.
    """

    def test_a_prompt_argument_name_in_the_block_does_not_pull_in_the_chunk(self):
        """Verify a description naming {text} does not receive the chunk."""
        constraints = 'MENTIONS  text  "as in {text}"'
        rendered = render(
            EXTRACT_PROPOSITIONS_PROMPT, constraints, text=TEXT, source_info='src'
        )
        # Once for the payload, and not a second time inside the block.
        assert rendered.count(TEXT) == 1
        assert 'as in { text }' in rendered

    def test_the_padded_name_still_reads_as_what_the_ontology_said(self):
        """Verify neutralizing keeps the text legible rather than escaping it."""
        rendered = render(
            EXTRACT_PROPOSITIONS_PROMPT, 'HAS_SOURCE  text  "see {source_info}"',
            text=TEXT, source_info='example.txt'
        )
        assert 'see { source_info }' in rendered
        assert '{{' not in rendered

    def test_prose_braces_are_left_alone(self):
        """Verify a JSON example in a description is not rewritten."""
        constraints = 'HAS_CONFIG  text  "a JSON object, e.g. {"a": 1}"'
        rendered = render(
            EXTRACT_PROPOSITIONS_PROMPT, constraints, text=TEXT, source_info='src'
        )
        assert '{"a": 1}' in rendered

    def test_an_unbalanced_brace_is_left_alone(self):
        """Verify a lone brace passes through untouched."""
        rendered = render(
            EXTRACT_PROPOSITIONS_PROMPT, 'HAS_SET  text  "{"', text=TEXT, source_info='src'
        )
        assert '"{"' in rendered

    def test_an_unknown_name_is_not_padded_away(self):
        """Verify only the substitution hazard is neutralized, not the words."""
        rendered = render(
            EXTRACT_PROPOSITIONS_PROMPT, 'HAS_SLOT  text  "{curly}"',
            text=TEXT, source_info='src'
        )
        # 'curly' is no prompt argument, so nothing could have substituted it -
        # but the padding rule is applied by shape, not by knowing the argument
        # names, which composition happens too early to know.
        assert '"{ curly }"' in rendered


class TestTopicExtractorRenderPoint:
    """TopicExtractor composes the block at the point it renders the prompt."""

    def extractor(self, **kwargs):
        from graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor import (
            TopicExtractor,
        )
        return TopicExtractor(llm=mock_llm_cache(), num_workers=1, **kwargs)

    def test_the_field_defaults_to_empty(self):
        """Verify an extractor with no ontology carries no constraints."""
        assert self.extractor().ontology_constraints == ''

    @pytest.mark.asyncio
    async def test_a_no_ontology_prompt_is_the_shipped_prompt(self):
        """Verify the prompt with no ontology is the shipped template's."""
        (prompts, patched) = capture_prompts()
        with patched:
            await self.extractor().aextract([TextNode(text=TEXT, id_='chunk-1')])

        expected = render(
            EXTRACT_TOPICS_PROMPT, '',
            text=format_text(TEXT),
            preferred_entity_classifications=format_list([]),
            preferred_topics=format_list([]),
        )
        assert prompts == [expected]

    @pytest.mark.asyncio
    async def test_the_vocabulary_reaches_the_prompt(self, company_topic_constraints):
        """Verify a configured block is in the prompt the model is handed."""
        (prompts, patched) = capture_prompts()
        with patched:
            await self.extractor(
                ontology_constraints=company_topic_constraints
            ).aextract([TextNode(text=TEXT, id_='chunk-1')])

        assert VOCABULARY_HEADER in prompts[0]
        assert 'WORKS_FOR' in prompts[0]
        assert f'{company_topic_constraints}\n\n{EXTRACT_TOPICS_ANCHOR}' in prompts[0]

    @pytest.mark.asyncio
    async def test_an_ontology_changes_the_cache_key(self, company_topic_constraints):
        """Verify the two runs would not share a cached response."""
        (prompts, patched) = capture_prompts()
        node = TextNode(text=TEXT, id_='chunk-1')
        with patched:
            await self.extractor().aextract([node])
            await self.extractor(
                ontology_constraints=company_topic_constraints
            ).aextract([node])

        (without, with_ontology) = (cache_key_of(prompt) for prompt in prompts)
        assert without != with_ontology


class TestLLMPropositionExtractorRenderPoint:
    """LLMPropositionExtractor composes the entity-type hint the same way."""

    def extractor(self, **kwargs):
        from graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor import (
            LLMPropositionExtractor,
        )
        return LLMPropositionExtractor(llm=mock_llm_cache(), num_workers=1, **kwargs)

    def test_the_field_defaults_to_empty(self):
        """Verify an extractor with no ontology carries no constraints."""
        assert self.extractor().ontology_constraints == ''

    @pytest.mark.asyncio
    async def test_a_no_ontology_prompt_is_the_shipped_prompt(self):
        """Verify the prompt with no ontology is the shipped template's."""
        (prompts, patched) = capture_prompts()
        with patched:
            await self.extractor().aextract([TextNode(text=TEXT, id_='chunk-1')])

        expected = render(
            EXTRACT_PROPOSITIONS_PROMPT, '',
            text=TEXT,
            source_info='',
            exclude_cache_keys=['source_info'],
        )
        assert prompts == [expected]

    @pytest.mark.asyncio
    async def test_the_entity_types_reach_the_prompt(self, company_proposition_constraints):
        """Verify the entity-type hint is in the prompt, at the anchor."""
        (prompts, patched) = capture_prompts()
        with patched:
            await self.extractor(
                ontology_constraints=company_proposition_constraints
            ).aextract([TextNode(text=TEXT, id_='chunk-1')])

        assert ENTITY_TYPES_HEADER in prompts[0]
        assert f'{company_proposition_constraints}\n\n{EXTRACT_PROPOSITIONS_ANCHOR}' in prompts[0]

    @pytest.mark.asyncio
    async def test_an_ontology_changes_the_cache_key(self, company_proposition_constraints):
        """Verify the two runs would not share a cached response."""
        (prompts, patched) = capture_prompts()
        node = TextNode(text=TEXT, id_='chunk-1')
        with patched:
            await self.extractor().aextract([node])
            await self.extractor(
                ontology_constraints=company_proposition_constraints
            ).aextract([node])

        (without, with_ontology) = (cache_key_of(prompt) for prompt in prompts)
        assert without != with_ontology


class TestBatchTopicExtractorSync:
    """The batch topic path carries the block into the request body."""

    MODULE = 'graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync'

    def extractor(self, tmp_path, **kwargs):
        from graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync import (
            BatchTopicExtractorSync,
        )
        return BatchTopicExtractorSync(
            batch_config=batch_config(),
            llm=mock_llm_cache(),
            batch_inference_dir=str(tmp_path / 'batch-topics'),
            **kwargs
        )

    def request_body(self, tmp_path, **kwargs):
        '''
        Run _get_json with get_request_body reduced to the message text.

        The real get_request_body branches on the Bedrock model id, which has
        nothing to do with what this test is about.
        '''
        extractor = self.extractor(tmp_path, **kwargs)
        with patch(
            f'{self.MODULE}.get_request_body',
            lambda llm, messages, parameters: [message.content for message in messages]
        ):
            return extractor._get_json(
                TextNode(text=TEXT, id_='chunk-1'), MockLLM(max_tokens=16), {}
            )

    def test_the_field_defaults_to_empty(self, tmp_path):
        """Verify the inherited field is there and empty by default."""
        assert self.extractor(tmp_path).ontology_constraints == ''

    def test_the_vocabulary_reaches_the_request_body(self, tmp_path, company_topic_constraints):
        """Verify the rendered modelInput contains the vocabulary block."""
        body = self.request_body(tmp_path, ontology_constraints=company_topic_constraints)
        rendered = '\n'.join(body['modelInput'])
        assert VOCABULARY_HEADER in rendered
        assert f'{company_topic_constraints}\n\n{EXTRACT_TOPICS_ANCHOR}' in rendered

    def test_no_ontology_leaves_the_request_body_alone(self, tmp_path):
        """Verify the batch path adds nothing when nothing is configured."""
        body = self.request_body(tmp_path)
        rendered = '\n'.join(body['modelInput'])
        assert VOCABULARY_HEADER not in rendered
        assert body['recordId'] == 'chunk-1'

    def test_run_non_batch_extractor_forwards_the_block(
        self, tmp_path, company_topic_constraints
    ):
        """Verify the fallback extractor is built with the same constraints."""
        extractor = self.extractor(tmp_path, ontology_constraints=company_topic_constraints)
        inner = Mock()
        inner.extract.return_value = [{TOPICS_KEY: {'topics': []}}]

        with patch(f'{self.MODULE}.TopicExtractor', return_value=inner) as constructed:
            extractor._run_non_batch_extractor([TextNode(text=TEXT, id_='chunk-1')])

        assert constructed.call_args.kwargs['ontology_constraints'] == company_topic_constraints


class TestBatchLLMPropositionExtractorSync:
    """The batch proposition path carries the hint into the request body."""

    MODULE = 'graphrag_toolkit.lexical_graph.indexing.extract.batch_llm_proposition_extractor_sync'

    def extractor(self, tmp_path, **kwargs):
        from graphrag_toolkit.lexical_graph.indexing.extract.batch_llm_proposition_extractor_sync import (
            BatchLLMPropositionExtractorSync,
        )
        return BatchLLMPropositionExtractorSync(
            batch_config=batch_config(),
            llm=mock_llm_cache(),
            batch_inference_dir=str(tmp_path / 'batch-propositions'),
            **kwargs
        )

    def request_body(self, tmp_path, **kwargs):
        '''
        Run _get_json with get_request_body reduced to the message text.
        '''
        extractor = self.extractor(tmp_path, **kwargs)
        with patch(
            f'{self.MODULE}.get_request_body',
            lambda llm, messages, parameters: [message.content for message in messages]
        ):
            return extractor._get_json(
                TextNode(text=TEXT, id_='chunk-1'), MockLLM(max_tokens=16), {}
            )

    def test_the_field_defaults_to_empty(self, tmp_path):
        """Verify the inherited field is there and empty by default."""
        assert self.extractor(tmp_path).ontology_constraints == ''

    def test_the_entity_types_reach_the_request_body(
        self, tmp_path, company_proposition_constraints
    ):
        """Verify the rendered modelInput contains the entity-type hint."""
        body = self.request_body(tmp_path, ontology_constraints=company_proposition_constraints)
        rendered = '\n'.join(body['modelInput'])
        assert ENTITY_TYPES_HEADER in rendered
        assert f'{company_proposition_constraints}\n\n{EXTRACT_PROPOSITIONS_ANCHOR}' in rendered

    def test_no_ontology_leaves_the_request_body_alone(self, tmp_path):
        """Verify the batch path adds nothing when nothing is configured."""
        body = self.request_body(tmp_path)
        rendered = '\n'.join(body['modelInput'])
        assert ENTITY_TYPES_HEADER not in rendered
        assert body['recordId'] == 'chunk-1'

    def test_run_non_batch_extractor_forwards_the_block(
        self, tmp_path, company_proposition_constraints
    ):
        """Verify the fallback extractor is built with the same constraints."""
        extractor = self.extractor(
            tmp_path, ontology_constraints=company_proposition_constraints
        )
        inner = Mock()
        inner.extract.return_value = [{PROPOSITIONS_KEY: []}]

        with patch(f'{self.MODULE}.LLMPropositionExtractor', return_value=inner) as constructed:
            extractor._run_non_batch_extractor([TextNode(text=TEXT, id_='chunk-1')])

        assert constructed.call_args.kwargs['ontology_constraints'] == company_proposition_constraints
