# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""`OntologyFilter` as a pipeline component.

Two claims that only a running pipeline can settle, and one that only a log can.

**It survives spawn.** `test_ontology_filter.py::TestItSurvivesTheProcessBoundary`
already pickles the component and asserts it classifies identically, holds only
plain data, and declares no custom `__init__`. Those are the preconditions, and
they are all in-process: a `pickle.dumps` round trip in the parent proves the
state is picklable, not that a *fresh interpreter* can rebuild the component. The
difference is real and it is where this feature's most plausible failure lives -
spawn re-imports every module from scratch, so an import that only resolves
because the parent imported something first, or a `GraphRAGConfig` value set
programmatically rather than from the environment, works in the parent and
vanishes in the worker. `run_pipeline` uses `ProcessPoolExecutor(mp_context=
'spawn')`, so the only way to know is to run one.

`FixedTopicsExtractor` stands in for the topic extractor because the real one
calls an LLM, and a `Mock` LLM is the one thing that certainly does not cross a
process boundary. It is declared at module level so `spawn` can import it by
name in the child - which is also why it is a class rather than a closure.

**Reporting.** `report_violations` is asserted in-process, because the claim is
about what the message says rather than about where it is emitted, and a log
record from a spawned worker does not reach `caplog` in the parent at all. That
limit is itself part of the contract: see
`test_it_reports_from_inside_the_worker`.
"""

import logging
import pickle

from pathlib import Path

import pytest
from llama_index.core.schema import Document, TextNode, TransformComponent

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract import ExtractionPipeline
from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import _init_worker
from graphrag_toolkit.lexical_graph import logging as graphrag_logging
from graphrag_toolkit.lexical_graph.logging import (
    apply_logging_config,
    get_applied_logging_config,
    set_logging_config,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import Ontology
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_filter import (
    FilterCounters,
    OntologyFilter,
)
from graphrag_toolkit.lexical_graph.indexing.model import (
    Entity,
    Fact,
    Relation,
    Statement,
    Topic,
    TopicCollection,
)

FIXTURES = Path(__file__).parent.parent.parent.parent / 'fixtures' / 'ontologies'

FILTER_MODULE = 'graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_filter'

@pytest.fixture(scope='module')
def company_index():
    return Ontology.load(FIXTURES / 'company.ttl').index()

@pytest.fixture
def restore_logging():
    """Put global logging state back after a test that reconfigures it.

    `set_logging_config` calls `logging.config.dictConfig`, which replaces the
    root logger's handlers process-wide. Without this, one test here would change
    how every later test in the session logs - and, because `pytest`'s own
    capturing sits on those handlers, could quietly break `caplog` assertions in
    the classes below.
    """
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_config = get_applied_logging_config()

    yield

    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)

    # Reset the remembered config through the module attribute rather than
    # `apply_logging_config`, which cannot restore None - and a stale config left
    # here would be propagated to the workers of every later spawned run.
    graphrag_logging._applied_logging_config = saved_config

def topics_the_model_might_emit():
    """One resolvable relation, one resolvable attribute, one of neither.

    Spelled the way `parse_extracted_topics` spells things - upper case with
    spaces - because that is what the filter has to recognise, and the point of
    the run is that it recognises it in a process that was not there when the
    ontology was loaded.
    """
    return TopicCollection(topics=[Topic(
        value='Employment',
        entities=[
            Entity(value='Amy Bell', classification='Person'),
            Entity(value='Example Corp', classification='Company'),
        ],
        statements=[Statement(
            value='Amy Bell works for Example Corp',
            facts=[
                Fact(
                    subject=Entity(value='Amy Bell', classification='Person'),
                    predicate=Relation(value='WORKS FOR'),
                    object=Entity(value='Example Corp', classification='Company'),
                ),
                Fact(
                    subject=Entity(value='Example Corp', classification='Company'),
                    predicate=Relation(value='OFFICIAL NAME'),
                    complement=Entity(value='Example Corporation'),
                ),
                Fact(
                    subject=Entity(value='Amy Bell', classification='Person'),
                    predicate=Relation(value='HIRED BY'),
                    object=Entity(value='Example Corp', classification='Company'),
                ),
            ],
        )],
    )])

class FixedTopicsExtractor(TransformComponent):
    """A stand-in for `TopicExtractor` that needs no LLM and pickles.

    Declared at module level and holding only a dict, so `spawn` can rebuild it
    in a child from `(module, qualname)` plus its state. A lambda, a closure, or
    a `Mock` could do none of that, which is the whole reason this class exists.
    """

    topics:dict = {}

    @classmethod
    def class_name(cls) -> str:
        return 'FixedTopicsExtractor'

    def __call__(self, nodes, **kwargs):
        for node in nodes:
            node.metadata[TOPICS_KEY] = self.topics
        return nodes

def topics_from(source_documents):
    """Every `TopicCollection` the pipeline emitted, revalidated."""
    return [
        TopicCollection.model_validate(node.metadata[TOPICS_KEY])
        for source_document in source_documents
        for node in source_document.nodes
        if TOPICS_KEY in node.metadata
    ]

def facts_of(topics):
    return [f for t in topics.topics for s in t.statements for f in s.facts]

def predicates_of(topics):
    return [f.predicate.value for f in facts_of(topics)]

def run_spawned(components, documents, num_workers=2):
    """Run the real `ExtractionPipeline`, which runs a real spawned pool.

    Constructed directly rather than through `ExtractionPipeline.create`, which
    wraps `extract` in a `Pipe` for `|` chaining and so has no `extract` of its
    own. Same object underneath.
    """
    pipeline = ExtractionPipeline(components=components, num_workers=num_workers)
    return list(pipeline.extract(documents))

def node_for(topics):
    return TextNode(text='chunk text', metadata={TOPICS_KEY: topics.model_dump()})

def stand_in_extractor():
    return FixedTopicsExtractor(topics=topics_the_model_might_emit().model_dump())

# Each spawned run costs about four seconds, almost all of it interpreter startup,
# so the runs are module-scoped fixtures and the assertions read from them. Four
# runs rather than one because they are four different configurations, and a
# configuration is exactly what these tests are about.

@pytest.fixture(scope='module')
def align_run(company_index):
    """`align`: normalize, no gates. One document, so one worker does the work."""
    return topics_from(run_spawned(
        [stand_in_extractor(), OntologyFilter(index=company_index, normalize_names=True)],
        [Document(text='Amy Bell works for Example Corp.')],
    ))

@pytest.fixture(scope='module')
def strict_run(company_index):
    """`strict`: normalize and all four gates."""
    return topics_from(run_spawned(
        [
            stand_in_extractor(),
            OntologyFilter(
                index=company_index,
                normalize_names=True,
                enforce_entity_types=True,
                enforce_relationship_types=True,
                enforce_domain_range=True,
                enforce_datatypes=True,
            ),
        ],
        [Document(text='Amy Bell works for Example Corp.')],
    ))

@pytest.fixture(scope='module')
def unfiltered_run():
    """The control: the same spawned pipeline with no filter in it."""
    return topics_from(run_spawned(
        [stand_in_extractor()], [Document(text='Amy Bell works for Example Corp.')]
    ))

@pytest.fixture(scope='module')
def many_document_run(company_index):
    """Enough documents that `node_batcher` fills both workers."""
    return topics_from(run_spawned(
        [stand_in_extractor(), OntologyFilter(index=company_index, normalize_names=True)],
        [Document(text=f'Document {i}') for i in range(6)],
    ))

class TestItSurvivesASpawnedPipeline:
    """The pipeline-wiring claims, run rather than argued."""

    def test_normalization_happens_in_the_worker(self, align_run):
        """`WORKS FOR` becomes `worksFor` on the far side of the boundary.

        The assertion is on the *output* of a spawned run, so it fails if the
        component cannot be rebuilt in a child, if its index arrives empty, or if
        `ontology_filter.py` grows an import that only resolves in the parent.
        """
        assert align_run, 'the spawned pipeline returned no topics at all'
        for topics in align_run:
            assert 'worksFor' in predicates_of(topics)
            assert 'WORKS FOR' not in predicates_of(topics)

    def test_two_workers_both_filter(self, many_document_run):
        """`node_batcher` splits by `num_workers`, so with a single document only
        one child ever runs and a filter that failed to rebuild in the *second*
        interpreter would go unnoticed."""
        assert len(many_document_run) >= 6
        assert all('worksFor' in predicates_of(t) for t in many_document_run)

    def test_annotation_crosses_the_boundary_too(self, align_run):
        """The annotation is written once, in extraction, and the
        build stage reads it rather than an ontology - so it has to arrive on the
        node that comes back out of the worker, not merely be computable."""
        for topics in align_run:
            annotated = [f for f in facts_of(topics) if f.predicate.propertyIri]
            assert annotated, 'nothing was annotated in the worker'
            for fact in annotated:
                assert fact.predicate.canonicalName
                assert fact.subject.classIri

    def test_enforcement_drops_in_the_worker(self, strict_run):
        """The gates, not just the rename. `HIRED BY` resolves to nothing the
        ontology declares, so `strict`'s relationship-type gate must discard it -
        in the child, where the decision actually gets made."""
        assert strict_run
        for topics in strict_run:
            assert 'HIRED BY' not in predicates_of(topics)
            assert 'worksFor' in predicates_of(topics)

    def test_the_unfiltered_pipeline_is_the_control(self, unfiltered_run):
        """Without the filter the same spawned run leaves `WORKS FOR` alone.

        Otherwise every assertion above could be satisfied by a stub extractor
        that happened to emit the authored spelling, and the tests would prove
        nothing about the filter at all.
        """
        assert unfiltered_run
        for topics in unfiltered_run:
            assert 'WORKS FOR' in predicates_of(topics)
            assert 'worksFor' not in predicates_of(topics)

    def test_the_stand_in_extractor_is_itself_picklable(self):
        """Named so that a failure here reads as a problem with the test's own
        scaffolding rather than with the filter."""
        extractor = stand_in_extractor()

        assert pickle.loads(pickle.dumps(extractor)).topics == extractor.topics

class TestTheReportIsReachableFromAWorker:
    """That the report is *emitted* is not that it is *seen*.

    Extraction components run only inside spawn-started workers, and until this
    was fixed a worker's logging was never configured: `_init_worker` re-applied
    the `GraphRAGConfig` snapshot but not `logging.config.dictConfig` state, which
    is separate global state and does not travel in the snapshot. So a worker's
    root logger sat at WARNING with no handler but `lastResort`, and every line
    `report_violations=True` produced was discarded. The feature emitted nothing
    in the only code path that runs it, and every in-process assertion about the
    report's wording passed throughout.

    `caplog` cannot catch this: a child's log record never reaches the parent's
    handlers. A file handler can, because the child opens the same path in append
    mode, so this asserts on the file the parent asked for.
    """

    def test_the_report_reaches_a_configured_handler(self, company_index, tmp_path, restore_logging):
        log_file = tmp_path / 'extraction.log'
        set_logging_config('INFO', filename=str(log_file))

        run_spawned(
            [
                stand_in_extractor(),
                OntologyFilter(
                    index=company_index,
                    normalize_names=True,
                    enforce_relationship_types=True,
                    report_violations=True,
                ),
            ],
            [Document(text='Amy Bell works for Example Corp.')],
            num_workers=1,
        )

        assert log_file.exists(), 'the worker never wrote to the configured log file'
        written = log_file.read_text()
        assert 'Ontology filter' in written, written
        assert 'enforce_relationship_types: 1' in written, written

    def test_an_unconfigured_parent_leaves_the_worker_alone(self, restore_logging):
        """None means "the parent never configured logging", and must not be
        turned into a config in the worker - a library that starts emitting to
        stdout because it spawned a process is worse than one that stays quiet."""
        root = logging.getLogger()
        root.setLevel(logging.CRITICAL)
        handlers_before = root.handlers[:]

        apply_logging_config(None)

        assert root.level == logging.CRITICAL
        assert root.handlers == handlers_before

    def test_the_worker_initializer_applies_it(self, restore_logging):
        """`_init_worker` is the seam, so it is asserted directly as well as
        end-to-end: the end-to-end test above would also pass if some other part
        of the stack happened to configure logging in the child."""
        set_logging_config('INFO')
        config = get_applied_logging_config()
        assert config is not None

        logging.getLogger().setLevel(logging.CRITICAL)

        _init_worker(GraphRAGConfig.get_config_snapshot(), config)

        assert logging.getLogger().level == logging.INFO

    def test_the_config_it_propagates_is_picklable(self, restore_logging):
        """It crosses the boundary in `initargs`, so an unpicklable entry would
        fail the pool's startup rather than degrade quietly."""
        set_logging_config('INFO')

        assert pickle.loads(pickle.dumps(get_applied_logging_config())) is not None

    def test_repeated_configuration_does_not_accumulate_handlers(self, tmp_path, restore_logging):
        """`set_advanced_logging_config` deep-copies its base config. With the
        shallow copy it used to make, a second call appended `file_handler` to a
        list shared with the module-level base, so every later call inherited the
        previous call's log file."""
        set_logging_config('INFO', filename=str(tmp_path / 'first.log'))
        set_logging_config('INFO', filename=str(tmp_path / 'second.log'))

        handlers = get_applied_logging_config()['loggers']['']['handlers']

        assert handlers.count('file_handler') == 1, handlers

    def test_configuring_without_a_filename_adds_no_file_handler(self, restore_logging):
        set_logging_config('INFO')

        assert 'file_handler' not in get_applied_logging_config()['loggers']['']['handlers']

class TestTheViolationReport:
    """What `report_violations=True` actually says."""

    def strict_filter(self, index, report_violations=True):
        return OntologyFilter(
            index=index,
            normalize_names=True,
            enforce_entity_types=True,
            enforce_relationship_types=True,
            enforce_domain_range=True,
            enforce_datatypes=True,
            report_violations=report_violations,
        )

    def test_nothing_is_logged_when_reporting_is_off(self, company_index, caplog):
        with caplog.at_level(logging.DEBUG, logger=FILTER_MODULE):
            self.strict_filter(company_index, report_violations=False)(
                [node_for(topics_the_model_might_emit())]
            )

        assert caplog.records == []

    def test_it_reports_rewrites_and_drops_at_info(self, company_index, caplog):
        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            self.strict_filter(company_index)([node_for(topics_the_model_might_emit())])

        assert len(caplog.records) == 1
        message = caplog.records[0].getMessage()
        assert 'predicates rewritten: ' in message
        assert 'classifications rewritten: ' in message
        assert 'facts dropped: 1' in message

    def test_it_names_the_setting_that_dropped_the_fact(self, company_index, caplog):
        """The count alone is not actionable. `HIRED BY` is dropped by the
        relationship-type gate, and the report has to say which gate, because that
        is the setting a user would turn off."""
        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            self.strict_filter(company_index)([node_for(topics_the_model_might_emit())])

        message = caplog.records[0].getMessage()
        assert 'enforce_relationship_types: 1' in message

    def test_it_never_names_a_gate_that_did_not_run(self, company_index, caplog):
        """A dimension the user left off has an unbreakable zero, so it must not
        appear - a report listing `enforce_datatypes: 0` invites the reader to
        conclude datatypes were checked."""
        ontology_filter = OntologyFilter(
            index=company_index,
            normalize_names=True,
            enforce_relationship_types=True,
            report_violations=True,
        )

        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            ontology_filter([node_for(topics_the_model_might_emit())])

        message = caplog.records[0].getMessage()
        assert 'enforce_relationship_types: 1' in message
        for setting in ('enforce_entity_types', 'enforce_domain_range', 'enforce_datatypes'):
            assert setting not in message, setting

    def test_a_call_that_changed_nothing_stays_below_info(self, company_index, caplog):
        """Every gate is off at `align`, so on a corpus the ontology already
        matches most batches change nothing. Those must not each emit a line of
        zeros at INFO, or the batches that did something get buried."""
        already_normalized = OntologyFilter(
            index=company_index, normalize_names=True, report_violations=True
        )
        node = node_for(topics_the_model_might_emit())
        already_normalized([node])

        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            already_normalized([node])

        assert caplog.records == []

        with caplog.at_level(logging.DEBUG, logger=FILTER_MODULE):
            already_normalized([node])

        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.DEBUG

    def test_it_says_how_many_nodes_the_counts_cover(self, company_index, caplog):
        """One `__call__` is one node batch in one worker, so a reader has to be
        able to add the lines up. Without the node count the line looks like a
        run-wide total."""
        nodes = [node_for(topics_the_model_might_emit()) for _ in range(3)]

        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            self.strict_filter(company_index)(nodes)

        assert 'nodes: 3' in caplog.records[0].getMessage()

    def test_a_node_without_topics_is_not_counted(self, company_index, caplog):
        """The count is of nodes the filter looked at, not of nodes it was
        handed - otherwise a batch of chunks that never reached the extractor
        reads as a batch that was filtered."""
        nodes = [node_for(topics_the_model_might_emit()), TextNode(text='no topics')]

        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            self.strict_filter(company_index)(nodes)

        assert 'nodes: 1' in caplog.records[0].getMessage()

    def test_one_line_per_call_and_no_state_between_calls(self, company_index, caplog):
        """The counters are a local, so a second call reports its own work and not
        the sum. Asserted because an instance attribute would pass every test
        above and then read as a total that is really one worker's share of one
        batch."""
        ontology_filter = self.strict_filter(company_index)

        with caplog.at_level(logging.INFO, logger=FILTER_MODULE):
            ontology_filter([node_for(topics_the_model_might_emit())])
            ontology_filter([node_for(topics_the_model_might_emit())])

        assert len(caplog.records) == 2
        assert caplog.records[0].getMessage() == caplog.records[1].getMessage()

    def test_it_reports_from_inside_the_worker(self, company_index):
        """The report is emitted where the filtering happens, which is a child
        process - so it reaches the configured logging handlers of that process
        and not `caplog` in the parent.

        Recorded as a test rather than a comment because it is the reason every
        other test in this class runs in-process, and because it is the thing a
        user will notice: `report_violations=True` with `num_workers=4` produces
        four processes' worth of lines, none of which are totals.
        """
        source = OntologyFilter._report.__doc__

        assert 'one node batch in one worker' in source

class TestTheCounterSummary:
    """`FilterCounters.summary` on its own, where the wording is cheap to pin."""

    def test_the_three_totals_are_always_present(self):
        summary = FilterCounters().summary()

        assert 'classifications rewritten: 0' in summary
        assert 'predicates rewritten: 0' in summary
        assert 'facts dropped: 0' in summary

    def test_no_breakdown_when_nothing_was_dropped(self):
        assert 'dropped by' not in FilterCounters(predicates_rewritten=3).summary()

    def test_the_breakdown_follows_the_gate_order(self):
        """Broadest gate first, matching `_enforce`, so the line reads in the
        order the decisions were taken."""
        summary = FilterCounters(
            facts_dropped_entity_type=1,
            facts_dropped_relationship_type=2,
            facts_dropped_domain_range=3,
            facts_dropped_datatype=4,
        ).summary()

        settings = [
            'enforce_entity_types', 'enforce_relationship_types',
            'enforce_domain_range', 'enforce_datatypes',
        ]
        positions = [summary.index(setting) for setting in settings]
        assert positions == sorted(positions), summary
        assert 'facts dropped: 10' in summary

    def test_annotation_alone_is_not_a_change(self):
        """Every surviving fact is annotated at every level, so counting it would
        make `any_change` always true and the report would carry no signal."""
        assert FilterCounters().any_change() is False

    @pytest.mark.parametrize('field', [
        'classifications_rewritten', 'predicates_rewritten',
        'facts_dropped_entity_type', 'facts_dropped_relationship_type',
        'facts_dropped_domain_range', 'facts_dropped_datatype',
    ])
    def test_any_single_count_is_a_change(self, field):
        assert FilterCounters(**{field: 1}).any_change() is True
