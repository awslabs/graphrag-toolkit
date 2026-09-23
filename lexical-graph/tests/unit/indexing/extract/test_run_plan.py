# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import pytest

from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.indexing.extract.run_plan import (
    RunPlan,
    RunPlanMismatch,
    RunPlanStore,
    run_plan_key,
)


def _plan(**overrides):
    fields = {
        'run_id': 'run-1',
        'document_ids': ['doc-a', 'doc-b', 'doc-c'],
        'num_workers': 8,
        'batch_size': 4,
        'config': {'extraction_llm': 'a-model', 'aws_region': 'us-east-1'},
    }
    fields.update(overrides)
    return RunPlan(**fields)


def _store(**kwargs):
    return RunPlanStore(bucket_name='b', key_prefix='p', collection_id='c', **kwargs)


def _s3_holding(plan=None, body=None):
    """An S3 client holding one plan, or none."""
    mock_s3 = MagicMock()

    if plan is None and body is None:
        mock_s3.get_object.side_effect = _missing_key()
        return mock_s3

    stream = MagicMock()
    stream.read.return_value = (body if body is not None else plan.to_json()).encode('UTF-8')
    mock_s3.get_object.return_value = {'Body': stream}

    return mock_s3


def _missing_key():
    return ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')


def _already_written():
    return ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')


def _written(mock_s3):
    """What a write put in the bucket, keyed by S3 key."""
    return {
        call.kwargs['Key']: json.loads(call.kwargs['Body'].decode('UTF-8'))
        for call in mock_s3.put_object.call_args_list
    }


class TestARunWritesItsPlanBeforeSubmittingWork:
    """
    A restart can only divide the collection as the original run did if the
    original run wrote down how it divided it.
    """

    def test_the_plan_is_written_under_its_run_id(self):
        mock_s3 = _s3_holding()

        _store().resolve(_plan(), mock_s3)

        assert list(_written(mock_s3)) == ['p/c/_runs/run-1/plan.json']

    def test_the_plan_records_the_document_order(self):
        mock_s3 = _s3_holding()

        _store().resolve(_plan(document_ids=['doc-c', 'doc-a']), mock_s3)

        written = _written(mock_s3)['p/c/_runs/run-1/plan.json']
        assert written['document_ids'] == ['doc-c', 'doc-a']

    def test_the_plan_records_the_sizing_settings(self):
        mock_s3 = _s3_holding()

        _store().resolve(_plan(num_workers=8, batch_size=4), mock_s3)

        written = _written(mock_s3)['p/c/_runs/run-1/plan.json']
        assert (written['num_workers'], written['batch_size']) == (8, 4)

    def test_the_plan_records_the_configuration_it_ran_under(self):
        mock_s3 = _s3_holding()

        _store().resolve(_plan(config={'extraction_llm': 'a-model'}), mock_s3)

        written = _written(mock_s3)['p/c/_runs/run-1/plan.json']
        assert written['config'] == {'extraction_llm': 'a-model'}

    def test_the_plan_is_encrypted_as_everything_else_is(self):
        mock_s3 = _s3_holding()

        _store().resolve(_plan(), mock_s3)

        assert mock_s3.put_object.call_args.kwargs['ServerSideEncryption'] == 'AES256'

    def test_a_run_id_that_would_leave_the_collection_is_rejected(self):
        with pytest.raises(ValueError, match='run_id'):
            run_plan_key('p', 'c', '../../elsewhere')


class TestAPlanIsFixedForTheLifeOfItsRun:
    """
    A run that rewrote its plan would repartition itself, which is the one
    thing the plan exists to prevent.
    """

    def test_an_existing_plan_is_not_overwritten(self):
        mock_s3 = _s3_holding(_plan())

        _store().resolve(_plan(), mock_s3)

        assert mock_s3.put_object.call_count == 0

    def test_the_recorded_plan_is_what_a_restart_gets_back(self):
        mock_s3 = _s3_holding(_plan(num_workers=8))

        resolved = _store().resolve(_plan(num_workers=8), mock_s3)

        assert resolved.num_workers == 8


class TestARestartFollowsThePlanNotTheCurrentConfiguration:
    """
    Extraction clamps the worker count to the host's core count, so a restart
    on a smaller machine would otherwise divide the collection differently and
    pay for work the first run already did.
    """

    def test_the_recorded_worker_count_outlives_a_smaller_host(self):
        mock_s3 = _s3_holding(_plan(num_workers=8))

        resolved = _store().resolve(_plan(num_workers=8), mock_s3)

        assert resolved.num_workers == 8

    def test_the_recorded_document_order_is_used_when_the_collection_returns_shuffled(self):
        mock_s3 = _s3_holding(_plan(document_ids=['doc-a', 'doc-b', 'doc-c']))

        resolved = _store().resolve(_plan(document_ids=['doc-c', 'doc-b', 'doc-a']), mock_s3)

        assert resolved.document_ids == ['doc-a', 'doc-b', 'doc-c']


class TestAChangedConfigurationFailsTheRestart:
    """
    A setting that decides how the collection divides cannot change mid-run.
    The error names the setting, so the operator knows what to put back.
    """

    def test_a_smaller_host_adopts_the_recorded_worker_count_rather_than_failing(self):
        mock_s3 = _s3_holding(_plan(num_workers=8))

        resolved = _store().resolve(_plan(num_workers=2), mock_s3)

        assert resolved.num_workers == 8

    def test_a_changed_batch_size_fails_naming_it(self):
        mock_s3 = _s3_holding(_plan(batch_size=4))

        with pytest.raises(RunPlanMismatch, match='batch_size'):
            _store().resolve(_plan(batch_size=16), mock_s3)

    def test_a_document_that_has_gone_fails_naming_it(self):
        mock_s3 = _s3_holding(_plan(document_ids=['doc-a', 'doc-b']))

        with pytest.raises(RunPlanMismatch, match='doc-b'):
            _store().resolve(_plan(document_ids=['doc-a']), mock_s3)

    def test_a_document_that_was_added_fails_naming_it(self):
        mock_s3 = _s3_holding(_plan(document_ids=['doc-a']))

        with pytest.raises(RunPlanMismatch, match='doc-z'):
            _store().resolve(_plan(document_ids=['doc-a', 'doc-z']), mock_s3)

    def test_a_repeated_document_that_has_gone_fails_naming_it(self):
        mock_s3 = _s3_holding(_plan(document_ids=['doc-a', 'doc-b', 'doc-b', 'doc-c']))

        with pytest.raises(RunPlanMismatch, match='doc-b'):
            _store().resolve(_plan(document_ids=['doc-a', 'doc-b', 'doc-c']), mock_s3)

    def test_a_repeated_document_that_was_added_fails_naming_it(self):
        mock_s3 = _s3_holding(_plan(document_ids=['doc-a', 'doc-b', 'doc-c']))

        with pytest.raises(RunPlanMismatch, match='doc-b'):
            _store().resolve(_plan(document_ids=['doc-a', 'doc-b', 'doc-b', 'doc-c']), mock_s3)

    def test_a_setting_that_does_not_decide_the_division_is_allowed(self, caplog):
        mock_s3 = _s3_holding(_plan(config={'extraction_llm': 'a-model'}))

        resolved = _store().resolve(_plan(config={'extraction_llm': 'another-model'}), mock_s3)

        assert resolved.config['extraction_llm'] == 'a-model'
        assert 'extraction_llm' in caplog.text


class TestTheRunArtifactsAreNotReadAsSourceDocuments:
    """
    The plan lives inside the collection, where a delimited listing would
    otherwise return its directory as a document prefix.
    """

    def test_the_runs_directory_is_not_listed_as_a_source_document(self):
        from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import list_collection

        paginator = MagicMock()
        paginator.paginate.return_value = [{
            'CommonPrefixes': [{'Prefix': 'p/c/doc-a/'}, {'Prefix': 'p/c/_runs/'}],
            'Contents': [],
        }]

        source_doc_prefixes, _ = list_collection('b', 'p', 'c', paginator)

        assert source_doc_prefixes == ['p/c/doc-a/']

    def test_a_collection_holding_only_a_run_plan_is_still_recorded(self):
        # Extraction writes the plan before staging starts, so the collection
        # is never empty by the time it is recorded.
        from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import S3ChunkUploader

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            'KeyCount': 1,
            'CommonPrefixes': [{'Prefix': 'p/c/_runs/'}],
        }
        uploader = S3ChunkUploader(bucket_name='b', collection_prefix='p/c')

        uploader.record_collection('p', 'c', mock_s3)

        assert mock_s3.put_object.call_args.kwargs['Key'] == 'p/c/_staging.json'


class TestAPlanSurvivesWhatStorageDoesToIt:
    """
    Two runs of one id race for the plan, and a plan written by a later build
    is read by an earlier one. Neither may take the run down.
    """

    def test_a_run_that_loses_the_race_obeys_the_plan_that_won(self):
        winner = _plan(document_ids=['doc-a', 'doc-b'])
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = [_missing_key(), _body_of(winner)]
        mock_s3.put_object.side_effect = _already_written()

        resolved = _store().resolve(_plan(document_ids=['doc-a', 'doc-b']), mock_s3)

        assert resolved.document_ids == ['doc-a', 'doc-b']

    def test_a_run_that_loses_the_race_still_checks_the_plan_that_won(self):
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = [_missing_key(), _body_of(_plan(batch_size=4))]
        mock_s3.put_object.side_effect = _already_written()

        with pytest.raises(RunPlanMismatch, match='batch_size'):
            _store().resolve(_plan(batch_size=16), mock_s3)

    def test_a_plan_carrying_an_unknown_field_still_restarts(self, caplog):
        recorded = json.loads(_plan().to_json())
        recorded['written_by_a_later_build'] = True

        mock_s3 = _s3_holding(body=json.dumps(recorded))

        resolved = _store().resolve(_plan(), mock_s3)

        assert resolved.document_ids == ['doc-a', 'doc-b', 'doc-c']
        assert 'written_by_a_later_build' in caplog.text

    def test_a_plan_is_written_only_when_this_run_has_none(self):
        mock_s3 = _s3_holding()

        _store().resolve(_plan(), mock_s3)

        assert mock_s3.put_object.call_args.kwargs['IfNoneMatch'] == '*'

    def test_a_storage_error_that_is_not_a_missing_plan_is_not_swallowed(self):
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError({'Error': {'Code': 'AccessDenied'}}, 'GetObject')

        with pytest.raises(ClientError):
            _store().resolve(_plan(), mock_s3)


class TestASettingThatDoesNotDecideTheDivision:
    """
    Recorded for diagnosis, not enforced. A setting appearing or disappearing
    is as much a change as one whose value moved.
    """

    def _resolve_under(self, recorded, current):
        return _store().resolve(_plan(config=current), _s3_holding(_plan(config=recorded)))

    def test_a_setting_that_changed_value_is_reported(self, caplog):
        self._resolve_under({'aws_region': 'us-east-1'}, {'aws_region': 'us-west-2'})

        assert 'aws_region' in caplog.text

    def test_a_setting_that_is_no_longer_set_is_reported(self, caplog):
        self._resolve_under({'aws_region': 'us-east-1'}, {})

        assert 'aws_region' in caplog.text and 'unset' in caplog.text

    def test_a_setting_that_was_not_set_before_is_reported(self, caplog):
        self._resolve_under({}, {'aws_region': 'us-west-2'})

        assert 'aws_region' in caplog.text and 'unset' in caplog.text


def _body_of(plan):
    stream = MagicMock()
    stream.read.return_value = plan.to_json().encode('UTF-8')
    return {'Body': stream}
