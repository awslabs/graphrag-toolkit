# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for FM observability utilities.

This module tests observability initialization, metric collection, metric reporting,
and integration with the system. Uses mocks to avoid actual metric reporting to
external services.

Validates Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.9, 14.1
"""

import pytest
import time
import threading
import queue
from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass
from typing import Dict, Any, List

from graphrag_toolkit.lexical_graph.utils.fm_observability import (
    FMObservabilityQueuePoller,
    FMObservabilityStats,
    FMObservabilitySubscriber,
    ConsoleFMObservabilitySubscriber,
    StatPrintingSubscriber,
    FMObservabilityPublisher,
    BedrockEnabledTokenCountingHandler,
    FMObservabilityHandler,
    get_patched_llm_token_counts
)
from llama_index.core.callbacks.schema import CBEventType, EventPayload, CBEvent


# Fixtures

@pytest.fixture
def mock_fm_queue():
    """Fixture providing a mock observability queue."""
    with patch('graphrag_toolkit.lexical_graph.utils.fm_observability._fm_observability_queue') as mock_queue:
        mock_queue.get = Mock(side_effect=queue.Empty)
        mock_queue.put = Mock()
        yield mock_queue


@pytest.fixture
def sample_llm_event():
    """Fixture providing a sample LLM event."""
    return CBEvent(
        event_type=CBEventType.LLM,
        payload={
            'model': 'anthropic.claude-v2',
            'duration_millis': 1500
        },
        id_='event-1'
    )


@pytest.fixture
def sample_embedding_event():
    """Fixture providing a sample embedding event."""
    return CBEvent(
        event_type=CBEventType.EMBEDDING,
        payload={
            'model': 'amazon.titan-embed-text-v1',
            'duration_millis': 500
        },
        id_='event-2'
    )


@pytest.fixture
def sample_token_event():
    """Fixture providing a sample token count event."""
    return CBEvent(
        event_type=CBEventType.LLM,
        payload={
            'llm_prompt_token_count': 100,
            'llm_completion_token_count': 50
        },
        id_='event-3'
    )


@pytest.fixture
def mock_subscriber():
    """Fixture providing a mock observability subscriber."""
    subscriber = Mock(spec=FMObservabilitySubscriber)
    subscriber.on_new_stats = Mock()
    return subscriber


# Test Classes

class TestObservabilityInitialization:
    """Tests for observability initialization (test_observability_initialization)."""
    
    def test_fm_observability_stats_initialization(self):
        """Verify FMObservabilityStats initializes with zero values."""
        stats = FMObservabilityStats()
        
        assert stats.total_llm_duration_millis == 0
        assert stats.total_llm_count == 0
        assert stats.total_llm_prompt_tokens == 0
        assert stats.total_llm_completion_tokens == 0
        assert stats.total_embedding_duration_millis == 0
        assert stats.total_embedding_count == 0
        assert stats.total_embedding_tokens == 0

    
    def test_console_subscriber_initialization(self):
        """Verify ConsoleFMObservabilitySubscriber initializes correctly."""
        subscriber = ConsoleFMObservabilitySubscriber()
        
        assert isinstance(subscriber.all_stats, FMObservabilityStats)
        assert subscriber.all_stats.total_llm_count == 0
        assert subscriber.all_stats.total_embedding_count == 0
    
    def test_stat_printing_subscriber_initialization(self):
        """Verify StatPrintingSubscriber initializes with cost parameters."""
        subscriber = StatPrintingSubscriber(
            cost_per_thousand_input_tokens_llm=0.003,
            cost_per_thousand_output_tokens_llm=0.015,
            cost_per_thousand_embedding_tokens=0.0001
        )
        
        assert isinstance(subscriber.all_stats, FMObservabilityStats)
        assert subscriber.cost_per_thousand_input_tokens_llm == 0.003
        assert subscriber.cost_per_thousand_output_tokens_llm == 0.015
        assert subscriber.cost_per_thousand_embedding_tokens == 0.0001
    
    def test_fm_observability_handler_initialization(self):
        """Verify FMObservabilityHandler initializes with empty in-flight events."""
        handler = FMObservabilityHandler()
        
        assert isinstance(handler.in_flight_events, dict)
        assert len(handler.in_flight_events) == 0
    

class TestMetricCollection:
    """Tests for metric collection (test_metric_collection)."""
    
    def test_stats_update_with_llm_event(self, sample_llm_event):
        """Verify stats update correctly with LLM event."""
        stats = FMObservabilityStats()
        
        stats.on_event(sample_llm_event)
        
        assert stats.total_llm_count == 1
        assert stats.total_llm_duration_millis == 1500
    
    def test_stats_update_with_embedding_event(self, sample_embedding_event):
        """Verify stats update correctly with embedding event."""
        stats = FMObservabilityStats()
        
        stats.on_event(sample_embedding_event)
        
        assert stats.total_embedding_count == 1
        assert stats.total_embedding_duration_millis == 500
    
    def test_stats_update_with_token_counts(self, sample_token_event):
        """Verify stats update correctly with token count event."""
        stats = FMObservabilityStats()
        
        stats.on_event(sample_token_event)
        
        assert stats.total_llm_prompt_tokens == 100
        assert stats.total_llm_completion_tokens == 50
    
    def test_stats_update_with_embedding_tokens(self):
        """Verify stats update correctly with embedding token counts."""
        stats = FMObservabilityStats()
        event = CBEvent(
            event_type=CBEventType.EMBEDDING,
            payload={'embedding_token_count': 384},
            id_='event-4'
        )
        
        stats.on_event(event)
        
        assert stats.total_embedding_tokens == 384

    
    def test_stats_update_multiple_events(self, sample_llm_event, sample_embedding_event):
        """Verify stats accumulate correctly across multiple events."""
        stats = FMObservabilityStats()
        
        stats.on_event(sample_llm_event)
        stats.on_event(sample_embedding_event)
        
        assert stats.total_llm_count == 1
        assert stats.total_embedding_count == 1
        assert stats.total_llm_duration_millis == 1500
        assert stats.total_embedding_duration_millis == 500
    
    def test_stats_update_from_another_stats_object(self):
        """Verify stats can be updated from another stats object."""
        stats1 = FMObservabilityStats()
        stats1.total_llm_count = 5
        stats1.total_llm_prompt_tokens = 500
        stats1.total_llm_completion_tokens = 250
        
        stats2 = FMObservabilityStats()
        result = stats2.update(stats1)
        
        assert result is True  # Returns True when counts > 0
        assert stats2.total_llm_count == 5
        assert stats2.total_llm_prompt_tokens == 500
        assert stats2.total_llm_completion_tokens == 250
    
    def test_stats_update_returns_false_for_empty_stats(self):
        """Verify stats update returns False when no events were added."""
        stats1 = FMObservabilityStats()
        stats2 = FMObservabilityStats()
        
        result = stats2.update(stats1)
        
        assert result is False
    
    def test_average_llm_duration_calculation(self):
        """Verify average LLM duration is calculated correctly."""
        stats = FMObservabilityStats()
        stats.total_llm_duration_millis = 3000
        stats.total_llm_count = 2
        
        assert stats.average_llm_duration_millis == 1500

    
    def test_average_llm_duration_zero_when_no_calls(self):
        """Verify average LLM duration is zero when no LLM calls made."""
        stats = FMObservabilityStats()
        
        assert stats.average_llm_duration_millis == 0
    
    def test_total_llm_tokens_calculation(self):
        """Verify total LLM tokens combines prompt and completion tokens."""
        stats = FMObservabilityStats()
        stats.total_llm_prompt_tokens = 100
        stats.total_llm_completion_tokens = 50
        
        assert stats.total_llm_tokens == 150
    
    def test_average_llm_prompt_tokens_calculation(self):
        """Verify average LLM prompt tokens is calculated correctly."""
        stats = FMObservabilityStats()
        stats.total_llm_prompt_tokens = 300
        stats.total_llm_count = 3
        
        assert stats.average_llm_prompt_tokens == 100
    
    def test_average_llm_completion_tokens_calculation(self):
        """Verify average LLM completion tokens is calculated correctly."""
        stats = FMObservabilityStats()
        stats.total_llm_completion_tokens = 150
        stats.total_llm_count = 3
        
        assert stats.average_llm_completion_tokens == 50
    
    def test_average_llm_tokens_calculation(self):
        """Verify average LLM tokens is calculated correctly."""
        stats = FMObservabilityStats()
        stats.total_llm_prompt_tokens = 300
        stats.total_llm_completion_tokens = 150
        stats.total_llm_count = 3
        
        assert stats.average_llm_tokens == 150

    
    def test_average_embedding_duration_calculation(self):
        """Verify average embedding duration is calculated correctly."""
        stats = FMObservabilityStats()
        stats.total_embedding_duration_millis = 1000
        stats.total_embedding_count = 4
        
        assert stats.average_embedding_duration_millis == 250
    
    def test_average_embedding_tokens_calculation(self):
        """Verify average embedding tokens is calculated correctly."""
        stats = FMObservabilityStats()
        stats.total_embedding_tokens = 1536
        stats.total_embedding_count = 4
        
        assert stats.average_embedding_tokens == 384
    
    def test_handler_tracks_event_start(self):
        """Verify FMObservabilityHandler tracks event start correctly."""
        handler = FMObservabilityHandler()
        
        payload = {
            EventPayload.MESSAGES: [{'role': 'user', 'content': 'test'}],
            EventPayload.SERIALIZED: {'model': 'anthropic.claude-v2'}
        }
        
        event_id = handler.on_event_start(
            event_type=CBEventType.LLM,
            payload=payload,
            event_id='test-event-1'
        )
        
        assert event_id == 'test-event-1'
        assert 'test-event-1' in handler.in_flight_events
        assert handler.in_flight_events['test-event-1'].payload['model'] == 'anthropic.claude-v2'
    
    def test_handler_ignores_events_in_ignore_list(self):
        """Verify handler ignores events in the ignore list."""
        handler = FMObservabilityHandler(event_ends_to_ignore=[CBEventType.LLM])
        
        payload = {
            EventPayload.MESSAGES: [{'role': 'user', 'content': 'test'}],
            EventPayload.SERIALIZED: {'model': 'anthropic.claude-v2'}
        }
        
        handler.on_event_start(
            event_type=CBEventType.LLM,
            payload=payload,
            event_id='test-event-2'
        )
        
        assert 'test-event-2' not in handler.in_flight_events



class TestMetricReporting:
    """Tests for metric reporting (test_metric_reporting)."""
    
    def test_console_subscriber_reports_stats(self, capsys):
        """Verify ConsoleFMObservabilitySubscriber prints stats correctly."""
        subscriber = ConsoleFMObservabilitySubscriber()
        
        stats = FMObservabilityStats()
        stats.total_llm_count = 5
        stats.total_llm_prompt_tokens = 500
        stats.total_llm_completion_tokens = 250
        stats.total_embedding_count = 3
        stats.total_embedding_tokens = 1152
        
        subscriber.on_new_stats(stats)
        
        captured = capsys.readouterr()
        assert 'LLM: count: 5' in captured.out
        assert 'total_prompt_tokens: 500' in captured.out
        assert 'total_completion_tokens: 250' in captured.out
        assert 'Embeddings: count: 3' in captured.out
        assert 'total_tokens: 1152' in captured.out
    
    def test_console_subscriber_does_not_print_empty_stats(self, capsys):
        """Verify ConsoleFMObservabilitySubscriber doesn't print when no events."""
        subscriber = ConsoleFMObservabilitySubscriber()
        
        stats = FMObservabilityStats()
        subscriber.on_new_stats(stats)
        
        captured = capsys.readouterr()
        assert captured.out == ''
    
    def test_stat_printing_subscriber_accumulates_stats(self):
        """Verify StatPrintingSubscriber accumulates stats correctly."""
        subscriber = StatPrintingSubscriber(
            cost_per_thousand_input_tokens_llm=0.003,
            cost_per_thousand_output_tokens_llm=0.015,
            cost_per_thousand_embedding_tokens=0.0001
        )
        
        stats = FMObservabilityStats()
        stats.total_llm_count = 2
        stats.total_llm_prompt_tokens = 200
        stats.total_llm_completion_tokens = 100
        
        subscriber.on_new_stats(stats)
        
        assert subscriber.all_stats.total_llm_count == 2
        assert subscriber.all_stats.total_llm_prompt_tokens == 200
        assert subscriber.all_stats.total_llm_completion_tokens == 100

    
    def test_stat_printing_subscriber_estimates_costs(self):
        """Verify StatPrintingSubscriber estimates costs correctly."""
        subscriber = StatPrintingSubscriber(
            cost_per_thousand_input_tokens_llm=0.003,
            cost_per_thousand_output_tokens_llm=0.015,
            cost_per_thousand_embedding_tokens=0.0001
        )
        
        subscriber.all_stats.total_llm_prompt_tokens = 1000
        subscriber.all_stats.total_llm_completion_tokens = 500
        subscriber.all_stats.total_embedding_tokens = 10000
        
        cost = subscriber.estimate_costs()
        
        # Expected: (1000/1000 * 0.003) + (500/1000 * 0.015) + (10000/1000 * 0.0001)
        # = 0.003 + 0.0075 + 0.001 = 0.0115
        assert abs(cost - 0.0115) < 0.0001
    
    def test_stat_printing_subscriber_returns_stats_dict(self):
        """Verify StatPrintingSubscriber returns comprehensive stats dictionary."""
        subscriber = StatPrintingSubscriber(
            cost_per_thousand_input_tokens_llm=0.003,
            cost_per_thousand_output_tokens_llm=0.015,
            cost_per_thousand_embedding_tokens=0.0001
        )
        
        subscriber.all_stats.total_llm_count = 5
        subscriber.all_stats.total_llm_prompt_tokens = 500
        subscriber.all_stats.total_llm_completion_tokens = 250
        subscriber.all_stats.total_llm_duration_millis = 7500
        subscriber.all_stats.total_embedding_count = 3
        subscriber.all_stats.total_embedding_tokens = 1152
        subscriber.all_stats.total_embedding_duration_millis = 1500
        
        stats_dict = subscriber.return_stats_dict()
        
        assert stats_dict['total_llm_count'] == 5
        assert stats_dict['total_prompt_tokens'] == 500
        assert stats_dict['total_completion_tokens'] == 250
        assert stats_dict['total_embedding_count'] == 3
        assert stats_dict['total_embedding_tokens'] == 1152
        assert stats_dict['total_llm_duration_millis'] == 7500
        assert stats_dict['total_embedding_duration_millis'] == 1500
        assert stats_dict['average_llm_duration_millis'] == 1500
        assert stats_dict['average_embedding_duration_millis'] == 500
        assert 'total_llm_cost' in stats_dict

    
    def test_stat_printing_subscriber_get_stats(self):
        """Verify StatPrintingSubscriber get_stats returns stats object."""
        subscriber = StatPrintingSubscriber(
            cost_per_thousand_input_tokens_llm=0.003,
            cost_per_thousand_output_tokens_llm=0.015,
            cost_per_thousand_embedding_tokens=0.0001
        )
        
        subscriber.all_stats.total_llm_count = 10
        
        stats = subscriber.get_stats()
        
        assert isinstance(stats, FMObservabilityStats)
        assert stats.total_llm_count == 10


class TestObservabilityIntegration:
    """Tests for observability integration (test_observability_integration)."""
    
    @patch('graphrag_toolkit.lexical_graph.utils.fm_observability.Queue')
    @patch('graphrag_toolkit.lexical_graph.utils.fm_observability.Settings')
    def test_publisher_initialization_sets_up_handlers(self, mock_settings, mock_queue_class):
        """Verify FMObservabilityPublisher sets up handlers on initialization."""
        mock_queue_instance = Mock()
        mock_queue_class.return_value = mock_queue_instance
        mock_callback_manager = Mock()
        mock_settings.callback_manager = mock_callback_manager
        
        subscriber = Mock(spec=FMObservabilitySubscriber)
        publisher = FMObservabilityPublisher(subscribers=[subscriber], interval_seconds=1.0)
        
        # Verify handlers were added
        assert mock_callback_manager.add_handler.call_count == 2
        
        # Verify poller was started
        assert publisher.poller is not None
        
        # Clean up
        publisher.close()
    
    @patch('graphrag_toolkit.lexical_graph.utils.fm_observability.Queue')
    @patch('graphrag_toolkit.lexical_graph.utils.fm_observability.Settings')
    def test_publisher_context_manager(self, mock_settings, mock_queue_class):
        """Verify FMObservabilityPublisher works as context manager."""
        mock_queue_instance = Mock()
        mock_queue_class.return_value = mock_queue_instance
        mock_callback_manager = Mock()
        mock_settings.callback_manager = mock_callback_manager
        
        subscriber = Mock(spec=FMObservabilitySubscriber)
        
        with FMObservabilityPublisher(subscribers=[subscriber], interval_seconds=1.0) as publisher:
            assert publisher.allow_continue is True
        
        # After exiting context, should be closed
        assert publisher.allow_continue is False

    
    @patch('graphrag_toolkit.lexical_graph.utils.fm_observability._fm_observability_queue')
    def test_handler_event_end_puts_event_in_queue(self, mock_queue):
        """Verify FMObservabilityHandler puts completed events in queue."""
        handler = FMObservabilityHandler()
        
        # Start an event
        start_payload = {
            EventPayload.MESSAGES: [{'role': 'user', 'content': 'test'}],
            EventPayload.SERIALIZED: {'model': 'anthropic.claude-v2'}
        }
        handler.on_event_start(
            event_type=CBEventType.LLM,
            payload=start_payload,
            event_id='test-event'
        )
        
        # End the event
        end_payload = {
            EventPayload.MESSAGES: [{'role': 'user', 'content': 'test'}],
            EventPayload.RESPONSE: Mock()
        }
        handler.on_event_end(
            event_type=CBEventType.LLM,
            payload=end_payload,
            event_id='test-event'
        )
        
        # Verify event was put in queue
        assert mock_queue.put.called
        # Verify event was removed from in-flight
        assert 'test-event' not in handler.in_flight_events
    
    def test_handler_reset_counts_clears_in_flight_events(self):
        """Verify reset_counts clears in-flight events."""
        handler = FMObservabilityHandler()
        
        # Add some in-flight events
        handler.in_flight_events['event-1'] = Mock()
        handler.in_flight_events['event-2'] = Mock()
        
        handler.reset_counts()
        
        assert len(handler.in_flight_events) == 0
    
    def test_handler_start_trace_does_not_raise(self):
        """Verify start_trace method exists and doesn't raise."""
        handler = FMObservabilityHandler()
        
        # Should not raise
        handler.start_trace(trace_id='trace-1')
    
    def test_handler_end_trace_does_not_raise(self):
        """Verify end_trace method exists and doesn't raise."""
        handler = FMObservabilityHandler()
        
        # Should not raise
        handler.end_trace(trace_id='trace-1', trace_map={})

    
    @patch('graphrag_toolkit.lexical_graph.utils.fm_observability._fm_observability_queue')
    def test_bedrock_token_counting_handler_on_event_end_embedding(self, mock_queue):
        """Verify BedrockEnabledTokenCountingHandler handles embedding event end."""
        handler = BedrockEnabledTokenCountingHandler()
        
        # Simulate embedding token counts being tracked
        from llama_index.core.callbacks.token_counting import TokenCountingEvent
        handler.embedding_token_counts = [
            TokenCountingEvent(
                event_id='test-event',
                prompt='test text',
                prompt_token_count=384,
                completion='',
                completion_token_count=0
            )
        ]
        
        payload = {
            EventPayload.EMBEDDINGS: [[0.1, 0.2, 0.3]]
        }
        
        handler.on_event_end(
            event_type=CBEventType.EMBEDDING,
            payload=payload,
            event_id='test-event'
        )
        
        # Verify event was put in queue with token count
        assert mock_queue.put.called
        call_args = mock_queue.put.call_args[0][0]
        assert call_args.payload['embedding_token_count'] == 384

    

    def test_queue_poller_stops_correctly(self):
        """Verify FMObservabilityQueuePoller stops and returns stats."""
        poller = FMObservabilityQueuePoller()
        
        # Don't actually start the thread for this test
        stats = poller.stop()
        
        assert isinstance(stats, FMObservabilityStats)
        assert poller._discontinue.is_set()


class TestGetPatchedLLMTokenCounts:
    """Tests for get_patched_llm_token_counts function."""
    
    def test_get_patched_llm_token_counts_with_prompt_and_completion(self):
        """Verify token counting with prompt and completion payload."""
        from llama_index.core.utilities.token_counting import TokenCounter
        
        token_counter = TokenCounter()
        payload = {
            EventPayload.PROMPT: 'This is a test prompt',
            EventPayload.COMPLETION: 'This is a test completion'
        }
        
        result = get_patched_llm_token_counts(token_counter, payload, 'event-1')
        
        assert result.event_id == 'event-1'
        assert result.prompt == 'This is a test prompt'
        assert result.completion == 'This is a test completion'
        assert result.prompt_token_count > 0
        assert result.completion_token_count > 0

    
    def test_get_patched_llm_token_counts_with_messages_and_response(self):
        """Verify token counting with messages and response payload."""
        from llama_index.core.utilities.token_counting import TokenCounter
        from llama_index.core.llms import ChatMessage
        
        token_counter = TokenCounter()
        
        messages = [
            ChatMessage(role='user', content='Hello'),
            ChatMessage(role='assistant', content='Hi there')
        ]
        
        response = Mock()
        response.raw = {
            'usage': {
                'prompt_tokens': 10,
                'completion_tokens': 5
            }
        }
        
        payload = {
            EventPayload.MESSAGES: messages,
            EventPayload.RESPONSE: response
        }
        
        result = get_patched_llm_token_counts(token_counter, payload, 'event-2')
        
        assert result.event_id == 'event-2'
        assert result.prompt_token_count == 10
        assert result.completion_token_count == 5
    
    def test_get_patched_llm_token_counts_estimates_when_no_usage(self):
        """Verify token counting estimates when usage data not available."""
        from llama_index.core.utilities.token_counting import TokenCounter
        from llama_index.core.llms import ChatMessage
        
        token_counter = TokenCounter()
        
        messages = [ChatMessage(role='user', content='Test message')]
        response = Mock()
        response.raw = None
        
        payload = {
            EventPayload.MESSAGES: messages,
            EventPayload.RESPONSE: response
        }
        
        result = get_patched_llm_token_counts(token_counter, payload, 'event-3')
        
        assert result.event_id == 'event-3'
        assert result.prompt_token_count > 0
        assert result.completion_token_count >= 0
    
    def test_get_patched_llm_token_counts_raises_on_invalid_payload(self):
        """Verify token counting raises ValueError for invalid payload."""
        from llama_index.core.utilities.token_counting import TokenCounter
        
        token_counter = TokenCounter()
        payload = {}  # Invalid: no prompt/completion or messages/response
        
        with pytest.raises(ValueError, match='Invalid payload'):
            get_patched_llm_token_counts(token_counter, payload, 'event-4')
