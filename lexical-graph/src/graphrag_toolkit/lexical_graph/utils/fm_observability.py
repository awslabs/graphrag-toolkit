# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
import time
import logging
import threading
import queue
from multiprocessing import Queue
from dataclasses import dataclass, field
from typing import Dict, Optional, Any, List, Callable, cast
from enum import Enum

import tiktoken

logger = logging.getLogger(__name__)


# --- Event types and payload keys (replaces LlamaIndex CBEventType/EventPayload) ---

class CBEventType(str, Enum):
    """Event types for observability callbacks."""
    LLM = "llm"
    EMBEDDING = "embedding"


class EventPayload(str, Enum):
    """Payload keys for observability events."""
    PROMPT = "prompt"
    COMPLETION = "completion"
    MESSAGES = "messages"
    RESPONSE = "response"
    SERIALIZED = "serialized"
    EMBEDDINGS = "embeddings"


@dataclass
class CBEvent:
    """A callback event with type, payload, and ID."""
    event_type: CBEventType
    payload: Dict[str, Any]
    id_: str = ""


@dataclass
class TokenCountingEvent:
    """Token counting result for an LLM call."""
    event_id: str = ""
    prompt: str = ""
    prompt_token_count: int = 0
    completion: str = ""
    completion_token_count: int = 0

    @property
    def total_token_count(self):
        return self.prompt_token_count + self.completion_token_count


# --- Token counter using tiktoken ---

class TokenCounter:
    """Counts tokens using tiktoken (cl100k_base encoding)."""

    def __init__(self, tokenizer: Optional[Callable[[str], List]] = None):
        if tokenizer:
            self._tokenizer = tokenizer
        else:
            enc = tiktoken.get_encoding("cl100k_base")
            self._tokenizer = enc.encode

    def get_string_tokens(self, text: str) -> int:
        """Count tokens in a string."""
        return len(self._tokenizer(text))

    def estimate_tokens_in_messages(self, messages: List[Any]) -> int:
        """Estimate tokens in a list of messages."""
        total = 0
        for msg in messages:
            total += self.get_string_tokens(str(msg))
        return total


# --- Callback handler base class ---

class BaseCallbackHandler(ABC):
    """Base class for callback handlers."""

    def __init__(self, event_starts_to_ignore=None, event_ends_to_ignore=None):
        self.event_starts_to_ignore = event_starts_to_ignore or []
        self.event_ends_to_ignore = event_ends_to_ignore or []

    @abstractmethod
    def on_event_start(self, event_type, payload=None, event_id="", parent_id="", **kwargs):
        pass

    @abstractmethod
    def on_event_end(self, event_type, payload=None, event_id="", **kwargs):
        pass

    def start_trace(self, trace_id=None):
        pass

    def end_trace(self, trace_id=None, trace_map=None):
        pass


# --- Callback manager ---

class CallbackManager:
    """Simple callback manager that dispatches events to handlers."""

    def __init__(self):
        self._handlers: List[BaseCallbackHandler] = []

    def add_handler(self, handler: BaseCallbackHandler):
        self._handlers.append(handler)

    def on_event_start(self, event_type, payload=None, event_id="", parent_id="", **kwargs):
        for handler in self._handlers:
            handler.on_event_start(event_type, payload, event_id, parent_id, **kwargs)

    def on_event_end(self, event_type, payload=None, event_id="", **kwargs):
        for handler in self._handlers:
            handler.on_event_end(event_type, payload, event_id, **kwargs)


# Module-level callback manager
_callback_manager = CallbackManager()

_fm_observability_queue = None


class FMObservabilityQueuePoller(threading.Thread):
    """
    FMObservabilityQueuePoller is a thread-based queue polling class.

    This class is used to continuously poll a queue for events and process them
    using the FMObservabilityStats instance.
    """
    def __init__(self):
        super().__init__()
        self.daemon = True
        self._discontinue = threading.Event()
        self.fm_observability = FMObservabilityStats()

    def run(self):
        """Polls the observability queue and processes events."""
        logging.debug('Starting queue poller')
        while not self._discontinue.is_set():
            try:
                event = _fm_observability_queue.get(timeout=1)
                if event:
                    self.fm_observability.on_event(event=event)
            except queue.Empty:
                pass

    def stop(self):
        """Stops the queue polling process and returns the observability stats."""
        logging.debug('Stopping queue poller')
        self._discontinue.set()
        return self.fm_observability


@dataclass
class FMObservabilityStats:
    """Tracks and updates observability statistics for LLM and embedding operations."""
    total_llm_duration_millis: float = 0
    total_llm_count: int = 0
    total_llm_prompt_tokens: float = 0
    total_llm_completion_tokens: float = 0
    total_embedding_duration_millis: float = 0
    total_embedding_count: int = 0
    total_embedding_tokens: float = 0

    def update(self, stats: Any):
        """Updates stats from another FMObservabilityStats instance."""
        self.total_llm_duration_millis += stats.total_llm_duration_millis
        self.total_llm_count += stats.total_llm_count
        self.total_llm_prompt_tokens += stats.total_llm_prompt_tokens
        self.total_llm_completion_tokens += stats.total_llm_completion_tokens
        self.total_embedding_duration_millis += stats.total_embedding_duration_millis
        self.total_embedding_count += stats.total_embedding_count
        self.total_embedding_tokens += stats.total_embedding_tokens
        return (stats.total_llm_count + stats.total_embedding_count) > 0

    def on_event(self, event: CBEvent):
        """Handles an event and updates related statistics."""
        if event.event_type == CBEventType.LLM:
            if 'model' in event.payload:
                self.total_llm_duration_millis += event.payload['duration_millis']
                self.total_llm_count += 1
            elif 'llm_prompt_token_count' in event.payload:
                self.total_llm_prompt_tokens += event.payload['llm_prompt_token_count']
                self.total_llm_completion_tokens += event.payload['llm_completion_token_count']
        elif event.event_type == CBEventType.EMBEDDING:
            if 'model' in event.payload:
                self.total_embedding_duration_millis += event.payload['duration_millis']
                self.total_embedding_count += 1
            elif 'embedding_token_count' in event.payload:
                self.total_embedding_tokens += event.payload['embedding_token_count']

    @property
    def average_llm_duration_millis(self) -> int:
        if self.total_llm_count > 0:
            return self.total_llm_duration_millis / self.total_llm_count
        else:
            return 0

    @property
    def total_llm_tokens(self) -> int:
        return self.total_llm_prompt_tokens + self.total_llm_completion_tokens

    @property
    def average_llm_prompt_tokens(self) -> int:
        if self.total_llm_count > 0:
            return self.total_llm_prompt_tokens / self.total_llm_count
        else:
            return 0

    @property
    def average_llm_completion_tokens(self) -> int:
        if self.total_llm_count > 0:
            return self.total_llm_completion_tokens / self.total_llm_count
        else:
            return 0

    @property
    def average_llm_tokens(self) -> int:
        if self.total_llm_count > 0:
            return self.total_llm_tokens / self.total_llm_count
        else:
            return 0

    @property
    def average_embedding_duration_millis(self) -> int:
        if self.total_embedding_count > 0:
            return self.total_embedding_duration_millis / self.total_embedding_count
        else:
            return 0

    @property
    def average_embedding_tokens(self) -> int:
        if self.total_embedding_count > 0:
            return self.total_embedding_tokens / self.total_embedding_count
        else:
            return 0


class FMObservabilitySubscriber(ABC):
    """Defines an interface for subscribers to receive observability statistics."""
    @abstractmethod
    def on_new_stats(self, stats: FMObservabilityStats):
        pass


class ConsoleFMObservabilitySubscriber(FMObservabilitySubscriber):
    """Subscriber that prints FM observability statistics to console."""
    def __init__(self):
        self.all_stats = FMObservabilityStats()

    def on_new_stats(self, stats: FMObservabilityStats):
        updated = self.all_stats.update(stats)
        if updated:
            print(f'LLM: count: {self.all_stats.total_llm_count}, total_prompt_tokens: {self.all_stats.total_llm_prompt_tokens}, total_completion_tokens: {self.all_stats.total_llm_completion_tokens}')
            print(f'Embeddings: count: {self.all_stats.total_embedding_count}, total_tokens: {self.all_stats.total_embedding_tokens}')


class StatPrintingSubscriber(FMObservabilitySubscriber):
    """Subscriber that collects stats and estimates costs."""
    cost_per_thousand_input_tokens_llm: float = 0
    cost_per_thousand_output_tokens_llm: float = 0
    cost_per_thousand_embedding_tokens: float = 0

    def __init__(self, cost_per_thousand_input_tokens_llm, cost_per_thousand_output_tokens_llm, cost_per_thousand_embedding_tokens):
        self.all_stats = FMObservabilityStats()
        self.cost_per_thousand_input_tokens_llm = cost_per_thousand_input_tokens_llm
        self.cost_per_thousand_output_tokens_llm = cost_per_thousand_output_tokens_llm
        self.cost_per_thousand_embedding_tokens = cost_per_thousand_embedding_tokens

    def on_new_stats(self, stats: FMObservabilityStats):
        self.all_stats.update(stats)

    def get_stats(self):
        return self.all_stats

    def estimate_costs(self) -> float:
        total_cost = self.all_stats.total_llm_prompt_tokens / 1000.0 * self.cost_per_thousand_input_tokens_llm \
            + self.all_stats.total_llm_completion_tokens / 1000.0 * self.cost_per_thousand_output_tokens_llm \
            + self.all_stats.total_embedding_tokens / 1000.0 * self.cost_per_thousand_embedding_tokens
        return total_cost

    def return_stats_dict(self) -> Dict[str, Any]:
        stats_dict = {}
        stats_dict['total_llm_count'] = self.all_stats.total_llm_count
        stats_dict['total_prompt_tokens'] = self.all_stats.total_llm_prompt_tokens
        stats_dict['total_completion_tokens'] = self.all_stats.total_llm_completion_tokens
        stats_dict['total_embedding_count'] = self.all_stats.total_embedding_count
        stats_dict['total_embedding_tokens'] = self.all_stats.total_embedding_tokens
        stats_dict["total_llm_duration_millis"] = self.all_stats.total_llm_duration_millis
        stats_dict["total_embedding_duration_millis"] = self.all_stats.total_embedding_duration_millis
        stats_dict["average_llm_duration_millis"] = self.all_stats.average_llm_duration_millis
        stats_dict["average_embedding_duration_millis"] = self.all_stats.average_embedding_duration_millis
        stats_dict['total_llm_cost'] = self.estimate_costs()
        return stats_dict


class FMObservabilityPublisher():
    """Manages publishing of observability statistics at regular intervals."""
    def __init__(self, subscribers: List[FMObservabilitySubscriber]=[], interval_seconds=15.0):
        global _fm_observability_queue
        _fm_observability_queue = Queue()

        _callback_manager.add_handler(BedrockEnabledTokenCountingHandler())
        _callback_manager.add_handler(FMObservabilityHandler())

        self.subscribers = subscribers
        self.interval_seconds = interval_seconds
        self.allow_continue = True
        self.poller = FMObservabilityQueuePoller()
        self.poller.start()

        t = threading.Timer(interval_seconds, self.publish_stats)
        t.daemon = True
        t.start()

    def close(self):
        self.allow_continue = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def publish_stats(self):
        stats = self.poller.stop()
        self.poller = FMObservabilityQueuePoller()
        self.poller.start()
        if self.allow_continue:
            logging.debug('Scheduling new poller')
            t = threading.Timer(self.interval_seconds, self.publish_stats)
            t.daemon = True
            t.start()
        else:
            logging.debug('Shutting down publisher')
        for subscriber in self.subscribers:
            subscriber.on_new_stats(stats)


def get_patched_llm_token_counts(
    token_counter: TokenCounter, payload: Dict[str, Any], event_id: str = ""
) -> TokenCountingEvent:
    """
    Calculates and returns token counts for LLM inputs and outputs.
    """
    if EventPayload.PROMPT in payload:
        prompt = str(payload.get(EventPayload.PROMPT))
        completion = str(payload.get(EventPayload.COMPLETION))

        return TokenCountingEvent(
            event_id=event_id,
            prompt=prompt,
            prompt_token_count=token_counter.get_string_tokens(prompt),
            completion=completion,
            completion_token_count=token_counter.get_string_tokens(completion),
        )

    elif EventPayload.MESSAGES in payload:
        messages = payload.get(EventPayload.MESSAGES, [])
        messages_str = "\n".join([str(x) for x in messages])

        response = payload.get(EventPayload.RESPONSE)
        response_str = str(response)

        # try getting attached token counts first
        try:
            messages_tokens = 0
            response_tokens = 0

            if response is not None and response.raw is not None:
                usage = response.raw.get("usage", None)

                if usage is not None:
                    if not isinstance(usage, dict):
                        usage = dict(usage)
                    messages_tokens = usage.get("prompt_tokens", usage.get("input_tokens", 0))
                    response_tokens = usage.get("completion_tokens", usage.get("output_tokens", 0))

                if messages_tokens == 0 or response_tokens == 0:
                    raise ValueError("Invalid token counts!")

                return TokenCountingEvent(
                    event_id=event_id,
                    prompt=messages_str,
                    prompt_token_count=messages_tokens,
                    completion=response_str,
                    completion_token_count=response_tokens,
                )

        except (ValueError, KeyError):
            pass

        # Count tokens ourselves
        messages_tokens = token_counter.estimate_tokens_in_messages(messages)
        response_tokens = token_counter.get_string_tokens(response_str)

        return TokenCountingEvent(
            event_id=event_id,
            prompt=messages_str,
            prompt_token_count=messages_tokens,
            completion=response_str,
            completion_token_count=response_tokens,
        )
    else:
        raise ValueError(
            "Invalid payload! Need prompt and completion or messages and response."
        )


class BedrockEnabledTokenCountingHandler(BaseCallbackHandler):
    """Handles token counting for Bedrock LLM/embedding calls."""

    def __init__(
        self,
        tokenizer: Optional[Callable[[str], List]] = None,
        event_starts_to_ignore: Optional[List[CBEventType]] = None,
        event_ends_to_ignore: Optional[List[CBEventType]] = None,
        verbose: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(
            event_starts_to_ignore=event_starts_to_ignore,
            event_ends_to_ignore=event_ends_to_ignore,
        )
        self.token_counter = TokenCounter(tokenizer=tokenizer)
        self.llm_token_counts: List[TokenCountingEvent] = []
        self.embedding_token_counts: List[TokenCountingEvent] = []
        self.verbose = verbose

    def on_event_start(self, event_type, payload=None, event_id="", parent_id="", **kwargs):
        return event_id

    def on_event_end(
        self,
        event_type: CBEventType,
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        event_payload = None

        if (
            event_type == CBEventType.LLM
            and event_type not in self.event_ends_to_ignore
            and payload is not None
        ):
            # Count tokens for this LLM event
            try:
                token_event = get_patched_llm_token_counts(self.token_counter, payload, event_id)
                self.llm_token_counts.append(token_event)
                event_payload = {
                    'llm_prompt_token_count': token_event.prompt_token_count,
                    'llm_completion_token_count': token_event.completion_token_count,
                }
            except (ValueError, Exception):
                pass

        elif (
            event_type == CBEventType.EMBEDDING
            and event_type not in self.event_ends_to_ignore
            and payload is not None
        ):
            # For embeddings, use the last stored embedding token count
            if self.embedding_token_counts:
                last = self.embedding_token_counts[-1]
                event_payload = {
                    'embedding_token_count': last.total_token_count,
                }

        if event_payload:
            event = CBEvent(
                event_type=event_type,
                payload=event_payload,
                id_=event_id,
            )
            _fm_observability_queue.put(event)

        if len(self.llm_token_counts) > 1000 or len(self.embedding_token_counts) > 1000:
            self.reset_counts()

    def reset_counts(self):
        self.llm_token_counts = []
        self.embedding_token_counts = []


class FMObservabilityHandler(BaseCallbackHandler):
    """Handler for managing and tracking observability events (timing, model info)."""

    def __init__(self, event_starts_to_ignore=None, event_ends_to_ignore=None):
        super().__init__(event_starts_to_ignore or [], event_ends_to_ignore or [])
        self.in_flight_events = {}

    def on_event_start(
        self,
        event_type: CBEventType,
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        parent_id: str = "",
        **kwargs: Any,
    ) -> str:
        if event_type not in self.event_ends_to_ignore and payload is not None:
            if (
                (event_type == CBEventType.LLM and EventPayload.MESSAGES in payload) or
                (event_type == CBEventType.EMBEDDING and EventPayload.SERIALIZED in payload)
            ):
                serialized = payload.get(EventPayload.SERIALIZED, {})
                ms = time.time_ns() // 1_000_000
                event_payload = {
                    'model': serialized.get('model', serialized.get('model_name', 'unknown')),
                    'start': ms
                }

                self.in_flight_events[event_id] = CBEvent(
                    event_type=event_type,
                    payload=event_payload,
                    id_=event_id,
                )
        return event_id

    def on_event_end(
        self,
        event_type: CBEventType,
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        if event_type not in self.event_ends_to_ignore and payload is not None:
            if (
                (event_type == CBEventType.LLM and EventPayload.MESSAGES in payload) or
                (event_type == CBEventType.EMBEDDING and EventPayload.EMBEDDINGS in payload)
            ):
                try:
                    event = self.in_flight_events.pop(event_id)

                    start_ms = event.payload['start']
                    end_ms = time.time_ns() // 1_000_000
                    event.payload['duration_millis'] = end_ms - start_ms

                    _fm_observability_queue.put(event)
                except KeyError:
                    pass

    def reset_counts(self) -> None:
        self.in_flight_events = {}

    def start_trace(self, trace_id: Optional[str] = None) -> None:
        pass

    def end_trace(
        self,
        trace_id: Optional[str] = None,
        trace_map: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        pass
