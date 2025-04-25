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

from llama_index.core import Settings
from llama_index.core.callbacks.base_handler import BaseCallbackHandler
from llama_index.core.callbacks.schema import CBEventType, EventPayload, CBEvent
from llama_index.core.callbacks import TokenCountingHandler
from llama_index.core.callbacks.schema import CBEventType, EventPayload
from llama_index.core.utilities.token_counting import TokenCounter
from llama_index.core.callbacks.token_counting import TokenCountingEvent

logger = logging.getLogger(__name__)

_fm_observability_queue = None

class FMObservabilityQueuePoller(threading.Thread):

    def __init__(self):
        super().__init__()
        self._discontinue = threading.Event()
        self.fm_observability = FMObservabilityStats()
   

    def run(self):
        logging.debug('Starting queue poller')
        while not self._discontinue.is_set():
            try:
                event = _fm_observability_queue.get(timeout=1)
                if event:
                    self.fm_observability.on_event(event=event)
            except queue.Empty:
                pass

    def stop(self):
        logging.debug('Stopping queue poller')
        self._discontinue.set()
        return self.fm_observability

@dataclass
class FMObservabilityStats:

    total_llm_duration_millis: float = 0
    total_llm_count: int = 0
    total_llm_prompt_tokens: float = 0
    total_llm_completion_tokens: float = 0
    total_embedding_duration_millis: float = 0
    total_embedding_count: int = 0
    total_embedding_tokens: float = 0

    def update(self, stats: Any):
        self.total_llm_duration_millis += stats.total_llm_duration_millis
        self.total_llm_count += stats.total_llm_count
        self.total_llm_prompt_tokens += stats.total_llm_prompt_tokens
        self.total_llm_completion_tokens += stats.total_llm_completion_tokens
        self.total_embedding_duration_millis += stats.total_embedding_duration_millis
        self.total_embedding_count += stats.total_embedding_count
        self.total_embedding_tokens += stats.total_embedding_tokens
        return (stats.total_llm_count + stats.total_embedding_count) > 0

    def on_event(self, event: CBEvent):
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
        """Get the average duration for an LLM call in millis."""
        if self.total_llm_count > 0:
            return self.total_llm_duration_millis / self.total_llm_count
        else:
            return 0
        
    @property
    def total_llm_tokens(self) -> int:
        """Get the current total of LLM prompt and completion tokens."""
        return self.total_llm_prompt_tokens + self.total_llm_completion_tokens
    
    @property
    def average_llm_prompt_tokens(self) -> int:
        """Get the average LLM prompt token count."""
        if self.total_llm_count > 0:
            return self.total_llm_prompt_tokens / self.total_llm_count
        else:
            return 0
        
    @property
    def average_llm_completion_tokens(self) -> int:
        """Get the average LLM completion token count."""
        if self.total_llm_count > 0:
            return self.total_llm_completion_tokens / self.total_llm_count
        else:
            return 0
        
    @property
    def average_llm_tokens(self) -> int:
        """Get the average LLM prompt and completion token count."""
        if self.total_llm_count > 0:
            return self.total_llm_tokens / self.total_llm_count
        else:
            return 0
    
    @property
    def average_embedding_duration_millis(self) -> int:
        """Get the average duration for an embedding call in millis."""
        if self.total_embedding_count > 0:
            return self.total_embedding_duration_millis / self.total_embedding_count
        else:
            return 0
        
    @property
    def average_embedding_tokens(self) -> int:
        """Get the average embedding token count."""
        if self.total_embedding_count > 0:
            return self.total_embedding_tokens / self.total_embedding_count
        else:
            return 0
        
class FMObservabilitySubscriber(ABC):
    
    @abstractmethod
    def on_new_stats(self, stats: FMObservabilityStats):
        pass

class ConsoleFMObservabilitySubscriber(FMObservabilitySubscriber):

    def __init__(self):
        self.all_stats = FMObservabilityStats()

    def on_new_stats(self, stats: FMObservabilityStats):
        updated = self.all_stats.update(stats)
        if updated:
            print(f'LLM: count: {self.all_stats.total_llm_count}, total_prompt_tokens: {self.all_stats.total_llm_prompt_tokens}, total_completion_tokens: {self.all_stats.total_llm_completion_tokens}')
            print(f'Embeddings: count: {self.all_stats.total_embedding_count}, total_tokens: {self.all_stats.total_embedding_tokens}')

class StatPrintingSubscriber(FMObservabilitySubscriber):
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
        total_cost = self.all_stats.total_llm_prompt_tokens / 1000.0 *self.cost_per_thousand_input_tokens_llm \
        + self.all_stats.total_llm_completion_tokens / 1000.0 * self.cost_per_thousand_output_tokens_llm \
        +self.all_stats.total_embedding_tokens / 1000.0 * self.cost_per_thousand_embedding_tokens
        return total_cost
        
    def return_stats_dict(self) -> Dict[str, Any]:
        stats_dict = {}
        stats_dict['total_llm_count'] = self.all_stats.total_llm_count
        stats_dict['total_prompt_tokens'] = self.all_stats.total_llm_prompt_tokens
        stats_dict['total_completion_tokens'] = self.all_stats.total_llm_completion_tokens
        # Now embeddings count and total embedding tokens
        stats_dict['total_embedding_count'] = self.all_stats.total_embedding_count
        stats_dict['total_embedding_tokens'] = self.all_stats.total_embedding_tokens
        # Now duration data
        stats_dict["total_llm_duration_millis"] = self.all_stats.total_llm_duration_millis
        stats_dict["total_embedding_duration_millis"] = self.all_stats.total_embedding_duration_millis
        stats_dict["average_llm_duration_millis"] = self.all_stats.average_llm_duration_millis
        stats_dict["average_embedding_duration_millis"] = self.all_stats.average_embedding_duration_millis
        # Now  costs
        stats_dict['total_llm_cost'] = self.estimate_costs()
        return stats_dict
        

class FMObservabilityPublisher():

    def __init__(self, subscribers: List[FMObservabilitySubscriber]=[], interval_seconds=15.0):

        global _fm_observability_queue
        _fm_observability_queue = Queue()

        Settings.callback_manager.add_handler(BedrockEnabledTokenCountingHandler())
        Settings.callback_manager.add_handler(FMObservabilityHandler())

        self.subscribers = subscribers
        self.interval_seconds = interval_seconds
        self.allow_continue = True
        self.poller = FMObservabilityQueuePoller()
        self.poller.start()

        threading.Timer(interval_seconds, self.publish_stats).start()

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
            threading.Timer(self.interval_seconds, self.publish_stats).start()
        else:
            logging.debug('Shutting down publisher')
        for subscriber in self.subscribers:
            subscriber.on_new_stats(stats)


def get_patched_llm_token_counts(
    token_counter: TokenCounter, payload: Dict[str, Any], event_id: str = ""
) -> TokenCountingEvent:
    from llama_index.core.llms import ChatMessage

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
        messages = cast(List[ChatMessage], payload.get(EventPayload.MESSAGES, []))
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
            # Invalid token counts, or no token counts attached
            pass

        # Should count tokens ourselves
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
    
class BedrockEnabledTokenCountingHandler(TokenCountingHandler):
    """Callback handler for counting tokens in LLM and Embedding events.
    Patched to support Bedrock Anthropic models.

    Args:
        tokenizer:
            Tokenizer to use. Defaults to the global tokenizer
            (see llama_index.core.utils.globals_helper).
        event_starts_to_ignore: List of event types to ignore at the start of a trace.
        event_ends_to_ignore: List of event types to ignore at the end of a trace.
    """

    def __init__(
        self,
        tokenizer: Optional[Callable[[str], List]] = None,
        event_starts_to_ignore: Optional[List[CBEventType]] = None,
        event_ends_to_ignore: Optional[List[CBEventType]] = None,
        verbose: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        import llama_index.core.callbacks.token_counting 
        llama_index.core.callbacks.token_counting.get_llm_token_counts = get_patched_llm_token_counts

        super().__init__(
            tokenizer=tokenizer, 
            event_starts_to_ignore=event_starts_to_ignore, 
            event_ends_to_ignore=event_ends_to_ignore, 
            verbose=verbose, 
            logger=logger
        )

    def on_event_end(
        self,
        event_type: CBEventType,
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        
        super().on_event_end(event_type, payload, event_id, **kwargs)
        
        event_payload = None
        
        """Count the LLM or Embedding tokens as needed."""
        if (
            event_type == CBEventType.LLM
            and event_type not in self.event_ends_to_ignore
            and payload is not None
        ):
            event_payload = {
                'llm_prompt_token_count': self.llm_token_counts[-1].prompt_token_count,
                'llm_completion_token_count': self.llm_token_counts[-1].completion_token_count
            }
        elif (
            event_type == CBEventType.EMBEDDING
            and event_type not in self.event_ends_to_ignore
            and payload is not None
        ):  
            event_payload = {
                'embedding_token_count': self.embedding_token_counts[-1].total_token_count
            }

        if event_payload:
            
            event = CBEvent(
                event_type = event_type, 
                payload = event_payload, 
                id_ = event_id
            )
            
            _fm_observability_queue.put(event)

        if len(self.llm_token_counts) > 1000 or len(self.embedding_token_counts) > 1000:
            self.reset_counts()

class FMObservabilityHandler(BaseCallbackHandler):
    def __init__(self, event_starts_to_ignore=[], event_ends_to_ignore=[]):
        super().__init__(event_starts_to_ignore, event_ends_to_ignore)
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
                    event_type = event_type, 
                    payload = event_payload, 
                    id_ = event_id
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
        """Reset the counts."""
        self.in_flight_events = {} 
        
    def start_trace(self, trace_id: Optional[str] = None) -> None:
        pass

    def end_trace(
        self,
        trace_id: Optional[str] = None,
        trace_map: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        pass

