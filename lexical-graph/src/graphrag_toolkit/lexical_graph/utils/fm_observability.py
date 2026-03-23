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
    """
    FMObservabilityQueuePoller is a thread-based queue polling class.

    This class is used to continuously poll a queue for events and process them
    using the FMObservabilityStats instance. It utilises threading to perform
    polling in a separate thread and provides methods to start and gracefully
    stop the polling operation.

    Attributes:
        _discontinue (threading.Event): An event object to signal and manage
            the termination of the thread's execution.
        fm_observability (FMObservabilityStats): An instance of the
            FMObservabilityStats class used to handle the events being processed.
    """
    def __init__(self):
        """
        Handles initialization of the class, setting up attributes and dependencies needed for
        observability and thread event management.

        Attributes:
            _discontinue (threading.Event): Threading event used to signal and manage thread
                lifecycle or operations that need coordination.
            fm_observability (FMObservabilityStats): Instance of FMObservabilityStats that
                handles observability-related statistics and functionality.
        """
        super().__init__()
        self.daemon = True
        self._discontinue = threading.Event()
        self.fm_observability = FMObservabilityStats()
   

    def run(self):
        """
        Runs the queue polling process to process events as they become available in the
        observability queue. It continuously checks for events unless interrupted by
        setting the `self._discontinue` event.

        The method polls the `_fm_observability_queue` at a regular interval and processes
        each event by invoking the `fm_observability.on_event` method.

        Raises:
            queue.Empty: If the queue is empty during a poll attempt.
        """
        logging.debug('Starting queue poller')
        while not self._discontinue.is_set():
            try:
                event = _fm_observability_queue.get(timeout=1)
                if event:
                    self.fm_observability.on_event(event=event)
            except queue.Empty:
                pass

    def stop(self):
        """
        Stops the queue polling process by signaling a discontinuation event and returns
        the observability object for further monitoring or assessment.

        This method is intended to halt the queue polling process initiated or operated
        by the current instance. By setting the discontinuation flag, the polling loop
        will cease operation. The method also provides access to the observability instance
        used during the process.

        Returns:
            Any: The observability object associated with the current process, which
            can be used for tracking or further evaluation.
        """
        logging.debug('Stopping queue poller')
        self._discontinue.set()
        return self.fm_observability

@dataclass
class FMObservabilityStats:
    """
    Tracks and updates observability statistics for LLM (Large Language Model) and embedding
    operations.

    This class monitors various metrics associated with LLM and embedding events, such as
    durations and token counts. It allows updating of the statistics using external data or
    events and provides convenient methods for accessing average values. The statistics tracked
    include both prompt and completion tokens for LLMs and tokens for embeddings.

    Attributes:
        total_llm_duration_millis (float): Total duration of all LLM calls in milliseconds.
        total_llm_count (int): Total count of LLM calls.
        total_llm_prompt_tokens (float): Total prompt tokens used in all LLM calls.
        total_llm_completion_tokens (float): Total completion tokens returned in all LLM calls.
        total_embedding_duration_millis (float): Total duration of all embedding calls in
            milliseconds.
        total_embedding_count (int): Total count of embedding calls.
        total_embedding_tokens (float): Total number of embedding tokens across all embedding
            calls.
    """
    total_llm_duration_millis: float = 0
    total_llm_count: int = 0
    total_llm_prompt_tokens: float = 0
    total_llm_completion_tokens: float = 0
    total_embedding_duration_millis: float = 0
    total_embedding_count: int = 0
    total_embedding_tokens: float = 0

    def update(self, stats: Any):
        """
        Updates the current object's statistical attributes with the values from the provided stats object and
        determines whether the sum of total_llm_count and total_embedding_count is greater than zero.

        This method is used to aggregate statistical data from another object into the current instance and
        helps analyze whether any meaningful statistical contributions are made by checking if the combined
        counts of LLM and embedding operations are non-zero.

        Args:
            stats (Any): An object containing the statistical data to update. The provided stats object must
                         have the attributes:
                         - total_llm_duration_millis (int)
                         - total_llm_count (int)
                         - total_llm_prompt_tokens (int)
                         - total_llm_completion_tokens (int)
                         - total_embedding_duration_millis (int)
                         - total_embedding_count (int)
                         - total_embedding_tokens (int)

        Returns:
            bool: True if the combined total_llm_count and total_embedding_count from the stats object is
                  greater than zero; otherwise, False.
        """
        self.total_llm_duration_millis += stats.total_llm_duration_millis
        self.total_llm_count += stats.total_llm_count
        self.total_llm_prompt_tokens += stats.total_llm_prompt_tokens
        self.total_llm_completion_tokens += stats.total_llm_completion_tokens
        self.total_embedding_duration_millis += stats.total_embedding_duration_millis
        self.total_embedding_count += stats.total_embedding_count
        self.total_embedding_tokens += stats.total_embedding_tokens
        return (stats.total_llm_count + stats.total_embedding_count) > 0

    def on_event(self, event: CBEvent):
        """
        Handles the processing of different types of events and updates related statistics
        based on the event type and payload data. Supports events of type LLM and EMBEDDING.

        Args:
            event (CBEvent): The event object containing the event type and associated
                payload data used to update relevant statistics.
        """
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
        """
        Gets the average duration of LLM operations in milliseconds.

        This property calculates the average duration of LLM operations by dividing the
        total duration spent in LLM operations by the total number of LLM operations
        performed. If no LLM operations were performed, the average duration is set to 0.

        Returns:
            int: The average duration of LLM operations in milliseconds.
        """
        if self.total_llm_count > 0:
            return self.total_llm_duration_millis / self.total_llm_count
        else:
            return 0
        
    @property
    def total_llm_tokens(self) -> int:
        """
        Calculates the total number of LLM tokens used, combining prompt and completion tokens.

        This property method calculates the total number of tokens used by summing up
        the number of tokens used in the prompt and completion phases by the LLM. It
        provides a convenient way to access the aggregate token usage.

        Returns:
            int: Total number of LLM tokens, including both prompt and completion tokens.
        """
        return self.total_llm_prompt_tokens + self.total_llm_completion_tokens
    
    @property
    def average_llm_prompt_tokens(self) -> int:
        """
        Calculates the average number of LLM prompt tokens used per LLM call.

        This method determines an average value by dividing the total LLM prompt tokens by
        the total number of LLM calls. If no LLM calls have been made, it will return 0 to
        avoid division by zero.

        Returns:
            int: The average number of LLM prompt tokens per LLM call. If no LLM calls have
            been made, it returns 0.
        """
        if self.total_llm_count > 0:
            return self.total_llm_prompt_tokens / self.total_llm_count
        else:
            return 0
        
    @property
    def average_llm_completion_tokens(self) -> int:
        """
        Calculates the average number of completion tokens generated by a language
        model (LLM) across multiple requests. If there are no LLM requests, it
        returns 0.

        The calculation is based on dividing the total number of LLM completion
        tokens by the total count of LLM requests. Acts as a property getter.

        Returns:
            int: The average number of LLM completion tokens per request. If no
            LLM requests exist, returns 0.
        """
        if self.total_llm_count > 0:
            return self.total_llm_completion_tokens / self.total_llm_count
        else:
            return 0
        
    @property
    def average_llm_tokens(self) -> int:
        """
        Gets the average number of LLM tokens. The property calculates the average by dividing
        the total number of LLM tokens by the total count of LLM instances. If no LLM instances
        exist, returns zero.

        Returns:
            int: The average number of LLM tokens. If the total LLM count is zero, returns 0.
        """
        if self.total_llm_count > 0:
            return self.total_llm_tokens / self.total_llm_count
        else:
            return 0
    
    @property
    def average_embedding_duration_millis(self) -> int:
        """
        Calculates the average duration of embedding in milliseconds.

        The average duration is computed by dividing the total embedding
        duration in milliseconds by the total count of embeddings. If the
        embedding count is zero, it returns 0 to avoid division by zero.

        Returns:
            int: The average embedding duration in milliseconds. Returns 0
            if there are no embeddings.
        """
        if self.total_embedding_count > 0:
            return self.total_embedding_duration_millis / self.total_embedding_count
        else:
            return 0
        
    @property
    def average_embedding_tokens(self) -> int:
        """
        Calculates the average number of embedding tokens per embedding.

        The `average_embedding_tokens` property retrieves the average of
        embedding tokens by dividing the total number of embedding tokens
        by the total count of embeddings. If the total embedding count is
        zero, it safely returns zero to avoid division errors.

        Attributes:
            total_embedding_tokens (int): The total number of embedding tokens.
            total_embedding_count (int): The total number of embeddings.

        Returns:
            int: The average number of embedding tokens per embedding. If the
            `total_embedding_count` is zero, returns 0.
        """
        if self.total_embedding_count > 0:
            return self.total_embedding_tokens / self.total_embedding_count
        else:
            return 0
        
class FMObservabilitySubscriber(ABC):
    """Defines an interface for subscribers to receive observability statistics.

    FMObservabilitySubscriber serves as an abstract base class that establishes
    a contract for implementing classes to handle new statistics from
    FMObservability. It ensures any subscriber provides a concrete implementation
    for the `on_new_stats` method, enabling consistent behavior across different
    subscriber implementations.

    Methods:
        on_new_stats(stats): Abstract method to handle new observability statistics.
    """
    @abstractmethod
    def on_new_stats(self, stats: FMObservabilityStats):
        """
        An abstract base class that defines a callback method to be invoked upon
        receiving new statistics related to observability.

        Methods:
            on_new_stats: Abstract method to handle new observability statistics.
        """
        pass

class ConsoleFMObservabilitySubscriber(FMObservabilitySubscriber):
    """
    A subscriber class for observing FM (Foundation Model) observability statistics.

    This class is a concrete implementation of FMObservabilitySubscriber, designed to collect
    and print observability statistics in real time. It maintains cumulative statistics and
    prints detailed data on occurrences of LLM (Large Language Model) events and Embedding
    events whenever new statistics are received.

    Attributes:
        all_stats (FMObservabilityStats): An instance of FMObservabilityStats that aggregates
        and maintains cumulative statistics for LLM and Embedding events.
    """
    def __init__(self):
        self.all_stats = FMObservabilityStats()

    def on_new_stats(self, stats: FMObservabilityStats):
        """
        Processes and updates the current statistics with new incoming statistics data.

        This method takes in an `FMObservabilityStats` instance containing new
        observability statistics and updates the aggregated statistics stored
        in `self.all_stats`. If the update is successful, a summary of the updated
        statistics is printed, including information about LLM (Large Language Model)
        usage and Embedding usage.

        Args:
            stats (FMObservabilityStats): The new observability statistics to be
                processed and integrated into the existing set of aggregated statistics.
        """
        updated = self.all_stats.update(stats)
        if updated:
            print(f'LLM: count: {self.all_stats.total_llm_count}, total_prompt_tokens: {self.all_stats.total_llm_prompt_tokens}, total_completion_tokens: {self.all_stats.total_llm_completion_tokens}')
            print(f'Embeddings: count: {self.all_stats.total_embedding_count}, total_tokens: {self.all_stats.total_embedding_tokens}')

class StatPrintingSubscriber(FMObservabilitySubscriber):
    """Represents a subscriber that collects and processes observability statistics
    and estimates costs associated with language model usage.

    This class is designed for subscribing to observability events, aggregating statistics,
    and calculating costs associated with specific observability data such as input tokens,
    output tokens, and embeddings. It provides utility methods for updating, retrieving,
    and summarizing observability stats in a structured format.

    Attributes:
        cost_per_thousand_input_tokens_llm (float): Cost per thousand input tokens for the
            language model.
        cost_per_thousand_output_tokens_llm (float): Cost per thousand output tokens for the
            language model.
        cost_per_thousand_embedding_tokens (float): Cost per thousand tokens for embeddings.
    """
    cost_per_thousand_input_tokens_llm: float = 0
    cost_per_thousand_output_tokens_llm: float = 0
    cost_per_thousand_embedding_tokens: float = 0

    def __init__(self, cost_per_thousand_input_tokens_llm, cost_per_thousand_output_tokens_llm, cost_per_thousand_embedding_tokens):
        """
        Initializes the object with the provided costs for input tokens, output tokens,
        and embedding tokens. This class is designed to allow tracking and managing
        factors related to these costs for specified parameters.

        Args:
            cost_per_thousand_input_tokens_llm: The cost per thousand input tokens
                used by the language model.
            cost_per_thousand_output_tokens_llm: The cost per thousand output tokens
                generated by the language model.
            cost_per_thousand_embedding_tokens: The cost per thousand embedding tokens
                used in the process.
        """
        self.all_stats = FMObservabilityStats()
        self.cost_per_thousand_input_tokens_llm = cost_per_thousand_input_tokens_llm
        self.cost_per_thousand_output_tokens_llm = cost_per_thousand_output_tokens_llm
        self.cost_per_thousand_embedding_tokens = cost_per_thousand_embedding_tokens

    def on_new_stats(self, stats: FMObservabilityStats):
        """
        Updates the current statistics with new observation data. This method takes in a new set of
        observability statistics and merges them into the existing aggregation of statistics.

        Args:
            stats: New FMObservabilityStats object containing the observation data to be processed and
                merged into the aggregated statistics.
        """
        self.all_stats.update(stats)
 
    def get_stats(self):
        """
        Retrieves all statistical data recorded and stored in the object.

        This method provides access to the collection of all statistics that
        have been aggregated or computed. It is intended to be used when the
        caller needs a comprehensive view of the stored statistical data.

        Returns:
            list: A list containing all statistical data stored within the
            object. The actual structure and content of the data may vary
            depending on the implementation details.
        """
        return self.all_stats
    
    def estimate_costs(self) -> float:
        """
        Estimates the total cost based on token usage statistics and predefined costs per
        thousand tokens for input, output, and embeddings.

        The method calculates the total cost by aggregating the costs associated with
        the total number of input prompt tokens, output completion tokens, and embedding
        tokens. Each cost component is determined by dividing the respective token count
        by 1000 and multiplying it with the respective cost per thousand tokens.

        Returns:
            float: The total estimated cost calculated from token usage statistics.
        """
        total_cost = self.all_stats.total_llm_prompt_tokens / 1000.0 *self.cost_per_thousand_input_tokens_llm \
        + self.all_stats.total_llm_completion_tokens / 1000.0 * self.cost_per_thousand_output_tokens_llm \
        +self.all_stats.total_embedding_tokens / 1000.0 * self.cost_per_thousand_embedding_tokens
        return total_cost
        
    def return_stats_dict(self) -> Dict[str, Any]:
        """
        Compiles and returns a dictionary containing various statistics and metrics related to LLM
        usage and embeddings. This includes information about token counts, duration, and associated
        costs.

        Returns:
            Dict[str, Any]: Dictionary containing the following keys and their associated values:
                - 'total_llm_count': The total count of LLM calls.
                - 'total_prompt_tokens': Total number of prompt tokens for all LLM calls.
                - 'total_completion_tokens': Total number of completion tokens for all LLM calls.
                - 'total_embedding_count': The total count of embedding operations performed.
                - 'total_embedding_tokens': Total number of tokens processed during embeddings.
                - 'total_llm_duration_millis': Total LLM execution duration in milliseconds.
                - 'total_embedding_duration_millis': Total embedding operation duration in milliseconds.
                - 'average_llm_duration_millis': Average duration of LLM executions in milliseconds.
                - 'average_embedding_duration_millis': Average duration of embedding operations in
                  milliseconds.
                - 'total_llm_cost': Estimated total monetary cost associated with LLM usage.
        """
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
    """
    Manages publishing of observability statistics at regular intervals to subscribed
    observers.

    The FMObservabilityPublisher is designed to poll a queue for observability statistics
    and notify registered subscribers. It uses a background thread to periodically invoke
    the publishing mechanism. The class also implements context manager capabilities for
    automatic resource management.

    Attributes:
        subscribers (List[FMObservabilitySubscriber]): A list of subscribers to be notified
            when new observability statistics are available.
        interval_seconds (float): The time interval, in seconds, between publishing
            observability statistics.
        allow_continue (bool): A flag indicating whether the publishing process should
            continue. If set to False, the process will stop.
        poller (FMObservabilityQueuePoller): The polling mechanism that retrieves data
            from the observability queue.
    """
    def __init__(self, subscribers: List[FMObservabilitySubscriber]=[], interval_seconds=15.0):
        """
        Initializes the FMObservabilityQueuePublisher class.

        This class is responsible for managing observability subscribers, configuring the
        poller to process observability queue data, and periodically publishing statistics
        to the registered subscribers. It also sets up handlers for token counting and
        observability events.

        Args:
            subscribers (List[FMObservabilitySubscriber], optional): A list of subscribers to
                receive the observability data. Defaults to an empty list.
            interval_seconds (float, optional): The interval in seconds for publishing
                statistics to the subscribers. Defaults to 15.0.
        """
        global _fm_observability_queue
        _fm_observability_queue = Queue()

        Settings.callback_manager.add_handler(BedrockEnabledTokenCountingHandler())
        Settings.callback_manager.add_handler(FMObservabilityHandler())

        self.subscribers = subscribers
        self.interval_seconds = interval_seconds
        self.allow_continue = True
        self.poller = FMObservabilityQueuePoller()
        self.poller.start()

        t = threading.Timer(interval_seconds, self.publish_stats)
        t.daemon = True
        t.start()

    def close(self):
        """
        Disables the continuation functionality by updating the internal flag.

        This method prevents further continuation of a process by setting the
        `allow_continue` attribute to `False`. Once invoked, the attribute will
        indicate that continuation should not occur.

        Attributes:
            allow_continue (bool): Indicates whether continuation is permitted or not.
                Set to `False` to disable continuation.
        """
        self.allow_continue = False

    def __enter__(self):
        """
        Allows the use of the object with the context manager (with statement). Ensures
        the proper setup and teardown of resources handled by the object.

        Returns:
            Self:
                Returns the instance of the object, allowing its methods/attributes
                to be accessible within the context.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def publish_stats(self):
        """
        Handles the collection, restart, and publishing of statistics through a polling mechanism.

        Starts a polling process to collect current statistics and ensures the continuity of the
        polling cycle based on the specified interval if allowed. Independently notifies all
        subscribers with the new statistics collected after each polling cycle.

        Args:
            None

        Raises:
            None

        Returns:
            None
        """
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
    Calculates and returns token counts for LLM inputs and outputs, properly handling
    different types of event payloads and ensuring accurate token usage tracking.

    This function examines the event payload to determine whether it contains a prompt and
    completion or a list of messages and a response. It calculates token counts for these
    inputs and outputs using the provided TokenCounter utility, either directly from the
    payload or by estimating them.

    Args:
        token_counter: An object or utility used to count tokens in strings or messages.
        payload: A dictionary containing event-related data, such as prompts, completions,
            messages, and responses.
        event_id: A unique identifier for the current event. Defaults to an empty string.

    Returns:
        TokenCountingEvent: An object encapsulating token count information for both prompt
            and completion or messages and response.

    Raises:
        ValueError: Raised if the payload is invalid or does not contain the required
            attributes for token counting.
    """
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
    """
    Handles token counting for systems utilizing Bedrock, extending functionality for
    event-based processing.

    Provides mechanisms to accurately count tokens used in language model (LLM) prompts
    and completions, or for embeddings processing. Additionally ensures integration
    with the observability queue to facilitate tracking and debugging.

    Attributes:
        event_starts_to_ignore (Optional[List[CBEventType]]): List of event types to
            be ignored at the start of events.
        event_ends_to_ignore (Optional[List[CBEventType]]): List of event types to
            be ignored at the end of events.
        tokenizer (Optional[Callable[[str], List]]): Tokenizer function used
            to split input into tokens.
        verbose (bool): Flag indicating whether detailed information should
            be logged.
        logger (Optional[logging.Logger]): Logger instance for logging events
            and information.
    """

    def __init__(
        self,
        tokenizer: Optional[Callable[[str], List]] = None,
        event_starts_to_ignore: Optional[List[CBEventType]] = None,
        event_ends_to_ignore: Optional[List[CBEventType]] = None,
        verbose: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initializes the class with specified parameters. The provided parameters will
        control the behavior of the callback system for token counting. This includes
        the tokenizer to be used, events to be ignored during start and end operations,
        verbosity level, and an optional logger for logging purposes.

        Args:
            tokenizer: Optional callable responsible for tokenizing input strings into
                a list of tokens.
            event_starts_to_ignore: Optional list of CBEventType items representing
                event types to ignore when a start event occurs.
            event_ends_to_ignore: Optional list of CBEventType items representing event
                types to ignore when an end event occurs.
            verbose: Boolean indicating whether to enable verbose output.
            logger: Optional logging.Logger instance for logging callback-related
                information.

        """
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
        """
        Handles the conclusion of specified events by processing token counts if applicable
        and updating the observability queue with event data. This method tracks token
        counts for LLM and Embedding events, ensuring that their respective token usages
        are appropriately logged. When certain thresholds for token count storage are exceeded,
        the method will reset those counts.

        Args:
            event_type (CBEventType): Type of the event being concluded, such as LLM or
                EMBEDDING.
            payload (Optional[Dict[str, Any]]): Additional event-specific data payload. Can
                be None if no payload accompanies the event.
            event_id (str): Identifier of the event. Defaults to an empty string if not
                provided.
            **kwargs (Any): Additional keyword arguments for extensibility. These are not
                directly utilized in this method.
        """
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
    """
    Handler for managing and tracking observability events.

    This class is a specialized callback handler used to monitor and manage events within a
    workflow. It tracks the start and end of specific events, records event metadata,
    and calculates the duration of event processing. The class provides methods to handle
    event-start and event-end logic, ensuring accurate observability data while ignoring
    specific event types based on configuration.

    Attributes:
        in_flight_events (dict): Stores events that are currently in progress. Each event is
            tracked by its ID and contains associated metadata.
    """
    def __init__(self, event_starts_to_ignore=[], event_ends_to_ignore=[]):
        """
        Initializes the object of the class with the given parameters, sets up the
        container for in-flight events, and calls the parent class initializer.

        Args:
            event_starts_to_ignore (list, optional): A list of event start identifiers
                that should be ignored. Defaults to an empty list.
            event_ends_to_ignore (list, optional): A list of event end identifiers that
                should be ignored. Defaults to an empty list.
        """
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
        """
        Handles the start of an event by processing the given event type and payload and
        storing event data if necessary. Returns the event identifier for the started
        event.

        Args:
            event_type (CBEventType): The type of the event being started, used to
                determine processing and logging behavior.
            payload (Optional[Dict[str, Any]]): The optional payload containing event
                data. Specific processing is done if the payload includes certain keys
                like 'MESSAGES' or a serialized model.
            event_id (str): The unique identifier for the event being started.
            parent_id (str): The unique identifier of the parent event, if any. This
                allows for hierarchical event tracking.
            **kwargs (Any): Additional optional key-value pairs that might be required
                for specific event handling.

        Returns:
            str: The event identifier for the started event.
        """
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
        """
        Handles the end of an event and manages its corresponding operations, such as
        removing it from the in-flight events, calculating its duration, and enqueuing
        the completed event for further processing. This is done only for specific event
        types and under certain conditions.

        Args:
            event_type: The type of the event being processed.
            payload: The detailed payload of the event, typically containing its data.
            event_id: A unique identifier for the event being processed.
            **kwargs: Additional optional parameters that may be passed for event handling.

        """
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
        """
        Resets the in-flight events counter.

        This method clears the `in_flight_events` dictionary, effectively resetting
        any tracked events or counts. It is used to reinitialize or clear current
        state information for tracking events in progress.

        Attributes:
            in_flight_events (dict): A dictionary that holds the current in-flight
                events being tracked.

        """
        self.in_flight_events = {} 
        
    def start_trace(self, trace_id: Optional[str] = None) -> None:
        """
        Starts a new trace with the given trace ID. If no trace ID is provided, a default or
        system-generated trace identifier is used. This function facilitates the beginning
        of tracking specific operations or sequences for logging and troubleshooting
        purposes.

        Args:
            trace_id (Optional[str]): The unique identifier for the trace. If None, a default
                or system-generated trace identifier will be applied.

        Returns:
            None
        """
        pass

    def end_trace(
        self,
        trace_id: Optional[str] = None,
        trace_map: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Ends an ongoing trace based on the provided trace identifier and updates the
        trace map accordingly.

        This function allows marking the completion of a trace identified by `trace_id`.
        It optionally accepts a trace mapping structure (`trace_map`) for updating its
        state after ending the trace. If `trace_map` is provided, this function modifies
        it to reflect the end of the specified trace.

        Args:
            trace_id: The unique identifier of the trace to be ended. If not provided,
                no specific trace is targeted.
            trace_map: A dictionary mapping trace identifiers to lists of related trace
                details. If specified, this will be updated to reflect the ended trace.
        """
        pass

