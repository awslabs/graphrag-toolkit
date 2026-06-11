# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.callbacks."""

from graphrag_toolkit.core.callbacks import (
    EMBEDDING_END,
    EMBEDDING_START,
    LLM_END,
    LLM_START,
    RETRIEVAL_END,
    RETRIEVAL_START,
    TRANSFORM_END,
    TRANSFORM_START,
    CallbackRegistry,
)


class TestCallbackRegistry:
    def setup_method(self):
        CallbackRegistry.clear()

    def teardown_method(self):
        CallbackRegistry.clear()

    def test_register_adds_handler(self):
        handler = lambda et, p: None
        CallbackRegistry.register(handler)
        assert len(CallbackRegistry._handlers) == 1

    def test_emit_calls_handlers(self):
        events = []
        CallbackRegistry.register(lambda et, p: events.append((et, p)))
        CallbackRegistry.emit("test_event", {"key": "val"})
        assert events == [("test_event", {"key": "val"})]

    def test_multiple_handlers_called_in_order(self):
        order = []
        CallbackRegistry.register(lambda et, p: order.append("first"))
        CallbackRegistry.register(lambda et, p: order.append("second"))
        CallbackRegistry.emit("x", {})
        assert order == ["first", "second"]

    def test_clear_removes_handlers(self):
        CallbackRegistry.register(lambda et, p: None)
        CallbackRegistry.clear()
        assert len(CallbackRegistry._handlers) == 0

    def test_emit_with_no_handlers(self):
        # Should not raise
        CallbackRegistry.emit("event", {"data": 1})

    def test_event_constants(self):
        assert LLM_START == "llm_start"
        assert LLM_END == "llm_end"
        assert EMBEDDING_START == "embedding_start"
        assert EMBEDDING_END == "embedding_end"
        assert RETRIEVAL_START == "retrieval_start"
        assert RETRIEVAL_END == "retrieval_end"
        assert TRANSFORM_START == "transform_start"
        assert TRANSFORM_END == "transform_end"
