from unittest.mock import MagicMock

import pytest
from graphrag_toolkit.byokg_rag.graph_retrievers.graph_retrievers import (
    GraphQueryRetriever,
)


class TestGraphQueryRetrieverErrorFeedback:
    @pytest.mark.parametrize(
        "exc",
        [
            SyntaxError("Variable `x` not defined"),
            RuntimeError("Connection timeout"),
            ValueError("Unknown label"),
        ],
    )
    def test_error_surfaces_type_and_message(self, exc):
        store = MagicMock()
        store.execute_query.side_effect = exc
        context, answers = GraphQueryRetriever(store).retrieve(
            "MATCH (n) RETURN n", return_answers=True
        )
        assert answers == []
        assert type(exc).__name__ in context[0]
        assert str(exc) in context[0]

    def test_successful_query_returns_results(self):
        store = MagicMock()
        store.execute_query.return_value = [{"name": "Alice"}]
        context, answers = GraphQueryRetriever(store).retrieve(
            "MATCH (n) RETURN n.name", return_answers=True
        )
        assert answers == [{"name": "Alice"}]
        assert "Execution Result" in context[0]
        assert "Error" not in context[0]


class TestCypherRetryFeedback:
    def _make_engine(self, retrieve_fn):
        from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine

        linker = MagicMock()
        linker.task_prompts = ""
        linker.is_cypher_linker.return_value = True

        executor = MagicMock()
        executor.retrieve.side_effect = retrieve_fn

        engine = object.__new__(ByoKGQueryEngine)
        engine.cypher_kg_linker = linker
        engine.kg_linker = None
        engine.graph_query_executor = executor
        engine.schema = "(:Person)-[:KNOWS]->(:Person)"
        engine.direct_query_linking = False
        engine.entity_linker = None
        engine.triplet_retriever = None
        engine.path_retriever = None
        return engine, linker

    @pytest.mark.parametrize("tag", ["opencypher", "opencypher-linking"])
    def test_error_gets_error_specific_feedback(self, tag):
        def fake(query, return_answers=False):
            return [f"Error executing query: {query}\nError: SyntaxError: bad"], []

        engine, linker = self._make_engine(fake)
        linker.generate_response.return_value = f"<{tag}>MATCH (n) RETURN m</{tag}>"
        linker.parse_response.return_value = {tag: ["MATCH (n) RETURN m"]}

        feedback = "\n".join(engine.query("test?", cypher_iterations=1))
        assert "review the error message" in feedback

    def test_empty_results_gets_generic_feedback(self):
        def fake(query, return_answers=False):
            return [f"Graph Query: {query}\nExecution Result: []"], []

        engine, linker = self._make_engine(fake)
        linker.generate_response.return_value = (
            "<opencypher>MATCH (n) RETURN n</opencypher>"
        )
        linker.parse_response.return_value = {"opencypher": ["MATCH (n) RETURN n"]}

        feedback = "\n".join(engine.query("test?", cypher_iterations=1))
        assert "focusing more on the given schema" in feedback
        assert "review the error message" not in feedback
