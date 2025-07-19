from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.document_graph.query.document_graph_query_engine import DocumentGraphQueryEngine

class DocumentGraphPromptProvider(PromptProvider):
    def __init__(self, config):
        self.query_engine = DocumentGraphQueryEngine(config.query_settings)
        self.prompt_template = config.prompt_template

    def get_prompt(self, input_context: dict) -> str:
        graph_results = self.query_engine.retrieve(input_context["query"])
        context_snippet = "\n".join([node.content for node in graph_results[:3]])

        return self.prompt_template.format(context=context_snippet, **input_context)
