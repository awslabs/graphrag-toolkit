# Copyright (c) Evan Erwee. All rights reserved.

"""LLM Enricher Plugin for enhancing documents with AI-generated content.

This module provides functionality to enrich document content using Large Language
Models (LLMs) via the OpenAI API. It can perform various tasks such as tagging,
summarization, and classification of document content, adding the AI-generated
outputs as new fields in the document records.

This enrichment is useful for adding semantic metadata to documents, generating
concise summaries, or categorizing content without manual intervention.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


import os
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


class LLMEnricherPlugin(TransformerProvider):
    """Enriches records using OpenAI's LLMs for content analysis and enhancement.
    
    This transformer uses OpenAI's API to process text content in records and
    generate additional metadata such as tags, summaries, or classifications.
    It adds the LLM-generated content as new fields in each record, preserving
    the original content.
    
    Configuration:
        task (str): The type of enrichment to perform. Options are:
                   - "tag": Generate relevant tags/categories for the content
                   - "summarize": Create a concise summary of the content
                   - "classify": Identify the topic or domain of the content
                   Defaults to "tag".
        field (str): The field name containing the text to analyze.
                    Defaults to "content".
        llm_model (str): The OpenAI model to use for generation.
                        Defaults to "gpt-4".
        api_key (str): OpenAI API key. If not provided, will look for
                      OPENAI_API_KEY environment variable.
    """

    def __init__(self, config):
        """Initialize the LLM enricher with configuration.
        
        This method sets up the OpenAI client and configures the enricher
        based on the provided configuration. It validates that the OpenAI
        library is installed and that an API key is available.
        
        Args:
            config: Configuration object containing transformer settings.
                   Must include an 'args' dictionary with task, field,
                   and model settings. Can optionally include an API key.
                   
        Raises:
            ImportError: If the 'openai' library is not installed.
            ValueError: If no OpenAI API key is provided in config or environment.
        """
        super().__init__(config)
        if OpenAI is None:
            raise ImportError("Please install the 'openai' library to use LLMEnricherPlugin")

        self.task = self.args.get("task", "tag")
        self.input_field = self.args.get("field", "content")
        self.model = self.args.get("llm_model", "gpt-4")
        
        api_key = self.args.get("api_key") or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Missing OpenAI API key in config or environment")
        
        self.client = OpenAI(api_key=api_key)

    def _make_prompt(self, text: str) -> str:
        """Create an appropriate prompt for the configured task.
        
        This method generates a task-specific prompt to send to the LLM,
        based on the task type configured for this enricher (tag, summarize,
        or classify).
        
        Args:
            text: The document text to be processed by the LLM
            
        Returns:
            A formatted prompt string that instructs the LLM on the task
            to perform on the provided text.
            
        Raises:
            ValueError: If the configured task is not one of the supported types.
        """
        if self.task == "tag":
            return f"Tag the following content with relevant categories:\n\n{text}"
        elif self.task == "summarize":
            return f"Summarize the following content:\n\n{text}"
        elif self.task == "classify":
            return f"Classify the topic or domain of the content below:\n\n{text}"
        else:
            raise ValueError(f"Unknown task: {self.task}")

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by enriching them with LLM-generated content.
        
        This method processes each record by:
        1. Extracting the text from the configured input field
        2. Creating a task-specific prompt for the LLM
        3. Sending the prompt to the OpenAI API
        4. Adding the LLM's response to the record in a new field
        
        The new field is named based on the task (e.g., "llm_tag", "llm_summarize").
        If the API call fails, an error field is added instead (e.g., "llm_tag_error").
        
        Records with empty or non-string content in the input field are passed
        through with minimal processing.
        
        Args:
            records: A list of record dictionaries to enrich
            
        Returns:
            A list of enriched records with LLM-generated content added.
            Each record maintains its original structure with additional
            fields containing the enrichment results.
        """
        enriched_records = []
        for record in records:
            enriched_record = record.copy()
            text = record.get(self.input_field, "")
            
            if text and isinstance(text, str) and text.strip():
                try:
                    prompt = self._make_prompt(text)
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.3,
                    )
                    output = response.choices[0].message.content.strip()
                    enriched_record[f"llm_{self.task}"] = output
                except Exception as e:
                    enriched_record[f"llm_{self.task}_error"] = str(e)
            
            enriched_records.append(enriched_record)

        return enriched_records