# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import json
from json.decoder import JSONDecodeError
from typing import List, Optional, Sequence, Dict, Any

from graphrag_toolkit.lexical_graph.indexing.model import Propositions
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY

from graphrag_toolkit.lexical_graph.indexing.extract.progress import run_jobs_with_progress

from llama_index.core.schema import BaseNode
from llama_index.core.bridge.pydantic import Field, PrivateAttr
from llama_index.core.extractors.interface import BaseExtractor

DEFAULT_PROPOSITION_MODEL = 'chentong00/propositionizer-wiki-flan-t5-large'


logger = logging.getLogger(__name__)

class PropositionExtractor(BaseExtractor):
    """
    Handles the extraction of propositions from textual data using a pre-trained transformer model.

    This class serves as a tool to extract structured propositions and entities from textual
    content. It leverages transformer-based models (`transformers` library) and integrates
    with an asynchronous workflow to process multiple nodes efficiently.

    Attributes:
        proposition_model_name (str): The name of the pre-trained model used for extracting
            propositions (e.g., AutoModelForSeq2SeqLM model).
        device (Optional[str]): The computational device to be used, such as 'cuda' or 'cpu'. If
            not specified, the most appropriate device is selected automatically.
        source_metadata_field (Optional[str]): The metadata field in nodes from which to extract
            propositions; if not provided, the text field of nodes is used.
        _proposition_tokenizer (Optional[Any]): Internal attribute to store the initialized tokenizer
            used for proposition extraction.
        _proposition_model (Optional[Any]): Internal attribute to store the initialized model
            for proposition extraction.
    """
    proposition_model_name: str = Field(
        default=DEFAULT_PROPOSITION_MODEL,
        description='The model name of the AutoModelForSeq2SeqLM model to use.',
    )
   
    device: Optional[str] = Field(
        default=None, 
        description="Device to run model on, i.e. 'cuda', 'cpu'"
    )
        
    source_metadata_field: Optional[str] = Field(
        description='Metadata field from which to extract propositions and entities'
    )

    _proposition_tokenizer: Optional[Any] = PrivateAttr(default=None)
    _proposition_model: Optional[Any] = PrivateAttr(default=None)

    @classmethod
    def class_name(cls) -> str:
        """
        Returns the name of the class as a string. Useful for identifying the class
        name programmatically, especially when dealing with multiple inheritance
        or dynamic class structures.

        Returns:
            str: The name of the class, 'PropositionExtractor'.
        """
        return 'PropositionExtractor'
    
    @property
    def proposition_tokenizer(self):
        """
        Provides a property for accessing a tokenizer specific to proposition models. This property ensures
        that the tokenizer is loaded only once and cached for future use. It attempts to dynamically import
        the required `transformers` library and initializes a tokenizer using the pre-defined model name.
        Raises an appropriate error if the `transformers` package is unavailable.

        Raises:
            ImportError: If the `transformers` package is not installed and cannot be imported.

        Returns:
            transformers.PreTrainedTokenizer: Tokenizer instance initialized with the proposition model
            specified by `proposition_model_name`.
        """
        if self._proposition_tokenizer is None:
            try:
                from transformers import AutoTokenizer
                self._proposition_tokenizer = AutoTokenizer.from_pretrained(self.proposition_model_name)  # nosec B615 - model name is user-configurable; pinning not feasible
            except ImportError as e:
                raise ImportError(
                        "transformers package not found, install with 'pip install transformers'"
                    ) from e
        return self._proposition_tokenizer
    
    @property
    def proposition_model(self):
        """
        Fetches and initializes the proposition model for sequence-to-sequence learning tasks.

        The method first checks if the `proposition_model` attribute is already initialized. If not, it attempts to import
        the required libraries, load the model specified by `proposition_model_name`, and move it to the appropriate device:
        either GPU (`cuda`) if available or CPU. If the necessary packages are not installed, the method raises an ImportError
        informing the user to install `torch` and `transformers`.

        Attributes:
            proposition_model (AutoModelForSeq2SeqLM): The proposition model used for sequence-to-sequence tasks.

        Raises:
            ImportError: Raised if `torch` and/or `transformers` packages are not installed on the system.
        """
        if self._proposition_model is None:
            try:
                import torch
                from transformers import AutoModelForSeq2SeqLM
                device = self.device or ('cuda' if torch.cuda.is_available() else 'cpu')
                self._proposition_model = AutoModelForSeq2SeqLM.from_pretrained(self.proposition_model_name).to(device)  # nosec B615 - model name is user-configurable; pinning not feasible
            except ImportError as e:
                raise ImportError(
                        "torch and/or transformers packages not found, install with 'pip install torch transformers'"
                    ) from e
        return self._proposition_model
    

    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        """
        Asynchronously extracts propositions from a sequence of nodes and returns a list of
        dictionaries containing the extracted proposition data.

        Processes the given sequence of nodes, performs extraction operations for their associated
        propositions, and aggregates the results into a list.

        Args:
            nodes (Sequence[BaseNode]): A sequence of nodes for which propositions need to be
                extracted.

        Returns:
            List[Dict]: A list containing dictionaries with the extracted proposition data.
        """
        proposition_entries = await self._extract_propositions_for_nodes(nodes)
        return [proposition_entry for proposition_entry in proposition_entries]
    
    async def _extract_propositions_for_nodes(self, nodes):
        """
        Extracts propositions for the given nodes asynchronously.

        This method takes a list of nodes and processes each node to extract
        propositions using a helper method. The tasks are processed in parallel
        with a configurable number of workers. Optionally, a progress bar can
        be displayed during the execution.

        Args:
            nodes (List[Any]): A list of nodes for which propositions need to
                be extracted.

        Returns:
            Any: The result of the asynchronous job execution for extracting
                propositions for the provided nodes.
        """
        jobs = [
            self._extract_propositions_for_node(node) for node in nodes
        ]
        return await run_jobs_with_progress(
            jobs,
            show_progress=self.show_progress,
            workers=self.num_workers,
            desc=f'Extracting propositions [nodes: {len(nodes)}, num_workers: {self.num_workers}]',
            total=len(nodes),
            logger=logger
        )
        
    async def _extract_propositions_for_node(self, node):
        """
        Asynchronously extracts propositions for a given node based on its text or specified
        metadata field. Logs debug information if debug level is enabled.

        Args:
            node: The node object containing metadata and text attributes used for proposition
                extraction.

        Returns:
            dict: A dictionary containing extracted propositions with a key defined as
                `PROPOSITIONS_KEY`.
        """
        logger.debug(f'Extracting propositions for node {node.node_id}')
        text = node.metadata.get(self.source_metadata_field, node.text) if self.source_metadata_field else node.text
        proposition_collection = await self._extract_propositions(text)

        if logger.isEnabledFor(logging.DEBUG):
            s = f"""====================================
text: {text}
------------------------------------
propositions: {proposition_collection}
"""
            logger.debug(s)
            
        return {
            PROPOSITIONS_KEY: proposition_collection.model_dump()['propositions']
        }
            
    async def _extract_propositions(self, text):
        """
        Extracts propositions from the provided text using a pre-trained model and tokenizer.

        This method generates an input text including a title, section, and content, encodes it using
        a tokenizer, and processes it through a proposition generation model. The model's output
        is decoded, and the propositions are parsed as JSON. In case of malformed output, attempts
        are made to repair the JSON format to ensure valid proposition extraction.

        Args:
            text (str): The input text content for which propositions need to be extracted.

        Returns:
            Propositions: An object containing the extracted propositions in a list.

        """
        title = ''
        section = ''
        
        input_text = f'Title: {title}. Section: {section}. Content: {text}'
        
        input_ids = self.proposition_tokenizer(input_text, return_tensors='pt').input_ids
        outputs = self.proposition_model.generate(input_ids.to(self.device), max_length=1024).cpu()
        
        output_text = self.proposition_tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        propositions = []

        if output_text:
        
            try:
                propositions = json.loads(output_text)
            except JSONDecodeError as e:
                # sometimes there are missing double quotes at the end of a proposition
                if output_text[-2] != '"':
                    # add missing double quotes to end of last entry
                    output_text = output_text[0:-1] + '"]'
                # add missing double quotes to other entries
                xss = [[str(i) for i in p.split(', "')] for p in output_text[2:-2].split('", "')]
                cleaned = [
                    x
                    for xs in xss
                    for x in xs
                ]                               
                try:
                    propositions = json.loads(json.dumps(cleaned))
                except JSONDecodeError as e:            
                    logger.exception(f'Failed to parse output text as JSON: {output_text}')
                    raise e

        return Propositions(propositions=[p for p in propositions])
