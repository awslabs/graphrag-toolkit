import os
import sys
# Add the parent directory to the Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utils import parse_response, load_yaml
from typing import List, Tuple, Dict, Any, Optional

class KGLinker:
    """
    A linker that handles LLM-specific tasks for knowledge graph operations.
    This class focuses on generating and parsing LLM responses for various graph tasks.
    """

    def __init__(self,
            llm_generator, 
            graph_store
            ):
        """
        Initialize the KGLinker.

        Args:
            llm_generator: Language model for generating responses
            graph_store: Component that provides access to graph data
        """
        self.AVAILABLE_TASKS = {
            "entity-extraction": {"pattern": r"<entities>(.*?)</entities>"},
            "path-extraction": {"pattern": r"<paths>(.*?)</paths>"},
            "opencypher": {"pattern": r"<opencypher>(.*?)</opencypher>"},
            "draft-answer-generation": {"pattern": r"<answers>(.*?)</answers>"},
        }

        self.tasks = graph_store.get_linker_tasks()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.task_prompt_file = os.path.join(current_dir, "prompts", "task_prompts.yaml")
        self.prompt = load_yaml(os.path.join(current_dir, "prompts", "kg_linker_prompt.yaml"))["kg-linker-prompt"]
        self.task_prompts = self._finalize_prompt()
        self.task_prompts_iterative = self._finalize_prompt_iterative_prompt()

        self.llm_generator = llm_generator

    def _finalize_prompt(self) -> str:
        """
        Combine task prompts into a single string.

        Returns:
            str: Combined task prompts
        """
        task_prompts = ""
        for task in self.tasks:
            task_prompt = load_yaml(self.task_prompt_file)[task]
            task_prompts += f"\n\n{task_prompt}\n\n"
        return task_prompts
    
    def _finalize_prompt(self) -> str:
        """
        Combine task prompts into a single string.

        Returns:
            str: Combined task prompts
        """
        task_prompts = ""
        for task in self.tasks:
            task_prompt = load_yaml(self.task_prompt_file)[task]
            task_prompts += f"\n\n{task_prompt}\n\n"
        return task_prompts
    
    def _finalize_prompt_iterative_prompt(self):
        task_prompts = ""
        for task in self.tasks:
            if task == "entity-extraction":
                task_prompt = load_yaml(self.task_prompt_file)["entity-extraction-iterative"]
            else:
                task_prompt = load_yaml(self.task_prompt_file)[task]
            task_prompts += f"\n\n{task_prompt}\n\n"

        return task_prompts

    def generate_response(self, question: str, schema: str, graph_context: str = "", task_prompts: Optional[str] = None) -> str:
        """
        Generate an LLM response for the given query and context.

        Args:
            question: The search query
            schema: Graph schema information
            graph_context: Retrieved graph context
            task_prompts: Optional custom task prompts

        Returns:
            str: Generated LLM response
        """
        if not graph_context:
            graph_context = "No graph context provided. See the above schema."
        
        user_prompt = self.prompt['user-prompt']
        system_prompt = self.prompt['system-prompt']

        if task_prompts is None:
            task_prompts = self.task_prompts
        user_prompt = user_prompt.replace("{{task_prompts}}", f"{task_prompts}")

        user_prompt_formatted = user_prompt.format(
            question=question, 
            schema=schema, 
            graph_context=graph_context
        )
        return self.llm_generator.generate(
            prompt=user_prompt_formatted, 
            system_prompt=system_prompt
        )

    def parse_response(self, llm_response: str) -> Dict[str, List[str]]:
        """
        Parse the LLM response into task-specific artifacts.

        Args:
            llm_response: Raw LLM response

        Returns:
            Dict mapping task names to extracted artifacts
        """
        artifacts = {}
        for task in self.tasks:
            pattern = self.AVAILABLE_TASKS[task]['pattern']
            artifacts[task] = parse_response(llm_response, pattern)
        return artifacts
