from typing import Any, List
from pathlib import Path
import inspect
from graphrag_toolkit.lexical_graph.logging import logging


from llama_index.core.schema import Document

logger = logging.getLogger(__name__)


class LlamaIndexReaderProviderBase:
    """
    Generic adapter for LlamaIndex reader classes.

    Automatically detects how to call `.load_data()` for different LlamaIndex reader types.
    This allows unified handling across various sources like PDFs, websites, YouTube, etc.
    """

    def __init__(self, reader_cls, **reader_kwargs):
        logger.debug(f"Instantiating reader: {reader_cls.__name__} with args: {reader_kwargs}")
        self._reader = reader_cls(**reader_kwargs)

    def read(self, input_source: Any) -> List[Document]:
        logger.debug("Starting read()")
        logger.debug(f"Reader class: {self._reader.__class__.__name__}")
        logger.debug(f"Input source: {input_source} (type={type(input_source)})")

        try:
            sig = inspect.signature(self._reader.load_data)
            param_names = list(sig.parameters.keys())
            logger.debug(f"Detected load_data() signature: {sig}")
            logger.debug(f"Parameter names: {param_names}")

            # Match the appropriate argument for load_data
            if "file_path" in param_names:
                kwargs = {"file_path": input_source}
            elif "path" in param_names:
                kwargs = {"path": input_source}
            elif "urls" in param_names:
                kwargs = {"urls": input_source}
            elif "file" in param_names:
                kwargs = {"file": Path(input_source)}
            elif "ytlinks" in param_names:
                kwargs = {"ytlinks": [input_source]}
            else:
                raise RuntimeError(
                    f"Cannot determine input parameter for {self._reader.__class__.__name__}. "
                    f"Parameters: {param_names}"
                )

            # Add optional parameters if accepted
            if "metadata" in param_names:
                kwargs["metadata"] = True
            if "extra_info" in param_names and "extra_info" not in kwargs:
                kwargs["extra_info"] = None

            logger.debug(f"Calling load_data() with kwargs: {kwargs}")
            return self._reader.load_data(**kwargs)

        except Exception as e:
            logger.exception("Error during read()")
            raise RuntimeError(
                f"Failed to read using {self._reader.__class__.__name__}: {e}"
            ) from e
