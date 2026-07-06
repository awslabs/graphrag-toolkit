# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic LlamaIndex reader plugin — wraps any reader from the LlamaIndex ecosystem.

Usage:
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
        LlamaIndexPluginReaderConfig,
    )
    from graphrag_toolkit.lexical_graph.indexing.load.readers.providers import (
        LlamaIndexPluginReaderProvider,
    )

    config = LlamaIndexPluginReaderConfig(
        package="llama-index-readers-confluence",
        module_path="llama_index.readers.confluence",
        reader_class="ConfluenceReader",
        init_args={"base_url": "https://mycompany.atlassian.net/wiki", "token": "..."},
        load_args={"space_key": "ENG"},
        timeout_seconds=60,
        max_retries=2,
    )
    provider = LlamaIndexPluginReaderProvider(config)
    docs = provider.read(None)
"""

import importlib
import signal
import time
from typing import Any, List, Optional
from functools import wraps

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
    LlamaIndexPluginReaderConfig,
)
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)


class ReaderImportError(ImportError):
    """Raised when the required LlamaIndex reader package is not installed."""
    pass


class ReaderAuthError(RuntimeError):
    """Raised when authentication fails (expired token, invalid credentials)."""
    pass


class ReaderTimeoutError(TimeoutError):
    """Raised when the reader exceeds the configured timeout."""
    pass


class LlamaIndexPluginReaderProvider:
    """Generic reader provider that wraps any LlamaIndex reader via configuration.

    Features:
        - Dynamic import of any LlamaIndex reader package
        - Configurable timeout (prevents hangs)
        - Retry with exponential backoff on transient failures
        - Graceful degradation (fail_on_error=False returns [] instead of raising)
        - Structured logging at every decision point
        - Auth failure detection (recognizes 401/403 patterns)
        - Empty result warnings
        - Metadata enrichment support
    """

    # Known auth-related error patterns (case-insensitive matching)
    _AUTH_ERROR_PATTERNS = (
        "401", "403", "unauthorized", "forbidden",
        "invalid token", "expired token", "authentication failed",
        "access denied", "invalid credentials", "token expired",
    )

    # Known transient error patterns (retry-worthy)
    _TRANSIENT_ERROR_PATTERNS = (
        "429", "500", "502", "503", "504",
        "rate limit", "too many requests", "timeout",
        "connection reset", "connection refused",
        "temporary failure", "service unavailable",
    )

    def __init__(self, config: LlamaIndexPluginReaderConfig):
        """Initialize by dynamically importing and instantiating the reader.

        Args:
            config: Plugin configuration specifying which reader to load and how.

        Raises:
            ReaderImportError: If the required package is not installed.
            ValueError: If required config fields are missing.
        """
        self._config = config
        self._reader = None
        self._validate_config()
        self._import_and_init_reader()

    def _validate_config(self) -> None:
        """Validate that required configuration fields are present."""
        if not self._config.reader_class:
            raise ValueError(
                "LlamaIndexPluginReaderConfig.reader_class is required "
                "(e.g. 'ConfluenceReader')"
            )
        if not self._config.module_path and not self._config.package:
            raise ValueError(
                "LlamaIndexPluginReaderConfig requires either module_path "
                "(e.g. 'llama_index.readers.confluence') or package "
                "(e.g. 'llama-index-readers-confluence')"
            )

    def _resolve_module_path(self) -> str:
        """Resolve the Python module path from config.

        Priority: module_path > derived from package name.
        """
        if self._config.module_path:
            return self._config.module_path
        # Derive from package: "llama-index-readers-confluence" → "llama_index.readers.confluence"
        parts = self._config.package.replace("-", "_").split("_")
        # Handle "llama_index_readers_X" pattern
        if len(parts) >= 4 and parts[0] == "llama" and parts[1] == "index":
            return f"llama_index.readers.{'.'.join(parts[3:])}"
        # Fallback: just replace dashes
        return self._config.package.replace("-", "_")

    def _import_and_init_reader(self) -> None:
        """Dynamically import the reader module and instantiate the reader class."""
        module_path = self._resolve_module_path()
        class_name = self._config.reader_class

        logger.info(
            f"LlamaIndexPlugin: importing {class_name} from {module_path} "
            f"(package: {self._config.package or 'not specified'})"
        )

        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            install_hint = self._config.package or module_path
            msg = (
                f"Failed to import '{module_path}'. "
                f"Install the reader package: pip install {install_hint}"
            )
            logger.error(msg)
            raise ReaderImportError(msg) from e

        try:
            reader_cls = getattr(module, class_name)
        except AttributeError as e:
            available = [a for a in dir(module) if not a.startswith("_")]
            msg = (
                f"Class '{class_name}' not found in '{module_path}'. "
                f"Available: {available[:10]}"
            )
            logger.error(msg)
            raise ReaderImportError(msg) from e

        # Instantiate the reader with init_args
        init_args = self._config.init_args or {}
        try:
            self._reader = reader_cls(**init_args)
            logger.info(
                f"LlamaIndexPlugin: {class_name} initialized successfully "
                f"(init_args keys: {list(init_args.keys())})"
            )
        except TypeError as e:
            msg = (
                f"Failed to instantiate {class_name} with args {list(init_args.keys())}. "
                f"Check init_args match the constructor signature. Error: {e}"
            )
            logger.error(msg)
            raise ValueError(msg) from e
        except Exception as e:
            if self._is_auth_error(e):
                msg = (
                    f"Authentication failed when initializing {class_name}. "
                    f"Check credentials in init_args. Error: {e}"
                )
                logger.error(msg)
                raise ReaderAuthError(msg) from e
            raise

    def _is_auth_error(self, error: Exception) -> bool:
        """Detect if an exception is an authentication/authorization failure."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in self._AUTH_ERROR_PATTERNS)

    def _is_transient_error(self, error: Exception) -> bool:
        """Detect if an exception is a transient/retryable failure."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in self._TRANSIENT_ERROR_PATTERNS)

    def read(self, input_source: Any = None) -> List[Document]:
        """Read documents using the configured LlamaIndex reader.

        Applies timeout, retry with backoff, and error classification.

        Args:
            input_source: Optional input passed to load_args (reader-specific).
                          Can be a space_key, URL, file path, etc.

        Returns:
            List of Document objects. Empty list if fail_on_error=False and read fails.

        Raises:
            ReaderAuthError: On authentication failures (not retried).
            ReaderTimeoutError: When timeout_seconds is exceeded.
            RuntimeError: On non-transient failures when fail_on_error=True.
        """
        load_fn = self._resolve_load_function()
        if load_fn is None:
            return []

        load_args = self._build_load_args(input_source)
        return self._execute_with_retries(load_fn, load_args)

    def _resolve_load_function(self) -> Optional[Any]:
        """Resolve the callable load method from the reader instance.

        Returns:
            The load function, or None if resolution fails (with appropriate error/log).
        """
        if self._reader is None:
            logger.error("LlamaIndexPlugin: reader not initialized — cannot read")
            if self._config.fail_on_error:
                raise RuntimeError("Reader not initialized")
            return None

        load_method_name = self._config.load_method or "load_data"
        load_fn = getattr(self._reader, load_method_name, None)

        if load_fn is None:
            msg = (
                f"Reader {self._config.reader_class} has no method '{load_method_name}'. "
                f"Available methods: {[m for m in dir(self._reader) if not m.startswith('_')][:15]}"
            )
            logger.error(msg)
            if self._config.fail_on_error:
                raise AttributeError(msg)
            return None

        return load_fn

    def _build_load_args(self, input_source: Any = None) -> dict:
        """Assemble the keyword arguments for the load function.

        Merges configured load_args with an optional input_source.
        """
        load_args = dict(self._config.load_args or {})
        if input_source is not None:
            load_args["input_source"] = input_source
        return load_args

    def _execute_with_retries(self, load_fn: Any, load_args: dict) -> List[Document]:
        """Execute load_fn with retry loop, timeout, and error classification.

        Args:
            load_fn: The reader's load method.
            load_args: Keyword arguments for the load method.

        Returns:
            List of Documents on success, or [] on graceful failure.
        """
        max_attempts = 1 + (self._config.max_retries or 0)
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                documents = self._execute_with_timeout(load_fn, load_args)
                return self._post_process(documents)
            except ReaderAuthError:
                raise
            except (ReaderTimeoutError, Exception) as e:
                last_error = e
                if not self._handle_attempt_error(
                    last_error, attempt, max_attempts
                ):
                    break

        return self._handle_exhausted_retries(max_attempts, last_error)

    def _handle_attempt_error(self, error: Exception, attempt: int, max_attempts: int) -> bool:
        """Classify and handle an error from a single attempt.

        Args:
            error: The exception that occurred.
            attempt: Current attempt number (1-based).
            max_attempts: Total allowed attempts.

        Returns:
            True if should continue retrying, False if should stop.
        """
        if isinstance(error, ReaderTimeoutError):
            logger.warning(
                f"LlamaIndexPlugin: timeout on attempt {attempt}/{max_attempts} "
                f"({self._config.timeout_seconds}s exceeded)"
            )
            if attempt < max_attempts:
                self._backoff(attempt)
                return True
            return False

        if self._is_auth_error(error):
            msg = (
                f"LlamaIndexPlugin: authentication failed — "
                f"{self._config.reader_class}: {error}"
            )
            logger.error(msg)
            raise ReaderAuthError(msg) from error

        if self._is_transient_error(error) and attempt < max_attempts:
            logger.warning(
                f"LlamaIndexPlugin: transient error on attempt "
                f"{attempt}/{max_attempts}: {error}"
            )
            self._backoff(attempt)
            return True

        # Non-transient, non-auth error — stop retrying
        load_method_name = self._config.load_method or "load_data"
        logger.error(
            f"LlamaIndexPlugin: {self._config.reader_class}.{load_method_name}() "
            f"failed on attempt {attempt}: {error}",
            exc_info=True,
        )
        return False

    def _handle_exhausted_retries(
        self, max_attempts: int, last_error: Optional[Exception]
    ) -> List[Document]:
        """Handle the case where all retry attempts have been exhausted.

        Returns:
            Empty list if fail_on_error=False.

        Raises:
            RuntimeError: If fail_on_error=True.
        """
        if self._config.fail_on_error:
            raise RuntimeError(
                f"LlamaIndexPlugin: all {max_attempts} attempt(s) failed for "
                f"{self._config.reader_class}. Last error: {last_error}"
            ) from last_error

        logger.warning(
            f"LlamaIndexPlugin: returning empty result after {max_attempts} failed "
            f"attempt(s). Last error: {last_error}"
        )
        return []

    def _execute_with_timeout(
        self, load_fn: Any, load_args: dict
    ) -> List[Document]:
        """Execute the load function with a timeout guard.

        Uses threading-based timeout (signal.alarm not safe in all contexts).
        """
        import concurrent.futures

        timeout = self._config.timeout_seconds or 120

        # Remove input_source from load_args if the reader doesn't accept it
        # Try with input_source first, fall back without it
        args_to_try = load_args.copy()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._call_load_fn, load_fn, args_to_try)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError as e:
                future.cancel()
                raise ReaderTimeoutError(
                    f"Reader {self._config.reader_class} exceeded "
                    f"timeout of {timeout}s"
                ) from e

    def _call_load_fn(self, load_fn: Any, load_args: dict) -> List[Document]:
        """Call the load function, handling input_source parameter flexibility."""
        try:
            return load_fn(**load_args)
        except TypeError as e:
            # If "input_source" isn't a valid param, try without it
            if "input_source" in load_args and "unexpected keyword argument" in str(e):
                logger.debug(
                    "LlamaIndexPlugin: reader doesn't accept 'input_source', "
                    "retrying without it"
                )
                args_without_source = {
                    k: v for k, v in load_args.items() if k != "input_source"
                }
                return load_fn(**args_without_source)
            raise

    def _post_process(self, documents: Any) -> List[Document]:
        """Validate and enrich returned documents."""
        # Handle generators/iterators
        if hasattr(documents, '__iter__') and not isinstance(documents, list):
            documents = list(documents)

        if not isinstance(documents, list):
            logger.warning(
                f"LlamaIndexPlugin: reader returned {type(documents).__name__} "
                f"instead of list — wrapping"
            )
            documents = [documents] if documents else []

        # Filter out non-Document items
        valid_docs = []
        for i, doc in enumerate(documents):
            if isinstance(doc, Document):
                valid_docs.append(doc)
            elif hasattr(doc, 'text') and hasattr(doc, 'metadata'):
                # Duck-typing: looks like a Document
                valid_docs.append(doc)
            else:
                logger.debug(
                    f"LlamaIndexPlugin: skipping non-Document item at index {i}: "
                    f"{type(doc).__name__}"
                )

        # Apply metadata enrichment
        if self._config.metadata_fn and valid_docs:
            source_id = self._config.package or self._config.reader_class
            try:
                extra_metadata = self._config.metadata_fn(source_id)
                if extra_metadata and isinstance(extra_metadata, dict):
                    for doc in valid_docs:
                        doc.metadata.update(extra_metadata)
            except Exception as e:
                logger.warning(f"LlamaIndexPlugin: metadata_fn failed: {e}")

        # Log results
        if not valid_docs:
            logger.warning(
                f"LlamaIndexPlugin: {self._config.reader_class} returned 0 documents"
            )
        else:
            logger.info(
                f"LlamaIndexPlugin: {self._config.reader_class} returned "
                f"{len(valid_docs)} document(s)"
            )

        return valid_docs

    def _backoff(self, attempt: int) -> None:
        """Exponential backoff between retry attempts."""
        base = self._config.retry_backoff_seconds or 2.0
        delay = base * (2 ** (attempt - 1))
        logger.info(f"LlamaIndexPlugin: backing off {delay:.1f}s before retry")
        time.sleep(delay)
