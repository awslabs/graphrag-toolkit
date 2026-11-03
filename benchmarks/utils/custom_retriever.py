# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Custom (external) retriever support for the benchmark harness.

A custom retriever is selected with a ``custom:<module_path>`` retriever ID,
for example::

    export BENCHMARK_RETRIEVER=custom:my_package.my_module.MyRetriever

``<module_path>`` is a dotted path to a class, or any other callable, that the
harness imports and calls with keyword arguments ``graph_store``,
``vector_store`` and ``llm`` (the token-tracking LLM cache the built-in
retrievers use for response generation), plus any extra keyword arguments given
as a JSON object in ``BENCHMARK_CUSTOM_RETRIEVER_KWARGS``. The object it returns
must satisfy :class:`CustomRetriever`.
"""

import importlib
import json
import logging
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable

from benchmarks.utils.benchmark_env import env_string

logger = logging.getLogger(__name__)

CUSTOM_RETRIEVER_PREFIX = 'custom:'

CUSTOM_RETRIEVER_KWARGS_ENV = 'BENCHMARK_CUSTOM_RETRIEVER_KWARGS'


@runtime_checkable
class CustomRetriever(Protocol):
    """The interface a custom retriever must satisfy to be benchmarked.

    ``query(question)`` is called once per benchmark question and must return an
    object with:

    - ``response``: the generated answer text (``str``; ``None`` or empty counts
      as an empty response).
    - ``metadata`` (optional): a dict whose ``retrieve_ms``, ``answer_ms`` and
      ``total_ms`` entries, when present, are reported as the query's latency.

    A llama-index ``Response`` (what ``LexicalGraphQueryEngine.query`` returns)
    already has this shape.

    A custom retriever may also define ``get_config() -> dict``. When it does,
    the returned dict is recorded as the run's ``hyperparameters`` in
    ``metrics_summary.json``; otherwise the constructor keyword arguments are.
    """

    def query(self, question: str) -> Any:
        ...


def is_custom_retriever_id(retriever_id: str) -> bool:
    """Return True if ``retriever_id`` has the ``custom:`` prefix."""
    return retriever_id.startswith(CUSTOM_RETRIEVER_PREFIX)


def get_module_path(retriever_id: str) -> str:
    """Return the ``<module>.<attribute>`` part of a ``custom:`` retriever ID.

    Raises:
        ValueError: If ``retriever_id`` is not of the form ``custom:<module>.<attribute>``.
    """
    module_path = retriever_id[len(CUSTOM_RETRIEVER_PREFIX):] if is_custom_retriever_id(retriever_id) else ''
    module_name, _, attribute = module_path.rpartition('.')
    if not module_name or not attribute:
        raise ValueError(
            f"Invalid custom retriever identifier: '{retriever_id}'. "
            f"Expected '{CUSTOM_RETRIEVER_PREFIX}<module>.<attribute>', "
            f"e.g. '{CUSTOM_RETRIEVER_PREFIX}my_package.my_module.MyRetriever'."
        )
    return module_path


def load_custom_retriever_factory(retriever_id: str) -> Callable[..., Any]:
    """Import and return the class or callable named by a ``custom:`` retriever ID.

    Raises:
        ValueError: If the ID is malformed, or the module has no such attribute,
            or the attribute is not callable.
        ImportError: If the module cannot be imported.
    """
    module_path = get_module_path(retriever_id)
    module_name, _, attribute = module_path.rpartition('.')

    module = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if factory is None:
        raise ValueError(f"Module '{module_name}' has no attribute '{attribute}' (from '{retriever_id}').")
    if not callable(factory):
        raise ValueError(f"'{module_path}' is not a class or callable (from '{retriever_id}').")
    return factory


def create_custom_retriever(
    retriever_id: str,
    graph_store,
    vector_store,
    llm=None,
    retriever_kwargs: Optional[Dict[str, Any]] = None,
) -> CustomRetriever:
    """Instantiate the custom retriever named by ``retriever_id``.

    Raises:
        TypeError: If the constructed object does not satisfy :class:`CustomRetriever`.
    """
    factory = load_custom_retriever_factory(retriever_id)
    logger.info(f"Creating custom retriever '{get_module_path(retriever_id)}' with kwargs {retriever_kwargs or {}}")
    retriever = factory(
        graph_store=graph_store,
        vector_store=vector_store,
        llm=llm,
        **(retriever_kwargs or {}),
    )
    if not isinstance(retriever, CustomRetriever):
        raise TypeError(
            f"'{get_module_path(retriever_id)}' returned a {type(retriever).__name__}, which has no "
            f"query(question) method. See benchmarks.utils.custom_retriever.CustomRetriever."
        )
    return retriever


def get_custom_retriever_config(
    retriever_id: str,
    response_llm: str,
    retriever: Optional[Any] = None,
    retriever_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return the ``retriever_config`` block for a custom retriever run.

    ``hyperparameters`` is the result of ``retriever.get_config()`` when the
    retriever defines it, otherwise the constructor keyword arguments. The
    module path and constructor keyword arguments are always recorded, so the
    run can be reconstructed from ``metrics_summary.json``. Values that are not
    JSON-serializable are recorded as strings rather than failing the run.
    """
    constructor_kwargs = dict(retriever_kwargs or {})
    get_config = getattr(retriever, 'get_config', None)

    if callable(get_config):
        hyperparameters = get_config()
        if not isinstance(hyperparameters, dict):
            raise TypeError(
                f"{type(retriever).__name__}.get_config() must return a dict, "
                f"got {type(hyperparameters).__name__}."
            )
    else:
        hyperparameters = constructor_kwargs

    return _to_json_safe({
        'retriever_id': retriever_id,
        'response_llm': response_llm,
        'module_path': get_module_path(retriever_id),
        'constructor_kwargs': constructor_kwargs,
        'hyperparameters': hyperparameters,
    })


def read_custom_retriever_kwargs(name: str = CUSTOM_RETRIEVER_KWARGS_ENV) -> Optional[Dict[str, Any]]:
    """Read the custom retriever's constructor keyword arguments from a JSON-object env var.

    Returns None when the variable is unset or empty.

    Raises:
        ValueError: If the value is not a JSON object.
    """
    raw = env_string(name, None)
    if raw is None:
        return None

    try:
        kwargs = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f'{name}={raw!r} is not valid JSON: {e}') from None

    if not isinstance(kwargs, dict):
        raise ValueError(f'{name} must be a JSON object, got {type(kwargs).__name__}.')

    return kwargs


def _to_json_safe(value: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(value, default=str))
