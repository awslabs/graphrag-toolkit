# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Token usage of Bedrock Converse calls, captured around the boto3 client.

BedrockGenerator returns only the text of a Converse response, so usage is recorded
by the client it calls: the harness builds the generator with this wrapper as its
client, and the toolkit under test is unchanged.
"""

import threading
from typing import Any, Callable, Dict

# Converse usage fields; the first two are always present.
USAGE_FIELDS = ('inputTokens', 'outputTokens', 'cacheReadInputTokens', 'cacheWriteInputTokens')
_RECORD_NAMES = {'inputTokens': 'input', 'outputTokens': 'output',
                 'cacheReadInputTokens': 'cache_read', 'cacheWriteInputTokens': 'cache_write'}


class UsageRecordingClient:
    """A bedrock-runtime client whose successful converse() calls add their token usage
    to a running total; every other attribute is the wrapped client's.

    make_client builds the wrapped client on first use, as BedrockGenerator does for
    its own, so constructing an engine still needs no AWS region or credentials.
    """

    def __init__(self, make_client: Callable[[], Any]):
        self._make_client = make_client
        self._client = None
        self._lock = threading.Lock()
        self.reset()

    @property
    def _inner(self):
        if self._client is None:
            self._client = self._make_client()
        return self._client

    def reset(self) -> None:
        with self._lock:
            self.calls = 0
            self._usage = {name: 0 for name in USAGE_FIELDS}

    def converse(self, **kwargs) -> Dict[str, Any]:
        response = self._inner.converse(**kwargs)
        usage = response.get('usage')
        if not isinstance(usage, dict) or not all(isinstance(usage.get(f), int) for f in USAGE_FIELDS[:2]):
            raise ValueError(f'converse response carries no token usage: {usage!r}')
        with self._lock:
            self.calls += 1
            for name in USAGE_FIELDS:
                self._usage[name] += int(usage.get(name) or 0)
        return response

    def snapshot(self) -> Dict[str, int]:
        """Tokens since the last reset(), as {'input', 'output', 'cache_read', 'cache_write'}."""
        with self._lock:
            return {_RECORD_NAMES[name]: value for name, value in self._usage.items()}

    def __getattr__(self, name):
        return getattr(self._inner, name)
