# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Response types for query engines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, List, Optional, Union


TokenGen = Iterator[str]


@dataclass
class Response:
    """Query response with source nodes and metadata."""

    response: str
    source_nodes: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class StreamingResponse:
    """Streaming query response."""

    response_gen: TokenGen
    source_nodes: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def get_response(self) -> "Response":
        """Consume the generator and return a complete Response."""
        text = "".join(self.response_gen)
        return Response(
            response=text,
            source_nodes=self.source_nodes,
            metadata=self.metadata,
        )

    @property
    def response(self) -> str:
        """Consume and return full text."""
        return self.get_response().response


RESPONSE_TYPE = Union[Response, StreamingResponse]
