# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Text splitters matching LlamaIndex SentenceSplitter and TokenTextSplitter behavior."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import partial
from typing import Callable, List, Optional, Tuple

import tiktoken

from graphrag_toolkit.core.types import Document, Node, NodeRef

_CHUNKING_REGEX = "[^,.;。？！]+[,.;。？！]?|[,.;。？！]"
_PARAGRAPH_SEP = "\n\n\n"


def _get_tokenizer() -> Callable[[str], list]:
    enc = tiktoken.get_encoding("cl100k_base")
    return partial(enc.encode, allowed_special="all")


def _split_by_sep(text: str, sep: str) -> List[str]:
    """Split keeping separator prepended to subsequent parts."""
    parts = text.split(sep)
    result = [sep + s if i > 0 else s for i, s in enumerate(parts)]
    return [s for s in result if s]


def _sentence_tokenize(text: str) -> List[str]:
    """Sentence tokenize using nltk punkt (matches LlamaIndex default)."""
    try:
        from nltk.tokenize import PunktSentenceTokenizer
        tokenizer = PunktSentenceTokenizer()
        spans = list(tokenizer.span_tokenize(text))
        sentences = []
        for i, span in enumerate(spans):
            start = span[0]
            end = spans[i + 1][0] if i < len(spans) - 1 else len(text)
            sentences.append(text[start:end])
        return sentences
    except ImportError:
        # Fallback regex-based sentence splitting
        parts = re.split(r'(?<=[.!?])\s+', text)
        return [p for p in parts if p]


@dataclass
class _Split:
    text: str
    is_sentence: bool
    token_size: int


class SentenceSplitter:
    """Token-aware text splitter that prefers sentence boundaries.

    Matches LlamaIndex SentenceSplitter behavior.
    """

    def __init__(
        self,
        chunk_size: int = 256,
        chunk_overlap: int = 25,
        separator: str = " ",
        paragraph_separator: str = _PARAGRAPH_SEP,
        secondary_chunking_regex: Optional[str] = _CHUNKING_REGEX,
    ):
        if chunk_overlap > chunk_size:
            raise ValueError(
                f"chunk_overlap ({chunk_overlap}) > chunk_size ({chunk_size})"
            )
        if chunk_overlap > chunk_size // 2:
            raise ValueError(
                f"chunk_overlap ({chunk_overlap}) must be <= chunk_size // 2 ({chunk_size // 2})"
            )
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._separator = separator
        self._paragraph_separator = paragraph_separator
        self._secondary_chunking_regex = secondary_chunking_regex
        self._tokenizer = _get_tokenizer()

    def _token_size(self, text: str) -> int:
        return len(self._tokenizer(text))

    def _get_splits_by_fns(self, text: str) -> Tuple[List[str], bool]:
        # Primary: paragraph sep, then sentence tokenizer
        for split_fn in [
            lambda t: _split_by_sep(t, self._paragraph_separator),
            _sentence_tokenize,
        ]:
            splits = split_fn(text)
            if len(splits) > 1:
                return splits, True

        # Sub-sentence: regex, word, char
        sub_fns: List[Callable] = []
        if self._secondary_chunking_regex:
            sub_fns.append(lambda t: re.findall(self._secondary_chunking_regex, t))
        sub_fns.append(lambda t: t.split(self._separator) if self._separator else [t])
        sub_fns.append(list)

        for split_fn in sub_fns:
            splits = split_fn(text)
            if len(splits) > 1:
                break

        return splits, False

    def _split(self, text: str, chunk_size: int) -> List[_Split]:
        token_size = self._token_size(text)
        if token_size <= chunk_size:
            return [_Split(text, is_sentence=True, token_size=token_size)]

        text_splits_by_fns, is_sentence = self._get_splits_by_fns(text)
        text_splits = []
        for part in text_splits_by_fns:
            ts = self._token_size(part)
            if ts <= chunk_size:
                text_splits.append(_Split(part, is_sentence=is_sentence, token_size=ts))
            else:
                text_splits.extend(self._split(part, chunk_size))
        return text_splits

    def _merge(self, splits: List[_Split], chunk_size: int) -> List[str]:
        chunks: List[str] = []
        cur_chunk: List[Tuple[str, int]] = []
        last_chunk: List[Tuple[str, int]] = []
        cur_chunk_len = 0
        new_chunk = True

        def close_chunk():
            nonlocal chunks, cur_chunk, last_chunk, cur_chunk_len, new_chunk
            chunks.append("".join(t for t, _ in cur_chunk))
            last_chunk = cur_chunk
            cur_chunk = []
            cur_chunk_len = 0
            new_chunk = True
            # Add overlap from end of last chunk
            if last_chunk:
                last_index = len(last_chunk) - 1
                while last_index >= 0 and cur_chunk_len + last_chunk[last_index][1] <= self.chunk_overlap:
                    cur_chunk_len += last_chunk[last_index][1]
                    cur_chunk.insert(0, last_chunk[last_index])
                    last_index -= 1

        i = 0
        while i < len(splits):
            cur_split = splits[i]
            if cur_split.token_size > chunk_size:
                raise ValueError("Single token exceeded chunk size")
            if cur_chunk_len + cur_split.token_size > chunk_size and not new_chunk:
                close_chunk()
            else:
                if new_chunk and cur_chunk_len + cur_split.token_size > chunk_size:
                    while cur_chunk and cur_chunk_len + cur_split.token_size > chunk_size:
                        _, length = cur_chunk.pop(0)
                        cur_chunk_len -= length
                if (
                    cur_split.is_sentence
                    or cur_chunk_len + cur_split.token_size <= chunk_size
                    or new_chunk
                ):
                    cur_chunk_len += cur_split.token_size
                    cur_chunk.append((cur_split.text, cur_split.token_size))
                    i += 1
                    new_chunk = False
                else:
                    close_chunk()

        if not new_chunk:
            chunks.append("".join(t for t, _ in cur_chunk))

        return [c.strip() for c in chunks if c.strip()]

    def split_text(self, text: str) -> List[str]:
        """Split text into chunks."""
        if text == "":
            return [text]
        splits = self._split(text, self.chunk_size)
        return self._merge(splits, self.chunk_size)

    def get_nodes_from_documents(self, documents: list) -> List[Node]:
        """Split documents into Node chunks with relationships and char indices."""
        all_nodes = []
        for doc in documents:
            text = doc.text
            chunks = self.split_text(text)
            nodes = []
            search_start = 0
            for chunk_text in chunks:
                start_idx = text.find(chunk_text, search_start)
                if start_idx == -1:
                    start_idx = None
                    end_idx = None
                else:
                    end_idx = start_idx + len(chunk_text)
                    search_start = start_idx + 1
                node = Node(
                    text=chunk_text,
                    metadata=dict(doc.metadata),
                    relationships={"source": NodeRef(node_id=doc.node_id, metadata=doc.metadata)},
                    start_char_idx=start_idx,
                    end_char_idx=end_idx,
                )
                nodes.append(node)

            # Set prev/next relationships
            for i, node in enumerate(nodes):
                if i > 0:
                    node.relationships["previous"] = NodeRef(node_id=nodes[i - 1].node_id)
                if i < len(nodes) - 1:
                    node.relationships["next"] = NodeRef(node_id=nodes[i + 1].node_id)

            all_nodes.extend(nodes)
        return all_nodes

    def __call__(self, nodes: list, **kwargs) -> list:
        """Transform interface for pipeline compatibility."""
        return self.get_nodes_from_documents(nodes)


class TokenTextSplitter:
    """Simple fixed-token-count splitter with overlap."""

    def __init__(self, chunk_size: int = 256, chunk_overlap: int = 25):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._tokenizer = _get_tokenizer()
        self._enc = tiktoken.get_encoding("cl100k_base")

    def split_text(self, text: str) -> List[str]:
        """Split text into chunks of chunk_size tokens with overlap."""
        tokens = self._tokenizer(text)
        if len(tokens) <= self.chunk_size:
            return [text]
        chunks = []
        start = 0
        while start < len(tokens):
            end = start + self.chunk_size
            chunk_tokens = tokens[start:end]
            chunks.append(self._enc.decode(chunk_tokens))
            if end >= len(tokens):
                break
            start += self.chunk_size - self.chunk_overlap
        return chunks
