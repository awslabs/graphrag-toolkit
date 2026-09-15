# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Measure storage-key collisions for the ids `IdGenerator` produces.

`create_source_id` returns `aws::{md5(text)[:w]}:{md5(metadata_str)[:4]}`, where
`w` is the configured `SourceIdWidth`. `IdRewriter` passes `''` when a node
carries no metadata, which makes the second component constant and leaves `4w`
bits to discriminate. Both S3 storage prefixes are built from the bare source id,
so two documents sharing one share a prefix.

Pass `--widths` to compare widths against the same corpus.

Keys are generated synthetically. Running extraction to produce them would cost
Bedrock time and measure nothing this question needs.

Two things are counted separately. A hash collision is two different texts
landing on one key. A duplicate is the same text twice, which produces the same
key by design and is deduplication rather than a defect. Only the first is a
reason to change the key.
"""

import argparse
import hashlib
import sys
from collections import Counter

import numpy as np

from graphrag_toolkit.lexical_graph.config import SourceIdWidth


CHUNK_ID_DELIMITER = '\x00'


def get_hash(s: str) -> str:
    """Reproduces indexing/utils/hash_utils.py::get_hash."""
    return hashlib.md5(s.encode('utf-8'), usedforsecurity=False).digest().hex()


# The metadata component is a fixed four hex characters in IdGenerator.
METADATA_HASH_CHARS = 4
METADATA_HASH_BITS = METADATA_HASH_CHARS * 4


def create_source_id(text: str, metadata_str: str,
                     width: SourceIdWidth = SourceIdWidth.FULL) -> str:
    """Reproduces IdGenerator.create_source_id (indexing/id_generator.py)."""
    return f'aws::{get_hash(text)[:width]}:{get_hash(metadata_str)[:METADATA_HASH_CHARS]}'


def create_chunk_id(source_id: str, text: str, metadata_str: str,
                    use_chunk_id_delimiter: bool = False) -> str:
    """
    Reproduces IdGenerator.create_chunk_id (indexing/id_generator.py:90).

    With the delimiter on, a null byte separates text from metadata before
    hashing. It is off by default, as it is in IdGenerator.
    """
    if use_chunk_id_delimiter:
        hash_input = text + CHUNK_ID_DELIMITER + metadata_str
    else:
        hash_input = text + metadata_str
    return f'{source_id}:{get_hash(hash_input)[:8]}'


def _key_of(identifier: str) -> int:
    """
    The hex components of an id, joined and read as one integer.

    Works for a source id and a chunk id alike: both are `aws::` followed by
    colon-separated hex, so everything past the empty second field is the key.
    """
    return int(''.join(identifier.split(':')[2:]), 16)


def _metadata_for(i, with_metadata):
    """Unique per-document metadata, or the empty string IdRewriter defaults to."""
    return f'file_path:doc-{i}.txt' if with_metadata else ''


def _pairs(counts):
    """Unordered pairs within each group of equal keys."""
    return sum(c * (c - 1) // 2 for c in counts)


def discriminating_bits(width: SourceIdWidth = SourceIdWidth.FULL,
                        with_metadata: bool = False) -> int:
    """
    Bits that actually separate two documents.

    Each hex character carries four bits. Without metadata the second component is
    constant, so only the text digest discriminates.
    """
    bits = width * 4
    return bits + METADATA_HASH_BITS if with_metadata else bits


def source_keys(texts, with_metadata, width: SourceIdWidth = SourceIdWidth.FULL):
    """
    with_metadata False reproduces a corpus loaded without metadata, where every
    document gets md5('')[:4] as its second component and only the text digest
    discriminates.

    A key is the whole id, both components, so it is wider than the discriminating
    width. Past 64 bits it no longer fits a uint64 and the keys come back as a list
    for the Counter path in count_collisions.
    """
    keys = (
        _key_of(create_source_id(text, _metadata_for(i, with_metadata), width))
        for i, text in enumerate(texts)
    )
    if width * 4 + METADATA_HASH_BITS > 64:
        return list(keys)

    out = np.empty(len(texts), dtype=np.uint64)
    for i, key in enumerate(keys):
        out[i] = key
    return out


def chunk_keys(texts, with_metadata, chunks_per_doc=3, use_chunk_id_delimiter=False,
               width: SourceIdWidth = SourceIdWidth.FULL):
    """Composite source+chunk keys, to test whether chunk width rescues the prefix."""
    out = []
    for i, text in enumerate(texts):
        metadata_str = _metadata_for(i, with_metadata)
        source_id = create_source_id(text, metadata_str, width)
        for c in range(chunks_per_doc):
            chunk_id = create_chunk_id(source_id, f'{text}::chunk{c}', metadata_str,
                                       use_chunk_id_delimiter)
            out.append(_key_of(chunk_id))
    return out


def synthetic_texts(n):
    """n distinct documents, so every collision found is a hash collision."""
    return [f'document {i} body text' for i in range(n)]


def corpus_texts(path):
    """One document per line, for measuring real duplicate rates."""
    with open(path, encoding='utf-8', errors='replace') as f:
        return [line.rstrip('\n') for line in f if line.strip()]


def count_collisions(keys):
    """
    Source keys fit in uint64 and arrive as a numpy array, which is what makes
    10M documents tractable. Composite source-plus-chunk keys are 80 bits, so
    those arrive as a list and count through a Counter instead.
    """
    if isinstance(keys, np.ndarray):
        _, counts = np.unique(keys, return_counts=True)
        counts = counts.tolist()
    else:
        counts = list(Counter(keys).values())
    return {
        'n': len(keys),
        'distinct': len(counts),
        'colliding_pairs': _pairs(counts),
        'max_group': max(counts) if counts else 0,
    }


def duplicate_pairs(texts):
    """Pairs of documents sharing identical text, which share a key by design."""
    return _pairs(Counter(texts).values())


def expected_pairs(n, bits):
    """Birthday expectation, against which the measured count is a check."""
    return n * (n - 1) / (2 * float(2 ** bits))


def p_any_collision(n, bits):
    """Poisson approximation to the chance of at least one collision."""
    return 1.0 - np.exp(-n * (n - 1) / (2 * float(2 ** bits)))


def _row(label, stats, bits):
    n = stats['n']
    return (f"{label:<34} {n:>12,} {stats['colliding_pairs']:>10,} "
            f"{expected_pairs(n, bits):>14.3f} {stats['max_group']:>6} "
            f"{p_any_collision(n, bits) * 100:>9.2f}%")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scales', default='100000,1000000,10000000')
    parser.add_argument('--widths', default=','.join(w.name for w in SourceIdWidth),
                        help='source id widths to compare, by name (LEGACY, FULL)')
    parser.add_argument('--corpus', help='one document per line, to measure duplicates')
    args = parser.parse_args(argv)

    widths = [SourceIdWidth.parse(w) for w in args.widths.split(',')]

    if args.corpus:
        texts = corpus_texts(args.corpus)
        unique_texts = len(set(texts))
        dup_pairs = duplicate_pairs(texts)
        print(f'corpus: {len(texts):,} documents, {unique_texts:,} distinct texts')
        print(f'  pairs sharing text (deduplication, by design): {dup_pairs:,}')
        for width in widths:
            stats = count_collisions(source_keys(texts, False, width))
            print(f'  {width.name}: {stats["colliding_pairs"]:,} pairs sharing a key, '
                  f'{stats["colliding_pairs"] - dup_pairs:,} hash collisions')
        return 0

    header = (f"{'case':<34} {'documents':>12} {'collided':>10} "
              f"{'expected':>14} {'worst':>6} {'P(any)':>10}")
    print(header)
    print('-' * len(header))
    for n in [int(s) for s in args.scales.split(',')]:
        texts = synthetic_texts(n)
        for width in widths:
            for with_metadata in (False, True):
                bits = discriminating_bits(width, with_metadata)
                label = (f"{width.name}, "
                         f"{'metadata' if with_metadata else 'no metadata'} ({bits} bit)")
                print(_row(label, count_collisions(source_keys(texts, with_metadata, width)), bits))
    return 0


if __name__ == '__main__':
    sys.exit(main())
