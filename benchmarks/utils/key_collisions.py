# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Measure storage-key collisions for the ids `IdGenerator` produces.

`create_source_id` returns `aws::{md5(text)[:8]}:{md5(metadata_str)[:4]}`, so a
source id discriminates on 48 bits. `IdRewriter` passes `''` when a node carries
no metadata, which makes the second component constant and leaves 32 bits. Both
S3 storage prefixes are built from the bare source id, so two documents sharing
one share a prefix.

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


def get_hash(s: str) -> str:
    """Reproduces indexing/utils/hash_utils.py::get_hash."""
    return hashlib.md5(s.encode('utf-8'), usedforsecurity=False).digest().hex()


def create_source_id(text: str, metadata_str: str) -> str:
    """Reproduces IdGenerator.create_source_id (indexing/id_generator.py:84)."""
    return f'aws::{get_hash(text)[:8]}:{get_hash(metadata_str)[:4]}'


def create_chunk_id(source_id: str, text: str, metadata_str: str) -> str:
    """Reproduces IdGenerator.create_chunk_id with the delimiter off (the default)."""
    return f'{source_id}:{get_hash(text + metadata_str)[:8]}'


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


def source_keys(texts, with_metadata):
    """
    with_metadata False reproduces a corpus loaded without metadata, where every
    document gets md5('')[:4] as its second component and 32 bits discriminate.
    """
    out = np.empty(len(texts), dtype=np.uint64)
    for i, text in enumerate(texts):
        out[i] = _key_of(create_source_id(text, _metadata_for(i, with_metadata)))
    return out


def chunk_keys(texts, with_metadata, chunks_per_doc=3):
    """Composite source+chunk keys, to test whether chunk width rescues the prefix."""
    out = []
    for i, text in enumerate(texts):
        metadata_str = _metadata_for(i, with_metadata)
        source_id = create_source_id(text, metadata_str)
        for c in range(chunks_per_doc):
            chunk_id = create_chunk_id(source_id, f'{text}::chunk{c}', metadata_str)
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
    Source keys fit in uint64 and go through numpy, which is what makes 10M
    documents tractable. Composite source-plus-chunk keys are 80 bits and do
    not, so those count through a Counter instead.
    """
    keys = list(keys)
    if keys and max(keys) < 2 ** 64:
        _, counts = np.unique(np.asarray(keys, dtype=np.uint64), return_counts=True)
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
    return (f"{label:<26} {n:>12,} {stats['colliding_pairs']:>10,} "
            f"{expected_pairs(n, bits):>14.3f} {stats['max_group']:>6} "
            f"{p_any_collision(n, bits) * 100:>9.2f}%")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scales', default='100000,1000000,10000000')
    parser.add_argument('--corpus', help='one document per line, to measure duplicates')
    args = parser.parse_args(argv)

    if args.corpus:
        texts = corpus_texts(args.corpus)
        unique_texts = len(set(texts))
        keys = source_keys(texts, with_metadata=False)
        stats = count_collisions(keys)
        dup_pairs = duplicate_pairs(texts)
        print(f'corpus: {len(texts):,} documents, {unique_texts:,} distinct texts')
        print(f'  pairs sharing text (deduplication, by design): {dup_pairs:,}')
        print(f'  pairs sharing a key: {stats["colliding_pairs"]:,}')
        print(f'  hash collisions: {stats["colliding_pairs"] - dup_pairs:,}')
        return 0

    header = (f"{'case':<26} {'documents':>12} {'collided':>10} "
              f"{'expected':>14} {'worst':>6} {'P(any)':>10}")
    print(header)
    print('-' * len(header))
    for n in [int(s) for s in args.scales.split(',')]:
        texts = synthetic_texts(n)
        print(_row('metadata absent (32 bit)', count_collisions(source_keys(texts, False)), 32))
        print(_row('metadata present (48 bit)', count_collisions(source_keys(texts, True)), 48))
    return 0


if __name__ == '__main__':
    sys.exit(main())
