# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator

from benchmarks.utils.key_collisions import (
    chunk_keys,
    count_collisions,
    create_chunk_id,
    create_source_id,
    duplicate_pairs,
    expected_pairs,
    source_keys,
    synthetic_texts,
)


class TestIdFidelity(unittest.TestCase):
    """
    These reproduce IdGenerator. If they drift, every number this module
    produces describes a key the toolkit does not write.
    """

    def test_source_id_matches_the_id_generator(self):
        generator = IdGenerator()

        for text, metadata_str in [('hello world', ''), ('', ''), ('doc 1', 'file_path:a.txt')]:
            self.assertEqual(
                create_source_id(text, metadata_str),
                generator.create_source_id(text, metadata_str),
            )

    def test_source_id_shape(self):
        # md5('hello world') starts 5eb63bbb; md5('') starts d41d.
        self.assertEqual(create_source_id('hello world', ''), 'aws::5eb63bbb:d41d')

    def test_chunk_id_matches_the_id_generator(self):
        source_id = create_source_id('hello world', '')

        for use_delimiter in [False, True]:
            generator = IdGenerator(use_chunk_id_delimiter=use_delimiter)
            for text, metadata_str in [('hello world', ''), ('a', 'bc'), ('ab', 'c')]:
                self.assertEqual(
                    create_chunk_id(source_id, text, metadata_str, use_delimiter),
                    generator.create_chunk_id(source_id, text, metadata_str),
                )

    def test_chunk_id_appends_to_the_source_id(self):
        source_id = create_source_id('hello world', '')

        self.assertEqual(
            create_chunk_id(source_id, 'hello world', ''),
            'aws::5eb63bbb:d41d:5eb63bbb',
        )


class TestCountCollisions(unittest.TestCase):

    def test_counts_pairs_not_excess_documents(self):
        # One key three times is three pairs, not two extra documents.
        self.assertEqual(count_collisions([7, 7, 7])['colliding_pairs'], 3)

    def test_reports_the_largest_group(self):
        self.assertEqual(count_collisions([1, 1, 1, 2])['max_group'], 3)

    def test_distinct_keys_do_not_collide(self):
        stats = count_collisions([1, 2, 3])

        self.assertEqual(stats['colliding_pairs'], 0)
        self.assertEqual(stats['distinct'], 3)

    def test_handles_keys_wider_than_uint64(self):
        # Composite source-plus-chunk keys are 80 bits, past the numpy path.
        wide = 2 ** 79

        self.assertEqual(count_collisions([wide, wide])['colliding_pairs'], 1)

    def test_empty(self):
        self.assertEqual(count_collisions([])['colliding_pairs'], 0)


class TestDuplicatePairs(unittest.TestCase):

    def test_identical_texts_pair_up(self):
        self.assertEqual(duplicate_pairs(['a', 'a', 'a', 'b']), 3)

    def test_distinct_texts_have_no_pairs(self):
        self.assertEqual(duplicate_pairs(['a', 'b']), 0)


class TestKeyGeneration(unittest.TestCase):

    def test_absent_metadata_leaves_32_bits_discriminating(self):
        keys = source_keys(synthetic_texts(64), with_metadata=False)
        # The low 16 bits are md5('')[:4] for every document.
        self.assertEqual(len({k & 0xFFFF for k in keys}), 1)

    def test_present_metadata_varies_the_second_component(self):
        keys = source_keys(synthetic_texts(64), with_metadata=True)

        self.assertGreater(len({int(k) & 0xFFFF for k in keys}), 1)

    def test_chunk_keys_are_three_per_document_by_default(self):
        self.assertEqual(len(chunk_keys(synthetic_texts(10), False)), 30)


class TestExpectedPairs(unittest.TestCase):

    def test_matches_the_closed_form_at_1m_on_32_bits(self):
        # n(n-1) / (2 * 2^32), the figure the design doc quotes.
        self.assertAlmostEqual(expected_pairs(1_000_000, 32), 116.4, places=1)

    def test_wider_keys_collide_less(self):
        self.assertLess(expected_pairs(10 ** 6, 48), expected_pairs(10 ** 6, 32))


if __name__ == '__main__':
    unittest.main()
