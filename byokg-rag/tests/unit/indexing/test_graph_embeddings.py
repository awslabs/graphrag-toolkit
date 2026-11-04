# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import numpy as np
import pytest

from graphrag_toolkit.byokg_rag.indexing import graph_embeddings as ge
from graphrag_toolkit.byokg_rag.indexing.graph_embeddings import (
    EmbeddingTable, GraphEmbeddings, embed_texts, is_opaque_id, load_or_embed, normalize_rows,
)
from tests.unit.hashing_embedding import DIM, HashingEmbedding

TRIPLETS = [
    ('Pat LaFontaine', 'award received (1990)', 'Bill Masterton Memorial Trophy'),
    ('Pat LaFontaine', 'member of sports team (1983 - 1991)', 'New York Islanders'),
    ('m.0abc', 'location.location.contains', 'New York Islanders'),
]


class TestOpaqueIds:
    @pytest.mark.parametrize('text', ['m.0abc', 'g.11b6x_y2', 'm.05n69q3'])
    def test_machine_ids(self, text):
        assert is_opaque_id(text)

    @pytest.mark.parametrize('text', ['Jamaica', 'm.', 'M.0abc', 'm.0ABC', 'mg.0abc', 'xm.0abc'])
    def test_names(self, text):
        assert not is_opaque_id(text)


class TestEmbedTexts:
    def test_rows_are_unit_length_float32(self):
        m = embed_texts(HashingEmbedding(), ['Jamaica', 'Jamaican English'])
        assert m.shape == (2, DIM) and m.dtype == np.float32
        assert np.allclose(np.linalg.norm(m, axis=1), 1.0)

    def test_chunks_large_inputs(self, monkeypatch):
        monkeypatch.setattr(ge, 'EMBED_CHUNK', 2)
        emb = HashingEmbedding()
        assert embed_texts(emb, ['a1', 'b2', 'c3', 'd4', 'e5']).shape == (5, DIM)
        assert emb.batch_calls == 3

    def test_wrong_vector_count_fails_loud(self):
        class Short(HashingEmbedding):
            def batch_embed(self, texts):
                return super().batch_embed(texts)[:-1]
        with pytest.raises(ValueError, match='returned 1 vectors for 2 texts'):
            embed_texts(Short(), ['a1', 'b2'])

    def test_zero_vector_fails_loud(self):
        with pytest.raises(ValueError, match='zero or non-finite'):
            normalize_rows(np.zeros((1, 4), dtype=np.float32))

    def test_nothing_to_embed_fails_loud(self):
        with pytest.raises(ValueError, match='nothing to embed'):
            embed_texts(HashingEmbedding(), [])

    def test_texts_over_the_model_limit_are_truncated(self):
        seen = []

        class Recording(HashingEmbedding):
            def batch_embed(self, texts):
                seen.extend(texts)
                return super().batch_embed(texts)
        long_name = 'x' * (ge.MAX_EMBED_CHARS + 217)  # longest real WebQSP/CWQ node: 2,265 chars
        assert embed_texts(Recording(), ['Jamaica', long_name]).shape == (2, DIM)
        assert [len(t) for t in seen] == [7, ge.MAX_EMBED_CHARS]

    def test_long_text_still_keys_its_row(self):
        long_name = 'y' * 3000
        g = GraphEmbeddings.build([(long_name, 'r', 'Jamaica')], HashingEmbedding())
        assert g.nodes.row(long_name) is not None

    def test_dimension_change_mid_build_fails_loud(self, monkeypatch):
        monkeypatch.setattr(ge, 'EMBED_CHUNK', 1)

        class Shifting(HashingEmbedding):
            def batch_embed(self, texts):
                self.dim = 8 if self.batch_calls else DIM
                return super().batch_embed(texts)
        with pytest.raises(ValueError, match='dimension changed mid-build: 1024 -> 8'):
            embed_texts(Shifting(), ['a1', 'b2'])


class TestCheckpoints:
    def test_failed_build_resumes_from_finished_chunks(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ge, 'EMBED_CHUNK', 2)
        texts = ['a1', 'b2', 'c3', 'd4', 'e5']

        class FailsOnThirdChunk(HashingEmbedding):
            def batch_embed(self, batch):
                if self.batch_calls == 2:
                    raise RuntimeError('ThrottlingException')
                return super().batch_embed(batch)
        with pytest.raises(RuntimeError, match='Throttling'):
            load_or_embed(texts, FailsOnThirdChunk(), str(tmp_path))
        assert len(list((tmp_path / 'partial').iterdir())) == 2, 'finished chunks kept'
        assert not (tmp_path / 'vectors.npy').exists(), 'no complete-looking cache'

        resumed = HashingEmbedding()
        table = load_or_embed(texts, resumed, str(tmp_path))
        assert resumed.batch_calls == 1, 'only the missing chunk is embedded'
        assert not (tmp_path / 'partial').exists()
        fresh = load_or_embed(texts, HashingEmbedding(), None)
        assert table.vectors.tobytes() == fresh.vectors.tobytes()

    def test_checkpoint_for_other_texts_is_not_reused(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ge, 'EMBED_CHUNK', 2)
        embed_texts(HashingEmbedding(), ['a1', 'b2'], checkpoint_dir=str(tmp_path))
        other = HashingEmbedding()
        embed_texts(other, ['c3', 'd4'], checkpoint_dir=str(tmp_path))
        assert other.batch_calls == 1

    def test_checkpoint_from_another_model_is_not_reused(self, tmp_path):
        embed_texts(HashingEmbedding(), ['a1', 'b2'], checkpoint_dir=str(tmp_path), model_id='m1')
        other = HashingEmbedding()
        embed_texts(other, ['a1', 'b2'], checkpoint_dir=str(tmp_path), model_id='m2')
        assert other.batch_calls == 1, 'vectors from one model must not be mixed into another'
        same = HashingEmbedding()
        embed_texts(same, ['a1', 'b2'], checkpoint_dir=str(tmp_path), model_id='m1')
        assert same.batch_calls == 0

    def test_texts_with_newlines_do_not_collide(self, tmp_path):
        embed_texts(HashingEmbedding(), ['a\nb'], checkpoint_dir=str(tmp_path))
        other = HashingEmbedding()
        embed_texts(other, ['a', 'b'], checkpoint_dir=str(tmp_path))
        assert other.batch_calls == 1


class TestCache:
    def test_miss_then_hit_skips_the_model(self, tmp_path):
        emb = HashingEmbedding()
        first = load_or_embed(['Jamaica', 'Haiti'], emb, str(tmp_path))
        assert (tmp_path / 'vectors.npy').exists() and (tmp_path / 'texts.json').exists()
        calls = emb.batch_calls
        second = load_or_embed(['Jamaica', 'Haiti'], emb, str(tmp_path))
        assert emb.batch_calls == calls, 'a cache hit must not call the model'
        assert np.array_equal(first.vectors, second.vectors) and second.texts == ['Jamaica', 'Haiti']

    def test_no_temporary_or_partial_files_left_behind(self, tmp_path):
        load_or_embed(['Jamaica'], HashingEmbedding(), str(tmp_path), model_id='m1')
        assert sorted(p.name for p in tmp_path.iterdir()) == ['meta.json', 'texts.json', 'vectors.npy']
        assert json.loads((tmp_path / 'meta.json').read_text()) == {'model_id': 'm1', 'dim': DIM, 'count': 1}

    def test_hit_returns_exactly_what_the_miss_computed(self, tmp_path):
        miss = load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))
        hit = load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))
        assert miss.vectors.tobytes() == hit.vectors.tobytes(), 'bitwise identical'

    def test_different_model_fails_loud(self, tmp_path):
        load_or_embed(['Jamaica'], HashingEmbedding(), str(tmp_path), model_id='cohere.embed-english-v3')
        with pytest.raises(ValueError, match="model 'cohere.embed-english-v3' vs 'amazon.titan'"):
            load_or_embed(['Jamaica'], HashingEmbedding(), str(tmp_path), model_id='amazon.titan')

    def test_wrong_dimension_fails_loud(self, tmp_path):
        load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))
        np.save(tmp_path / 'vectors.npy', normalize_rows(np.ones((2, 8), dtype=np.float32)))
        with pytest.raises(ValueError, match=r'vectors float32 \(2, 8\) vs float32 \(2, 1024\)'):
            load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))

    def test_non_unit_vectors_fail_loud(self, tmp_path):
        load_or_embed(['Jamaica'], HashingEmbedding(), str(tmp_path))
        np.save(tmp_path / 'vectors.npy', np.full((1, DIM), 3.0, dtype=np.float32))
        with pytest.raises(ValueError, match='not unit length'):
            load_or_embed(['Jamaica'], HashingEmbedding(), str(tmp_path))

    def test_different_texts_fail_loud(self, tmp_path):
        load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))
        with pytest.raises(ValueError, match='does not match'):
            load_or_embed(['Jamaica', 'Cuba'], HashingEmbedding(), str(tmp_path))

    def test_corrupt_row_count_fails_loud(self, tmp_path):
        load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))
        np.save(tmp_path / 'vectors.npy', np.ones((3, DIM), dtype=np.float32))
        with pytest.raises(ValueError, match='does not match'):
            load_or_embed(['Jamaica', 'Haiti'], HashingEmbedding(), str(tmp_path))

    def test_no_cache_dir_writes_nothing(self, tmp_path):
        load_or_embed(['Jamaica'], HashingEmbedding(), None)
        assert list(tmp_path.iterdir()) == []


class TestTables:
    def test_row_lookup(self):
        t = EmbeddingTable(['a', 'b'], np.eye(2, dtype=np.float32))
        assert (t.row('b'), t.row('zzz'), t.dim) == (1, None, 2)

    def test_misaligned_table_fails_loud(self):
        with pytest.raises(ValueError, match='2 texts but 3 vectors'):
            EmbeddingTable(['a', 'b'], np.eye(3, dtype=np.float32))

    def test_dim_mismatch_fails_loud(self):
        with pytest.raises(ValueError, match='node dim 2 != relation dim 3'):
            GraphEmbeddings(EmbeddingTable(['a', 'b'], np.eye(2, dtype=np.float32)),
                            EmbeddingTable(['r', 's', 't'], np.eye(3, dtype=np.float32)))


class TestBuild:
    def test_named_nodes_and_relations_sorted_opaque_skipped(self):
        g = GraphEmbeddings.build(TRIPLETS, HashingEmbedding(), extra_nodes=['Jamaica'])
        assert g.nodes.texts == ['Bill Masterton Memorial Trophy', 'Jamaica', 'New York Islanders',
                                 'Pat LaFontaine']
        assert g.relations.texts == sorted({r for _, r, _ in TRIPLETS})
        assert g.nodes.row('m.0abc') is None

    def test_cache_layout(self, tmp_path):
        GraphEmbeddings.build(TRIPLETS, HashingEmbedding(), cache_dir=str(tmp_path))
        assert json.loads((tmp_path / 'relations' / 'texts.json').read_text())[0] == 'award received (1990)'
        assert (tmp_path / 'nodes' / 'vectors.npy').exists()

    def test_malformed_triplet_fails_loud(self):
        with pytest.raises(ValueError, match=r'\(head, relation, tail\)'):
            GraphEmbeddings.build([('a', 'r')], HashingEmbedding())


class TestEmptyTables:
    OPAQUE = [('m.0a', 'people.person.place_of_birth', 'm.0b'), ('m.0c', 'film.film.directed_by', 'g.11x')]

    def test_all_opaque_graph_gets_an_empty_node_table(self):
        emb = HashingEmbedding()
        g = GraphEmbeddings.build(self.OPAQUE, emb)
        assert g.nodes.texts == [] and g.nodes.vectors.shape == (0, DIM)
        assert g.relations.vectors.shape == (2, DIM) and g.dim == DIM
        assert emb.batch_calls == 1, 'only the relation labels are embedded'

    def test_isolated_nodes_without_triplets_get_an_empty_relation_table(self):
        g = GraphEmbeddings.build([], HashingEmbedding(), extra_nodes=['Jamaica'])
        assert g.nodes.texts == ['Jamaica'] and g.relations.vectors.shape == (0, DIM)

    def test_empty_graph_embeds_nothing(self):
        emb = HashingEmbedding()
        g = GraphEmbeddings.build([], emb)
        assert (g.nodes.vectors.shape, g.relations.vectors.shape, g.dim) == ((0, 0), (0, 0), 0)
        assert emb.batch_calls == 0

    def test_empty_table_round_trips_through_the_cache(self, tmp_path, monkeypatch):
        GraphEmbeddings.build(self.OPAQUE, HashingEmbedding(), cache_dir=str(tmp_path), model_id='m1')
        assert json.loads((tmp_path / 'nodes' / 'texts.json').read_text()) == []
        assert json.loads((tmp_path / 'nodes' / 'meta.json').read_text()) == {'model_id': 'm1', 'dim': DIM, 'count': 0}
        assert np.load(tmp_path / 'nodes' / 'vectors.npy').shape == (0, DIM), 'the completion marker exists'
        # An empty table makes no model calls either way, so a hit shows as no writes.
        writes = []
        monkeypatch.setattr(ge, '_save_atomic', lambda path, array: writes.append(path))
        monkeypatch.setattr(ge, '_write_json_atomic', lambda path, value: writes.append(path))
        g = GraphEmbeddings.build(self.OPAQUE, HashingEmbedding(), cache_dir=str(tmp_path), model_id='m1')
        assert writes == [], 'both tables, the empty one included, are cache hits'
        assert g.nodes.vectors.shape == (0, DIM)

    def test_empty_texts_without_a_dim_fail_loud(self):
        with pytest.raises(ValueError, match='an empty table needs dim'):
            load_or_embed([], HashingEmbedding())
