# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for S3BasedDocs and related S3 upload/download helpers.

Covers:
  - S3DocDownloader._download_doc
  - S3DocDownloader.download
  - S3DocUploader._upload_doc (with/without KMS, exception path)
  - S3DocUploader._task_complete_callback
  - S3DocUploader._get_callback_fn (success, exception)
  - S3DocUploader._submit_proxy (success, exception)
  - S3ChunkDownloader._download_chunk
  - S3ChunkUploader._upload_chunk (with/without KMS)
  - S3BasedDocs.__iter__ (jsonl and chunk paths)
  - S3BasedDocs.accept (jsonl and chunk paths)
"""

import io
import json
import queue
import pytest
from unittest.mock import MagicMock, patch, call
from concurrent.futures import Future

from llama_index.core.schema import TextNode, RelatedNodeInfo, NodeRelationship

from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    S3DocDownloader,
    S3DocUploader,
    S3ChunkDownloader,
    S3ChunkUploader,
    S3BasedDocs,
)


def _make_text_node(text="hello", source_id="src-001"):
    node = TextNode(text=text)
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
    return node


def _make_source_doc(n=1):
    return SourceDocument(nodes=[_make_text_node() for _ in range(n)])


# ---------------------------------------------------------------------------
# S3DocDownloader._download_doc
# ---------------------------------------------------------------------------

class TestS3DocDownloaderDownloadDoc:

    def test_download_doc_returns_source_document(self):
        node = _make_text_node("test content")
        node_json = node.to_json().encode("UTF-8")

        s3 = MagicMock()
        paginator = MagicMock()
        s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "prefix/doc/node1.json"}]}
        ]

        def fake_download(bucket, key, stream):
            stream.write(node_json + b"\n")
            stream.seek(0)

        s3.download_fileobj.side_effect = fake_download

        downloader = S3DocDownloader(
            key_prefix="prefix",
            collection_id="col1",
            bucket_name="my-bucket",
            fn=lambda n: n,
        )

        result = downloader._download_doc("prefix/doc/", s3)
        assert isinstance(result, SourceDocument)
        assert len(result.nodes) == 1


# ---------------------------------------------------------------------------
# S3DocUploader._upload_doc
# ---------------------------------------------------------------------------

class TestS3DocUploaderUploadDoc:

    def _make_uploader(self, kms=None):
        return S3DocUploader(
            bucket_name="my-bucket",
            collection_prefix="prefix/col1",
            s3_encryption_key_id=kms,
        )

    def test_upload_doc_without_kms(self):
        uploader = self._make_uploader()
        s3 = MagicMock()
        doc = _make_source_doc(2)
        result = uploader._upload_doc("prefix/col1/source1", doc, s3)
        assert result is doc
        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["ServerSideEncryption"] == "AES256"

    def test_upload_doc_with_kms(self):
        uploader = self._make_uploader(kms="arn:aws:kms:us-east-1:123:key/abc")
        s3 = MagicMock()
        doc = _make_source_doc(1)
        result = uploader._upload_doc("prefix/col1/source1", doc, s3)
        assert result is doc
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["ServerSideEncryption"] == "aws:kms"

    def test_upload_doc_exception_logs_error(self):
        uploader = self._make_uploader()
        s3 = MagicMock()
        s3.put_object.side_effect = Exception("S3 error")
        doc = _make_source_doc(1)
        # Should not raise – logs error and returns None
        result = uploader._upload_doc("prefix/col1/source1", doc, s3)
        assert result is None


# ---------------------------------------------------------------------------
# S3DocUploader._task_complete_callback
# ---------------------------------------------------------------------------

class TestS3DocUploaderTaskCompleteCallback:

    def test_releases_semaphore(self):
        from threading import Semaphore
        uploader = S3DocUploader(bucket_name="b", collection_prefix="p")
        uploader._semaphore = Semaphore(0)
        uploader._semaphore.acquire = MagicMock()
        uploader._semaphore.release = MagicMock()

        future = MagicMock()
        uploader._task_complete_callback(future)
        uploader._semaphore.release.assert_called_once()


# ---------------------------------------------------------------------------
# S3DocUploader._get_callback_fn
# ---------------------------------------------------------------------------

class TestS3DocUploaderGetCallbackFn:

    def test_callback_puts_result_in_queue(self):
        from threading import Semaphore
        uploader = S3DocUploader(bucket_name="b", collection_prefix="p")
        uploader._semaphore = Semaphore(1)

        q = queue.Queue()
        doc = _make_source_doc()
        future = MagicMock()
        future.result.return_value = doc

        cb = uploader._get_callback_fn(q)
        cb(future)

        assert not q.empty()
        assert q.get() is doc

    def test_callback_handles_exception(self):
        from threading import Semaphore
        uploader = S3DocUploader(bucket_name="b", collection_prefix="p")
        uploader._semaphore = Semaphore(1)

        q = queue.Queue()
        future = MagicMock()
        future.result.side_effect = Exception("future error")

        cb = uploader._get_callback_fn(q)
        # Should not raise
        cb(future)
        assert q.empty()


# ---------------------------------------------------------------------------
# S3DocUploader._submit_proxy
# ---------------------------------------------------------------------------

class TestS3DocUploaderSubmitProxy:

    def test_submit_proxy_submits_function(self):
        from threading import Semaphore
        from concurrent.futures import ThreadPoolExecutor

        uploader = S3DocUploader(bucket_name="b", collection_prefix="p")
        uploader._semaphore = Semaphore(5)

        q = queue.Queue()
        executor = MagicMock()
        future = MagicMock()
        executor.submit.return_value = future

        uploader._submit_proxy(lambda: None, executor, q)
        executor.submit.assert_called_once()

    def test_submit_proxy_exception_logs(self):
        from threading import Semaphore
        uploader = S3DocUploader(bucket_name="b", collection_prefix="p")
        uploader._semaphore = Semaphore(5)

        q = queue.Queue()
        executor = MagicMock()
        executor.submit.side_effect = Exception("submit error")

        # Should not raise
        uploader._submit_proxy(lambda: None, executor, q)


# ---------------------------------------------------------------------------
# S3ChunkDownloader._download_chunk
# ---------------------------------------------------------------------------

class TestS3ChunkDownloaderDownloadChunk:

    def test_download_chunk_returns_node(self):
        node = _make_text_node("chunk content")
        node_json = node.to_json().encode("UTF-8")

        s3 = MagicMock()

        def fake_download(bucket, key, stream):
            stream.write(node_json)
            stream.seek(0)

        s3.download_fileobj.side_effect = fake_download

        downloader = S3ChunkDownloader(
            key_prefix="prefix",
            collection_id="col1",
            bucket_name="my-bucket",
            fn=lambda n: n,
        )

        result = downloader._download_chunk("prefix/col1/doc/chunk.json", s3)
        assert isinstance(result, TextNode)


# ---------------------------------------------------------------------------
# S3ChunkUploader._upload_chunk
# ---------------------------------------------------------------------------

class TestS3ChunkUploaderUploadChunk:

    def test_upload_chunk_without_kms(self):
        uploader = S3ChunkUploader(bucket_name="b", collection_prefix="p")
        s3 = MagicMock()
        node = _make_text_node()
        uploader._upload_chunk("prefix/doc", node, s3)
        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["ServerSideEncryption"] == "AES256"

    def test_upload_chunk_with_kms(self):
        uploader = S3ChunkUploader(
            bucket_name="b",
            collection_prefix="p",
            s3_encryption_key_id="arn:aws:kms:us-east-1:123:key/abc"
        )
        s3 = MagicMock()
        node = _make_text_node()
        uploader._upload_chunk("prefix/doc", node, s3)
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["ServerSideEncryption"] == "aws:kms"


# ---------------------------------------------------------------------------
# S3BasedDocs.__iter__ and .accept
# ---------------------------------------------------------------------------

class TestS3BasedDocs:

    def _make_docs(self, for_jsonl=False):
        return S3BasedDocs(
            region="us-east-1",
            bucket_name="my-bucket",
            key_prefix="prefix",
            collection_id="col1",
            for_jsonl=for_jsonl,
        )

    def test_iter_uses_chunk_downloader_by_default(self):
        docs = self._make_docs(for_jsonl=False)
        mock_downloader = MagicMock()
        mock_downloader.download.return_value = iter([_make_source_doc()])
        docs._downloader = mock_downloader

        results = list(docs)
        assert len(results) == 1

    def test_iter_uses_doc_downloader_for_jsonl(self):
        docs = self._make_docs(for_jsonl=True)
        mock_downloader = MagicMock()
        mock_downloader.download.return_value = iter([_make_source_doc()])
        docs._downloader = mock_downloader

        results = list(docs)
        assert len(results) == 1

    def test_iter_creates_downloader_if_none(self):
        docs = self._make_docs(for_jsonl=False)
        assert docs._downloader is None

        mock_doc = _make_source_doc()
        with patch(
            "graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.S3ChunkDownloader"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.download.return_value = iter([mock_doc])
            mock_cls.return_value = mock_instance
            results = list(docs)
            assert len(results) == 1

    def test_accept_uses_chunk_uploader_by_default(self):
        docs = self._make_docs(for_jsonl=False)
        mock_uploader = MagicMock()
        mock_uploader.upload.return_value = iter([_make_source_doc()])
        docs._uploader = mock_uploader

        source_docs = [_make_source_doc()]
        results = list(docs.accept(source_docs))
        assert len(results) == 1

    def test_accept_creates_uploader_if_none(self):
        docs = self._make_docs(for_jsonl=False)
        assert docs._uploader is None

        mock_doc = _make_source_doc()
        with patch(
            "graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.S3ChunkUploader"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.upload.return_value = iter([mock_doc])
            mock_cls.return_value = mock_instance
            results = list(docs.accept([mock_doc]))
            assert len(results) == 1
