# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

pymupdf = pytest.importorskip("pymupdf")

from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.advanced_pdf_reader_provider import (
    AdvancedPDFReaderProvider,
)
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
    PDFReaderConfig,
)


def _draw_grid(page, rows, x0=60, y0=60, cw=140, ch=30):
    """Draw a ruled grid table that find_tables() detects."""
    n_rows, n_cols = len(rows), len(rows[0])
    for r in range(n_rows + 1):
        y = y0 + r * ch
        page.draw_line((x0, y), (x0 + n_cols * cw, y))
    for c in range(n_cols + 1):
        x = x0 + c * cw
        page.draw_line((x, y0), (x, y0 + n_rows * ch))
    for r in range(n_rows):
        for c in range(n_cols):
            page.insert_text((x0 + c * cw + 5, y0 + r * ch + 20), rows[r][c])


def _make_pdf(path, with_table=True):
    """Write a one-page PDF, optionally with a single ruled grid table."""
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text((60, 40), "Quarterly results")
    if with_table:
        _draw_grid(page, [["Region", "Revenue"], ["East", "100"], ["West", "250"]])
    doc.save(str(path))
    doc.close()
    return str(path)


def _make_two_table_pdf(path):
    """Write a one-page PDF with two separate ruled grid tables."""
    doc = pymupdf.open()
    page = doc.new_page()
    _draw_grid(page, [["Region", "Revenue"], ["East", "100"]], y0=60)
    _draw_grid(page, [["Product", "Units"], ["Widget", "42"]], y0=260)
    doc.save(str(path))
    doc.close()
    return str(path)


def _make_two_page_pdf(path):
    """Write a two-page PDF: page 1 has a table, page 2 has none."""
    doc = pymupdf.open()
    p1 = doc.new_page()
    p1.insert_text((60, 40), "Page one")
    _draw_grid(p1, [["Region", "Revenue"], ["East", "100"]])
    p2 = doc.new_page()
    p2.insert_text((60, 40), "Page two, no table")
    doc.save(str(path))
    doc.close()
    return str(path)


def test_extracts_table_as_markdown_preserving_rows_and_cols(tmp_path):
    pdf = _make_pdf(tmp_path / "table.pdf", with_table=True)
    docs = AdvancedPDFReaderProvider(PDFReaderConfig()).read(pdf)

    assert len(docs) == 1
    text = docs[0].text
    # Row/column relationships survive as a pipe-delimited markdown table.
    assert "|Region|Revenue|" in text
    assert "|East|100|" in text
    assert "|West|250|" in text
    assert docs[0].metadata["table_count"] == 1


def test_table_block_is_marked(tmp_path):
    pdf = _make_pdf(tmp_path / "table.pdf", with_table=True)
    docs = AdvancedPDFReaderProvider(PDFReaderConfig()).read(pdf)
    assert "[TABLE_1_0]" in docs[0].text


def test_extract_tables_false_skips_table_parsing(tmp_path):
    pdf = _make_pdf(tmp_path / "table.pdf", with_table=True)
    docs = AdvancedPDFReaderProvider(PDFReaderConfig(extract_tables=False)).read(pdf)

    text = docs[0].text
    assert "[TABLE_1_0]" not in text
    assert "|Region|Revenue|" not in text
    assert docs[0].metadata["table_count"] == 0
    # Raw page text is still returned.
    assert "Region" in text


def test_page_without_tables_reports_zero(tmp_path):
    pdf = _make_pdf(tmp_path / "plain.pdf", with_table=False)
    docs = AdvancedPDFReaderProvider(PDFReaderConfig()).read(pdf)

    assert len(docs) == 1
    assert docs[0].metadata["table_count"] == 0
    assert docs[0].metadata["source"] == "advanced_pdf"
    assert "Quarterly results" in docs[0].text


def test_multiple_tables_on_a_page(tmp_path):
    pdf = _make_two_table_pdf(tmp_path / "two.pdf")
    docs = AdvancedPDFReaderProvider(PDFReaderConfig()).read(pdf)

    text = docs[0].text
    assert docs[0].metadata["table_count"] == 2
    assert "[TABLE_1_0]" in text
    assert "[TABLE_1_1]" in text
    assert "|Region|Revenue|" in text
    assert "|Product|Units|" in text


def test_page_numbering_and_counts_across_pages(tmp_path):
    """Two-page PDF: page numbers are 1-indexed and per-page table_count is
    independent (page 1 has a table, page 2 has none)."""
    pdf = _make_two_page_pdf(tmp_path / "two_pages.pdf")
    docs = AdvancedPDFReaderProvider(PDFReaderConfig()).read(pdf)

    assert len(docs) == 2

    assert docs[0].metadata["page_number"] == 1
    assert docs[0].metadata["table_count"] == 1
    assert "[TABLE_1_0]" in docs[0].text

    assert docs[1].metadata["page_number"] == 2
    assert docs[1].metadata["table_count"] == 0
    assert "[TABLE_" not in docs[1].text


def test_cell_text_is_not_duplicated(tmp_path):
    """Strip-then-append: each cell value lands once, not twice.
    get_text() includes the table cells inline, so without stripping the
    table region every value would appear both inline and in the markdown.
    """
    pdf = _make_pdf(tmp_path / "table.pdf", with_table=True)
    docs = AdvancedPDFReaderProvider(PDFReaderConfig(extract_tables=True)).read(pdf)

    text = docs[0].text
    assert text.count("Region") == 1
    assert text.count("East") == 1
    assert text.count("250") == 1
    assert "Quarterly results" in text


def test_page_text_excludes_image_blocks(tmp_path):
    """blocks mode returns image blocks too; only text blocks are kept so image
    placeholders never leak into the page text."""
    provider = AdvancedPDFReaderProvider(PDFReaderConfig())

    class _Table:
        bbox = (0, 0, 10, 10)

    class _Page:
        def get_text(self, mode=None):
            return [
                (100, 100, 200, 120, "Outside text\n", 0, 0),
                (300, 300, 400, 400, "<image: DeviceRGB, width 8>", 1, 1),
            ]

    text = provider._page_text(_Page(), [_Table()])
    assert "Outside text" in text
    assert "<image" not in text


def test_find_tables_failure_is_swallowed(tmp_path):
    """A page whose find_tables() raises yields an empty table list."""
    provider = AdvancedPDFReaderProvider(PDFReaderConfig())

    class _BoomPage:
        def find_tables(self):
            raise RuntimeError("boom")

    assert provider._find_tables(_BoomPage(), 0) == []


def test_table_count_excludes_render_failures(tmp_path):
    """A table that fails to_markdown() is skipped and not counted."""
    provider = AdvancedPDFReaderProvider(PDFReaderConfig())

    class _GoodTable:
        def to_markdown(self):
            return "|a|b|\n|---|---|\n|1|2|"

    class _BadTable:
        def to_markdown(self):
            raise ValueError("nope")

    text, count = provider._append_tables(1, [_GoodTable(), _BadTable()], "body")
    assert count == 1
    assert "[TABLE_1_0]" in text
    assert "[TABLE_1_1]" not in text
