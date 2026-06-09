#!/usr/bin/env python3
"""Execute hybrid-dev notebooks cell-by-cell with skip logic and per-cell reporting.

Runs inside the Jupyter container. Produces JSON and markdown reports.
"""

import argparse
import json
import os
import sys
from dotenv import load_dotenv
import time

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

ALL_NOTEBOOKS = [
    "00-Setup.ipynb",
    "01-Local-Extract-Batch.ipynb",
    "02-Cloud-Setup.ipynb",
    "03-Cloud-Build.ipynb",
    "04-Cloud-Querying.ipynb",
]

# (notebook_index, cell_index) -> reason
CUDA_SKIPS = {(4, 22): "GPU/CUDA BGEReranker"}
BATCH_SKIPS = {(1, 9): "Requires SOURCE_DIR with PDF files"}


def extract_output(cell):
    parts = []
    for o in cell.get("outputs", []):
        if o.get("output_type") == "stream":
            parts.append(o.get("text", ""))
        elif o.get("output_type") == "execute_result":
            data = o.get("data", {})
            if "text/plain" in data:
                parts.append(data["text/plain"])
        elif o.get("output_type") == "error":
            parts.append("\n".join(o.get("traceback", [])[-3:]))
    text = "".join(parts).strip()
    lines = text.split("\n")[:20]
    return "\n".join(lines) if lines and lines[0] else "(no output)"


def run_notebook(nb_idx, nb_name, work_dir, skip_cells):
    results = []
    nb_path = os.path.join(work_dir, nb_name)
    nb = nbformat.read(nb_path, as_version=4)
    client = NotebookClient(
        nb, timeout=600, kernel_name="python3",
        resources={"metadata": {"path": work_dir}},
    )
    print(f"\n{'=' * 60}\nNOTEBOOK {nb_idx}: {nb_name}\n{'=' * 60}", flush=True)

    with client.setup_kernel():
        for cell_idx, cell in enumerate(nb.cells):
            key = (nb_idx, cell_idx)
            if key in skip_cells:
                reason = skip_cells[key]
                print(f"  Cell {cell_idx} [{cell.cell_type}]: SKIPPED ({reason})", flush=True)
                results.append(dict(
                    notebook=nb_name, cell_index=cell_idx, cell_type=cell.cell_type,
                    status="SKIPPED", output_summary=f"Skipped: {reason}",
                    exec_time_s=0, error=None, source_preview=cell.source[:150],
                ))
                continue

            if cell.cell_type != "code":
                results.append(dict(
                    notebook=nb_name, cell_index=cell_idx, cell_type=cell.cell_type,
                    status="SUCCESS", output_summary="Markdown cell",
                    exec_time_s=0, error=None, source_preview=cell.source[:150],
                ))
                continue

            start = time.time()
            error_detail = None
            try:
                client.execute_cell(cell, cell_idx)
                status = "SUCCESS"
            except CellExecutionError as e:
                status = "FAILED"
                error_detail = str(e)[-800:]
            except Exception as e:
                status = "FAILED"
                error_detail = f"{type(e).__name__}: {str(e)[:500]}"
            elapsed = round(time.time() - start, 2)

            output_summary = extract_output(cell)
            print(f"  Cell {cell_idx} [code]: {status} ({elapsed}s)", flush=True)
            if status == "FAILED":
                print(f"    ERROR: {(error_detail or 'unknown')[:200]}", flush=True)

            results.append(dict(
                notebook=nb_name, cell_index=cell_idx, cell_type="code",
                status=status, output_summary=output_summary,
                exec_time_s=elapsed, error=error_detail,
                source_preview=cell.source[:150],
            ))
    return results


def write_markdown_report(report, path):
    with open(path, "w") as f:
        success = sum(1 for r in report if r["status"] == "SUCCESS")
        failed = sum(1 for r in report if r["status"] == "FAILED")
        skipped = sum(1 for r in report if r["status"] == "SKIPPED")
        f.write("# Notebook Execution Report\n\n")
        f.write(f"| Metric | Count |\n|--------|-------|\n")
        f.write(f"| Total cells | {len(report)} |\n")
        f.write(f"| SUCCESS | {success} |\n| FAILED | {failed} |\n| SKIPPED | {skipped} |\n\n")

        current_nb = None
        for r in report:
            if r["notebook"] != current_nb:
                current_nb = r["notebook"]
                f.write(f"## {current_nb}\n\n")
                f.write("| Cell | Type | Status | Time | Output Summary |\n")
                f.write("|------|------|--------|------|----------------|\n")
            summary = r["output_summary"].replace("\n", " ")[:100]
            f.write(f"| {r['cell_index']} | {r['cell_type']} | {r['status']} | {r['exec_time_s']}s | {summary} |\n")
            if r["error"]:
                f.write(f"\n**Error (Cell {r['cell_index']}):** `{r['error'][:200]}`\n\n")
        f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Run hybrid-dev notebooks")
    parser.add_argument("--work-dir", default="/home/jovyan/notebooks")
    parser.add_argument("--output-dir", default="/home/jovyan/notebooks")
    parser.add_argument("--skip-cuda", default="true", choices=["true", "false"])
    parser.add_argument("--skip-batch", default="true", choices=["true", "false"])
    parser.add_argument("--notebooks", nargs="*", help="Specific notebooks to run")
    args = parser.parse_args()

    load_dotenv(os.path.join(args.work_dir, ".env"))

    notebooks = args.notebooks or ALL_NOTEBOOKS
    skip_cells = {}
    if args.skip_cuda == "true":
        skip_cells.update(CUDA_SKIPS)
    if args.skip_batch == "true":
        skip_cells.update(BATCH_SKIPS)

    report = []
    for nb_idx, nb_name in enumerate(ALL_NOTEBOOKS):
        if nb_name not in notebooks:
            continue
        report.extend(run_notebook(nb_idx, nb_name, args.work_dir, skip_cells))

    # Write reports
    json_path = os.path.join(args.output_dir, "execution_report.json")
    md_path = os.path.join(args.output_dir, "execution_report.md")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    write_markdown_report(report, md_path)

    failed = sum(1 for r in report if r["status"] == "FAILED")
    success = sum(1 for r in report if r["status"] == "SUCCESS")
    skipped = sum(1 for r in report if r["status"] == "SKIPPED")
    print(f"\n\nDone. {len(report)} cells: {success} SUCCESS, {failed} FAILED, {skipped} SKIPPED")
    print(f"Reports: {json_path}, {md_path}")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
