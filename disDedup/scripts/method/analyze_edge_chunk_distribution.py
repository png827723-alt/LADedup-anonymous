#!/usr/bin/env python3
"""Analyze edge chunk coverage per file from mean_go_v1 input + Case1 placement output.

Definitions used in this script:
- edge file: a file whose bytes are 100% covered by chunks placed on edge nodes.
- partial edge file: a file not fully on edge (0% < coverage < 100%).
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"json root must be object: {path}")
    return data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze edge chunk distribution and partial coverage files"
    )
    parser.add_argument(
        "--input-json",
        default="disDedup/scripts/data/input/8KB/fileInfo-zipf-s0.7.json",
        help="mean_go_v1 fileInfo json",
    )
    parser.add_argument(
        "--placement-json",
        default="disDedup/scripts/data/output/github_repo_8KB_outputs/case1_cluster_python_8KB_fileInfo-zipf-s0.7.json",
        help="case1 placement output json with hash2edge_node_id",
    )
    parser.add_argument(
        "--report-json",
        default="disDedup/scripts/data/output/github_repo_8KB_outputs/edge_chunk_distribution_fileInfo-zipf-s0.7.json",
        help="output report json path",
    )
    parser.add_argument(
        "--partial-tsv",
        default="disDedup/scripts/data/output/github_repo_8KB_outputs/partial_edge_files_fileInfo-zipf-s0.7.tsv",
        help="output tsv for files with 0 < edge_coverage_ratio < 1",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=20,
        help="print top-k partial files by edge_coverage_ratio then edge_bytes",
    )
    return parser.parse_args()


def safe_int_list(value: Any, name: str) -> list[int]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be list")
    out: list[int] = []
    for i, item in enumerate(value):
        if not isinstance(item, int):
            raise ValueError(f"{name}[{i}] must be int")
        out.append(item)
    return out


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_json)
    placement_path = Path(args.placement_json)

    input_data = load_json(input_path)
    placement_data = load_json(placement_path)

    files = input_data.get("files")
    uni_fingerprint = input_data.get("uni_fingerprint")
    hash2edge_node_id = placement_data.get("hash2edge_node_id")

    if not isinstance(files, list):
        raise ValueError("input json field 'files' must be list")
    if not isinstance(uni_fingerprint, list):
        raise ValueError("input json field 'uni_fingerprint' must be list")
    if not isinstance(hash2edge_node_id, dict):
        raise ValueError("placement json field 'hash2edge_node_id' must be object")

    # Build chunk-id -> edge-node mapping using input uni_fingerprint list (1-based ids).
    chunk_id_to_node: dict[int, int] = {}
    for chunk_id, fp in enumerate(uni_fingerprint, start=1):
        node_id = hash2edge_node_id.get(fp)
        if node_id is None:
            continue
        if not isinstance(node_id, int):
            raise ValueError(f"hash2edge_node_id[{fp!r}] must be int")
        chunk_id_to_node[chunk_id] = node_id

    full_edge_files = []
    partial_edge_files = []
    zero_edge_file_count = 0

    full_node_chunk_counter: Counter[int] = Counter()
    full_node_byte_counter: Counter[int] = Counter()
    partial_node_chunk_counter: Counter[int] = Counter()
    partial_node_byte_counter: Counter[int] = Counter()

    for idx, file_entry in enumerate(files):
        if not isinstance(file_entry, dict):
            raise ValueError(f"files[{idx}] must be object")

        path = file_entry.get("path", f"<unknown-{idx}>")
        if not isinstance(path, str):
            path = str(path)

        chunk_ids = safe_int_list(file_entry.get("chunk_ids"), f"files[{idx}].chunk_ids")
        chunk_sizes = safe_int_list(file_entry.get("chunk_sizes"), f"files[{idx}].chunk_sizes")
        if len(chunk_ids) != len(chunk_sizes):
            raise ValueError(f"files[{idx}] chunk_ids/chunk_sizes length mismatch")

        total_chunks = len(chunk_ids)
        total_bytes = int(sum(chunk_sizes))

        edge_chunk_count = 0
        edge_bytes = 0
        file_node_chunk_counter: Counter[int] = Counter()
        file_node_byte_counter: Counter[int] = Counter()

        for cid, size in zip(chunk_ids, chunk_sizes):
            node = chunk_id_to_node.get(cid)
            if node is None:
                continue
            edge_chunk_count += 1
            edge_bytes += size
            file_node_chunk_counter[node] += 1
            file_node_byte_counter[node] += size

        edge_coverage_ratio = (edge_bytes / total_bytes) if total_bytes > 0 else 0.0
        base_record = {
            "file_index": idx,
            "path": path,
            "total_chunks": total_chunks,
            "edge_chunks": edge_chunk_count,
            "total_bytes": total_bytes,
            "edge_bytes": edge_bytes,
            "edge_coverage_ratio": edge_coverage_ratio,
            "edge_node_chunk_distribution": dict(sorted(file_node_chunk_counter.items())),
            "edge_node_byte_distribution": dict(sorted(file_node_byte_counter.items())),
        }

        if edge_bytes == total_bytes and total_bytes > 0:
            full_edge_files.append(base_record)
            full_node_chunk_counter.update(file_node_chunk_counter)
            full_node_byte_counter.update(file_node_byte_counter)
        elif edge_bytes > 0:
            partial_edge_files.append(base_record)
            partial_node_chunk_counter.update(file_node_chunk_counter)
            partial_node_byte_counter.update(file_node_byte_counter)
        else:
            zero_edge_file_count += 1

    partial_edge_files.sort(
        key=lambda x: (float(x["edge_coverage_ratio"]), int(x["edge_bytes"]), -int(x["file_index"])),
        reverse=True,
    )

    total_file_count = len(files)
    full_count = len(full_edge_files)
    partial_count = len(partial_edge_files)

    full_total_edge_chunks = sum(full_node_chunk_counter.values())
    full_total_edge_bytes = sum(full_node_byte_counter.values())
    partial_total_edge_chunks = sum(partial_node_chunk_counter.values())
    partial_total_edge_bytes = sum(partial_node_byte_counter.values())

    full_distribution = []
    for node_id in sorted(full_node_chunk_counter):
        c = full_node_chunk_counter[node_id]
        b = full_node_byte_counter[node_id]
        full_distribution.append(
            {
                "edge_node_id": node_id,
                "chunks": c,
                "chunk_ratio": (c / full_total_edge_chunks) if full_total_edge_chunks else 0.0,
                "bytes": b,
                "byte_ratio": (b / full_total_edge_bytes) if full_total_edge_bytes else 0.0,
            }
        )

    partial_distribution = []
    for node_id in sorted(partial_node_chunk_counter):
        c = partial_node_chunk_counter[node_id]
        b = partial_node_byte_counter[node_id]
        partial_distribution.append(
            {
                "edge_node_id": node_id,
                "chunks": c,
                "chunk_ratio": (c / partial_total_edge_chunks) if partial_total_edge_chunks else 0.0,
                "bytes": b,
                "byte_ratio": (b / partial_total_edge_bytes) if partial_total_edge_bytes else 0.0,
            }
        )

    report = {
        "input_json": str(input_path),
        "placement_json": str(placement_path),
        "definitions": {
            "edge_file": "total file bytes are fully covered by edge-mapped chunks",
            "partial_edge_file": "file is not fully on edge, but has some bytes on edge",
        },
        "summary": {
            "total_files": total_file_count,
            "edge_files": full_count,
            "partial_edge_files": partial_count,
            "no_edge_chunk_files": zero_edge_file_count,
            "edge_file_ratio": (full_count / total_file_count) if total_file_count else 0.0,
            "partial_file_ratio": (partial_count / total_file_count) if total_file_count else 0.0,
        },
        "edge_files_chunk_distribution_by_node": {
            "total_edge_chunks": full_total_edge_chunks,
            "total_edge_bytes": full_total_edge_bytes,
            "by_node": full_distribution,
        },
        "partial_files_chunk_distribution_by_node": {
            "total_edge_chunks": partial_total_edge_chunks,
            "total_edge_bytes": partial_total_edge_bytes,
            "by_node": partial_distribution,
        },
        "partial_edge_files": partial_edge_files,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

    partial_tsv_path = Path(args.partial_tsv)
    partial_tsv_path.parent.mkdir(parents=True, exist_ok=True)
    with partial_tsv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(
            [
                "file_index",
                "path",
                "total_chunks",
                "edge_chunks",
                "total_bytes",
                "edge_bytes",
                "edge_coverage_ratio",
            ]
        )
        for rec in partial_edge_files:
            writer.writerow(
                [
                    rec["file_index"],
                    rec["path"],
                    rec["total_chunks"],
                    rec["edge_chunks"],
                    rec["total_bytes"],
                    rec["edge_bytes"],
                    f"{float(rec['edge_coverage_ratio']):.8f}",
                ]
            )

    print("=== Summary ===")
    print(f"total_files: {total_file_count}")
    print(f"edge_files (100% covered): {full_count}")
    print(f"partial_edge_files (0<ratio<1): {partial_count}")
    print(f"no_edge_chunk_files (ratio=0): {zero_edge_file_count}")

    print("\n=== Edge File Chunk Distribution By Node ===")
    for row in full_distribution:
        print(
            f"edge{row['edge_node_id']}: "
            f"chunks={row['chunks']} ({row['chunk_ratio']:.4%}), "
            f"bytes={row['bytes']} ({row['byte_ratio']:.4%})"
        )

    print("\n=== Partial Files (Top by edge_coverage_ratio) ===")
    topk = max(0, int(args.topk))
    for rec in partial_edge_files[:topk]:
        print(
            f"idx={rec['file_index']} ratio={float(rec['edge_coverage_ratio']):.4%} "
            f"edge_bytes={rec['edge_bytes']} total_bytes={rec['total_bytes']} path={rec['path']}"
        )

    print(f"\nReport JSON saved: {report_path}")
    print(f"Partial TSV saved: {partial_tsv_path}")


if __name__ == "__main__":
    main()
