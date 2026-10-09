#!/usr/bin/env python3
"""
Run a dedup-only file selection baseline.

This baseline ignores file heat and restore-time modeling. It greedily selects
the candidate that maximizes logical bytes made edge-restorable per newly stored
unique byte:

  score_i = sum_{j completed by adding i} raw_size_j / delta_unique_size_i

The selection is completion-aware, so a file is counted only when all of its
chunks become available at the edge.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import List


def _load_greedy_module() -> ModuleType:
    target = Path(__file__).with_name("Greedy_benefit_size.py")
    spec = importlib.util.spec_from_file_location("dedup_only_greedy_base", target)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load greedy helpers from {target}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


greedy = _load_greedy_module()
base = greedy.base


def main() -> None:
    parser = argparse.ArgumentParser(description="Run dedup-only file selection with balanced chunk placement")
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument("--output-json", default="results/case1_dedup_only.json", help="output summary path")
    parser.add_argument("--env-file", default="../../compose.paths.env", help="accepted for compatibility")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="total storage capacity ratio")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for placement")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="accepted for compatibility")
    parser.add_argument("--workers", type=int, default=1, help="accepted for compatibility; unused")
    parser.add_argument("--k", type=int, default=1000, help="accepted for compatibility; unused")
    parser.add_argument("--alpha", type=int, default=5, help="accepted for compatibility; unused")
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="show progress bars (default: on)",
    )
    args = parser.parse_args()

    input_path = Path(args.input_json)
    if not input_path.exists():
        raise FileNotFoundError(f"input json not found: {input_path}")

    files, uni_size, uni_fingerprint, total_size = base.load_mean_go_v1(input_path, max_files=args.max_files)
    original_n = len(files)
    print(f"Loaded {original_n} files from {input_path}")
    print(
        "Dedup-only model:",
        f"capacity_ratio={float(args.capacity_ratio):.3f}",
        f"server_num={int(args.server_num)}",
        "selection=logical_bytes_per_unique_byte",
        "placement=balanced_new_chunks",
    )

    raw_size_benefits: List[float] = [float(file_info.raw_size) for file_info in files]
    total_capacity = float(total_size) * float(args.capacity_ratio)
    case1 = greedy.run_benefit_size_greedy(
        files=files,
        uni_size=uni_size,
        uni_fingerprint=uni_fingerprint,
        total_capacity=total_capacity,
        server_num=int(args.server_num),
        show_progress=bool(args.progress),
        file_restore_benefits=[1.0 for _ in files],
        file_weighted_benefits=raw_size_benefits,
    )

    completed_raw_bytes = sum(
        float(files[line - 1].raw_size)
        for line in case1["edge_hit_lines"]
        if 1 <= int(line) <= len(files)
    )
    logical_to_unique_ratio = (
        completed_raw_bytes / float(case1["now_size_unique"])
        if float(case1["now_size_unique"]) > 0.0
        else 0.0
    )
    print(
        "Dedup-only done:",
        f"selected_files={case1['selected_file_count']}",
        f"edge_hit_files={case1['edge_hit_file_count']}",
        f"stored_chunks={len(case1['storage_chunks'])}",
        f"now_size={case1['now_size']:.0f}/{case1['total_capacity']:.0f}",
        f"logical_to_unique={logical_to_unique_ratio:.3f}",
        f"mapped_hashes={len(case1['hash2edge_node_id'])}",
    )

    out = {
        "input_json": str(input_path),
        "env_file": str(Path(args.env_file)),
        "format": "mean_py_case1_dedup_only_v1",
        "params": {
            "k": int(args.k),
            "alpha": int(args.alpha),
            "capacity_ratio": float(args.capacity_ratio),
            "max_files": int(args.max_files),
            "server_num": int(args.server_num),
            "workers": max(1, int(args.workers)),
            "progress": bool(args.progress),
            "restore_batch_size": int(args.restore_batch_size),
        },
        "selection": {
            "algorithm": "completion_aware_dedup_only_greedy",
            "score_definition": "score_i = sum_{j completed by adding i} raw_size_j / delta_unique_size_i",
            "benefit_definition": "logical raw bytes made fully edge-restorable",
            "delta_unique_size_definition": "bytes of chunks in file i not already cached at the edge",
            "completion_definition": "a file contributes only when all of its chunks become cached at the edge",
            "uses_heat": False,
            "uses_restore_time": False,
            "recompute_score_each_round": True,
            "placement_definition": "place only new chunks and balance them across edge nodes",
        },
        "summary": {
            "original_file_count": original_n,
            "clustered_file_count": original_n,
            "cluster_rounds_executed": 0,
            "unique_chunk_count": len(uni_size),
            "selected_file_count": case1["selected_file_count"],
            "edge_hit_file_count": case1["edge_hit_file_count"],
            "selected_original_file_count": len(case1["selected_original_files"]),
            "selected_chunk_count": len(case1["storage_chunks"]),
            "total_size": int(total_size),
            "selected_raw_bytes": case1["selected_raw_bytes"],
            "selected_unique_bytes": case1["now_size_unique"],
            "completed_raw_bytes": completed_raw_bytes,
            "logical_to_unique_ratio": logical_to_unique_ratio,
        },
        "hash2edge_node_id": case1["hash2edge_node_id"],
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
