#!/usr/bin/env python3
"""
Generate a pure-edge placement JSON from a mean_go_v1 fileInfo input.

All unique chunks are assigned to edge nodes. This ignores capacity-ratio as a
hard limit on purpose so it can serve as a strict edge-only baseline.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType


def _load_base_module() -> ModuleType:
    target = Path(__file__).with_name("run_case1_cluster_theoretical_weighted.py")
    spec = importlib.util.spec_from_file_location("case1_cluster_theoretical_weighted_base", target)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load base algorithm from {target}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


base_mod = _load_base_module()


def build_hash2edge_node_id(uni_fingerprint: list[str], server_num: int) -> dict[str, int]:
    if server_num <= 0:
        raise ValueError("server_num must be positive")
    hash2edge_node_id: dict[str, int] = {}
    for idx, fingerprint in enumerate(uni_fingerprint):
        hash2edge_node_id[str(fingerprint)] = (idx % server_num) + 1
    return hash2edge_node_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a pure-edge placement from mean_go_v1 JSON")
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument(
        "--output-json",
        default="results/case1_cluster_edge_only.json",
        help="output placement json path",
    )
    parser.add_argument("--env-file", default="../../compose.paths.env", help="accepted for compatibility")
    parser.add_argument("--k", type=int, default=1000, help="accepted for compatibility")
    parser.add_argument("--alpha", type=int, default=5, help="accepted for compatibility")
    parser.add_argument("--capacity-ratio", type=float, default=1.0, help="reported for reference only")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for placement")
    parser.add_argument("--workers", type=int, default=1, help="accepted for compatibility")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="accepted for compatibility")
    parser.add_argument("--baseline-edge-ratio", type=float, default=0.0, help="accepted for compatibility")
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="accepted for compatibility",
    )
    args = parser.parse_args()

    input_path = Path(args.input_json)
    if not input_path.exists():
        raise FileNotFoundError(f"input json not found: {input_path}")

    files, uni_size, uni_fingerprint, total_size = base_mod.load_mean_go_v1(input_path, max_files=args.max_files)
    stored_unique_bytes = int(sum(int(v) for v in uni_size))
    hash2edge_node_id = build_hash2edge_node_id(list(uni_fingerprint), int(args.server_num))
    output_path = Path(args.output_json)

    out = {
        "input_json": str(input_path),
        "env_file": str(Path(args.env_file)),
        "format": "mean_py_case1_cluster_edge_only_v1",
        "params": {
            "k": args.k,
            "alpha": args.alpha,
            "capacity_ratio": float(args.capacity_ratio),
            "max_files": args.max_files,
            "server_num": int(args.server_num),
            "workers": max(1, int(args.workers)),
            "progress": bool(args.progress),
            "restore_batch_size": int(args.restore_batch_size),
            "baseline_edge_ratio": float(args.baseline_edge_ratio),
        },
        "placement_mode": "edge_only",
        "summary": {
            "original_file_count": len(files),
            "clustered_file_count": len(files),
            "cluster_rounds_executed": 0,
            "unique_chunk_count": len(uni_size),
            "total_size": int(total_size),
            "selected_row_count": len(files),
            "stored_unique_chunk_count": len(uni_size),
            "stored_unique_bytes": stored_unique_bytes,
            "total_theoretical_gain": 0.0,
        },
        "hash2edge_node_id": hash2edge_node_id,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, ensure_ascii=True, indent=2), encoding="utf-8")
    print(
        "Edge-only placement done:",
        f"files={len(files)}",
        f"unique_chunks={len(uni_size)}",
        f"mapped_hashes={len(hash2edge_node_id)}",
        f"server_num={int(args.server_num)}",
    )
    print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
