#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def normalize_request_path(raw: str, dataset_prefix: str) -> str:
    path = raw.strip()
    prefix = dataset_prefix.rstrip("/")
    if path.startswith(prefix + "/"):
        return path[len(prefix) + 1 :]
    return path.lstrip("/")


def load_request_counts(request_file: Path, dataset_prefix: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for raw in request_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if not parts:
            continue
        first = parts[0].strip()
        input_path = first
        try:
            float(first)
            if len(parts) < 2:
                continue
            input_path = parts[1].strip()
        except ValueError:
            pass
        if not input_path:
            continue
        counts[normalize_request_path(input_path, dataset_prefix)] += 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Build HotDedup input JSON from fileInfo and restore requests.")
    parser.add_argument("--fileinfo-json", required=True, help="path to mean_go_v1 fileInfo json")
    parser.add_argument("--request-file", required=True, help="restore request file used to derive file lambda")
    parser.add_argument("--output-json", required=True, help="output HotDedup input json path")
    parser.add_argument("--edge-count", type=int, default=10, help="number of edge nodes")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="edge capacity ratio in unique-chunk units")
    parser.add_argument("--dataset-prefix", default="/input/github_repo", help="input path prefix to strip from request file paths")
    parser.add_argument("--only-requested", action="store_true", help="include only files that appear in the request file")
    args = parser.parse_args()

    if args.edge_count <= 0:
        raise SystemExit("--edge-count must be > 0")
    if args.capacity_ratio <= 0:
        raise SystemExit("--capacity-ratio must be > 0")

    fileinfo_path = Path(args.fileinfo_json).resolve()
    request_path = Path(args.request_file).resolve()
    output_path = Path(args.output_json).resolve()

    meta = json.loads(fileinfo_path.read_text(encoding="utf-8"))
    if meta.get("format_version") != "mean_go_v1":
        raise SystemExit(f"unsupported fileinfo format: {meta.get('format_version')!r}")

    uni_fp = meta.get("uni_fingerprint") or []
    files = meta.get("files") or []
    counts = load_request_counts(request_path, args.dataset_prefix)
    total_requests = sum(counts.values())
    if total_requests <= 0:
        raise SystemExit("request file contains no valid requests")

    hot_files = []
    missing = []
    for entry in files:
        rel_path = str(entry.get("path", "")).strip().lstrip("/")
        if not rel_path:
            continue
        req_count = counts.get(rel_path, 0)
        if args.only_requested and req_count <= 0:
            continue
        chunk_ids = entry.get("chunk_ids") or []
        chunks = []
        for chunk_id in chunk_ids:
            idx = int(chunk_id) - 1
            if idx < 0 or idx >= len(uni_fp):
                raise SystemExit(f"invalid chunk id {chunk_id} for file {rel_path}")
            chunks.append(uni_fp[idx])
        hot_files.append(
            {
                "name": f"{args.dataset_prefix.rstrip('/')}/{rel_path}",
                "lambda": req_count / total_requests,
                "chunks": chunks,
            }
        )

    requested_set = set(counts)
    fileinfo_set = {str(entry.get("path", "")).strip().lstrip("/") for entry in files}
    for rel in sorted(requested_set - fileinfo_set):
        missing.append(rel)

    capacity_unique_chunks = int(len(uni_fp) * args.capacity_ratio)
    if capacity_unique_chunks <= 0:
        raise SystemExit("derived capacity_unique_chunks is <= 0")

    out = {
        "edge_nodes": [f"edge{i}" for i in range(1, args.edge_count + 1)],
        "capacity_unique_chunks": capacity_unique_chunks,
        "files": hot_files,
        "meta": {
            "fileinfo_json": str(fileinfo_path),
            "request_file": str(request_path),
            "capacity_ratio": args.capacity_ratio,
            "total_unique_chunks": len(uni_fp),
            "requested_file_count": len(counts),
            "included_file_count": len(hot_files),
            "missing_request_files": missing,
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"saved {output_path}")
    print(f"total_unique_chunks={len(uni_fp)}")
    print(f"capacity_unique_chunks={capacity_unique_chunks}")
    print(f"requested_files={len(counts)} included_files={len(hot_files)}")
    print(f"missing_request_files={len(missing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
