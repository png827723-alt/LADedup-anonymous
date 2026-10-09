#!/usr/bin/env python3
"""
Run a heat-only file selection baseline with spread-by-file chunk placement.

Selection model:
  - rank original files by heat descending
  - deduplicate against already selected chunk hashes during selection
  - charge capacity only by newly introduced unique chunk bytes
  - greedily admit files while incremental unique bytes can still be placed

Placement model:
  - when a file is admitted, place only its cache-miss chunks
  - balance new chunks across edge nodes as evenly as possible
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

ChunkRow = Tuple[int, int]


class _EmbeddedBase:
    pass


base = _EmbeddedBase()


class ProgressPrinter:
    def __init__(self, total: int, label: str, enabled: bool, width: int = 28):
        self.total = max(1, int(total))
        self.label = label
        self.enabled = enabled
        self.width = width
        self.done = 0
        self.last_render = 0.0
        if self.enabled:
            self._render(force=True)

    def update(self, delta: int = 1) -> None:
        if not self.enabled:
            return
        self.done += int(delta)
        now = time.time()
        if now - self.last_render >= 0.08 or self.done >= self.total:
            self._render(force=False)

    def close(self) -> None:
        if self.enabled:
            if self.done < self.total:
                self.done = self.total
                self._render(force=True)
            sys.stdout.write("\n")
            sys.stdout.flush()

    def _render(self, force: bool) -> None:
        ratio = min(1.0, self.done / self.total)
        fill = int(self.width * ratio)
        bar = "#" * fill + "-" * (self.width - fill)
        pct = int(ratio * 100)
        line = f"\r{self.label}: [{bar}] {pct:3d}% ({self.done}/{self.total})"
        if force or line:
            sys.stdout.write(line)
            sys.stdout.flush()
            self.last_render = time.time()


@dataclass
class FileInfo:
    chunks: List[ChunkRow]
    heat: float
    heat_dsize: float
    raw_size: int
    file_ids: List[int]
    select: bool = False


def unique_rows_stable(rows: Iterable[ChunkRow]) -> List[ChunkRow]:
    seen = set()
    out: List[ChunkRow] = []
    for row in rows:
        if row not in seen:
            seen.add(row)
            out.append(row)
    return out


def union_ints_stable(a: Sequence[int], b: Sequence[int]) -> List[int]:
    seen = set()
    out: List[int] = []
    for v in list(a) + list(b):
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def load_mean_go_v1(path: Path, max_files: int) -> Tuple[List[FileInfo], List[int], List[str], int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "files" not in data or "uni_size" not in data or "uni_fingerprint" not in data or "total_size" not in data:
        raise ValueError("input json missing required fields: files, uni_size, uni_fingerprint, total_size")

    files_raw = data["files"]
    if max_files > 0:
        files_raw = files_raw[:max_files]

    files: List[FileInfo] = []
    for idx, item in enumerate(files_raw, start=1):
        chunk_ids = item["chunk_ids"]
        chunk_sizes = item["chunk_sizes"]
        if len(chunk_ids) != len(chunk_sizes):
            raise ValueError(f"file index {idx} has mismatched chunk_ids/chunk_sizes length")

        chunks = [(int(cid), int(sz)) for cid, sz in zip(chunk_ids, chunk_sizes)]
        file_bytes = sum(sz for _, sz in chunks)
        heat = float(item["heat"])
        if "heat_dsize" in item:
            hd = float(item["heat_dsize"])
        else:
            hd = heat / file_bytes if file_bytes > 0 else 0.0

        files.append(FileInfo(chunks=chunks, heat=heat, heat_dsize=hd, raw_size=file_bytes, file_ids=[idx]))

    uni_size = [int(v) for v in data["uni_size"]]
    uni_fingerprint = [str(v) for v in data["uni_fingerprint"]]
    if len(uni_fingerprint) != len(uni_size):
        raise ValueError("uni_fingerprint and uni_size length mismatch")
    total_size = int(data["total_size"])
    return files, uni_size, uni_fingerprint, total_size


base.FileInfo = FileInfo
base.ProgressPrinter = ProgressPrinter
base.union_ints_stable = union_ints_stable
base.load_mean_go_v1 = load_mean_go_v1
base.os = os


def _ordered_new_chunk_ids(
    file_info: base.FileInfo,
    known_chunk_ids: set[int],
    uni_size: Sequence[int],
) -> List[int]:
    seen_in_file: set[int] = set()
    out: List[int] = []
    for cid, _ in file_info.chunks:
        cc = int(cid)
        if cc in seen_in_file or cc in known_chunk_ids:
            continue
        if not (1 <= cc <= len(uni_size)):
            continue
        seen_in_file.add(cc)
        out.append(cc)
    return out


def _plan_balanced_chunk_placement(
    chunk_ids: Sequence[int],
    uni_size: Sequence[int],
    per_server_limit: Sequence[float],
    server_used: Sequence[float],
    server_chunk_counts: Sequence[int],
) -> Tuple[List[Tuple[int, int]], List[float], List[int]] | None:
    planned: List[Tuple[int, int]] = []
    next_used = [float(v) for v in server_used]
    next_counts = [int(v) for v in server_chunk_counts]

    for chunk_id in chunk_ids:
        sz = float(uni_size[chunk_id - 1])
        candidates = [
            sid
            for sid in range(len(per_server_limit))
            if next_used[sid] + sz <= float(per_server_limit[sid])
        ]
        if not candidates:
            return None

        sid = min(candidates, key=lambda item: (next_used[item], next_counts[item], item))
        planned.append((int(chunk_id), sid + 1))
        next_used[sid] += sz
        next_counts[sid] += 1

    return planned, next_used, next_counts


def build_hash2edge_node_id(
    chunk2server_by_chunk_id: Sequence[int],
    uni_fingerprint: Sequence[str],
) -> dict:
    hash2edge_node_id = {}
    for chunk_id, node_id in enumerate(chunk2server_by_chunk_id, start=1):
        if node_id > 0 and chunk_id <= len(uni_fingerprint):
            hash2edge_node_id[uni_fingerprint[chunk_id - 1]] = node_id
    return {"hash2edge_node_id": hash2edge_node_id}


def run_heat_only_selection(
    files: List[base.FileInfo],
    uni_size: Sequence[int],
    uni_fingerprint: Sequence[str],
    total_capacity: float,
    server_num: int,
    show_progress: bool,
) -> dict:
    if server_num <= 0:
        raise ValueError("server_num must be positive")

    indexed = list(enumerate(files))
    indexed.sort(key=lambda item: (-float(item[1].heat), item[0]))

    per_server_limit = [float(total_capacity) / float(server_num)] * server_num
    server_used = [0.0] * server_num
    server_chunk_counts = [0] * server_num
    chunk2server_by_chunk_id: List[int] = [0 for _ in range(len(uni_size))]
    storage_lines: List[int] = []
    storage_chunks: set[int] = set()
    selected_original_files: List[int] = []
    now_size = 0.0
    total_heat = 0.0
    selected_raw_bytes = 0.0
    pb = base.ProgressPrinter(total=len(indexed), label="case1 select", enabled=show_progress)

    for idx, file_info in indexed:
        new_chunk_ids = _ordered_new_chunk_ids(file_info, storage_chunks, uni_size)
        plan = _plan_balanced_chunk_placement(
            chunk_ids=new_chunk_ids,
            uni_size=uni_size,
            per_server_limit=per_server_limit,
            server_used=server_used,
            server_chunk_counts=server_chunk_counts,
        )
        if plan is None:
            pb.update(1)
            continue

        planned_chunks, next_server_used, next_server_chunk_counts = plan
        delta_size = sum(float(uni_size[cid - 1]) for cid, _ in planned_chunks)
        if now_size + delta_size > total_capacity:
            pb.update(1)
            continue

        line = idx + 1
        storage_lines.append(line)
        now_size += delta_size
        total_heat += float(file_info.heat)
        selected_raw_bytes += float(file_info.raw_size)
        for cid, sid in planned_chunks:
            storage_chunks.add(int(cid))
            chunk2server_by_chunk_id[cid - 1] = int(sid)
        server_used = next_server_used
        server_chunk_counts = next_server_chunk_counts
        selected_original_files = base.union_ints_stable(selected_original_files, file_info.file_ids)
        pb.update(1)

    pb.close()
    storage_lines.sort()
    return {
        "storage_lines": storage_lines,
        "selected_original_files": selected_original_files,
        "storage_chunks": sorted(storage_chunks),
        "now_size": now_size,
        "selected_raw_bytes": selected_raw_bytes,
        "total_capacity": total_capacity,
        "capacity_left": total_capacity - now_size,
        "total_heat": total_heat,
        "raw_selected_file_count": len(storage_lines),
        "now_size_unique": now_size,
        "chunk2server_by_chunk_id": chunk2server_by_chunk_id,
        "server_used": server_used,
        "server_chunk_counts": server_chunk_counts,
        "hash2edge_node_id": build_hash2edge_node_id(chunk2server_by_chunk_id, uni_fingerprint)["hash2edge_node_id"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run heat-only file selection with spread-by-file placement")
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument("--output-json", default="results/case1_cluster_heat_only_spread.json", help="output summary path")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="total storage capacity ratio")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for placement")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (base.os.cpu_count() or 1) - 1),
        help="compatibility flag; unused in this baseline",
    )
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
        "Heat-only model:",
        f"capacity_ratio={float(args.capacity_ratio):.3f}",
        f"server_num={int(args.server_num)}",
        "selection=heat_only_dedup_aware",
        "placement=balanced_new_chunks",
    )

    total_capacity = float(total_size) * float(args.capacity_ratio)
    case1 = run_heat_only_selection(
        files=files,
        uni_size=uni_size,
        uni_fingerprint=uni_fingerprint,
        total_capacity=total_capacity,
        server_num=int(args.server_num),
        show_progress=bool(args.progress),
    )
    print(
        "Case1 done:",
        f"selected_files={case1['raw_selected_file_count']}",
        f"selected_original_files={len(case1['selected_original_files'])}",
        f"edge_now_size={case1['now_size']:.0f}/{case1['total_capacity']:.0f}",
        f"selected_raw_bytes={case1['selected_raw_bytes']:.0f}",
        f"unique_now_size={case1['now_size_unique']:.0f}",
        f"mapped_hashes={len(case1['hash2edge_node_id'])}",
    )

    out = {
        "input_json": str(input_path),
        "format": "mean_py_case1_cluster_heat_only_spread_v1",
        "params": {
            "capacity_ratio": args.capacity_ratio,
            "max_files": args.max_files,
            "server_num": int(args.server_num),
            "workers": max(1, args.workers),
            "progress": bool(args.progress),
        },
        "selection": {
            "priority_definition": "priority(file) = heat(file)",
            "capacity_charge_definition": "incremental_unique_chunk_bytes",
            "dedup_aware_selection": True,
            "placement_definition": "place only new chunks and balance them across edges",
        },
        "summary": {
            "original_file_count": original_n,
            "selected_file_count": case1["raw_selected_file_count"],
            "selected_original_file_count": len(case1["selected_original_files"]),
            "unique_chunk_count": len(uni_size),
            "selected_chunk_count": len(case1["storage_chunks"]),
            "total_size": total_size,
            "total_heat": case1["total_heat"],
            "selected_raw_bytes": case1["selected_raw_bytes"],
            "selected_unique_bytes": case1["now_size_unique"],
        },
        "hash2edge_node_id": case1["hash2edge_node_id"],
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
