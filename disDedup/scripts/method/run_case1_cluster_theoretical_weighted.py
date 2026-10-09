#!/usr/bin/env python3
"""
Run MEAN clustering + Case1 selection with a theoretical restore-gain score.

For each original file i, define:

  benefit_i = T_baseline(i) - T_full_edge(i)
  score_i   = heat_i * benefit_i

Case1 selection then uses:

  priority(cluster) = sum(score_i for i in cluster) / delta_unique_bytes(cluster)

The benefit model is derived from configured network parameters instead of
post-hoc measurements.
"""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np

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


def union_rows_stable(a: Sequence[ChunkRow], b: Sequence[ChunkRow]) -> List[ChunkRow]:
    return unique_rows_stable(list(a) + list(b))


def intersect_rows_stable(a: Sequence[ChunkRow], b: Sequence[ChunkRow]) -> List[ChunkRow]:
    bset = set(b)
    seen = set()
    out: List[ChunkRow] = []
    for row in a:
        if row in bset and row not in seen:
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


def _default_pair_dist(file_a: FileInfo, file_b: FileInfo) -> float:
    inter = intersect_rows_stable(file_a.chunks, file_b.chunks)
    inter_size = sum(sz for _, sz in inter)
    a_size = sum(sz for _, sz in file_a.chunks)
    b_size = sum(sz for _, sz in file_b.chunks)
    union_size = a_size + b_size - inter_size
    if union_size <= 0:
        return 1.0

    merged_hd = (file_a.heat + file_b.heat) / union_size
    if merged_hd < max(file_a.heat_dsize, file_b.heat_dsize):
        return 1.0
    return 1.0 - (inter_size / union_size)


_WORKER_FILES: Sequence[FileInfo] | None = None


def _pool_init(files: Sequence[FileInfo]) -> None:
    global _WORKER_FILES
    _WORKER_FILES = files


def _split_ranges(total: int, parts: int) -> List[Tuple[int, int]]:
    if total <= 0:
        return []
    parts = max(1, min(parts, total))
    base_step = total // parts
    rem = total % parts
    out = []
    cur = 0
    for i in range(parts):
        step = base_step + (1 if i < rem else 0)
        out.append((cur, cur + step))
        cur += step
    return out


def _row_blocks(total: int, block_size: int) -> List[Tuple[int, int]]:
    if total <= 0:
        return []
    block_size = max(1, block_size)
    out = []
    start = 0
    while start < total:
        end = min(total, start + block_size)
        out.append((start, end))
        start = end
    return out


def _dist_rows_task(row_range: Tuple[int, int]) -> List[Tuple[int, List[float]]]:
    files = _WORKER_FILES
    if files is None:
        raise RuntimeError("worker files not initialized")

    n = len(files)
    start, end = row_range
    out: List[Tuple[int, List[float]]] = []
    for i in range(start, end):
        row = [1.0] * n
        fi = files[i]
        for j in range(i + 1, n):
            row[j] = base.pair_dist(fi, files[j])
        out.append((i, row))
    return out


def _dist_new_cols_task(cols: List[int]) -> List[Tuple[int, List[float]]]:
    files = _WORKER_FILES
    if files is None:
        raise RuntimeError("worker files not initialized")

    n = len(files)
    out: List[Tuple[int, List[float]]] = []
    for j in cols:
        col = [1.0] * n
        fj = files[j]
        for i in range(j):
            col[i] = base.pair_dist(files[i], fj)
        out.append((j, col))
    return out


def _run_pool_map(
    task_fn,
    task_inputs,
    files: Sequence[FileInfo],
    workers: int,
    show_progress: bool = False,
    progress_label: str = "progress",
):
    pb = ProgressPrinter(total=len(task_inputs), label=progress_label, enabled=show_progress)
    if workers <= 1:
        _pool_init(files)
        out = []
        for x in task_inputs:
            out.append(task_fn(x))
            pb.update(1)
        pb.close()
        return out
    try:
        with mp.Pool(processes=workers, initializer=_pool_init, initargs=(files,)) as pool:
            out = []
            for item in pool.imap_unordered(task_fn, task_inputs):
                out.append(item)
                pb.update(1)
            pb.close()
            return out
    except (PermissionError, OSError) as exc:
        print(f"[warn] multiprocessing unavailable ({exc}); fallback to single process.")
        _pool_init(files)
        out = []
        for x in task_inputs:
            out.append(task_fn(x))
            pb.update(1)
        pb.close()
        return out


def build_dist_table(files: Sequence[FileInfo], workers: int, show_progress: bool) -> np.ndarray:
    n = len(files)
    table = np.ones((n, n), dtype=np.float64)
    row_block = max(1, n // max(1, workers * 16))
    ranges = _row_blocks(n, row_block)
    results = _run_pool_map(
        _dist_rows_task,
        ranges,
        files,
        workers,
        show_progress=show_progress,
        progress_label="dist init",
    )
    for part in results:
        for i, row in part:
            table[i, :] = row
    return table


def mink_file_pair_func_style(dist_table: np.ndarray, k: int) -> List[Tuple[int, int, float]]:
    n = dist_table.shape[0]
    if k <= 0:
        return []

    cluster_rows: List[Tuple[int, int, float]] = [(i, -1, float("inf")) for i in range(k)]
    max_dist = cluster_rows[0][2]
    max_idx = 0
    for i in range(n):
        for j in range(i + 1, n):
            d = float(dist_table[i, j])
            if d < max_dist:
                del cluster_rows[max_idx]
                new_row = (i, j, d)
                if new_row not in cluster_rows:
                    cluster_rows.append(new_row)

                max_idx = 0
                max_dist = cluster_rows[0][2]
                for idx in range(1, len(cluster_rows)):
                    if cluster_rows[idx][2] > max_dist:
                        max_dist = cluster_rows[idx][2]
                        max_idx = idx
    return cluster_rows


def build_merge_clusters(pairs: Sequence[Tuple[int, int, float]]) -> List[List[int]]:
    merge_pair: List[List[int]] = []
    for i, j, _ in pairs:
        merge_pair.append(union_ints_stable([i], [j]))

    k = len(merge_pair)
    if k > 1:
        for i in range(k):
            if not merge_pair[i]:
                continue
            set_i = set(merge_pair[i])
            for j in range(i + 1, k):
                if not merge_pair[j]:
                    continue
                if set_i.intersection(merge_pair[j]):
                    merge_pair[i] = union_ints_stable(merge_pair[i], merge_pair[j])
                    set_i = set(merge_pair[i])
                    merge_pair[j] = []
    return [x for x in merge_pair if x]


def _compute_new_columns(
    files: Sequence[FileInfo], new_col_indexes: List[int], workers: int, show_progress: bool
) -> List[Tuple[int, List[float]]]:
    if not new_col_indexes:
        return []
    col_groups = _split_ranges(len(new_col_indexes), workers)
    task_inputs: List[List[int]] = []
    for a, b in col_groups:
        task_inputs.append(new_col_indexes[a:b])
    results = _run_pool_map(
        _dist_new_cols_task,
        task_inputs,
        files,
        workers,
        show_progress=show_progress,
        progress_label="dist update",
    )
    flat: List[Tuple[int, List[float]]] = []
    for part in results:
        flat.extend(part)
    flat.sort(key=lambda x: x[0])
    return flat


def merge_once(
    files: Sequence[FileInfo], dist_table: np.ndarray, k: int, workers: int, show_progress: bool
) -> Tuple[List[FileInfo], np.ndarray, bool]:
    pairs = mink_file_pair_func_style(dist_table, k)
    if len(pairs) < k:
        return list(files), dist_table, False
    if any(d >= 1.0 for _, _, d in pairs):
        return list(files), dist_table, False

    clusters = build_merge_clusters(pairs)
    if not clusters:
        return list(files), dist_table, False

    merged_indexes = set()
    new_files: List[FileInfo] = []
    for cluster in clusters:
        merged_indexes.update(cluster)
        first = cluster[0]
        new_chunks = list(files[first].chunks)
        new_heat = files[first].heat
        new_file_ids = list(files[first].file_ids)

        for idx in cluster[1:]:
            new_chunks = union_rows_stable(new_chunks, files[idx].chunks)
            new_heat += files[idx].heat
            new_file_ids = union_ints_stable(new_file_ids, files[idx].file_ids)

        denom = sum(sz for _, sz in new_chunks)
        new_hd = new_heat / denom if denom > 0 else 0.0
        new_files.append(
            FileInfo(
                chunks=new_chunks,
                heat=new_heat,
                heat_dsize=new_hd,
                raw_size=sum(files[idx].raw_size for idx in cluster),
                file_ids=new_file_ids,
                select=False,
            )
        )

    keep_idx = [i for i in range(len(files)) if i not in merged_indexes]
    remain = [files[i] for i in keep_idx]
    reduced = dist_table[np.ix_(keep_idx, keep_idx)].copy()

    updated_files = remain + new_files
    old_n = len(remain)
    new_total_n = len(updated_files)
    new_m = len(new_files)

    updated_dist = np.ones((new_total_n, new_total_n), dtype=np.float64)
    if old_n > 0:
        updated_dist[:old_n, :old_n] = reduced

    if new_m > 0:
        new_cols = list(range(old_n, new_total_n))
        col_values = _compute_new_columns(
            updated_files,
            new_cols,
            workers=workers,
            show_progress=show_progress,
        )
        for j, col in col_values:
            updated_dist[:, j] = np.asarray(col, dtype=np.float64)

    return updated_files, updated_dist, True


def cluster_files(
    files: List[FileInfo], k: int, alpha: int, workers: int, show_progress: bool
) -> Tuple[List[FileInfo], int]:
    current = list(files)
    rounds = 0
    if alpha <= 0 or len(current) < 2:
        return current, rounds

    dist_table = build_dist_table(current, workers=workers, show_progress=show_progress)
    for round_idx in range(alpha):
        if show_progress:
            print(f"cluster round {round_idx + 1}/{alpha}: files={len(current)}")
        current, dist_table, merged = merge_once(
            current,
            dist_table,
            k,
            workers=workers,
            show_progress=show_progress,
        )
        if not merged:
            break
        rounds += 1

    return current, rounds


base.FileInfo = FileInfo
base.ProgressPrinter = ProgressPrinter
base.union_rows_stable = union_rows_stable
base.intersect_rows_stable = intersect_rows_stable
base.union_ints_stable = union_ints_stable
base.load_mean_go_v1 = load_mean_go_v1
base.cluster_files = cluster_files
base.pair_dist = _default_pair_dist
base.os = os


def parse_env_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def parse_rate_bps(raw: str) -> float:
    s = str(raw).strip().lower()
    mult = 1.0
    for suffix, scale in (
        ("gbit", 1e9),
        ("mbit", 1e6),
        ("kbit", 1e3),
        ("bit", 1.0),
        ("gbps", 1e9),
        ("mbps", 1e6),
        ("kbps", 1e3),
        ("bps", 1.0),
    ):
        if s.endswith(suffix):
            mult = scale
            s = s[: -len(suffix)].strip()
            break
    return float(s) * mult


def parse_seconds(raw: str) -> float:
    s = str(raw).strip().lower()
    for suffix, scale in (("ms", 1e-3), ("s", 1.0), ("us", 1e-6)):
        if s.endswith(suffix):
            return float(s[: -len(suffix)].strip()) * scale
    return float(s)


def effective_link(rate_a_bps: float, rate_b_bps: float, one_way_a_s: float, one_way_b_s: float) -> Tuple[float, float]:
    return min(rate_a_bps, rate_b_bps), max(0.0, one_way_a_s) + max(0.0, one_way_b_s)


def full_restore_time(bytes_n: int, chunk_count: int, batch_size: int, bw_bps: float, rtt_s: float) -> float:
    if bytes_n <= 0 or chunk_count <= 0:
        return 0.0
    batches = int(math.ceil(float(chunk_count) / float(max(1, batch_size))))
    return batches * rtt_s + (float(bytes_n) * 8.0 / max(1.0, bw_bps))


def hybrid_restore_time(
    bytes_n: int,
    chunk_count: int,
    edge_ratio: float,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    cloud_bw_bps: float,
    cloud_rtt_s: float,
) -> float:
    if bytes_n <= 0 or chunk_count <= 0:
        return 0.0
    r = min(1.0, max(0.0, float(edge_ratio)))
    edge_chunks = int(math.ceil(chunk_count * r))
    cloud_chunks = max(0, chunk_count - edge_chunks)
    edge_bytes = float(bytes_n) * r
    cloud_bytes = max(0.0, float(bytes_n) - edge_bytes)

    edge_t = full_restore_time(int(edge_bytes), edge_chunks, batch_size, edge_bw_bps, edge_rtt_s)
    cloud_t = full_restore_time(int(cloud_bytes), cloud_chunks, batch_size, cloud_bw_bps, cloud_rtt_s)
    return max(edge_t, cloud_t)


def theoretical_file_gain(
    file_bytes: int,
    chunk_count: int,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    cloud_bw_bps: float,
    cloud_rtt_s: float,
    baseline_edge_ratio: float,
) -> float:
    t_edge = full_restore_time(file_bytes, chunk_count, batch_size, edge_bw_bps, edge_rtt_s)
    if baseline_edge_ratio <= 0.0:
        t_base = full_restore_time(file_bytes, chunk_count, batch_size, cloud_bw_bps, cloud_rtt_s)
    elif baseline_edge_ratio >= 1.0:
        t_base = t_edge
    else:
        t_base = hybrid_restore_time(
            file_bytes,
            chunk_count,
            baseline_edge_ratio,
            batch_size,
            edge_bw_bps,
            edge_rtt_s,
            cloud_bw_bps,
            cloud_rtt_s,
        )
    return max(0.0, t_base - t_edge)


def cluster_gain(file_info: base.FileInfo, original_scores: Sequence[float]) -> float:
    total = 0.0
    for fid in file_info.file_ids:
        idx = int(fid) - 1
        if 0 <= idx < len(original_scores):
            total += float(original_scores[idx])
    return total


def make_theoretical_pair_dist(original_scores: Sequence[float]):
    def pair_dist(file_a: base.FileInfo, file_b: base.FileInfo) -> float:
        inter = base.intersect_rows_stable(file_a.chunks, file_b.chunks)
        inter_size = sum(sz for _, sz in inter)
        a_size = sum(sz for _, sz in file_a.chunks)
        b_size = sum(sz for _, sz in file_b.chunks)
        union_size = a_size + b_size - inter_size
        if union_size <= 0:
            return 1.0

        gain_a = cluster_gain(file_a, original_scores)
        gain_b = cluster_gain(file_b, original_scores)
        merged_hd = (gain_a + gain_b) / union_size
        current_hd = max(
            gain_a / a_size if a_size > 0 else 0.0,
            gain_b / b_size if b_size > 0 else 0.0,
        )
        if merged_hd < current_hd:
            return 1.0
        return 1.0 - (inter_size / union_size)

    return pair_dist


def run_case1_theoretical(
    files: List[base.FileInfo],
    uni_size: Sequence[int],
    total_capacity: float,
    show_progress: bool,
    original_scores: Sequence[float],
) -> dict:
    file_unique_chunk_ids: List[set[int]] = [{int(cid) for cid, _ in f.chunks} for f in files]

    storage_chunks: set[int] = set()
    storage_lines: List[int] = []
    now_size = 0.0
    capacity_left = total_capacity
    total_heat = 0.0
    total_theoretical_gain = 0.0
    is_full = False
    pb = base.ProgressPrinter(total=len(files), label="case1 select", enabled=show_progress)

    while now_size < total_capacity and sum(1 for f in files if f.select) < len(files) and not is_full:
        delta_unique_by_idx: List[float] = [0.0 for _ in files]
        gain_by_idx: List[float] = [0.0 for _ in files]
        for idx, f in enumerate(files):
            if f.select:
                f.heat_dsize = 0.0
                continue

            delta_size = 0.0
            for cid in file_unique_chunk_ids[idx]:
                if cid not in storage_chunks and 1 <= cid <= len(uni_size):
                    delta_size += float(uni_size[cid - 1])
            delta_unique_by_idx[idx] = delta_size

            gain = cluster_gain(f, original_scores)
            gain_by_idx[idx] = gain
            if capacity_left >= delta_size:
                f.heat_dsize = math.inf if delta_size == 0 else (gain / delta_size)
            else:
                f.heat_dsize = 0.0
                f.select = True

        max_hd = max(f.heat_dsize for f in files)
        if max_hd == 0.0:
            is_full = True
            break

        if not storage_lines:
            select_idx = max(i for i, f in enumerate(files) if f.heat_dsize == max_hd)
        else:
            select_idx = next(i for i, f in enumerate(files) if f.heat_dsize == max_hd)

        selected = files[select_idx]
        selected_ids = file_unique_chunk_ids[select_idx]
        selected_delta = delta_unique_by_idx[select_idx]
        storage_chunks.update(selected_ids)
        now_size += selected_delta

        line = select_idx + 1
        if line not in storage_lines:
            storage_lines.append(line)
            storage_lines.sort()

        selected.select = True
        total_heat += selected.heat
        total_theoretical_gain += gain_by_idx[select_idx]
        capacity_left = total_capacity - now_size
        pb.update(1)

    pb.close()

    selected_original_files: List[int] = []
    for line in storage_lines:
        selected_original_files = base.union_ints_stable(selected_original_files, files[line - 1].file_ids)

    result = {
        "storage_lines": storage_lines,
        "selected_original_files": selected_original_files,
        "storage_chunks": sorted(storage_chunks),
        "now_size": now_size,
        "now_size_unique": float(sum(uni_size[cid - 1] for cid in storage_chunks)),
        "total_capacity": total_capacity,
        "capacity_left": capacity_left,
        "total_heat": total_heat,
        "total_theoretical_gain": total_theoretical_gain,
    }
    result["storageLines"] = result["storage_lines"]
    result["storageChunks_MEAN"] = result["storage_chunks"]
    result["totalHeat"] = result["total_heat"]
    result["nowSize"] = result["now_size"]
    return result


def _plan_group_placement(
    chunk_ids: Sequence[int],
    uni_size: Sequence[int],
    per_server_capacity: Sequence[float],
    chunk2server_by_chunk_id: Sequence[int],
    preferred_sid: int,
) -> Tuple[List[Tuple[int, int]], List[float]] | None:
    if not per_server_capacity:
        return None

    next_capacity = [float(v) for v in per_server_capacity]
    planned: List[Tuple[int, int]] = []
    server_num = len(next_capacity)
    for offset, chunk_id in enumerate(chunk_ids):
        if not (1 <= int(chunk_id) <= len(uni_size)):
            continue
        if chunk2server_by_chunk_id[int(chunk_id) - 1] > 0:
            continue

        sz = float(uni_size[int(chunk_id) - 1])
        assigned_sid = -1
        for probe in range(server_num):
            sid = (int(preferred_sid) + int(offset) + probe) % server_num
            if next_capacity[sid] >= sz:
                assigned_sid = sid
                break
        if assigned_sid < 0:
            return None

        next_capacity[assigned_sid] -= sz
        planned.append((int(chunk_id), assigned_sid + 1))

    return planned, next_capacity


def output_result_uni_matlab_style(
    files: Sequence[base.FileInfo],
    storage_lines: Sequence[int],
    uni_size: Sequence[int],
    uni_fingerprint: Sequence[str],
    storage_chunks: Sequence[int],
    total_size: int,
    capacity_ratio: float,
    server_num: int,
    show_progress: bool,
) -> dict:
    """
    Build a file-aware spread placement for the selected rows.
    """
    if server_num <= 0:
        raise ValueError("server_num must be positive")

    per_server_capacity = [float(total_size) * float(capacity_ratio) / float(server_num)] * server_num
    chunk2server_by_chunk_id: List[int] = [0 for _ in range(len(uni_size))]
    ordered_groups: List[List[int]] = []
    seen_chunk_ids: set[int] = set()
    file_start_sid = 0

    for line in storage_lines:
        idx = int(line) - 1
        if idx < 0 or idx >= len(files):
            continue
        group: List[int] = []
        for cid, _ in files[idx].chunks:
            cc = int(cid)
            if cc not in seen_chunk_ids:
                seen_chunk_ids.add(cc)
                group.append(cc)
        if group:
            ordered_groups.append(group)

    for cid in storage_chunks:
        cc = int(cid)
        if cc not in seen_chunk_ids:
            seen_chunk_ids.add(cc)
            ordered_groups.append([cc])

    total_ordered = sum(len(group) for group in ordered_groups)
    pb = base.ProgressPrinter(total=max(1, total_ordered), label="case1 place", enabled=show_progress)
    for group in ordered_groups:
        if not group:
            continue
        preferred_sid = file_start_sid % server_num
        plan = _plan_group_placement(
            chunk_ids=group,
            uni_size=uni_size,
            per_server_capacity=per_server_capacity,
            chunk2server_by_chunk_id=chunk2server_by_chunk_id,
            preferred_sid=preferred_sid,
        )
        placed_in_group = 0
        if plan is not None:
            planned_chunks, next_capacity = plan
            for chunk_id, sid in planned_chunks:
                chunk2server_by_chunk_id[int(chunk_id) - 1] = int(sid)
            per_server_capacity = next_capacity
            placed_in_group = len(planned_chunks)
        for _ in group:
            pb.update(1)
        file_start_sid = (file_start_sid + max(1, placed_in_group)) % server_num
    pb.close()

    hash2edge_node_id = {}
    for chunk_id, node_id in enumerate(chunk2server_by_chunk_id, start=1):
        if node_id > 0 and chunk_id <= len(uni_fingerprint):
            hash2edge_node_id[uni_fingerprint[chunk_id - 1]] = node_id

    return {
        "hash2edge_node_id": hash2edge_node_id,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run theoretical-gain MEAN cluster + Case1 from mean_go_v1 JSON")
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument("--output-json", default="results/case1_cluster_theoretical_weighted.json", help="output summary path")
    parser.add_argument("--env-file", default="../../compose.paths.env", help="compose env file with network parameters")
    parser.add_argument("--k", type=int, default=1000, help="k pairs with minimum distance per clustering round")
    parser.add_argument("--alpha", type=int, default=5, help="number of clustering rounds")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="total storage capacity ratio")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for Case1 placement")
    parser.add_argument("--workers", type=int, default=max(1, ((base.os.cpu_count() or 1) - 1)), help="parallel worker processes for clustering distance computation")
    parser.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True, help="show progress bars (default: on)")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="batch size used in theoretical restore model")
    parser.add_argument("--baseline-edge-ratio", type=float, default=0.0, help="baseline edge ratio in [0,1]; 0 means full-cloud baseline")
    args = parser.parse_args()

    input_path = Path(args.input_json)
    env_path = Path(args.env_file)
    if not input_path.exists():
        raise FileNotFoundError(f"input json not found: {input_path}")
    if not env_path.exists():
        raise FileNotFoundError(f"env file not found: {env_path}")

    env = parse_env_file(env_path)
    edge_bw_bps, edge_rtt_s = effective_link(
        parse_rate_bps(env.get("M2E_RATE", "1500mbit")),
        parse_rate_bps(env.get("EDGE_NET_RATE", "1500mbit")),
        parse_seconds(env.get("M2E_DELAY", "1ms")),
        parse_seconds(env.get("EDGE_NET_DELAY", "1ms")),
    )
    cloud_bw_bps, cloud_rtt_s = effective_link(
        parse_rate_bps(env.get("M2C_RATE", "100mbit")),
        parse_rate_bps(env.get("CLOUD_NET_RATE", "100mbit")),
        parse_seconds(env.get("M2C_DELAY", "25ms")),
        parse_seconds(env.get("CLOUD_NET_DELAY", "25ms")),
    )

    files, uni_size, uni_fingerprint, total_size = base.load_mean_go_v1(input_path, max_files=args.max_files)
    original_n = len(files)
    print(f"Loaded {original_n} files from {input_path}")
    print(
        "Theoretical model:",
        f"edge_bw={edge_bw_bps:.0f}bps",
        f"edge_rtt={edge_rtt_s:.6f}s",
        f"cloud_bw={cloud_bw_bps:.0f}bps",
        f"cloud_rtt={cloud_rtt_s:.6f}s",
        f"batch={args.restore_batch_size}",
        f"baseline_edge_ratio={args.baseline_edge_ratio:.3f}",
    )

    original_scores: List[float] = []
    gains_only: List[float] = []
    for f in files:
        benefit = theoretical_file_gain(
            file_bytes=int(f.raw_size),
            chunk_count=len(f.chunks),
            batch_size=max(1, int(args.restore_batch_size)),
            edge_bw_bps=edge_bw_bps,
            edge_rtt_s=edge_rtt_s,
            cloud_bw_bps=cloud_bw_bps,
            cloud_rtt_s=cloud_rtt_s,
            baseline_edge_ratio=float(args.baseline_edge_ratio),
        )
        gains_only.append(benefit)
        original_scores.append(float(f.heat) * benefit)

    base.pair_dist = make_theoretical_pair_dist(original_scores)
    clustered, rounds = base.cluster_files(
        files,
        k=args.k,
        alpha=args.alpha,
        workers=max(1, args.workers),
        show_progress=bool(args.progress),
    )
    print(f"Clustering done: rounds={rounds}, files {original_n} -> {len(clustered)}")

    total_capacity = float(total_size) * float(args.capacity_ratio)
    case1 = run_case1_theoretical(
        clustered,
        uni_size,
        total_capacity,
        show_progress=bool(args.progress),
        original_scores=original_scores,
    )
    placement = output_result_uni_matlab_style(
        files=clustered,
        storage_lines=case1["storage_lines"],
        uni_size=uni_size,
        uni_fingerprint=uni_fingerprint,
        storage_chunks=case1["storage_chunks"],
        total_size=total_size,
        capacity_ratio=float(args.capacity_ratio),
        server_num=int(args.server_num),
        show_progress=bool(args.progress),
    )
    print(
        "Case1 done:",
        f"selected_rows={len(case1['storage_lines'])}",
        f"stored_chunks={len(case1['storage_chunks'])}",
        f"now_size={case1['now_size']:.0f}/{case1['total_capacity']:.0f}",
        f"mapped_hashes={len(placement['hash2edge_node_id'])}",
    )

    out = {
        "input_json": str(input_path),
        "env_file": str(env_path),
        "format": "mean_py_case1_cluster_theoretical_weighted_v1",
        "params": {
            "k": args.k,
            "alpha": args.alpha,
            "capacity_ratio": args.capacity_ratio,
            "max_files": args.max_files,
            "server_num": int(args.server_num),
            "workers": max(1, args.workers),
            "progress": bool(args.progress),
            "restore_batch_size": int(args.restore_batch_size),
            "baseline_edge_ratio": float(args.baseline_edge_ratio),
        },
        "theory": {
            "edge_bandwidth_bps": edge_bw_bps,
            "edge_rtt_seconds": edge_rtt_s,
            "cloud_bandwidth_bps": cloud_bw_bps,
            "cloud_rtt_seconds": cloud_rtt_s,
            "benefit_definition": "benefit_i = T_baseline(i) - T_full_edge(i)",
            "score_definition": "score_i = heat_i * benefit_i",
            "priority_definition": "priority(cluster) = sum(score_i)/delta_unique_bytes(cluster)",
            "gain_summary": {
                "mean_seconds": (sum(gains_only) / len(gains_only)) if gains_only else 0.0,
                "max_seconds": max(gains_only) if gains_only else 0.0,
                "min_seconds": min(gains_only) if gains_only else 0.0,
            },
        },
        "summary": {
            "original_file_count": original_n,
            "clustered_file_count": len(clustered),
            "cluster_rounds_executed": rounds,
            "unique_chunk_count": len(uni_size),
            "total_size": total_size,
            "total_theoretical_gain": case1["total_theoretical_gain"],
        },
        "hash2edge_node_id": placement["hash2edge_node_id"],
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
