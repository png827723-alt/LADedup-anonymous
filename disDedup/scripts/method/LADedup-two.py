#!/usr/bin/env python3
"""
Run direct-protect hot files first, then theoretical-gain MEAN cluster + Case1.

Compared with directprotect_v1, this v2 variant changes only the Stage-2
theoretical gain model: full-edge restore time can benefit from concurrent
pulls from multiple edge nodes.
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
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import numpy as np

ChunkRow = Tuple[int, int]


class _EmbeddedBase:
    pass


class _EmbeddedTheory:
    pass


base = _EmbeddedBase()
theory = _EmbeddedTheory()


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
        heat_dsize = float(item["heat_dsize"]) if "heat_dsize" in item else (heat / file_bytes if file_bytes > 0 else 0.0)
        files.append(FileInfo(chunks=chunks, heat=heat, heat_dsize=heat_dsize, raw_size=file_bytes, file_ids=[idx]))

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


base.FileInfo = FileInfo
base.ProgressPrinter = ProgressPrinter
base.union_rows_stable = union_rows_stable
base.intersect_rows_stable = intersect_rows_stable
base.union_ints_stable = union_ints_stable
base.load_mean_go_v1 = load_mean_go_v1
base.cluster_files = cluster_files
base.pair_dist = _default_pair_dist
base.os = os

theory.parse_env_file = parse_env_file
theory.parse_rate_bps = parse_rate_bps
theory.parse_seconds = parse_seconds
theory.effective_link = effective_link
theory.full_restore_time = full_restore_time


def ladedup_t_full_edge_parallel(
    *,
    file_bytes: int,
    chunk_count: int,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    parallel_nodes: int,
) -> float:
    """Compute the paper's T_full_edge_parallel(f_i)."""
    if file_bytes <= 0 or chunk_count <= 0:
        return 0.0
    eff_nodes = max(1, min(int(parallel_nodes), int(chunk_count)))
    batches = int(math.ceil(float(chunk_count) / float(max(1, batch_size) * eff_nodes)))
    transfer = (float(file_bytes) / float(eff_nodes)) * 8.0 / max(1.0, edge_bw_bps)
    return batches * edge_rtt_s + transfer


def ladedup_t_hybrid_restore(
    *,
    file_bytes: int,
    chunk_count: int,
    edge_ratio: float,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    cloud_bw_bps: float,
    cloud_rtt_s: float,
    parallel_nodes: int,
) -> float:
    """Estimate restore time when part of a file is served by edge and the rest by cloud."""
    if file_bytes <= 0 or chunk_count <= 0:
        return 0.0

    r = min(1.0, max(0.0, float(edge_ratio)))
    edge_chunks = int(math.ceil(chunk_count * r))
    cloud_chunks = max(0, chunk_count - edge_chunks)
    edge_bytes = float(file_bytes) * r
    cloud_bytes = max(0.0, float(file_bytes) - edge_bytes)

    edge_t = ladedup_t_full_edge_parallel(
        file_bytes=int(edge_bytes),
        chunk_count=edge_chunks,
        batch_size=batch_size,
        edge_bw_bps=edge_bw_bps,
        edge_rtt_s=edge_rtt_s,
        parallel_nodes=parallel_nodes,
    )
    cloud_t = full_restore_time(
        int(cloud_bytes),
        cloud_chunks,
        batch_size,
        cloud_bw_bps,
        cloud_rtt_s,
    )
    return max(edge_t, cloud_t)


def ladedup_t_baseline(
    *,
    file_bytes: int,
    chunk_count: int,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    cloud_bw_bps: float,
    cloud_rtt_s: float,
    baseline_edge_ratio: float,
    parallel_nodes: int,
) -> float:
    """Compute T_baseline(f_i); this script uses full-cloud restore as the default baseline."""
    if baseline_edge_ratio <= 0.0:
        return full_restore_time(file_bytes, chunk_count, batch_size, cloud_bw_bps, cloud_rtt_s)
    if baseline_edge_ratio >= 1.0:
        return ladedup_t_full_edge_parallel(
            file_bytes=file_bytes,
            chunk_count=chunk_count,
            batch_size=batch_size,
            edge_bw_bps=edge_bw_bps,
            edge_rtt_s=edge_rtt_s,
            parallel_nodes=parallel_nodes,
        )
    return ladedup_t_hybrid_restore(
        file_bytes=file_bytes,
        chunk_count=chunk_count,
        edge_ratio=baseline_edge_ratio,
        batch_size=batch_size,
        edge_bw_bps=edge_bw_bps,
        edge_rtt_s=edge_rtt_s,
        cloud_bw_bps=cloud_bw_bps,
        cloud_rtt_s=cloud_rtt_s,
        parallel_nodes=parallel_nodes,
    )


def ladedup_benefit(
    *,
    file_bytes: int,
    chunk_count: int,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    cloud_bw_bps: float,
    cloud_rtt_s: float,
    baseline_edge_ratio: float,
    parallel_nodes: int,
) -> float:
    """Compute benefit_i = max(0, T_baseline(f_i) - T_full_edge_parallel(f_i))."""
    t_base = ladedup_t_baseline(
        file_bytes=file_bytes,
        chunk_count=chunk_count,
        batch_size=batch_size,
        edge_bw_bps=edge_bw_bps,
        edge_rtt_s=edge_rtt_s,
        cloud_bw_bps=cloud_bw_bps,
        cloud_rtt_s=cloud_rtt_s,
        baseline_edge_ratio=baseline_edge_ratio,
        parallel_nodes=parallel_nodes,
    )
    t_edge = ladedup_t_full_edge_parallel(
        file_bytes=file_bytes,
        chunk_count=chunk_count,
        batch_size=batch_size,
        edge_bw_bps=edge_bw_bps,
        edge_rtt_s=edge_rtt_s,
        parallel_nodes=parallel_nodes,
    )
    return max(0.0, t_base - t_edge)


def ladedup_cluster_gain(file_info: FileInfo, score_i_list: Sequence[float]) -> float:
    """Sum the single-file latency-aware scores carried by one cluster candidate."""
    total = 0.0
    for fid in file_info.file_ids:
        idx = int(fid) - 1
        if 0 <= idx < len(score_i_list):
            total += float(score_i_list[idx])
    return total


def ladedup_build_pair_dist(score_i_list: Sequence[float]):
    """Build the pair-distance matrix for latency-aware dedup clustering."""

    def pair_dist(file_a: FileInfo, file_b: FileInfo) -> float:
        inter = intersect_rows_stable(file_a.chunks, file_b.chunks)
        inter_size = sum(sz for _, sz in inter)
        a_size = sum(sz for _, sz in file_a.chunks)
        b_size = sum(sz for _, sz in file_b.chunks)
        union_size = a_size + b_size - inter_size
        if union_size <= 0:
            return 1.0

        gain_a = ladedup_cluster_gain(file_a, score_i_list)
        gain_b = ladedup_cluster_gain(file_b, score_i_list)
        merged_hd = (gain_a + gain_b) / union_size
        current_hd = max(
            gain_a / a_size if a_size > 0 else 0.0,
            gain_b / b_size if b_size > 0 else 0.0,
        )
        if merged_hd < current_hd:
            return 1.0
        return 1.0 - (inter_size / union_size)

    return pair_dist


def ladedup_cluster_files(file_set, *, k: int, alpha: int, workers: int, show_progress: bool):
    """Cluster the remaining candidate files into Phi before selection."""
    return cluster_files(
        file_set,
        k=k,
        alpha=alpha,
        workers=workers,
        show_progress=show_progress,
    )


def ladedup_pick_protected_file_ids(original_heats: Sequence[float], top_ratio: float) -> List[int]:
    """Pick the hottest original files for stage-1 direct protection."""
    indexed = [(idx + 1, float(heat)) for idx, heat in enumerate(original_heats)]
    indexed.sort(key=lambda item: (-item[1], item[0]))
    count = min(len(indexed), max(1, int(math.ceil(len(indexed) * max(0.0, float(top_ratio))))))
    return [fid for fid, _ in indexed[:count]]


def ladedup_pick_best_candidate_idx(scores: Sequence[float], first_pick: bool) -> int | None:
    """Pick the best current candidate, preserving the original tie-breaking rule."""
    max_score = max(scores) if scores else 0.0
    if max_score == 0.0:
        return None
    if first_pick:
        return max(i for i, score in enumerate(scores) if score == max_score)
    return next(i for i, score in enumerate(scores) if score == max_score)


def clone_file_info(file_info: FileInfo) -> FileInfo:
    return FileInfo(
        chunks=list(file_info.chunks),
        heat=float(file_info.heat),
        heat_dsize=float(file_info.heat_dsize),
        raw_size=int(file_info.raw_size),
        file_ids=list(file_info.file_ids),
        select=False,
    )


def clone_file_list(files: Sequence[FileInfo]) -> List[FileInfo]:
    return [clone_file_info(f) for f in files]


def _ordered_new_chunk_ids(file_info: FileInfo, known_chunk_ids: Set[int], uni_size: Sequence[int]) -> List[int]:
    seen_in_file: Set[int] = set()
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


def ladedup_build_hash_to_edge_node_map(
    chunk2server_by_chunk_id: Sequence[int],
    uni_fingerprint: Sequence[str],
) -> dict:
    """Build the final hash -> edge-node map after chunk placement is fixed."""
    hash2edge_node_id = {}
    for chunk_id, node_id in enumerate(chunk2server_by_chunk_id, start=1):
        if node_id > 0 and chunk_id <= len(uni_fingerprint):
            hash2edge_node_id[uni_fingerprint[chunk_id - 1]] = node_id
    return {"hash2edge_node_id": hash2edge_node_id}


def ladedup_stage1_direct_protect(
    files: List[FileInfo],
    uni_size: Sequence[int],
    total_capacity: float,
    server_num: int,
    show_progress: bool,
    protected_file_ids: Sequence[int],
    score_i_list: Sequence[float],
) -> dict:
    """Stage 1: admit the hottest files directly while respecting deduped capacity and placement feasibility."""
    if server_num <= 0:
        raise ValueError("server_num must be positive")

    storage_chunks: Set[int] = set()
    selected_file_ids: List[int] = []
    chunk2server_by_chunk_id: List[int] = [0 for _ in range(len(uni_size))]
    per_server_capacity = [float(total_capacity) / float(server_num)] * server_num
    now_size = 0.0
    total_heat = 0.0
    total_theoretical_gain = 0.0
    file_start_sid = 0

    pb = ProgressPrinter(
        total=max(1, len(protected_file_ids)),
        label="stage1 direct protect",
        enabled=show_progress,
    )
    for fid in protected_file_ids:
        idx = int(fid) - 1
        if idx < 0 or idx >= len(files):
            pb.update(1)
            continue

        file_info = files[idx]
        new_chunk_ids = _ordered_new_chunk_ids(file_info, storage_chunks, uni_size)
        delta_size = sum(float(uni_size[cid - 1]) for cid in new_chunk_ids)
        if now_size + delta_size > total_capacity:
            pb.update(1)
            continue

        preferred_sid = file_start_sid % server_num
        plan = _plan_group_placement(
            chunk_ids=new_chunk_ids,
            uni_size=uni_size,
            per_server_capacity=per_server_capacity,
            chunk2server_by_chunk_id=chunk2server_by_chunk_id,
            preferred_sid=preferred_sid,
        )
        if plan is None:
            pb.update(1)
            continue

        planned_chunks, next_capacity = plan
        for cid, sid in planned_chunks:
            storage_chunks.add(int(cid))
            chunk2server_by_chunk_id[int(cid) - 1] = int(sid)
        per_server_capacity = next_capacity
        file_start_sid = (file_start_sid + max(1, len(planned_chunks))) % server_num
        now_size += delta_size
        total_heat += float(file_info.heat)
        total_theoretical_gain += float(score_i_list[idx]) if 0 <= idx < len(score_i_list) else 0.0
        selected_file_ids = union_ints_stable(selected_file_ids, file_info.file_ids)
        pb.update(1)

    pb.close()
    return {
        "selected_original_files": selected_file_ids,
        "storage_chunks": sorted(storage_chunks),
        "now_size": now_size,
        "capacity_left": total_capacity - now_size,
        "total_capacity": total_capacity,
        "total_heat": total_heat,
        "total_theoretical_gain": total_theoretical_gain,
        "chunk2server_by_chunk_id": chunk2server_by_chunk_id,
        "per_server_capacity": per_server_capacity,
        "file_start_sid": file_start_sid,
    }


def ladedup_select_clusters(
    *,
    phi,
    uni_size,
    capacity: float,
    score_i_list,
    initial_storage_chunks,
    show_progress: bool,
):
    """Select the final LADedup clusters under the remaining edge capacity."""
    file_unique_chunk_ids: List[set[int]] = [{int(cid) for cid, _ in f.chunks} for f in phi]

    storage_chunks: Set[int] = {int(v) for v in initial_storage_chunks}
    initial_chunk_count = len(storage_chunks)
    storage_lines: List[int] = []
    now_size = 0.0
    capacity_left = capacity
    total_heat = 0.0
    total_theoretical_gain = 0.0
    pb = ProgressPrinter(total=max(1, len(phi)), label="stage2 theory", enabled=show_progress)
    is_full = False
    while now_size < capacity and sum(1 for f in phi if f.select) < len(phi) and not is_full:
        delta_unique_by_idx: List[float] = [0.0 for _ in phi]
        gain_by_idx: List[float] = [0.0 for _ in phi]
        for idx, f in enumerate(phi):
            if f.select:
                f.heat_dsize = 0.0
                continue

            delta_size = 0.0
            for cid in file_unique_chunk_ids[idx]:
                if cid not in storage_chunks and 1 <= cid <= len(uni_size):
                    delta_size += float(uni_size[cid - 1])
            delta_unique_by_idx[idx] = delta_size

            gain = ladedup_cluster_gain(f, score_i_list)
            gain_by_idx[idx] = gain
            if capacity_left >= delta_size:
                f.heat_dsize = math.inf if delta_size == 0 else (gain / delta_size)
            else:
                f.heat_dsize = 0.0

        select_idx = ladedup_pick_best_candidate_idx(
            [f.heat_dsize for f in phi],
            first_pick=(len(storage_lines) == 0),
        )
        if select_idx is None:
            is_full = True
            break

        selected = phi[select_idx]
        selected_ids = file_unique_chunk_ids[select_idx]
        selected_delta = delta_unique_by_idx[select_idx]
        storage_chunks.update(selected_ids)
        now_size += selected_delta
        capacity_left = capacity - now_size

        line = select_idx + 1
        if line not in storage_lines:
            storage_lines.append(line)
            storage_lines.sort()

        selected.select = True
        total_heat += selected.heat
        total_theoretical_gain += gain_by_idx[select_idx]
        pb.update(1)

    pb.close()

    selected_original_files: List[int] = []
    for line in storage_lines:
        selected_original_files = union_ints_stable(selected_original_files, phi[line - 1].file_ids)
    result = {
        "storage_lines": storage_lines,
        "selected_original_files": selected_original_files,
        "storage_chunks": sorted(storage_chunks),
        "new_storage_chunks": sorted(storage_chunks.difference(set(int(v) for v in initial_storage_chunks))),
        "now_size": now_size,
        "now_size_unique": float(sum(uni_size[cid - 1] for cid in storage_chunks)),
        "total_capacity": capacity,
        "capacity_left": capacity_left,
        "total_heat": total_heat,
        "total_theoretical_gain": total_theoretical_gain,
        "initial_chunk_count": initial_chunk_count,
    }
    result["storageLines"] = result["storage_lines"]
    result["storageChunks_MEAN"] = result["storage_chunks"]
    result["totalHeat"] = result["total_heat"]
    result["nowSize"] = result["now_size"]
    return result


def ladedup_build_final_placement(
    files: Sequence[FileInfo],
    storage_lines: Sequence[int],
    uni_size: Sequence[int],
    uni_fingerprint: Sequence[str],
    chunk2server_by_chunk_id: Sequence[int],
    per_server_capacity: Sequence[float],
    file_start_sid: int,
    show_progress: bool,
) -> dict:
    """Convert selected clusters into the final chunk placement and hash -> node output."""
    server_num = len(per_server_capacity)
    if server_num <= 0:
        raise ValueError("server_num must be positive")

    next_capacity = [float(v) for v in per_server_capacity]
    next_chunk2server = [int(v) for v in chunk2server_by_chunk_id]
    seen_chunk_ids: set[int] = set()
    ordered_groups: List[List[int]] = []

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

    total_ordered = sum(len(group) for group in ordered_groups)
    pb = ProgressPrinter(total=max(1, total_ordered), label="case1 place", enabled=show_progress)
    next_file_start_sid = int(file_start_sid)

    for group in ordered_groups:
        if not group:
            continue
        preferred_sid = next_file_start_sid % server_num
        plan = _plan_group_placement(
            chunk_ids=group,
            uni_size=uni_size,
            per_server_capacity=next_capacity,
            chunk2server_by_chunk_id=next_chunk2server,
            preferred_sid=preferred_sid,
        )
        placed_in_group = 0
        if plan is not None:
            planned_chunks, next_capacity = plan
            for chunk_id, sid in planned_chunks:
                next_chunk2server[int(chunk_id) - 1] = int(sid)
            placed_in_group = len(planned_chunks)
        for _ in group:
            pb.update(1)

        next_file_start_sid = (next_file_start_sid + max(1, placed_in_group)) % server_num

    pb.close()
    return {
        "hash2edge_node_id": ladedup_build_hash_to_edge_node_map(
            next_chunk2server,
            uni_fingerprint,
        )["hash2edge_node_id"],
        "chunk2server_by_chunk_id": next_chunk2server,
        "per_server_capacity": next_capacity,
        "file_start_sid": next_file_start_sid,
    }


def ladedup_cluster_selection(
    *,
    file_set,
    uni_size,
    capacity: float,
    score_i_list,
    initial_storage_chunks,
    k: int,
    alpha: int,
    workers: int,
    show_progress: bool,
):
    """Run LADedup stage 2: pair-distance construction, clustering, and capacity-aware selection."""
    base.pair_dist = ladedup_build_pair_dist(score_i_list)
    phi, rounds = ladedup_cluster_files(
        file_set,
        k=k,
        alpha=alpha,
        workers=workers,
        show_progress=show_progress,
    )
    if not phi:
        return [], None, rounds
    selection = ladedup_select_clusters(
        phi=phi,
        uni_size=uni_size,
        capacity=capacity,
        score_i_list=score_i_list,
        initial_storage_chunks=initial_storage_chunks,
        show_progress=show_progress,
    )
    return phi, selection, rounds


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run direct-protect hot files first, then parallel-edge theoretical-gain MEAN cluster + Case1"
    )
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument(
        "--output-json",
        default="results/case1_cluster_theoretical_weighted_directprotect_v2.json",
        help="output summary path",
    )
    parser.add_argument("--env-file", default="../../compose.paths.env", help="compose env file with network parameters")
    parser.add_argument("--k", type=int, default=1000, help="k pairs with minimum distance per clustering round")
    parser.add_argument("--alpha", type=int, default=5, help="number of clustering rounds")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="total storage capacity ratio")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for Case1 placement")
    parser.add_argument(
        "--parallel-edge-nodes",
        type=int,
        default=0,
        help="effective average parallel edge nodes for restore; 0 means use server-num",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, ((os.cpu_count() or 1) - 1)),
        help="parallel worker processes for clustering distance computation",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="show progress bars (default: on)",
    )
    parser.add_argument("--restore-batch-size", type=int, default=128, help="batch size used in theoretical restore model")
    parser.add_argument(
        "--protect-top-ratio",
        type=float,
        default=0.15,
        help="fraction of hottest original files to protect in stage 1",
    )
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

    files, uni_size, uni_fingerprint, total_size = load_mean_go_v1(input_path, max_files=args.max_files)
    original_n = len(files)
    baseline_edge_ratio = 0.0
    parallel_nodes = int(args.parallel_edge_nodes) if int(args.parallel_edge_nodes) > 0 else int(args.server_num)
    parallel_nodes = max(1, min(int(args.server_num), parallel_nodes))
    print(f"Loaded {original_n} files from {input_path}")
    print(
        "Direct-protect parallel theoretical model:",
        f"edge_bw={edge_bw_bps:.0f}bps",
        f"edge_rtt={edge_rtt_s:.6f}s",
        f"cloud_bw={cloud_bw_bps:.0f}bps",
        f"cloud_rtt={cloud_rtt_s:.6f}s",
        f"batch={args.restore_batch_size}",
        "baseline=full_cloud",
        f"parallel_nodes={parallel_nodes}",
        f"protect_top_ratio={args.protect_top_ratio:.3f}",
    )

    original_heats = [float(f.heat) for f in files]
    score_i_list = []
    benefit_i_list = []
    for f in files:
        # Algorithm 1: compute benefit_i and then score_i = h_i * benefit_i.
        benefit = ladedup_benefit(
            file_bytes=int(f.raw_size),
            chunk_count=len(f.chunks),
            batch_size=max(1, int(args.restore_batch_size)),
            edge_bw_bps=edge_bw_bps,
            edge_rtt_s=edge_rtt_s,
            cloud_bw_bps=cloud_bw_bps,
            cloud_rtt_s=cloud_rtt_s,
            baseline_edge_ratio=baseline_edge_ratio,
            parallel_nodes=parallel_nodes,
        )
        benefit_i_list.append(benefit)
        score_i_list.append(float(f.heat) * benefit)

    protected_file_ids = ladedup_pick_protected_file_ids(
        original_heats=original_heats,
        top_ratio=float(args.protect_top_ratio),
    )
    print("Protection set:", f"target_files={len(protected_file_ids)}")

    total_capacity = float(total_size) * float(args.capacity_ratio)
    stage1 = ladedup_stage1_direct_protect(
        files=clone_file_list(files),
        uni_size=uni_size,
        total_capacity=total_capacity,
        server_num=int(args.server_num),
        show_progress=bool(args.progress),
        protected_file_ids=protected_file_ids,
        score_i_list=score_i_list,
    )
    print(
        "Stage1 done:",
        f"protected_selected={len(stage1['selected_original_files'])}/{len(protected_file_ids)}",
        f"stage1_size={stage1['now_size']:.0f}/{stage1['total_capacity']:.0f}",
    )

    stage1_selected_id_set = {int(v) for v in stage1["selected_original_files"]}
    remaining_files = [
        clone_file_info(f)
        for idx, f in enumerate(files, start=1)
        if idx not in stage1_selected_id_set
    ]
    phi_stage2 = []
    stage2 = None
    rounds = 0
    if remaining_files and stage1["capacity_left"] > 0.0:
        # LADedup stage 2:
        # 1) build pair distances from {score_i}
        # 2) cluster remaining files into Phi
        # 3) select clusters under the remaining capacity
        phi_stage2, stage2, rounds = ladedup_cluster_selection(
            file_set=remaining_files,
            uni_size=uni_size,
            capacity=float(stage1["capacity_left"]),
            score_i_list=score_i_list,
            initial_storage_chunks=stage1["storage_chunks"],
            k=args.k,
            alpha=args.alpha,
            workers=max(1, args.workers),
            show_progress=bool(args.progress),
        )
    print("Clustering done:", f"rounds={rounds}", f"files {len(remaining_files)} -> {len(phi_stage2) if phi_stage2 else 0}")

    if phi_stage2 and stage2 is not None and stage1["capacity_left"] > 0.0:
        placement = ladedup_build_final_placement(
            files=phi_stage2,
            storage_lines=stage2["storage_lines"],
            uni_size=uni_size,
            uni_fingerprint=uni_fingerprint,
            chunk2server_by_chunk_id=stage1["chunk2server_by_chunk_id"],
            per_server_capacity=stage1["per_server_capacity"],
            file_start_sid=int(stage1["file_start_sid"]),
            show_progress=bool(args.progress),
        )
    else:
        stage2 = {
            "storage_lines": [],
            "selected_original_files": [],
            "storage_chunks": list(stage1["storage_chunks"]),
            "new_storage_chunks": [],
            "now_size": 0.0,
            "now_size_unique": float(sum(uni_size[cid - 1] for cid in stage1["storage_chunks"])),
            "total_capacity": float(stage1["capacity_left"]),
            "capacity_left": float(stage1["capacity_left"]),
            "total_heat": 0.0,
            "total_theoretical_gain": 0.0,
            "initial_chunk_count": len(stage1["storage_chunks"]),
        }
        placement = {
            "hash2edge_node_id": ladedup_build_hash_to_edge_node_map(
                stage1["chunk2server_by_chunk_id"], uni_fingerprint
            )["hash2edge_node_id"],
            "chunk2server_by_chunk_id": list(stage1["chunk2server_by_chunk_id"]),
            "per_server_capacity": list(stage1["per_server_capacity"]),
            "file_start_sid": int(stage1["file_start_sid"]),
        }

    combined_selected_original_files = union_ints_stable(
        stage1["selected_original_files"], stage2["selected_original_files"]
    )
    print(
        "Case1 done:",
        f"stage1_files={len(stage1['selected_original_files'])}",
        f"stage2_rows={len(stage2['storage_lines'])}",
        f"selected_original_files={len(combined_selected_original_files)}",
        f"now_size={(stage1['now_size'] + stage2['now_size']):.0f}/{total_capacity:.0f}",
        f"mapped_hashes={len(placement['hash2edge_node_id'])}",
    )

    out = {
        "input_json": str(input_path),
        "env_file": str(env_path),
        "format": "mean_py_case1_cluster_theoretical_weighted_directprotect_v2",
        "params": {
            "k": args.k,
            "alpha": args.alpha,
            "capacity_ratio": args.capacity_ratio,
            "max_files": args.max_files,
            "server_num": int(args.server_num),
            "parallel_edge_nodes": parallel_nodes,
            "workers": max(1, args.workers),
            "progress": bool(args.progress),
            "restore_batch_size": int(args.restore_batch_size),
            "baseline_model": "full_cloud",
            "baseline_edge_ratio": baseline_edge_ratio,
            "protect_top_ratio": float(args.protect_top_ratio),
        },
        "theory": {
            "edge_bandwidth_bps": edge_bw_bps,
            "edge_rtt_seconds": edge_rtt_s,
            "cloud_bandwidth_bps": cloud_bw_bps,
            "cloud_rtt_seconds": cloud_rtt_s,
            "baseline_model": "full_cloud",
            "parallel_edge_nodes": parallel_nodes,
            "benefit_definition": "benefit_i = max(0, T_baseline(i) - T_full_edge_parallel(i))",
            "score_definition": "score_i = heat_i * benefit_i",
            "priority_definition_stage1": "direct hot-file admission by heat rank, with immediate dedup and chunk placement",
            "priority_definition_stage2": "priority(cluster) = sum(score_i)/delta_unique_bytes(cluster)",
            "t_edge_definition": "T_full_edge_parallel(i) = ceil(chunks_i / (batch_size * p_i)) * edge_rtt + (bytes_i / p_i) * 8 / edge_bw",
            "gain_summary": {
                "mean_seconds": (sum(benefit_i_list) / len(benefit_i_list)) if benefit_i_list else 0.0,
                "max_seconds": max(benefit_i_list) if benefit_i_list else 0.0,
                "min_seconds": min(benefit_i_list) if benefit_i_list else 0.0,
            },
        },
        "protection": {
            "protected_file_ids": protected_file_ids,
            "protected_target_count": len(protected_file_ids),
            "protected_target_heat_sum": sum(original_heats[fid - 1] for fid in protected_file_ids),
            "protected_selected_count": len(stage1["selected_original_files"]),
            "stage1_selected_original_files": len(stage1["selected_original_files"]),
            "stage1_total_protected_heat": stage1["total_heat"],
            "stage1_total_theoretical_gain": stage1["total_theoretical_gain"],
            "stage2_input_excludes_stage1_selected_files": True,
        },
        "summary": {
            "original_file_count": original_n,
            "stage1_direct_file_count": len(stage1["selected_original_files"]),
            "stage2_input_file_count": len(remaining_files),
            "clustered_file_count": len(phi_stage2),
            "cluster_rounds_executed": rounds,
            "unique_chunk_count": len(uni_size),
            "total_size": total_size,
            "stage1_unique_size": stage1["now_size"],
            "stage2_unique_size": stage2["now_size"],
            "total_unique_size": stage1["now_size"] + stage2["now_size"],
            "total_theoretical_gain": stage1["total_theoretical_gain"] + stage2["total_theoretical_gain"],
            "selected_original_file_count": len(combined_selected_original_files),
        },
        "stage1": {
            "selected_original_files": stage1["selected_original_files"],
            "storage_chunks": stage1["storage_chunks"],
            "now_size": stage1["now_size"],
            "capacity_left": stage1["capacity_left"],
            "total_heat": stage1["total_heat"],
        },
        "stage2": {
            "selected_rows": stage2["storage_lines"],
            "selected_original_files": stage2["selected_original_files"],
            "new_storage_chunks": stage2["new_storage_chunks"],
            "now_size": stage2["now_size"],
            "capacity_left": stage2["capacity_left"],
            "total_heat": stage2["total_heat"],
            "total_theoretical_gain": stage2["total_theoretical_gain"],
        },
        "hash2edge_node_id": placement["hash2edge_node_id"],
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"Saved {output_path}")


def run_ladedup_two_stage_generation() -> None:
    """Standalone LADedup entry point with the same behavior as the CLI main function."""
    main()


if __name__ == "__main__":
    main()
