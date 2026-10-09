#!/usr/bin/env python3
"""
Run a dedup-aware benefit/size greedy selection baseline.

Selection score:

  score_i = heat_i * (T_cloud_i - T_edge_i) / delta_unique_size_i

The implementation is completion-aware: the numerator also includes other files
that become fully edge-restorable if the candidate's missing chunks are added.
This matches restore-fileinfo-batch, where a requested file only benefits from
the edge when all of its chunks are present.
"""

from __future__ import annotations

import argparse
import heapq
import importlib.util
import json
import math
import sys
from pathlib import Path
from types import ModuleType
from typing import List, Sequence, Tuple


def _load_base_module() -> ModuleType:
    target = Path(__file__).with_name("run_case1_cluster_theoretical_weighted.py")
    spec = importlib.util.spec_from_file_location("case1_cluster_theoretical_weighted_base", target)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load base algorithm from {target}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


base = _load_base_module()


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
        sz = float(uni_size[int(chunk_id) - 1])
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


def _build_hash2edge_node_id(
    chunk2server_by_chunk_id: Sequence[int],
    uni_fingerprint: Sequence[str],
) -> dict[str, int]:
    hash2edge_node_id: dict[str, int] = {}
    for chunk_id, node_id in enumerate(chunk2server_by_chunk_id, start=1):
        if int(node_id) > 0 and chunk_id <= len(uni_fingerprint):
            hash2edge_node_id[str(uni_fingerprint[chunk_id - 1])] = int(node_id)
    return hash2edge_node_id


def _score(weighted_benefit: float, delta_size: float) -> float:
    if delta_size == 0.0:
        return math.inf if weighted_benefit > 0.0 else 0.0
    return float(weighted_benefit) / float(delta_size)


def _marginal_completion_gain(
    new_chunk_ids: Sequence[int],
    chunk_to_files: dict[int, List[int]],
    missing_count_by_file: Sequence[int],
    completed: Sequence[bool],
    file_weighted_benefits: Sequence[float],
) -> float:
    if not new_chunk_ids:
        return 0.0

    hit_missing: dict[int, int] = {}
    for cid in new_chunk_ids:
        for file_idx in chunk_to_files.get(int(cid), []):
            if completed[file_idx]:
                continue
            hit_missing[file_idx] = hit_missing.get(file_idx, 0) + 1

    gain = 0.0
    for file_idx, hit_count in hit_missing.items():
        if hit_count >= int(missing_count_by_file[file_idx]):
            gain += float(file_weighted_benefits[file_idx])
    return gain


def _candidate_score(
    idx: int,
    file_unique_chunk_ids: Sequence[Sequence[int]],
    delta_size_by_idx: Sequence[float],
    chunk_to_files: dict[int, List[int]],
    missing_count_by_file: Sequence[int],
    completed: Sequence[bool],
    file_weighted_benefits: Sequence[float],
) -> float:
    if completed[idx]:
        return 0.0
    return _score(
        _marginal_completion_gain(
            file_unique_chunk_ids[idx],
            chunk_to_files,
            missing_count_by_file,
            completed,
            file_weighted_benefits,
        ),
        max(0.0, float(delta_size_by_idx[idx])),
    )


def _pop_best_feasible_candidate(
    heap: List[Tuple[float, int, int]],
    versions: Sequence[int],
    selected: Sequence[bool],
    file_unique_chunk_ids: Sequence[Sequence[int]],
    delta_size_by_idx: Sequence[float],
    chunk_to_files: dict[int, List[int]],
    missing_count_by_file: Sequence[int],
    completed: Sequence[bool],
    file_weighted_benefits: Sequence[float],
    uni_size: Sequence[int],
    total_capacity: float,
    now_size: float,
    per_server_limit: Sequence[float],
    server_used: Sequence[float],
    server_chunk_counts: Sequence[int],
) -> Tuple[int, float, List[Tuple[int, int]], List[float], List[int]] | None:
    skipped: List[Tuple[float, int, int]] = []
    best: Tuple[int, float, List[Tuple[int, int]], List[float], List[int]] | None = None

    while heap:
        neg_score, idx, version = heapq.heappop(heap)
        if selected[idx] or completed[idx] or version != versions[idx]:
            continue

        current_score = -float(neg_score)
        exact_score = _candidate_score(
            idx,
            file_unique_chunk_ids,
            delta_size_by_idx,
            chunk_to_files,
            missing_count_by_file,
            completed,
            file_weighted_benefits,
        )
        if abs(exact_score - current_score) > max(1e-12, abs(current_score) * 1e-9):
            if exact_score > 0.0:
                heapq.heappush(heap, (-exact_score, idx, version))
            continue
        current_score = exact_score
        if current_score <= 0.0:
            continue

        delta_size = float(delta_size_by_idx[idx])
        if now_size + delta_size > total_capacity:
            skipped.append((neg_score, idx, version))
            continue

        plan = _plan_balanced_chunk_placement(
            chunk_ids=file_unique_chunk_ids[idx],
            uni_size=uni_size,
            per_server_limit=per_server_limit,
            server_used=server_used,
            server_chunk_counts=server_chunk_counts,
        )
        if plan is None:
            skipped.append((neg_score, idx, version))
            continue

        planned_chunks, next_server_used, next_server_chunk_counts = plan
        best = (idx, delta_size, planned_chunks, next_server_used, next_server_chunk_counts)
        break

    for item in skipped:
        heapq.heappush(heap, item)

    return best


def run_benefit_size_greedy(
    files: List[base.FileInfo],
    uni_size: Sequence[int],
    uni_fingerprint: Sequence[str],
    total_capacity: float,
    server_num: int,
    show_progress: bool,
    file_restore_benefits: Sequence[float],
    file_weighted_benefits: Sequence[float],
) -> dict:
    if server_num <= 0:
        raise ValueError("server_num must be positive")

    selected = [False for _ in files]
    versions = [0 for _ in files]
    file_unique_chunk_ids: List[List[int]] = []
    delta_size_by_idx: List[float] = []
    chunk_to_files: dict[int, List[int]] = {}
    for idx, file_info in enumerate(files):
        chunk_ids = _ordered_new_chunk_ids(file_info, set(), uni_size)
        file_unique_chunk_ids.append(chunk_ids)
        delta_size_by_idx.append(sum(float(uni_size[cid - 1]) for cid in chunk_ids))
        for cid in chunk_ids:
            chunk_to_files.setdefault(int(cid), []).append(idx)
    missing_count_by_file = [len(chunk_ids) for chunk_ids in file_unique_chunk_ids]
    completed = [count == 0 for count in missing_count_by_file]

    heap: List[Tuple[float, int, int]] = []
    for idx in range(len(files)):
        score = _candidate_score(
            idx,
            file_unique_chunk_ids,
            delta_size_by_idx,
            chunk_to_files,
            missing_count_by_file,
            completed,
            file_weighted_benefits,
        )
        if score > 0.0:
            heapq.heappush(heap, (-score, idx, versions[idx]))

    per_server_limit = [float(total_capacity) / float(server_num)] * int(server_num)
    server_used = [0.0] * int(server_num)
    server_chunk_counts = [0] * int(server_num)
    chunk2server_by_chunk_id: List[int] = [0 for _ in range(len(uni_size))]
    storage_lines: List[int] = []
    storage_chunks: set[int] = set()
    selected_original_files: List[int] = []
    now_size = 0.0
    total_heat = 0.0
    total_restore_benefit = 0.0
    total_weighted_benefit = 0.0
    selected_raw_bytes = 0.0

    pb = base.ProgressPrinter(total=len(files), label="benefit/size greedy", enabled=show_progress)
    while True:
        best = _pop_best_feasible_candidate(
            heap=heap,
            versions=versions,
            selected=selected,
            file_unique_chunk_ids=file_unique_chunk_ids,
            delta_size_by_idx=delta_size_by_idx,
            chunk_to_files=chunk_to_files,
            missing_count_by_file=missing_count_by_file,
            completed=completed,
            file_weighted_benefits=file_weighted_benefits,
            uni_size=uni_size,
            total_capacity=total_capacity,
            now_size=now_size,
            per_server_limit=per_server_limit,
            server_used=server_used,
            server_chunk_counts=server_chunk_counts,
        )
        if best is None:
            break

        idx, delta_size, planned_chunks, next_server_used, next_server_chunk_counts = best
        selected[idx] = True
        storage_lines.append(idx + 1)
        now_size += float(delta_size)
        total_heat += float(files[idx].heat)
        total_restore_benefit += float(file_restore_benefits[idx])
        total_weighted_benefit += float(file_weighted_benefits[idx])
        selected_raw_bytes += float(files[idx].raw_size)

        affected_files: set[int] = set()
        affected_candidates: set[int] = set()
        for cid, sid in planned_chunks:
            storage_chunks.add(int(cid))
            chunk2server_by_chunk_id[int(cid) - 1] = int(sid)
            for affected_idx in chunk_to_files.get(int(cid), []):
                affected_files.add(affected_idx)
                if missing_count_by_file[affected_idx] > 0:
                    missing_count_by_file[affected_idx] -= 1
                if not selected[affected_idx] and not completed[affected_idx]:
                    delta_size_by_idx[affected_idx] -= float(uni_size[int(cid) - 1])
                    file_unique_chunk_ids[affected_idx] = [
                        old_cid for old_cid in file_unique_chunk_ids[affected_idx] if int(old_cid) != int(cid)
                    ]
                    affected_candidates.add(affected_idx)

        newly_completed: List[int] = []
        for affected_idx in affected_files:
            if not completed[affected_idx] and missing_count_by_file[affected_idx] <= 0:
                completed[affected_idx] = True
                newly_completed.append(affected_idx)

        for affected_idx in affected_candidates:
            if selected[affected_idx] or completed[affected_idx]:
                continue
            versions[affected_idx] += 1
            next_score = _candidate_score(
                affected_idx,
                file_unique_chunk_ids,
                delta_size_by_idx,
                chunk_to_files,
                missing_count_by_file,
                completed,
                file_weighted_benefits,
            )
            if next_score > 0.0:
                heapq.heappush(heap, (-next_score, affected_idx, versions[affected_idx]))
        server_used = next_server_used
        server_chunk_counts = next_server_chunk_counts
        selected_original_files = base.union_ints_stable(selected_original_files, files[idx].file_ids)
        pb.update(1)

    pb.close()
    storage_lines.sort()
    edge_hit_lines = [idx + 1 for idx, is_completed in enumerate(completed) if is_completed]
    return {
        "storage_lines": storage_lines,
        "edge_hit_lines": edge_hit_lines,
        "selected_original_files": selected_original_files,
        "storage_chunks": sorted(storage_chunks),
        "now_size": now_size,
        "now_size_unique": now_size,
        "total_capacity": total_capacity,
        "capacity_left": total_capacity - now_size,
        "total_heat": total_heat,
        "total_restore_benefit": total_restore_benefit,
        "total_weighted_benefit": total_weighted_benefit,
        "selected_raw_bytes": selected_raw_bytes,
        "selected_file_count": len(storage_lines),
        "edge_hit_file_count": len(edge_hit_lines),
        "chunk2server_by_chunk_id": chunk2server_by_chunk_id,
        "server_used": server_used,
        "server_chunk_counts": server_chunk_counts,
        "hash2edge_node_id": _build_hash2edge_node_id(chunk2server_by_chunk_id, uni_fingerprint),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run benefit/size greedy file selection")
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument("--output-json", default="results/case1_greedy_benefit_size.json", help="output summary path")
    parser.add_argument("--env-file", default="../../compose.paths.env", help="compose env file with network parameters")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="total storage capacity ratio")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for placement")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="batch size used in restore model")
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
    env_path = Path(args.env_file)
    if not input_path.exists():
        raise FileNotFoundError(f"input json not found: {input_path}")
    if not env_path.exists():
        raise FileNotFoundError(f"env file not found: {env_path}")

    env = base.parse_env_file(env_path)
    edge_bw_bps, edge_rtt_s = base.effective_link(
        base.parse_rate_bps(env.get("M2E_RATE", "1500mbit")),
        base.parse_rate_bps(env.get("EDGE_NET_RATE", "1500mbit")),
        base.parse_seconds(env.get("M2E_DELAY", "1ms")),
        base.parse_seconds(env.get("EDGE_NET_DELAY", "1ms")),
    )
    cloud_bw_bps, cloud_rtt_s = base.effective_link(
        base.parse_rate_bps(env.get("M2C_RATE", "100mbit")),
        base.parse_rate_bps(env.get("CLOUD_NET_RATE", "100mbit")),
        base.parse_seconds(env.get("M2C_DELAY", "25ms")),
        base.parse_seconds(env.get("CLOUD_NET_DELAY", "25ms")),
    )

    files, uni_size, uni_fingerprint, total_size = base.load_mean_go_v1(input_path, max_files=args.max_files)
    original_n = len(files)
    print(f"Loaded {original_n} files from {input_path}")
    print(
        "Benefit/size greedy model:",
        f"edge_bw={edge_bw_bps:.0f}bps",
        f"edge_rtt={edge_rtt_s:.6f}s",
        f"cloud_bw={cloud_bw_bps:.0f}bps",
        f"cloud_rtt={cloud_rtt_s:.6f}s",
        f"batch={int(args.restore_batch_size)}",
        f"capacity_ratio={float(args.capacity_ratio):.3f}",
        f"server_num={int(args.server_num)}",
    )

    restore_benefits: List[float] = []
    weighted_benefits: List[float] = []
    for file_info in files:
        t_cloud = base.full_restore_time(
            int(file_info.raw_size),
            len(file_info.chunks),
            max(1, int(args.restore_batch_size)),
            cloud_bw_bps,
            cloud_rtt_s,
        )
        t_edge = base.full_restore_time(
            int(file_info.raw_size),
            len(file_info.chunks),
            max(1, int(args.restore_batch_size)),
            edge_bw_bps,
            edge_rtt_s,
        )
        benefit = max(0.0, float(t_cloud) - float(t_edge))
        restore_benefits.append(benefit)
        weighted_benefits.append(float(file_info.heat) * benefit)

    total_capacity = float(total_size) * float(args.capacity_ratio)
    case1 = run_benefit_size_greedy(
        files=files,
        uni_size=uni_size,
        uni_fingerprint=uni_fingerprint,
        total_capacity=total_capacity,
        server_num=int(args.server_num),
        show_progress=bool(args.progress),
        file_restore_benefits=restore_benefits,
        file_weighted_benefits=weighted_benefits,
    )
    print(
        "Greedy done:",
        f"selected_files={case1['selected_file_count']}",
        f"selected_original_files={len(case1['selected_original_files'])}",
        f"stored_chunks={len(case1['storage_chunks'])}",
        f"now_size={case1['now_size']:.0f}/{case1['total_capacity']:.0f}",
        f"mapped_hashes={len(case1['hash2edge_node_id'])}",
    )

    out = {
        "input_json": str(input_path),
        "env_file": str(env_path),
        "format": "mean_py_case1_greedy_benefit_size_v1",
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
            "algorithm": "completion_aware_dedup_greedy",
            "score_definition": "score_i = sum_{j completed by adding i} heat_j * max(0, T_cloud_j - T_edge_j) / delta_unique_size_i",
            "benefit_definition": "benefit_i = max(0, T_cloud_i - T_edge_i)",
            "delta_unique_size_definition": "bytes of chunks in file i not already cached at the edge",
            "completion_definition": "a file contributes only when all of its chunks become cached at the edge",
            "recompute_score_each_round": True,
            "placement_definition": "place only new chunks and balance them across edge nodes",
        },
        "theory": {
            "edge_bandwidth_bps": edge_bw_bps,
            "edge_rtt_seconds": edge_rtt_s,
            "cloud_bandwidth_bps": cloud_bw_bps,
            "cloud_rtt_seconds": cloud_rtt_s,
            "gain_summary": {
                "mean_seconds": (sum(restore_benefits) / len(restore_benefits)) if restore_benefits else 0.0,
                "max_seconds": max(restore_benefits) if restore_benefits else 0.0,
                "min_seconds": min(restore_benefits) if restore_benefits else 0.0,
            },
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
            "total_heat": case1["total_heat"],
            "total_restore_benefit": case1["total_restore_benefit"],
            "total_weighted_benefit": case1["total_weighted_benefit"],
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
