#!/usr/bin/env python3
"""
Run graph-community + greedy-knapsack selection.

This baseline makes the shared-chunk relation explicit:

  1. Build a sparse file graph from shared chunks.
  2. Extract connected graph communities.
  3. Select graph-derived candidates by the dedup-aware marginal score:

       score_C = sum_{i completed by C} heat_i * max(0, T_cloud_i - T_edge_i) / new_unique_bytes(C)

The graph builder is intentionally sparse so it can run on datasets with many
thousands of files. Low-degree chunks add complete pair edges. High-degree
chunks add star edges to a few high-value hub files instead of exploding into
all pairs.

The candidate set includes connected communities, hot prefixes inside larger
communities, and singleton files. That avoids the common failure mode where a
large connected component is too coarse for a tight capacity budget.
"""

from __future__ import annotations

import argparse
import collections
import heapq
import importlib.util
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Deque, Iterable, List, Sequence, Tuple


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


def _load_greedy_module() -> ModuleType:
    target = Path(__file__).with_name("Greedy_benefit_size.py")
    spec = importlib.util.spec_from_file_location("case1_greedy_benefit_size", target)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load greedy algorithm from {target}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


greedy = _load_greedy_module()


@dataclass
class Community:
    file_ids: List[int]
    chunk_ids: List[int]
    heat: float
    restore_benefit: float
    weighted_benefit: float
    raw_bytes: float


class UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.size = [1] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> bool:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return False
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        return True


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
    communities: Sequence[Community],
    community_new_chunk_ids: Sequence[Sequence[int]],
    delta_size_by_idx: Sequence[float],
    completed: Sequence[bool],
    file_weighted_benefits: Sequence[float],
) -> float:
    gain = sum(
        float(file_weighted_benefits[file_idx])
        for file_idx in communities[idx].file_ids
        if not completed[file_idx]
    )
    return _score(
        gain,
        max(0.0, float(delta_size_by_idx[idx])),
    )


def _unique_chunk_ids(file_info: base.FileInfo, uni_size: Sequence[int]) -> List[int]:
    seen: set[int] = set()
    out: List[int] = []
    for cid, _ in file_info.chunks:
        cc = int(cid)
        if cc in seen or not (1 <= cc <= len(uni_size)):
            continue
        seen.add(cc)
        out.append(cc)
    return out


def _build_file_chunks(
    files: Sequence[base.FileInfo],
    uni_size: Sequence[int],
) -> Tuple[List[List[int]], List[float], dict[int, List[int]]]:
    file_chunk_ids: List[List[int]] = []
    file_unique_bytes: List[float] = []
    chunk_to_files: dict[int, List[int]] = {}

    for idx, file_info in enumerate(files):
        chunk_ids = _unique_chunk_ids(file_info, uni_size)
        file_chunk_ids.append(chunk_ids)
        file_unique_bytes.append(sum(float(uni_size[cid - 1]) for cid in chunk_ids))
        for cid in chunk_ids:
            chunk_to_files.setdefault(int(cid), []).append(idx)

    return file_chunk_ids, file_unique_bytes, chunk_to_files


def _add_edge(
    edges: dict[int, int],
    n_files: int,
    a: int,
    b: int,
    weight: int,
    max_graph_edges: int,
) -> bool:
    if a == b:
        return True
    if a > b:
        a, b = b, a
    key = int(a) * int(n_files) + int(b)
    if key not in edges and max_graph_edges > 0 and len(edges) >= max_graph_edges:
        return False
    edges[key] = int(edges.get(key, 0)) + int(weight)
    return True


def build_sparse_shared_chunk_graph(
    chunk_to_files: dict[int, List[int]],
    uni_size: Sequence[int],
    file_weighted_benefits: Sequence[float],
    n_files: int,
    pair_degree_limit: int,
    high_degree_hubs: int,
    high_degree_neighbors: int,
    max_pairs_per_chunk: int,
    max_graph_edges: int,
    show_progress: bool,
) -> Tuple[dict[int, int], dict]:
    edges: dict[int, int] = {}
    stats = {
        "shared_chunk_count": 0,
        "complete_pair_chunk_count": 0,
        "star_pair_chunk_count": 0,
        "single_file_chunk_count": 0,
        "overflow_skipped_edge_events": 0,
        "max_graph_edges": int(max_graph_edges),
    }

    items = list(chunk_to_files.items())
    pb = base.ProgressPrinter(total=len(items), label="build graph", enabled=show_progress)
    for chunk_id, posting in items:
        files_for_chunk = sorted(set(int(v) for v in posting))
        degree = len(files_for_chunk)
        if degree <= 1:
            stats["single_file_chunk_count"] += 1
            pb.update(1)
            continue

        stats["shared_chunk_count"] += 1
        weight = int(uni_size[int(chunk_id) - 1])
        complete_pairs = degree * (degree - 1) // 2
        added_for_chunk = 0

        if degree <= int(pair_degree_limit) and (
            max_pairs_per_chunk <= 0 or complete_pairs <= int(max_pairs_per_chunk)
        ):
            stats["complete_pair_chunk_count"] += 1
            for pos, a in enumerate(files_for_chunk):
                for b in files_for_chunk[pos + 1 :]:
                    if max_pairs_per_chunk > 0 and added_for_chunk >= int(max_pairs_per_chunk):
                        break
                    if _add_edge(edges, n_files, a, b, weight, int(max_graph_edges)):
                        added_for_chunk += 1
                    else:
                        stats["overflow_skipped_edge_events"] += 1
                if max_pairs_per_chunk > 0 and added_for_chunk >= int(max_pairs_per_chunk):
                    break
        else:
            stats["star_pair_chunk_count"] += 1
            hub_count = max(1, min(int(high_degree_hubs), degree))
            neighbor_count = max(1, min(int(high_degree_neighbors), hub_count))
            hubs = sorted(
                files_for_chunk,
                key=lambda idx: (-float(file_weighted_benefits[idx]), idx),
            )[:hub_count]
            for file_idx in files_for_chunk:
                connected = 0
                for hub_idx in hubs:
                    if file_idx == hub_idx:
                        continue
                    if max_pairs_per_chunk > 0 and added_for_chunk >= int(max_pairs_per_chunk):
                        break
                    if _add_edge(edges, n_files, file_idx, hub_idx, weight, int(max_graph_edges)):
                        added_for_chunk += 1
                    else:
                        stats["overflow_skipped_edge_events"] += 1
                    connected += 1
                    if connected >= neighbor_count:
                        break
                if max_pairs_per_chunk > 0 and added_for_chunk >= int(max_pairs_per_chunk):
                    break
        pb.update(1)
    pb.close()

    stats["graph_edge_count"] = len(edges)
    return edges, stats


def build_fast_shared_chunk_communities(
    chunk_to_files: dict[int, List[int]],
    file_weighted_benefits: Sequence[float],
    n_files: int,
    pair_degree_limit: int,
    high_degree_hubs: int,
    high_degree_neighbors: int,
    show_progress: bool,
) -> Tuple[List[List[int]], dict]:
    uf = UnionFind(n_files)
    stats = {
        "shared_chunk_count": 0,
        "single_file_chunk_count": 0,
        "low_degree_chunk_count": 0,
        "high_degree_chunk_count": 0,
        "union_edge_count": 0,
    }

    items = list(chunk_to_files.items())
    pb = base.ProgressPrinter(total=len(items), label="union graph", enabled=show_progress)
    for _chunk_id, posting in items:
        files_for_chunk = sorted(set(int(v) for v in posting))
        degree = len(files_for_chunk)
        if degree <= 1:
            stats["single_file_chunk_count"] += 1
            pb.update(1)
            continue

        stats["shared_chunk_count"] += 1
        if degree <= int(pair_degree_limit):
            stats["low_degree_chunk_count"] += 1
            hub = files_for_chunk[0]
            for file_idx in files_for_chunk[1:]:
                if uf.union(hub, file_idx):
                    stats["union_edge_count"] += 1
        else:
            stats["high_degree_chunk_count"] += 1
            hub_count = max(1, min(int(high_degree_hubs), degree))
            neighbor_count = max(1, min(int(high_degree_neighbors), hub_count))
            hubs = sorted(
                files_for_chunk,
                key=lambda idx: (-float(file_weighted_benefits[idx]), idx),
            )[:hub_count]
            for file_idx in files_for_chunk:
                connected = 0
                for hub in hubs:
                    if file_idx == hub:
                        continue
                    if uf.union(file_idx, hub):
                        stats["union_edge_count"] += 1
                    connected += 1
                    if connected >= neighbor_count:
                        break
        pb.update(1)
    pb.close()

    groups: dict[int, List[int]] = {}
    for idx in range(n_files):
        groups.setdefault(uf.find(idx), []).append(idx)
    communities = [sorted(v) for v in groups.values()]
    communities.sort(key=lambda ids: (ids[0] if ids else -1, len(ids)))
    stats.update(
        {
            "community_count": len(communities),
            "max_community_files": max((len(c) for c in communities), default=0),
            "mean_community_files": (
                sum(len(c) for c in communities) / float(len(communities)) if communities else 0.0
            ),
        }
    )
    return communities, stats


def build_communities_from_graph(
    edges: dict[int, int],
    n_files: int,
    file_unique_bytes: Sequence[float],
    min_shared_bytes: float,
    min_shared_ratio: float,
    max_community_files: int,
    show_progress: bool,
) -> Tuple[List[List[int]], dict]:
    adjacency: List[List[int]] = [[] for _ in range(n_files)]
    kept_edges = 0
    dropped_edges = 0

    pb = base.ProgressPrinter(total=max(1, len(edges)), label="filter graph", enabled=show_progress)
    for key, shared_bytes in edges.items():
        a = int(key) // int(n_files)
        b = int(key) % int(n_files)
        denom = min(float(file_unique_bytes[a]), float(file_unique_bytes[b]))
        shared_ratio = (float(shared_bytes) / denom) if denom > 0.0 else 0.0
        if float(shared_bytes) >= float(min_shared_bytes) and shared_ratio >= float(min_shared_ratio):
            adjacency[a].append(b)
            adjacency[b].append(a)
            kept_edges += 1
        else:
            dropped_edges += 1
        pb.update(1)
    pb.close()

    communities: List[List[int]] = []
    visited = [False for _ in range(n_files)]
    cap = int(max_community_files)
    pb2 = base.ProgressPrinter(total=n_files, label="graph communities", enabled=show_progress)
    for start in range(n_files):
        if visited[start]:
            continue

        group: List[int] = []
        queue: Deque[int] = collections.deque([start])
        queued = {start}
        while queue:
            cur = queue.popleft()
            if visited[cur]:
                continue
            visited[cur] = True
            group.append(cur)
            pb2.update(1)
            if cap > 0 and len(group) >= cap:
                break
            for nxt in adjacency[cur]:
                if not visited[nxt] and nxt not in queued:
                    queued.add(nxt)
                    queue.append(nxt)

        communities.append(sorted(group))
    pb2.close()

    stats = {
        "kept_edge_count": kept_edges,
        "dropped_edge_count": dropped_edges,
        "community_count": len(communities),
        "max_community_files": max((len(c) for c in communities), default=0),
        "mean_community_files": (
            sum(len(c) for c in communities) / float(len(communities)) if communities else 0.0
        ),
    }
    return communities, stats


def _ordered_union_chunk_ids(file_ids: Iterable[int], file_chunk_ids: Sequence[Sequence[int]]) -> List[int]:
    seen: set[int] = set()
    out: List[int] = []
    for file_idx in file_ids:
        for cid in file_chunk_ids[int(file_idx)]:
            if int(cid) in seen:
                continue
            seen.add(int(cid))
            out.append(int(cid))
    return out


def build_community_candidates(
    community_file_ids: Sequence[Sequence[int]],
    files: Sequence[base.FileInfo],
    file_chunk_ids: Sequence[Sequence[int]],
    file_unique_bytes: Sequence[float],
    file_restore_benefits: Sequence[float],
    file_weighted_benefits: Sequence[float],
    show_progress: bool,
) -> List[Community]:
    communities: List[Community] = []
    seen: set[Tuple[int, ...]] = set()

    def add_candidate(ids: Sequence[int]) -> None:
        sorted_ids = sorted(set(int(v) for v in ids))
        if not sorted_ids:
            return
        key = tuple(sorted_ids)
        if key in seen:
            return
        seen.add(key)
        chunk_ids = _ordered_union_chunk_ids(sorted_ids, file_chunk_ids)
        communities.append(
            Community(
                file_ids=sorted_ids,
                chunk_ids=chunk_ids,
                heat=sum(float(files[i].heat) for i in sorted_ids),
                restore_benefit=sum(float(file_restore_benefits[i]) for i in sorted_ids),
                weighted_benefit=sum(float(file_weighted_benefits[i]) for i in sorted_ids),
                raw_bytes=sum(float(files[i].raw_size) for i in sorted_ids),
            )
        )

    pb = base.ProgressPrinter(total=len(community_file_ids), label="community candidates", enabled=show_progress)
    prefix_sizes = (2, 4, 8, 16, 32, 64)
    max_full_candidate_files = 64
    for ids in community_file_ids:
        sorted_ids = sorted(int(v) for v in ids)
        if len(sorted_ids) <= max_full_candidate_files:
            add_candidate(sorted_ids)
        if len(sorted_ids) > 1:
            hot_order = sorted(
                sorted_ids,
                key=lambda idx: (
                    -_score(float(file_weighted_benefits[idx]), max(1.0, float(file_unique_bytes[idx]))),
                    -float(file_weighted_benefits[idx]),
                    idx,
                ),
            )
            for size in prefix_sizes:
                if size <= len(hot_order):
                    add_candidate(hot_order[:size])
        pb.update(1)
    pb.close()

    pb2 = base.ProgressPrinter(total=len(files), label="singleton candidates", enabled=show_progress)
    for idx in range(len(files)):
        add_candidate([idx])
        pb2.update(1)
    pb2.close()
    return communities


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


def _pop_best_feasible_community(
    heap: List[Tuple[float, int, int]],
    versions: Sequence[int],
    selected: Sequence[bool],
    communities: Sequence[Community],
    community_new_chunk_ids: Sequence[Sequence[int]],
    delta_size_by_idx: Sequence[float],
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
        if selected[idx] or version != versions[idx]:
            continue

        current_score = -float(neg_score)
        exact_score = _candidate_score(
            idx,
            communities,
            community_new_chunk_ids,
            delta_size_by_idx,
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
            community_new_chunk_ids[idx],
            uni_size,
            per_server_limit,
            server_used,
            server_chunk_counts,
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


def run_community_greedy_selection(
    communities: Sequence[Community],
    file_chunk_ids: Sequence[Sequence[int]],
    chunk_to_files: dict[int, List[int]],
    file_weighted_benefits: Sequence[float],
    uni_size: Sequence[int],
    uni_fingerprint: Sequence[str],
    total_capacity: float,
    server_num: int,
    show_progress: bool,
) -> dict:
    if server_num <= 0:
        raise ValueError("server_num must be positive")

    selected = [False for _ in communities]
    versions = [0 for _ in communities]
    community_new_chunk_ids: List[List[int]] = [list(c.chunk_ids) for c in communities]
    delta_size_by_idx = [
        sum(float(uni_size[int(cid) - 1]) for cid in community.chunk_ids) for community in communities
    ]
    missing_count_by_file = [len(chunks) for chunks in file_chunk_ids]
    completed = [count == 0 for count in missing_count_by_file]

    chunk_to_communities: dict[int, List[int]] = {}
    for idx, community in enumerate(communities):
        for cid in community.chunk_ids:
            chunk_to_communities.setdefault(int(cid), []).append(idx)

    heap: List[Tuple[float, int, int]] = []
    for idx in range(len(communities)):
        score = _candidate_score(
            idx,
            communities,
            community_new_chunk_ids,
            delta_size_by_idx,
            completed,
            file_weighted_benefits,
        )
        if score > 0.0:
            heapq.heappush(heap, (-score, idx, versions[idx]))

    per_server_limit = [float(total_capacity) / float(server_num)] * int(server_num)
    server_used = [0.0] * int(server_num)
    server_chunk_counts = [0] * int(server_num)
    chunk2server_by_chunk_id = [0 for _ in range(len(uni_size))]
    storage_communities: List[int] = []
    storage_lines: List[int] = []
    storage_chunks: set[int] = set()
    selected_original_files: List[int] = []
    now_size = 0.0
    total_heat = 0.0
    total_restore_benefit = 0.0
    total_weighted_benefit = 0.0
    selected_raw_bytes = 0.0

    pb = base.ProgressPrinter(total=len(communities), label="community greedy", enabled=show_progress)
    while True:
        best = _pop_best_feasible_community(
            heap,
            versions,
            selected,
            communities,
            community_new_chunk_ids,
            delta_size_by_idx,
            completed,
            file_weighted_benefits,
            uni_size,
            total_capacity,
            now_size,
            per_server_limit,
            server_used,
            server_chunk_counts,
        )
        if best is None:
            break

        idx, delta_size, planned_chunks, next_server_used, next_server_chunk_counts = best
        community = communities[idx]
        selected[idx] = True
        storage_communities.append(idx + 1)
        now_size += float(delta_size)
        total_heat += float(community.heat)
        total_restore_benefit += float(community.restore_benefit)
        total_weighted_benefit += float(community.weighted_benefit)
        selected_raw_bytes += float(community.raw_bytes)

        for file_idx in community.file_ids:
            line = int(file_idx) + 1
            if line not in storage_lines:
                storage_lines.append(line)
                selected_original_files.append(line)

        affected_files: set[int] = set()
        affected_communities: set[int] = set()
        for cid, sid in planned_chunks:
            storage_chunks.add(int(cid))
            chunk2server_by_chunk_id[int(cid) - 1] = int(sid)
            for affected_file_idx in chunk_to_files.get(int(cid), []):
                affected_files.add(affected_file_idx)
                if missing_count_by_file[affected_file_idx] > 0:
                    missing_count_by_file[affected_file_idx] -= 1
            for affected_idx in chunk_to_communities.get(int(cid), []):
                if selected[affected_idx]:
                    continue
                delta_size_by_idx[affected_idx] -= float(uni_size[int(cid) - 1])
                community_new_chunk_ids[affected_idx] = [
                    old_cid for old_cid in community_new_chunk_ids[affected_idx] if int(old_cid) != int(cid)
                ]
                affected_communities.add(affected_idx)

        for affected_file_idx in affected_files:
            if not completed[affected_file_idx] and missing_count_by_file[affected_file_idx] <= 0:
                completed[affected_file_idx] = True

        for affected_idx in affected_communities:
            if selected[affected_idx]:
                continue
            versions[affected_idx] += 1
            next_score = _candidate_score(
                affected_idx,
                communities,
                community_new_chunk_ids,
                delta_size_by_idx,
                completed,
                file_weighted_benefits,
            )
            if next_score > 0.0:
                heapq.heappush(heap, (-next_score, affected_idx, versions[affected_idx]))

        server_used = next_server_used
        server_chunk_counts = next_server_chunk_counts
        pb.update(1)

    pb.close()
    storage_lines.sort()
    selected_original_files = sorted(set(selected_original_files))
    edge_hit_lines = [idx + 1 for idx, is_completed in enumerate(completed) if is_completed]
    return {
        "storage_communities": storage_communities,
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
        "selected_community_count": len(storage_communities),
        "selected_file_count": len(storage_lines),
        "edge_hit_file_count": len(edge_hit_lines),
        "chunk2server_by_chunk_id": chunk2server_by_chunk_id,
        "server_used": server_used,
        "server_chunk_counts": server_chunk_counts,
        "hash2edge_node_id": _build_hash2edge_node_id(chunk2server_by_chunk_id, uni_fingerprint),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run graph-community benefit/size greedy selection")
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument(
        "--output-json",
        default="results/case1_graph_community_benefit_size.json",
        help="output summary path",
    )
    parser.add_argument("--env-file", default="../../compose.paths.env", help="compose env file with network parameters")
    parser.add_argument("--capacity-ratio", type=float, default=0.2, help="total storage capacity ratio")
    parser.add_argument("--max-files", type=int, default=0, help="debug only: limit input file count")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge servers for placement")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="batch size used in restore model")
    parser.add_argument("--workers", type=int, default=1, help="accepted for compatibility; unused")
    parser.add_argument("--k", type=int, default=1000, help="accepted for compatibility; unused")
    parser.add_argument("--alpha", type=int, default=5, help="accepted for compatibility; unused")
    parser.add_argument("--pair-degree-limit", type=int, default=16, help="complete-pair chunks up to this degree")
    parser.add_argument("--high-degree-hubs", type=int, default=4, help="hub candidates for high-degree chunks")
    parser.add_argument("--high-degree-neighbors", type=int, default=2, help="star edges per file for high-degree chunks")
    parser.add_argument("--max-pairs-per-chunk", type=int, default=1000, help="edge additions cap per chunk; <=0 disables")
    parser.add_argument("--max-graph-edges", type=int, default=200000, help="global graph edge cap; <=0 disables")
    parser.add_argument("--min-shared-bytes", type=float, default=1.0, help="minimum shared bytes for a graph edge")
    parser.add_argument("--min-shared-ratio", type=float, default=0.0, help="minimum shared_bytes/min(file_bytes)")
    parser.add_argument("--max-community-files", type=int, default=256, help="split BFS communities at this size; <=0 disables")
    parser.add_argument(
        "--community-bonus",
        type=float,
        default=0.25,
        help="extra weight for files that belong to larger shared-chunk communities",
    )
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
        "Graph-community model:",
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

    file_chunk_ids, file_unique_bytes, chunk_to_files = _build_file_chunks(files, uni_size)
    community_file_ids, graph_stats = build_fast_shared_chunk_communities(
        chunk_to_files=chunk_to_files,
        file_weighted_benefits=weighted_benefits,
        n_files=original_n,
        pair_degree_limit=int(args.pair_degree_limit),
        high_degree_hubs=int(args.high_degree_hubs),
        high_degree_neighbors=int(args.high_degree_neighbors),
        show_progress=bool(args.progress),
    )
    community_stats = graph_stats
    community_bonus_by_file = [0.0 for _ in files]
    for ids in community_file_ids:
        if len(ids) <= 1:
            continue
        bonus = float(args.community_bonus) * min(1.0, math.log1p(len(ids)) / math.log(65.0))
        for file_idx in ids:
            community_bonus_by_file[file_idx] = max(community_bonus_by_file[file_idx], bonus)
    boosted_weighted_benefits = [
        float(weighted_benefits[idx]) * (1.0 + float(community_bonus_by_file[idx]))
        for idx in range(len(weighted_benefits))
    ]
    print(
        "Communities built:",
        f"union_edges={graph_stats['union_edge_count']}",
        f"communities={len(community_file_ids)}",
        f"max_files={community_stats['max_community_files']}",
    )

    total_capacity = float(total_size) * float(args.capacity_ratio)
    case1 = greedy.run_benefit_size_greedy(
        files=files,
        uni_size=uni_size,
        uni_fingerprint=uni_fingerprint,
        total_capacity=total_capacity,
        server_num=int(args.server_num),
        show_progress=bool(args.progress),
        file_restore_benefits=restore_benefits,
        file_weighted_benefits=boosted_weighted_benefits,
    )
    print(
        "Community-guided greedy done:",
        f"selected_files={case1['selected_file_count']}",
        f"edge_hit_files={case1['edge_hit_file_count']}",
        f"stored_chunks={len(case1['storage_chunks'])}",
        f"now_size={case1['now_size']:.0f}/{case1['total_capacity']:.0f}",
        f"mapped_hashes={len(case1['hash2edge_node_id'])}",
    )

    out = {
        "input_json": str(input_path),
        "env_file": str(env_path),
        "format": "mean_py_case1_graph_community_benefit_size_v1",
        "params": {
            "k": int(args.k),
            "alpha": int(args.alpha),
            "capacity_ratio": float(args.capacity_ratio),
            "max_files": int(args.max_files),
            "server_num": int(args.server_num),
            "workers": max(1, int(args.workers)),
            "progress": bool(args.progress),
            "restore_batch_size": int(args.restore_batch_size),
            "pair_degree_limit": int(args.pair_degree_limit),
            "high_degree_hubs": int(args.high_degree_hubs),
            "high_degree_neighbors": int(args.high_degree_neighbors),
            "max_pairs_per_chunk": int(args.max_pairs_per_chunk),
            "max_graph_edges": int(args.max_graph_edges),
            "min_shared_bytes": float(args.min_shared_bytes),
            "min_shared_ratio": float(args.min_shared_ratio),
            "max_community_files": int(args.max_community_files),
            "community_bonus": float(args.community_bonus),
        },
        "selection": {
            "algorithm": "fast_shared_chunk_community_guided_completion_greedy",
            "score_definition": "score_i = completion-aware greedy score using graph-community boosted file benefit",
            "benefit_definition": "benefit_i = max(0, T_cloud_i - T_edge_i)",
            "community_definition": "union-find connected components from a sparse shared-chunk file graph",
            "completion_definition": "a file contributes only when all of its chunks become cached at the edge",
            "graph_edge_weight": "unweighted sparse connectivity induced by shared chunks",
            "high_degree_chunk_policy": "union files to high weighted-benefit hub files",
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
        "graph": graph_stats,
        "communities": community_stats,
        "summary": {
            "original_file_count": original_n,
            "clustered_file_count": len(community_file_ids),
            "cluster_rounds_executed": 0,
            "unique_chunk_count": len(uni_size),
            "selected_community_count": 0,
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
