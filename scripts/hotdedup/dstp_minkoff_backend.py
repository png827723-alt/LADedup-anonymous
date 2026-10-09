#!/usr/bin/env python3
"""
DSTP with a Minkoff-thesis k-MST backend.

What this implements
--------------------
This program keeps the outer DSTP flow from the HotDedup paper:
  1) build the full delta-similarity graph G = dupGraph(F)
  2) transform G -> G0 by splitting each file vertex i into i' and i''
  3) apply Minkoff's positive-prize scaling
  4) solve a sequence of quota problems under bisection on Q

For the *inner* quota/k-MST routine, this file follows Maria Minkoff's thesis
("The Prize Collecting Steiner Tree Problem", MIT 2000) Section 4.1.1:
  - a quota problem with positive integer prizes can be converted to k-MST by
    replacing each prize-pi vertex with a zero-cost star of pi unit-prize nodes
  - the thesis also observes that, for Garg-style k-MST algorithms, those
    artificial leaves can be kept conceptually collapsed because they would be
    immediately absorbed into the star center by zero-cost edges.

This implementation uses that collapsed-star viewpoint directly.
Concretely, instead of explicitly materializing lambda_hat_s unit-prize leaves,
we solve the mathematically equivalent collapsed quota problem on G0 exactly.
That gives a runnable reference backend that is faithful to the thesis's
reduction, while avoiding pseudo-polynomial blow-up from explicit star graphs.

Important boundary
------------------
This is NOT a line-by-line implementation of Garg's 3-approximation algorithm.
Minkoff's thesis proves that Garg's algorithm can be adapted, but does not spell
out all of Garg's internal steps. So this file implements the thesis reduction
and collapsed-star idea exactly, but uses an exact exponential backend for the
collapsed k-MST/quota subproblem.

As a result:
  - no top-k sparsification / postings truncation / custom engineering heuristics
  - fully faithful outer DSTP transformation
  - exact, thesis-aligned inner reference backend
  - intended for paper examples and small instances, not large datasets
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


@dataclass
class FileObject:
    file_id: int
    name: str
    chunks: Set[str]
    chunk_sizes: Dict[str, int]
    prize: int

    @property
    def mu(self) -> int:
        return sum(self.chunk_sizes[h] for h in self.chunks)


@dataclass
class DeltaGraph:
    files: List[FileObject]
    vertex_costs: Dict[int, int]
    vertex_prizes: Dict[int, int]
    edge_costs: Dict[Tuple[int, int], int]


@dataclass
class G0Graph:
    nodes: List[str]
    prizes: Dict[str, int]
    edges: Dict[Tuple[str, str], int]
    attachment_of_file: Dict[int, Tuple[str, str]]


@dataclass
class TreeSolution:
    nodes: Set[str]
    edges: List[Tuple[str, str, int]]
    total_cost: int
    total_prize: int


@dataclass
class InputSummary:
    total_size: int
    unique_chunk_count: int
    unique_chunk_bytes: int


def canonical_edge(u: str, v: str) -> Tuple[str, str]:
    return (u, v) if u < v else (v, u)


def sha1_hex(data: bytes) -> str:
    h = hashlib.sha1()
    h.update(data)
    return h.hexdigest()


def iter_files(root: Path, include_exts: Optional[Set[str]]) -> Iterable[Path]:
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        if include_exts is not None and p.suffix.lower() not in include_exts:
            continue
        yield p


def chunk_file_as_set(path: Path, chunk_size: int) -> Tuple[Set[str], Dict[str, int]]:
    chunks: Set[str] = set()
    sizes: Dict[str, int] = {}
    with path.open("rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            hh = sha1_hex(block)
            chunks.add(hh)
            sizes[hh] = len(block)
    return chunks, sizes


def load_rates(rate_file: Optional[Path], lambda_scale: int) -> Dict[str, int]:
    if rate_file is None:
        return {}
    raw = json.loads(rate_file.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("rate file must be a JSON object: {filename_or_relpath: rate}")
    out: Dict[str, int] = {}
    for k, v in raw.items():
        if isinstance(v, int):
            out[k] = v
        elif isinstance(v, float):
            out[k] = math.ceil(v * lambda_scale)
        else:
            raise ValueError(f"invalid rate for {k!r}: {v!r}")
        if out[k] < 0:
            raise ValueError(f"negative rate for {k!r}")
    return out


def build_file_objects_from_directory(root: Path, chunk_size: int, include_exts: Optional[Set[str]],
                                      rate_file: Optional[Path], lambda_scale: int) -> List[FileObject]:
    rates = load_rates(rate_file, lambda_scale)
    files: List[FileObject] = []
    for idx, p in enumerate(iter_files(root, include_exts), start=1):
        chunks, sizes = chunk_file_as_set(p, chunk_size)
        rel = str(p.relative_to(root)).replace("\\", "/")
        prize = rates.get(rel, rates.get(p.name, 1))
        files.append(FileObject(idx, rel, chunks, sizes, prize))
    if not files:
        raise ValueError("no input files found")
    return files


def summarize_input_files(files: Sequence[FileObject]) -> InputSummary:
    total_size = 0
    global_sizes: Dict[str, int] = {}
    for f in files:
        total_size += sum(f.chunk_sizes[h] for h in f.chunks)
        for h in f.chunks:
            sz = f.chunk_sizes[h]
            if h in global_sizes and global_sizes[h] != sz:
                raise ValueError(f"inconsistent chunk size for fingerprint {h}: {global_sizes[h]} vs {sz}")
            global_sizes[h] = sz
    return InputSummary(
        total_size=total_size,
        unique_chunk_count=len(global_sizes),
        unique_chunk_bytes=sum(global_sizes.values()),
    )


def prize_from_json_entry(entry: Dict[str, Any], rel_name: str, rates: Dict[str, int], lambda_scale: int) -> int:
    if rel_name in rates:
        return rates[rel_name]
    base_name = Path(rel_name).name
    if base_name in rates:
        return rates[base_name]

    if "prize" in entry:
        raw = entry["prize"]
        if isinstance(raw, bool):
            raise ValueError(f"invalid prize for {rel_name!r}: {raw!r}")
        if isinstance(raw, int):
            return max(0, raw)
        if isinstance(raw, float):
            return max(0, math.ceil(raw * lambda_scale))
        raise ValueError(f"invalid prize for {rel_name!r}: {raw!r}")

    if "heat" in entry:
        raw = entry["heat"]
        if isinstance(raw, bool):
            raise ValueError(f"invalid heat for {rel_name!r}: {raw!r}")
        if isinstance(raw, int):
            return max(0, raw)
        if isinstance(raw, float):
            return max(0, math.ceil(raw * lambda_scale))
        raise ValueError(f"invalid heat for {rel_name!r}: {raw!r}")

    return 1


def build_file_objects_from_fixed_json(input_json: Path, rate_file: Optional[Path], lambda_scale: int) -> List[FileObject]:
    raw = json.loads(input_json.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("input json must be an object")

    format_version = str(raw.get("format_version", "")).strip()
    chunking_method = str(raw.get("chunking_method", "")).strip().lower()
    if format_version not in {"dstp_fixed_go_v1", "fixed_go_v1"}:
        raise ValueError(
            "unsupported json format_version for dstp_minkoff_backend.py; "
            "expected dstp_fixed_go_v1 or fixed_go_v1"
        )
    if chunking_method and chunking_method != "fixed":
        raise ValueError(f"unsupported chunking_method for dstp_minkoff_backend.py: {chunking_method!r}")

    uni_fp_raw = raw.get("uni_fingerprint")
    files_raw = raw.get("files")
    if not isinstance(uni_fp_raw, list) or not isinstance(files_raw, list):
        raise ValueError("input json missing required list fields: uni_fingerprint, files")

    uni_fp = [str(v) for v in uni_fp_raw]
    rates = load_rates(rate_file, lambda_scale)
    files: List[FileObject] = []

    for idx, entry in enumerate(files_raw, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"files[{idx - 1}] must be an object")

        rel_name = str(entry.get("path") or entry.get("name") or "").strip().lstrip("/")
        if not rel_name:
            raise ValueError(f"files[{idx - 1}] missing path/name")

        chunk_ids_raw = entry.get("chunk_ids")
        chunk_sizes_raw = entry.get("chunk_sizes")
        if not isinstance(chunk_ids_raw, list) or not isinstance(chunk_sizes_raw, list):
            raise ValueError(f"files[{idx - 1}] missing chunk_ids/chunk_sizes arrays")
        if len(chunk_ids_raw) != len(chunk_sizes_raw):
            raise ValueError(f"files[{idx - 1}] has mismatched chunk_ids/chunk_sizes length")

        chunks: Set[str] = set()
        chunk_sizes: Dict[str, int] = {}
        for pos, (cid_raw, sz_raw) in enumerate(zip(chunk_ids_raw, chunk_sizes_raw), start=1):
            cid = int(cid_raw)
            sz = int(sz_raw)
            if not (1 <= cid <= len(uni_fp)):
                raise ValueError(f"files[{idx - 1}] invalid chunk id at position {pos}: {cid}")
            if sz < 0:
                raise ValueError(f"files[{idx - 1}] invalid chunk size at position {pos}: {sz}")
            fp = uni_fp[cid - 1]
            if fp in chunk_sizes and chunk_sizes[fp] != sz:
                raise ValueError(
                    f"files[{idx - 1}] fingerprint {fp} has inconsistent sizes "
                    f"{chunk_sizes[fp]} vs {sz}"
                )
            chunks.add(fp)
            chunk_sizes[fp] = sz

        prize = prize_from_json_entry(entry, rel_name, rates, lambda_scale)
        files.append(FileObject(idx, rel_name, chunks, chunk_sizes, prize))

    if not files:
        raise ValueError("no input files found in json")
    return files


def load_input_summary_from_fixed_json(input_json: Path) -> InputSummary:
    raw = json.loads(input_json.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("input json must be an object")
    total_size = int(raw.get("total_size", 0))
    uni_size_raw = raw.get("uni_size")
    if not isinstance(uni_size_raw, list):
        raise ValueError("input json missing required list field: uni_size")
    unique_sizes = [int(v) for v in uni_size_raw]
    return InputSummary(
        total_size=total_size,
        unique_chunk_count=len(unique_sizes),
        unique_chunk_bytes=sum(unique_sizes),
    )


def build_demo_files() -> List[FileObject]:
    specs = [
        ("F1", ["A", "B", "C"], 2),
        ("F2", ["A", "B", "C", "F"], 1),
        ("F3", ["A", "F"], 1),
        ("F4", ["B", "C", "G"], 4),
        ("F5", ["H", "G", "I"], 5),
    ]
    files: List[FileObject] = []
    for idx, (name, chunks, prize) in enumerate(specs, start=1):
        chset = set(chunks)
        sizes = {c: 1 for c in chset}
        files.append(FileObject(idx, name, chset, sizes, prize))
    return files


def overlap_bytes(a: FileObject, b: FileObject) -> int:
    common = a.chunks & b.chunks
    if not common:
        return 0
    return sum(a.chunk_sizes[h] for h in common)


def build_delta_similarity_graph(files: List[FileObject]) -> DeltaGraph:
    vertex_costs = {f.file_id: f.mu for f in files}
    vertex_prizes = {f.file_id: f.prize for f in files}
    edge_costs: Dict[Tuple[int, int], int] = {}
    for i in range(len(files)):
        for j in range(i + 1, len(files)):
            ov = overlap_bytes(files[i], files[j])
            if ov > 0:
                edge_costs[(files[i].file_id, files[j].file_id)] = -ov
    return DeltaGraph(files, vertex_costs, vertex_prizes, edge_costs)


def build_g0(graph: DeltaGraph) -> G0Graph:
    nodes: List[str] = []
    prizes: Dict[str, int] = {}
    edges: Dict[Tuple[str, str], int] = {}
    attachment_of_file: Dict[int, Tuple[str, str]] = {}

    for f in graph.files:
        ip = f"{f.file_id}p"
        ipp = f"{f.file_id}pp"
        nodes.extend([ip, ipp])
        prizes[ip] = 0
        prizes[ipp] = graph.vertex_prizes[f.file_id]
        edges[canonical_edge(ip, ipp)] = graph.vertex_costs[f.file_id]
        attachment_of_file[f.file_id] = (ip, ipp)

    for (i, j), c in graph.edge_costs.items():
        ip, _ = attachment_of_file[i]
        jp, _ = attachment_of_file[j]
        edges[canonical_edge(ip, jp)] = c

    return G0Graph(nodes, prizes, edges, attachment_of_file)


# -------------------------------
# Minkoff thesis reduction pieces
# -------------------------------

def minkoff_scaled_prizes(g0: G0Graph) -> Tuple[Dict[str, int], int]:
    """
    Thesis Lemma 4.1 scaling:
        pi_hat(v) = 2|V| pi(v) + 1
        Q_hat    = 2|V| Q
    Here |V| means the node count of the current quota instance graph.
    """
    n = len(g0.nodes)
    scaled = {s: 2 * n * g0.prizes[s] + 1 for s in g0.nodes}
    return scaled, n


def materialized_star_node_count(scaled_prizes: Dict[str, int]) -> int:
    """How large the explicit thesis k-MST star graph would be."""
    return sum(scaled_prizes.values())


# -------------------------------------------
# Exact collapsed k-MST / quota reference code
# -------------------------------------------

def induced_edges(nodes_subset: Set[str], all_edges: Dict[Tuple[str, str], int]) -> List[Tuple[str, str, int]]:
    out: List[Tuple[str, str, int]] = []
    for (u, v), w in all_edges.items():
        if u in nodes_subset and v in nodes_subset:
            out.append((u, v, w))
    return out


def is_connected(nodes_subset: Set[str], edges_subset: List[Tuple[str, str, int]]) -> bool:
    if not nodes_subset:
        return False
    adj: Dict[str, List[str]] = {u: [] for u in nodes_subset}
    for u, v, _ in edges_subset:
        adj[u].append(v)
        adj[v].append(u)
    start = next(iter(nodes_subset))
    seen = {start}
    stack = [start]
    while stack:
        u = stack.pop()
        for v in adj[u]:
            if v not in seen:
                seen.add(v)
                stack.append(v)
    return seen == nodes_subset


def mst_cost_over_subset(nodes_subset: Set[str], edges_subset: List[Tuple[str, str, int]]) -> Optional[Tuple[int, List[Tuple[str, str, int]]]]:
    if len(nodes_subset) == 1:
        return 0, []

    parent = {u: u for u in nodes_subset}
    rank = {u: 0 for u in nodes_subset}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> bool:
        ra, rb = find(a), find(b)
        if ra == rb:
            return False
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1
        return True

    total = 0
    tree: List[Tuple[str, str, int]] = []
    for u, v, w in sorted(edges_subset, key=lambda x: x[2]):
        if union(u, v):
            tree.append((u, v, w))
            total += w
            if len(tree) == len(nodes_subset) - 1:
                return total, tree
    return None


def exact_collapsed_minkoff_kmst(g0: G0Graph, scaled_prizes: Dict[str, int], q_hat: int) -> Optional[TreeSolution]:
    """
    Exact solver for the thesis-reduced quota/k-MST instance, using a *file-cluster*
    collapsed view that is consistent with HotDedup's G -> G0 construction.

    Section 4.1.1 of the thesis converts a positive-prize quota instance into k-MST by
    replacing a node of prize pi_hat with a zero-cost star of pi_hat unit-prize vertices.
    The same section points out that these artificial leaves may be kept conceptually
    collapsed because zero-cost leaves would be absorbed into their center immediately.

    In HotDedup's G -> G0 construction, however, every original file i becomes a pair
    (i', i'') where:
      - i'' carries the file prize
      - edge (i', i'') carries the file storage cost mu_i
      - edges between i' and j' carry pairwise overlap costs

    For the partitioning semantics to remain valid, we must treat each file pair
    (i', i'') as a *closed cluster*: either the file is selected and both nodes are present,
    or it is not selected and neither node may participate. Allowing i' alone would create
    a Steiner connector that exploits overlap edges without paying mu_i, which would no
    longer correspond to a valid selected-file tree in the original delta-similarity graph.

    So this routine performs exact search over closed file-clusters. That keeps the solver
    faithful both to Minkoff's collapsed-star idea and to HotDedup's file-selection meaning.
    """
    best: Optional[TreeSolution] = None
    file_ids = sorted(g0.attachment_of_file.keys())

    for r in range(1, len(file_ids) + 1):
        for chosen_files in itertools.combinations(file_ids, r):
            subset: Set[str] = set()
            for fid in chosen_files:
                ip, ipp = g0.attachment_of_file[fid]
                subset.add(ip)
                subset.add(ipp)
            prize = sum(scaled_prizes[x] for x in subset)
            if prize < q_hat:
                continue
            e_subset = induced_edges(subset, g0.edges)
            if not is_connected(subset, e_subset):
                continue
            mst = mst_cost_over_subset(subset, e_subset)
            if mst is None:
                continue
            cost, tree_edges = mst
            sol = TreeSolution(nodes=subset, edges=tree_edges, total_cost=cost, total_prize=prize)
            if best is None or sol.total_cost < best.total_cost or (
                sol.total_cost == best.total_cost and sol.total_prize > best.total_prize
            ):
                best = sol
    return best


def selected_files_from_g0_tree(g0: G0Graph, tree_nodes: Set[str]) -> Set[int]:
    selected: Set[int] = set()
    for fid, (_ip, ipp) in g0.attachment_of_file.items():
        if ipp in tree_nodes:
            selected.add(fid)
    return selected


def actual_dedup_usage(files_by_id: Dict[int, FileObject], selected_file_ids: Set[int]) -> int:
    union_chunks: Set[str] = set()
    chunk_sizes: Dict[str, int] = {}
    for fid in selected_file_ids:
        f = files_by_id[fid]
        union_chunks |= f.chunks
        chunk_sizes.update(f.chunk_sizes)
    return sum(chunk_sizes[h] for h in union_chunks)


def spread_selected_files_across_edges(
    files_by_id: Dict[int, FileObject],
    selected_file_ids: Sequence[int],
    budget_bytes: int,
    server_num: int,
) -> Dict[str, object]:
    if server_num <= 0:
        raise ValueError("server_num must be positive")
    if budget_bytes < 0:
        raise ValueError("budget_bytes must be non-negative")

    base_cap = budget_bytes // server_num
    extra = budget_bytes % server_num
    per_server_capacity = [base_cap + (1 if i < extra else 0) for i in range(server_num)]
    hash2edge_node_id: Dict[str, int] = {}
    file_start_sid = 0

    for fid in selected_file_ids:
        f = files_by_id[fid]
        group = [h for h in sorted(f.chunks) if h not in hash2edge_node_id]
        if not group:
            file_start_sid = (file_start_sid + 1) % server_num
            continue

        preferred_sid = file_start_sid % server_num
        placed_in_group = 0
        for offset, chunk_hash in enumerate(group):
            sz = int(f.chunk_sizes[chunk_hash])
            assigned_sid = -1
            for probe in range(server_num):
                sid = (preferred_sid + offset + probe) % server_num
                if per_server_capacity[sid] >= sz:
                    assigned_sid = sid
                    break
            if assigned_sid >= 0:
                hash2edge_node_id[chunk_hash] = assigned_sid + 1
                per_server_capacity[assigned_sid] -= sz
                placed_in_group += 1

        # Rotate the leading node per selected file so successive files are
        # naturally spread across multiple edges instead of piling onto edge1.
        file_start_sid = (file_start_sid + max(1, placed_in_group)) % server_num

    remaining_bytes = [int(v) for v in per_server_capacity]
    mapped_bytes = 0
    for chunk_hash in hash2edge_node_id:
        for fid in selected_file_ids:
            f = files_by_id[fid]
            if chunk_hash in f.chunk_sizes:
                mapped_bytes += int(f.chunk_sizes[chunk_hash])
                break
    return {
        "hash2edge_node_id": hash2edge_node_id,
        "placement": {
            "strategy": "spread_by_file_across_edges",
            "server_num": int(server_num),
            "mapped_hash_count": len(hash2edge_node_id),
            "mapped_unique_bytes": mapped_bytes,
            "per_server_remaining_bytes": remaining_bytes,
        },
    }


def build_dstp_result(
    files: Sequence[FileObject],
    budget_bytes: int,
    server_num: int,
    input_summary: InputSummary,
    core_result: Dict[str, object],
) -> Dict[str, object]:
    out = dict(core_result)
    files_by_id = {f.file_id: f for f in files}
    selected_ids = [int(v) for v in core_result.get("selected_file_ids", [])]
    placement = spread_selected_files_across_edges(files_by_id, selected_ids, budget_bytes, server_num)
    out.update(placement)
    out["format"] = "dstp_minkoff_fixed_input_v1"
    out["params"] = {
        "budget_bytes": int(budget_bytes),
        "server_num": int(server_num),
    }
    out["summary"] = {
        "original_file_count": len(files),
        "clustered_file_count": len(files),
        "selected_file_count": len(selected_ids),
        "unique_chunk_count": int(input_summary.unique_chunk_count),
        "total_size": int(input_summary.total_size),
        "total_unique_bytes": int(input_summary.unique_chunk_bytes),
        "total_theoretical_gain": None,
    }
    out["protection"] = {}
    out["theory"] = {"gain_summary": {}}
    return out


def run_dstp_minkoff(files: List[FileObject], budget_bytes: int, epsilon_cost: int = 0) -> Dict[str, object]:
    graph = build_delta_similarity_graph(files)
    g0 = build_g0(graph)
    scaled_prizes, v0_size = minkoff_scaled_prizes(g0)
    total_original_prize = sum(f.prize for f in files)
    files_by_id = {f.file_id: f for f in files}

    q_lo = 0
    q_hi = total_original_prize
    best_feasible: Optional[Tuple[int, TreeSolution]] = None
    history: List[Dict[str, object]] = []

    while q_lo <= q_hi:
        q_mid = (q_lo + q_hi) // 2
        q_hat = 2 * q_mid * v0_size

        if q_hat == 0:
            empty = TreeSolution(nodes=set(), edges=[], total_cost=0, total_prize=0)
            best_feasible = (q_mid, empty)
            history.append({"Q": q_mid, "Q_hat": q_hat, "S_est": 0, "status": "feasible-empty"})
            q_lo = q_mid + 1
            continue

        sol = exact_collapsed_minkoff_kmst(g0, scaled_prizes, q_hat)
        if sol is None:
            history.append({"Q": q_mid, "Q_hat": q_hat, "status": "infeasible-quota"})
            q_hi = q_mid - 1
            continue

        s_est = sol.total_cost
        selected_ids = selected_files_from_g0_tree(g0, sol.nodes)
        d_actual = actual_dedup_usage(files_by_id, selected_ids) if selected_ids else 0
        history.append({
            "Q": q_mid,
            "Q_hat": q_hat,
            "S_est": s_est,
            "D_actual": d_actual,
            "selected_ids": sorted(selected_ids),
            "status": "feasible" if s_est <= budget_bytes else "too-high",
        })

        if s_est <= budget_bytes + epsilon_cost:
            best_feasible = (q_mid, sol)
            q_lo = q_mid + 1
        else:
            q_hi = q_mid - 1

    if best_feasible is None:
        return {
            "status": "no-feasible-tree",
            "budget": budget_bytes,
            "graph_stats": {
                "m_files": len(files),
                "|V|": len(graph.files),
                "|E|": len(graph.edge_costs),
                "|V0|": len(g0.nodes),
                "|E0|": len(g0.edges),
                "explicit_star_graph_nodes_if_materialized": materialized_star_node_count(scaled_prizes),
            },
            "history": history,
        }

    best_q, best_sol = best_feasible
    best_selected = selected_files_from_g0_tree(g0, best_sol.nodes)
    selected_names = [files_by_id[i].name for i in sorted(best_selected)]
    actual_usage = actual_dedup_usage(files_by_id, best_selected) if best_selected else 0
    actual_prize = sum(files_by_id[i].prize for i in best_selected)

    return {
        "status": "ok",
        "budget": budget_bytes,
        "best_original_quota_Q": best_q,
        "selected_file_ids": sorted(best_selected),
        "selected_file_names": selected_names,
        "edge_service_rate": actual_prize,
        "estimated_storage_S": best_sol.total_cost,
        "actual_dedup_storage_D": actual_usage,
        "tree_nodes_g0": sorted(best_sol.nodes),
        "tree_edges_g0": [{"u": u, "v": v, "cost": w} for (u, v, w) in best_sol.edges],
        "graph_stats": {
            "m_files": len(files),
            "|V|": len(graph.files),
            "|E|": len(graph.edge_costs),
            "|V0|": len(g0.nodes),
            "|E0|": len(g0.edges),
            "explicit_star_graph_nodes_if_materialized": materialized_star_node_count(scaled_prizes),
        },
        "history": history,
        "note": (
            "HotDedup outer DSTP + Minkoff Section 4.1.1 quota-to-kMST reduction, "
            "implemented through the collapsed-star exact backend rather than an explicit "
            "materialized unit-prize star graph or a line-by-line Garg 3-approx implementation."
        ),
    }


def parse_exts(s: Optional[str]) -> Optional[Set[str]]:
    if not s:
        return None
    parts = [x.strip().lower() for x in s.split(",") if x.strip()]
    if not parts:
        return None
    out = set()
    for p in parts:
        out.add(p if p.startswith(".") else "." + p)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="DSTP with a Minkoff-thesis k-MST backend (collapsed exact reference implementation).")
    ap.add_argument("--input-json", type=str, help="fixed-chunk metadata json generated for dstp input")
    ap.add_argument("--root", type=str, help="input root directory")
    ap.add_argument("--output-json", type=str, default=None, help="write full result json to this path")
    ap.add_argument("--budget-bytes", type=int, help="edge storage budget B", default=None)
    ap.add_argument("--capacity-ratio", type=float, default=None, help="derive budget as total_size * capacity_ratio")
    ap.add_argument("--chunk-size", type=int, default=4096, help="fixed chunk size in bytes")
    ap.add_argument("--include-exts", type=str, default=None, help="comma-separated extensions, e.g. .csv,.txt")
    ap.add_argument("--rate-file", type=str, default=None, help="JSON file: {relative_path_or_filename: rate}")
    ap.add_argument("--lambda-scale", type=int, default=1, help="scale factor used when rates are floats")
    ap.add_argument("--server-num", type=int, default=10, help="number of edge servers for output placement spreading")
    ap.add_argument("--summary-json", type=str, default=None, help="write summary JSON to this path")
    ap.add_argument("--demo", action="store_true", help="run the Figure-1 style toy example")
    args = ap.parse_args()

    if args.demo:
        files = build_demo_files()
        input_summary = summarize_input_files(files)
        budget = 4 if args.budget_bytes is None else args.budget_bytes
    else:
        if bool(args.root) == bool(args.input_json):
            raise SystemExit("exactly one of --root or --input-json is required unless --demo is used")
        if args.input_json:
            input_json = Path(args.input_json)
            if not input_json.exists():
                raise SystemExit(f"input json does not exist: {input_json}")
            files = build_file_objects_from_fixed_json(
                input_json=input_json,
                rate_file=Path(args.rate_file) if args.rate_file else None,
                lambda_scale=args.lambda_scale,
            )
            input_summary = load_input_summary_from_fixed_json(input_json)
        else:
            root = Path(args.root)
            if not root.exists():
                raise SystemExit(f"input root does not exist: {root}")
            files = build_file_objects_from_directory(
                root=root,
                chunk_size=args.chunk_size,
                include_exts=parse_exts(args.include_exts),
                rate_file=Path(args.rate_file) if args.rate_file else None,
                lambda_scale=args.lambda_scale,
            )
            input_summary = summarize_input_files(files)

        if args.capacity_ratio is not None:
            if not (0.0 < float(args.capacity_ratio) <= 1.0):
                raise SystemExit("--capacity-ratio must be in (0, 1]")
            budget = int(round(float(input_summary.total_size) * float(args.capacity_ratio)))
        elif args.budget_bytes is not None:
            budget = int(args.budget_bytes)
        else:
            raise SystemExit("--budget-bytes or --capacity-ratio is required unless --demo is used")

    core_result = run_dstp_minkoff(files, budget)
    result = build_dstp_result(files, budget, int(args.server_num), input_summary, core_result)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if args.summary_json:
        Path(args.summary_json).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
