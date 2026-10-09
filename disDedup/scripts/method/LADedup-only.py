#!/usr/bin/env python3
"""
Run MEAN clustering + Case1 selection with a parallel-edge theoretical score.

For each original file i, define:

  T_cloud(i) = ceil(chunks_i / batch_size) * cloud_rtt + bytes_i * 8 / cloud_bw
  T_edge(i)  = ceil(chunks_i / (batch_size * p_i)) * edge_rtt + (bytes_i / p_i) * 8 / edge_bw
  benefit_i  = max(0, T_baseline(i) - T_edge(i))
  score_i    = heat_i * benefit_i

Case1 selection then uses:

  priority(cluster) = sum(score_i for i in cluster) / delta_unique_bytes(cluster)

Here p_i is the effective number of edge nodes that can serve file i in
parallel. This v3 model keeps the original clustering / Case1 flow, but
replaces the restore-gain model so full-edge restore can benefit from
concurrent pulls from multiple edge nodes.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
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


def rough_parallel_full_edge_time(
    file_bytes: int,
    chunk_count: int,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    parallel_nodes: int,
) -> float:
    if file_bytes <= 0 or chunk_count <= 0:
        return 0.0
    eff_nodes = max(1, min(int(parallel_nodes), int(chunk_count)))
    batches = int(math.ceil(float(chunk_count) / float(max(1, batch_size) * eff_nodes)))
    transfer = (float(file_bytes) / float(eff_nodes)) * 8.0 / max(1.0, edge_bw_bps)
    return batches * edge_rtt_s + transfer


def rough_parallel_hybrid_restore_time(
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
    if file_bytes <= 0 or chunk_count <= 0:
        return 0.0

    r = min(1.0, max(0.0, float(edge_ratio)))
    edge_chunks = int(math.ceil(chunk_count * r))
    cloud_chunks = max(0, chunk_count - edge_chunks)
    edge_bytes = float(file_bytes) * r
    cloud_bytes = max(0.0, float(file_bytes) - edge_bytes)

    edge_t = rough_parallel_full_edge_time(
        file_bytes=int(edge_bytes),
        chunk_count=edge_chunks,
        batch_size=batch_size,
        edge_bw_bps=edge_bw_bps,
        edge_rtt_s=edge_rtt_s,
        parallel_nodes=parallel_nodes,
    )
    cloud_t = base_mod.full_restore_time(
        int(cloud_bytes),
        cloud_chunks,
        batch_size,
        cloud_bw_bps,
        cloud_rtt_s,
    )
    return max(edge_t, cloud_t)


def rough_parallel_baseline_time(
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
    if baseline_edge_ratio <= 0.0:
        return base_mod.full_restore_time(file_bytes, chunk_count, batch_size, cloud_bw_bps, cloud_rtt_s)
    if baseline_edge_ratio >= 1.0:
        return rough_parallel_full_edge_time(
            file_bytes=file_bytes,
            chunk_count=chunk_count,
            batch_size=batch_size,
            edge_bw_bps=edge_bw_bps,
            edge_rtt_s=edge_rtt_s,
            parallel_nodes=parallel_nodes,
        )
    return rough_parallel_hybrid_restore_time(
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


def rough_parallel_file_gain(
    file_bytes: int,
    chunk_count: int,
    batch_size: int,
    edge_bw_bps: float,
    edge_rtt_s: float,
    cloud_bw_bps: float,
    cloud_rtt_s: float,
    baseline_edge_ratio: float,
    parallel_nodes: int,
) -> tuple[float, float, float]:
    t_base = rough_parallel_baseline_time(
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
    t_edge = rough_parallel_full_edge_time(
        file_bytes=file_bytes,
        chunk_count=chunk_count,
        batch_size=batch_size,
        edge_bw_bps=edge_bw_bps,
        edge_rtt_s=edge_rtt_s,
        parallel_nodes=parallel_nodes,
    )
    return max(0.0, t_base - t_edge), t_base, t_edge


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run parallel-edge theoretical-gain MEAN cluster + Case1 from mean_go_v1 JSON"
    )
    parser.add_argument("--input-json", default="datasets/fileInfo-src.json", help="input metadata json path")
    parser.add_argument(
        "--output-json",
        default="results/case1_cluster_theoretical_weighted_version3.json",
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
        "--baseline-edge-ratio",
        type=float,
        default=0.0,
        help="baseline edge ratio in [0,1]; 0 means full-cloud baseline",
    )
    args = parser.parse_args()

    input_path = Path(args.input_json)
    env_path = Path(args.env_file)
    if not input_path.exists():
        raise FileNotFoundError(f"input json not found: {input_path}")
    if not env_path.exists():
        raise FileNotFoundError(f"env file not found: {env_path}")

    env = base_mod.parse_env_file(env_path)
    edge_bw_bps, edge_rtt_s = base_mod.effective_link(
        base_mod.parse_rate_bps(env.get("M2E_RATE", "1500mbit")),
        base_mod.parse_rate_bps(env.get("EDGE_NET_RATE", "1500mbit")),
        base_mod.parse_seconds(env.get("M2E_DELAY", "1ms")),
        base_mod.parse_seconds(env.get("EDGE_NET_DELAY", "1ms")),
    )
    cloud_bw_bps, cloud_rtt_s = base_mod.effective_link(
        base_mod.parse_rate_bps(env.get("M2C_RATE", "100mbit")),
        base_mod.parse_rate_bps(env.get("CLOUD_NET_RATE", "100mbit")),
        base_mod.parse_seconds(env.get("M2C_DELAY", "25ms")),
        base_mod.parse_seconds(env.get("CLOUD_NET_DELAY", "25ms")),
    )

    files, uni_size, uni_fingerprint, total_size = base_mod.load_mean_go_v1(input_path, max_files=args.max_files)
    original_n = len(files)
    parallel_nodes = int(args.parallel_edge_nodes) if int(args.parallel_edge_nodes) > 0 else int(args.server_num)
    parallel_nodes = max(1, min(int(args.server_num), parallel_nodes))

    print(f"Loaded {original_n} files from {input_path}")
    print(
        "Parallel restore model:",
        f"edge_bw={edge_bw_bps:.0f}bps",
        f"edge_rtt={edge_rtt_s:.6f}s",
        f"cloud_bw={cloud_bw_bps:.0f}bps",
        f"cloud_rtt={cloud_rtt_s:.6f}s",
        f"batch={args.restore_batch_size}",
        f"baseline_edge_ratio={args.baseline_edge_ratio:.3f}",
        f"parallel_nodes={parallel_nodes}",
    )

    original_scores: list[float] = []
    gains_only: list[float] = []
    tbase_values: list[float] = []
    tedge_values: list[float] = []
    for f in files:
        benefit, t_base, t_edge = rough_parallel_file_gain(
            file_bytes=int(f.raw_size),
            chunk_count=len(f.chunks),
            batch_size=max(1, int(args.restore_batch_size)),
            edge_bw_bps=edge_bw_bps,
            edge_rtt_s=edge_rtt_s,
            cloud_bw_bps=cloud_bw_bps,
            cloud_rtt_s=cloud_rtt_s,
            baseline_edge_ratio=float(args.baseline_edge_ratio),
            parallel_nodes=parallel_nodes,
        )
        gains_only.append(benefit)
        tbase_values.append(t_base)
        tedge_values.append(t_edge)
        original_scores.append(float(f.heat) * benefit)

    base_mod.base.pair_dist = base_mod.make_theoretical_pair_dist(original_scores)
    clustered, rounds = base_mod.base.cluster_files(
        files,
        k=args.k,
        alpha=args.alpha,
        workers=max(1, args.workers),
        show_progress=bool(args.progress),
    )
    print(f"Clustering done: rounds={rounds}, files {original_n} -> {len(clustered)}")

    total_capacity = float(total_size) * float(args.capacity_ratio)
    case1 = base_mod.run_case1_theoretical(
        clustered,
        uni_size,
        total_capacity,
        show_progress=bool(args.progress),
        original_scores=original_scores,
    )
    placement = base_mod.output_result_uni_matlab_style(
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
        "format": "mean_py_case1_cluster_theoretical_weighted_parallel_v3",
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
            "baseline_edge_ratio": float(args.baseline_edge_ratio),
        },
        "theory": {
            "edge_bandwidth_bps": edge_bw_bps,
            "edge_rtt_seconds": edge_rtt_s,
            "cloud_bandwidth_bps": cloud_bw_bps,
            "cloud_rtt_seconds": cloud_rtt_s,
            "parallel_edge_nodes": parallel_nodes,
            "benefit_definition": "benefit_i = max(0, T_baseline(i) - T_edge_parallel(i))",
            "t_cloud_definition": "T_cloud(i) = ceil(chunks_i / batch_size) * cloud_rtt + bytes_i * 8 / cloud_bw",
            "t_edge_definition": "T_edge_parallel(i) = ceil(chunks_i / (batch_size * p_i)) * edge_rtt + (bytes_i / p_i) * 8 / edge_bw",
            "t_baseline_definition": "T_baseline(i) uses the same parallel-edge model on the edge-served part and full_restore_time on the cloud-served part, then takes max(edge_part, cloud_part)",
            "score_definition": "score_i = heat_i * benefit_i",
            "priority_definition": "priority(cluster) = sum(score_i)/delta_unique_bytes(cluster)",
            "gain_summary": {
                "mean_seconds": (sum(gains_only) / len(gains_only)) if gains_only else 0.0,
                "max_seconds": max(gains_only) if gains_only else 0.0,
                "min_seconds": min(gains_only) if gains_only else 0.0,
                "mean_t_baseline_seconds": (sum(tbase_values) / len(tbase_values)) if tbase_values else 0.0,
                "mean_t_edge_seconds": (sum(tedge_values) / len(tedge_values)) if tedge_values else 0.0,
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
