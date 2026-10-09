#!/usr/bin/env python3
"""
Run a shuffled-heat placement experiment suite end-to-end.

Workflow:
1. Generate shuffled fileInfo JSON variants from one source fileInfo JSON.
   The set of heat values is preserved; only the heat-to-file mapping changes.
2. For each input JSON in the suite:
   - generate placement with a selected algorithm script under scripts/case1_cluster
   - create an isolated STORE_ROOT and per-run compose env file
   - start cloud/edge/manager containers
   - optionally prepare shared cloud full files once via manager cloud-sync
   - run dedup with the generated placement
   - generate heat-weighted restore requests from the same shuffled JSON
   - run sequential restore via manager restore-batch
   - stop the containers
3. Summarize per-run metrics and arithmetic averages across all runs.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def load_env_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def write_env_file(base_env: Path, out_env: Path, overrides: Dict[str, str]) -> None:
    lines = base_env.read_text(encoding="utf-8").splitlines()
    remaining = dict(overrides)
    out_lines: List[str] = []
    for raw in lines:
        stripped = raw.strip()
        if stripped and not stripped.startswith("#") and "=" in raw:
            key, _ = raw.split("=", 1)
            key = key.strip()
            if key in remaining:
                out_lines.append(f"{key}={remaining.pop(key)}")
                continue
        out_lines.append(raw)
    for key, value in remaining.items():
        out_lines.append(f"{key}={value}")
    out_env.parent.mkdir(parents=True, exist_ok=True)
    out_env.write_text("\n".join(out_lines) + "\n", encoding="utf-8")


def run_cmd(
    cmd: List[str],
    *,
    cwd: Path,
    log_path: Path,
    extra_env: Optional[Dict[str, str]] = None,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    print(f"[run] {' '.join(cmd)}")
    with log_path.open("w", encoding="utf-8") as fh:
        fh.write(f"$ {' '.join(cmd)}\n\n")
        fh.flush()
        subprocess.run(cmd, cwd=str(cwd), env=env, stdout=fh, stderr=subprocess.STDOUT, check=True)


def write_log_message(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(message.rstrip() + "\n", encoding="utf-8")


def docker_compose_cmd(compose_file: Path, env_file: Path, *args: str) -> List[str]:
    return ["docker", "compose", "--env-file", str(env_file), "-f", str(compose_file), *args]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def stable_json_digest(obj: object) -> str:
    payload = json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(payload)


def materialize_cached_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def generate_shuffled_json(source_json: Path, out_path: Path, seed: int) -> Path:
    source = json.loads(source_json.read_text(encoding="utf-8"))
    heats = [float(item["heat"]) for item in source["files"]]
    rng = random.Random(seed)
    shuffled = heats[:]
    rng.shuffle(shuffled)
    data = copy.deepcopy(source)
    for item, heat in zip(data["files"], shuffled):
        size = sum(int(x) for x in item["chunk_sizes"])
        item["heat"] = heat
        item["heat_dsize"] = (heat / float(size)) if size > 0 else 0.0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    return out_path


def resolve_shuffled_jsons(source_json: Path, output_dir: Path, seeds: Iterable[int]) -> Dict[int, Dict[str, object]]:
    resolved: Dict[int, Dict[str, object]] = {}
    for seed in seeds:
        existing_path = source_json.with_name(f"{source_json.stem}-shuffle-seed{seed:04d}.json")
        if existing_path.exists():
            resolved[seed] = {"path": existing_path, "origin": "existing"}
            continue

        generated_path = output_dir / f"{source_json.stem}-shuffle-seed{seed:04d}.json"
        if generated_path.exists():
            resolved[seed] = {"path": generated_path, "origin": "generated_cached"}
            continue

        resolved[seed] = {
            "path": generate_shuffled_json(source_json, generated_path, seed),
            "origin": "generated_new",
        }
    return resolved


def path_has_entries(path: Path) -> bool:
    return path.exists() and any(path.iterdir())


def to_container_restore_request_path(host_request_path: Path, store_root: Path) -> str:
    rel = host_request_path.resolve().relative_to((store_root / "manager").resolve())
    return f"/data/{rel.as_posix()}"


def read_log_tail(path: Path, max_lines: int = 40) -> List[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-max_lines:]


def summarize_failure(log_path: Path, exc: BaseException) -> str:
    tail = read_log_tail(log_path)
    if tail:
        keywords = ("error", "failed", "traceback", "exception", "panic", "not found", "no such file")
        picked = [line.strip() for line in tail if any(k in line.lower() for k in keywords)]
        if picked:
            return " | ".join(picked[-5:])
        compact = [line.strip() for line in tail if line.strip()]
        if compact:
            return " | ".join(compact[-5:])
    return f"{type(exc).__name__}: {exc}"


def host_path_to_container_input_path(host_path: Path, data_root: Path) -> str:
    host_abs = host_path.resolve()
    data_root_abs = data_root.resolve()
    if host_abs == data_root_abs:
        return "/input"
    try:
        rel = host_abs.relative_to(data_root_abs)
    except ValueError as exc:
        raise ValueError(
            f"dataset root is outside DATA_ROOT: {host_abs} (DATA_ROOT={data_root_abs})"
        ) from exc
    return f"/input/{rel.as_posix()}"


def cleanup_edge_data(store_root: Path, server_num: int) -> List[str]:
    cleaned: List[str] = []
    for i in range(1, max(0, server_num) + 1):
        edge_dir = store_root / f"edge{i}"
        if not edge_dir.exists():
            continue
        keep_name = f"edge{i}.yaml"
        try:
            for child in edge_dir.iterdir():
                if child.name == keep_name:
                    continue
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        except PermissionError:
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-v",
                    f"{edge_dir}:/target",
                    "--entrypoint",
                    "sh",
                    "disdedup:latest",
                    "-lc",
                    (
                        f"for p in /target/*; do "
                        f'base="$(basename "$p")"; '
                        f'if [ "$base" != "{keep_name}" ]; then rm -rf -- "$p"; fi; '
                        f"done"
                    ),
                ],
                check=True,
            )
        cleaned.append(str(edge_dir))
    return cleaned


def script_supports_flag(script_path: Path, flag: str) -> bool:
    text = script_path.read_text(encoding="utf-8")
    quoted = f'"{flag}"'
    squoted = f"'{flag}'"
    return quoted in text or squoted in text


def build_algorithm_cmd(
    algorithm_script: Path,
    *,
    input_json: Path,
    env_file: Path,
    output_json: Path,
    capacity_ratio: float,
    protect_top_ratio: float,
    k: int,
    alpha: int,
    server_num: int,
    parallel_edge_nodes: int,
    workers: int,
    restore_batch_size: int,
    algorithm_extra_args: List[str],
) -> List[str]:
    cmd: List[str] = ["python3", str(algorithm_script)]
    if script_supports_flag(algorithm_script, "--input-json"):
        cmd.extend(["--input-json", str(input_json)])
    if script_supports_flag(algorithm_script, "--env-file"):
        cmd.extend(["--env-file", str(env_file)])
    if script_supports_flag(algorithm_script, "--output-json"):
        cmd.extend(["--output-json", str(output_json)])
    if script_supports_flag(algorithm_script, "--capacity-ratio"):
        cmd.extend(["--capacity-ratio", str(capacity_ratio)])
    if script_supports_flag(algorithm_script, "--k"):
        cmd.extend(["--k", str(k)])
    if script_supports_flag(algorithm_script, "--alpha"):
        cmd.extend(["--alpha", str(alpha)])
    if script_supports_flag(algorithm_script, "--server-num"):
        cmd.extend(["--server-num", str(server_num)])
    if script_supports_flag(algorithm_script, "--parallel-edge-nodes"):
        cmd.extend(["--parallel-edge-nodes", str(parallel_edge_nodes)])
    if script_supports_flag(algorithm_script, "--workers"):
        cmd.extend(["--workers", str(max(1, workers))])
    if script_supports_flag(algorithm_script, "--restore-batch-size"):
        cmd.extend(["--restore-batch-size", str(restore_batch_size)])
    if script_supports_flag(algorithm_script, "--protect-top-ratio"):
        cmd.extend(["--protect-top-ratio", str(protect_top_ratio)])
    if script_supports_flag(algorithm_script, "--no-progress"):
        cmd.append("--no-progress")
    cmd.extend(algorithm_extra_args)
    return cmd


def build_placement_cache_key(
    *,
    shuffled_json: Path,
    algorithm_script: Path,
    env_file: Path,
    capacity_ratio: float,
    protect_top_ratio: float,
    k: int,
    alpha: int,
    server_num: int,
    parallel_edge_nodes: int,
    workers: int,
    restore_batch_size: int,
    algorithm_extra_args: List[str],
) -> str:
    return stable_json_digest(
        {
            "kind": "placement",
            "input_json_sha256": sha256_file(shuffled_json),
            "algorithm_script_sha256": sha256_file(algorithm_script),
            "env_file_sha256": sha256_file(env_file),
            "capacity_ratio": capacity_ratio,
            "protect_top_ratio": protect_top_ratio,
            "k": k,
            "alpha": alpha,
            "server_num": server_num,
            "parallel_edge_nodes": parallel_edge_nodes,
            "workers": workers,
            "restore_batch_size": restore_batch_size,
            "algorithm_extra_args": list(algorithm_extra_args),
        }
    )


def build_request_cache_key(
    *,
    dataset_root: Path,
    shuffled_json: Path,
    request_seed: int,
    request_count: int,
    request_multiplier: int,
    dedup_mode: str,
) -> str:
    return stable_json_digest(
        {
            "kind": "restore_requests",
            "dataset_root": str(dataset_root.resolve()),
            "input_json_sha256": sha256_file(shuffled_json),
            "request_seed": request_seed,
            "request_count": request_count,
            "request_multiplier": request_multiplier,
            "dedup_mode": dedup_mode,
            "output_dir": "/output/restored/shared",
        }
    )


def extract_run_metrics(
    placement_path: Path,
    dedup_stats_path: Path,
    restore_stats_path: Path,
) -> Dict[str, Optional[float]]:
    placement = json.loads(placement_path.read_text(encoding="utf-8"))
    dedup = json.loads(dedup_stats_path.read_text(encoding="utf-8"))
    restore = json.loads(restore_stats_path.read_text(encoding="utf-8"))

    dedup_avg = dedup.get("overall_average", {})
    restore_avg = restore.get("overall_average", {})
    protection = placement.get("protection", {})
    gain_summary = placement.get("theory", {}).get("gain_summary", {})
    summary = placement.get("summary", {})
    hash_map = placement.get("hash2edge_node_id", {})

    return {
        "placement_clustered_file_count": float(summary["clustered_file_count"]) if "clustered_file_count" in summary else None,
        "placement_unique_chunk_count": float(summary["unique_chunk_count"]) if "unique_chunk_count" in summary else None,
        "placement_total_theoretical_gain": float(summary["total_theoretical_gain"]) if "total_theoretical_gain" in summary else None,
        "placement_mapped_hash_count": float(len(hash_map)) if isinstance(hash_map, dict) else None,
        "placement_protected_target_count": float(protection["protected_target_count"]) if "protected_target_count" in protection else None,
        "placement_protected_selected_count": float(protection["protected_selected_count"]) if "protected_selected_count" in protection else None,
        "placement_stage1_selected_rows": float(protection["stage1_selected_rows"]) if "stage1_selected_rows" in protection else None,
        "placement_stage1_total_protected_heat": float(protection["stage1_total_protected_heat"]) if "stage1_total_protected_heat" in protection else None,
        "placement_gain_mean_seconds": float(gain_summary["mean_seconds"]) if "mean_seconds" in gain_summary else None,
        "placement_gain_max_seconds": float(gain_summary["max_seconds"]) if "max_seconds" in gain_summary else None,
        "dedup_total_original_bytes": float(dedup_avg["total_original_bytes"]) if "total_original_bytes" in dedup_avg else None,
        "dedup_total_unique_bytes": float(dedup_avg["total_unique_bytes"]) if "total_unique_bytes" in dedup_avg else None,
        "dedup_total_network_sent_bytes": float(dedup_avg["total_network_sent_bytes"]) if "total_network_sent_bytes" in dedup_avg else None,
        "dedup_total_duration_seconds": float(dedup_avg["total_duration_seconds"]) if "total_duration_seconds" in dedup_avg else None,
        "dedup_throughput_mb_s": float(dedup_avg["throughput_mb_s"]) if "throughput_mb_s" in dedup_avg else None,
        "dedup_ratio": float(dedup_avg["dedup_ratio"]) if "dedup_ratio" in dedup_avg else None,
        "restore_file_count": float(restore_avg["file_count"]) if "file_count" in restore_avg else None,
        "restore_cloud_full_download_files": float(restore_avg["cloud_full_download_files"]) if "cloud_full_download_files" in restore_avg else None,
        "restore_total_bytes": float(restore_avg["total_bytes"]) if "total_bytes" in restore_avg else None,
        "restore_total_duration_seconds": float(restore_avg["total_duration_seconds"]) if "total_duration_seconds" in restore_avg else None,
        "restore_throughput_mb_s": float(restore_avg["throughput_mb_s"]) if "throughput_mb_s" in restore_avg else None,
        "restore_average_duration_seconds_per_file": float(restore_avg["average_duration_seconds_per_file"]) if "average_duration_seconds_per_file" in restore_avg else None,
    }


def average_metrics(metric_rows: List[Dict[str, Optional[float]]]) -> Dict[str, Optional[float]]:
    if not metric_rows:
        return {}
    keys = set()
    for row in metric_rows:
        keys.update(row.keys())
    out: Dict[str, Optional[float]] = {}
    for key in keys:
        values = [float(row[key]) for row in metric_rows if row.get(key) is not None]
        out[key] = (sum(values) / float(len(values))) if values else None
    return out


def write_summary_csv(out_path: Path, rows: List[Dict[str, object]], average_row: Dict[str, object]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    for key in average_row.keys():
        if key not in fieldnames:
            fieldnames.append(key)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        writer.writerow(average_row)


def write_rows_csv(out_path: Path, rows: List[Dict[str, object]]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    disdedup_dir = repo_root / "disDedup"
    default_env = disdedup_dir / "compose.paths.env"
    default_source = disdedup_dir / "scripts" / "data" / "input" / "8KB" / "fileInfo-zipf-s0.7.json"
    default_algorithm = disdedup_dir / "scripts" / "case1_cluster" / "run_case1_cluster_theoretical_weighted_topfirst_version2.0.py"

    parser = argparse.ArgumentParser(description="Run shuffled-heat placement experiments and average the results.")
    parser.add_argument("--source-json", default=str(default_source), help="source fileInfo json used to generate shuffled variants")
    parser.add_argument("--algorithm-script", default=str(default_algorithm), help="placement algorithm script under scripts/case1_cluster")
    parser.add_argument(
        "--algorithm-extra-arg",
        action="append",
        default=[],
        help="extra raw argument passed to the algorithm script; repeat as needed",
    )
    parser.add_argument("--base-env-file", default=str(default_env), help="base compose env file")
    parser.add_argument("--dataset-root", default="", help="host dataset root; default is DATA_ROOT/github_repo from env file")
    parser.add_argument("--suite-root", default="", help="host directory for all generated inputs, logs, and per-run outputs")
    parser.add_argument("--shared-cloud-root", default="", help="shared CLOUD_STORE_ROOT for all runs")
    parser.add_argument(
        "--include-original",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="include the original source json as one run in the suite",
    )
    parser.add_argument("--num-shuffles", type=int, default=0, help="number of shuffled JSON variants to generate")
    parser.add_argument("--shuffle-seed-start", type=int, default=1, help="first shuffle seed; seeds are contiguous")
    parser.add_argument("--capacity-ratio", type=float, default=0.30, help="capacity ratio for TopFirst placement")
    parser.add_argument("--protect-top-ratio", type=float, default=0.25, help="TopFirst protect-top-ratio")
    parser.add_argument("--k", type=int, default=1000, help="clustering k parameter")
    parser.add_argument("--alpha", type=int, default=5, help="clustering alpha parameter")
    parser.add_argument("--server-num", type=int, default=10, help="number of edge nodes")
    parser.add_argument("--parallel-edge-nodes", type=int, default=0, help="effective parallel edge nodes for TopFirst theory model")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 1) - 1), help="worker count for clustering")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="TopFirst theoretical restore batch size")
    parser.add_argument(
        "--dedup-mode",
        choices=["real", "synthetic", "synthetic_edge", "synthetic_manager"],
        default="synthetic_edge",
        help="real: chunk original dataset; synthetic/synthetic_edge: manager sends hash+size and node generates chunks; synthetic_manager: manager generates fake chunk bytes",
    )
    parser.add_argument(
        "--synthetic-dataset-prefix",
        default="",
        help="container-side dataset prefix used for recipe naming in synthetic dedup mode; default is inferred from dataset-root",
    )
    parser.add_argument("--request-count", type=int, default=0, help="restore request count; overrides request-multiplier when > 0")
    parser.add_argument("--request-multiplier", type=int, default=2, help="restore request multiplier when request-count is 0")
    parser.add_argument("--request-seed", type=int, default=42, help="random seed for restore request generation")
    parser.add_argument(
        "--prepare-cloud-mode",
        choices=["auto", "always", "never"],
        default="auto",
        help="prepare shared cloud full files via manager cloud-sync",
    )
    parser.add_argument(
        "--cleanup-edge-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="delete per-run edge node data after compose down; keep only edge YAML configs",
    )
    parser.add_argument(
        "--shared-artifact-cache-root",
        default="",
        help="shared cache dir for reusable placement JSON and restore request files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    source_json = Path(args.source_json).resolve()
    algorithm_script = Path(args.algorithm_script).resolve()
    base_env_file = Path(args.base_env_file).resolve()
    if not source_json.exists():
        raise FileNotFoundError(f"source json not found: {source_json}")
    if not algorithm_script.exists():
        raise FileNotFoundError(f"algorithm script not found: {algorithm_script}")
    if not base_env_file.exists():
        raise FileNotFoundError(f"base env file not found: {base_env_file}")

    env_map = load_env_file(base_env_file)
    repo_root = Path(__file__).resolve().parents[3]
    disdedup_dir = repo_root / "disDedup"
    compose_file = disdedup_dir / "docker-compose.yml"
    sync_script = disdedup_dir / "scripts" / "config" / "sync_cluster_configs.sh"
    batch_restore_script = disdedup_dir / "scripts" / "restore" / "batch_restore_from_dir.sh"

    dataset_root = Path(args.dataset_root).resolve() if args.dataset_root else Path(env_map.get("DATA_ROOT", "")) / "github_repo"
    if not dataset_root.exists():
        raise FileNotFoundError(f"dataset root not found: {dataset_root}")
    data_root = Path(env_map.get("DATA_ROOT", "")).resolve() if env_map.get("DATA_ROOT") else None
    if data_root is None:
        raise ValueError("DATA_ROOT must be set in the base env file")
    dataset_container_root = host_path_to_container_input_path(dataset_root, data_root)
    synthetic_dataset_prefix = args.synthetic_dataset_prefix.strip() or dataset_container_root

    source = json.loads(source_json.read_text(encoding="utf-8"))
    chunk_size = int(source.get("chunk_bytes", 8192))
    cap_pct = int(round(args.capacity_ratio * 100.0))

    base_store_root = Path(env_map.get("STORE_ROOT", "")).resolve() if env_map.get("STORE_ROOT") else (repo_root / "tmp_store")
    if args.suite_root:
        suite_root = Path(args.suite_root).resolve()
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        algo_stem = algorithm_script.stem
        suite_root = base_store_root.parent / f"{algo_stem}_cap{cap_pct}_{source_json.stem}_{stamp}"
    suite_root.mkdir(parents=True, exist_ok=True)

    if args.shared_cloud_root:
        shared_cloud_root = Path(args.shared_cloud_root).resolve()
    elif env_map.get("CLOUD_STORE_ROOT"):
        shared_cloud_root = Path(env_map["CLOUD_STORE_ROOT"]).resolve()
    else:
        shared_cloud_root = suite_root / "shared_cloud"
    shared_cloud_root.mkdir(parents=True, exist_ok=True)

    if args.shared_artifact_cache_root:
        shared_artifact_cache_root = Path(args.shared_artifact_cache_root).resolve()
    else:
        shared_artifact_cache_root = repo_root / "disDedup" / "scripts" / "data" / "output" / "_shared_artifact_cache"
    shared_artifact_cache_root.mkdir(parents=True, exist_ok=True)

    if not (0.0 < args.capacity_ratio <= 1.0):
        raise ValueError("--capacity-ratio must be in (0, 1]")
    if not (0.0 <= args.protect_top_ratio <= 1.0):
        raise ValueError("--protect-top-ratio must be in [0, 1]")
    if args.server_num <= 0 or args.server_num > 10:
        raise ValueError("--server-num must be in [1, 10]")
    if args.request_count <= 0 and args.request_multiplier <= 0:
        raise ValueError("set --request-count > 0 or --request-multiplier > 0")

    if args.num_shuffles < 0:
        raise ValueError("--num-shuffles must be >= 0")
    shuffle_seeds = list(range(args.shuffle_seed_start, args.shuffle_seed_start + args.num_shuffles))
    fileinfo_dir = suite_root / "fileinfo"
    summary_dir = suite_root / "summary"
    shuffled_json_map = resolve_shuffled_jsons(source_json, fileinfo_dir, shuffle_seeds)
    shuffled_jsons = [Path(str(shuffled_json_map[seed]["path"])) for seed in shuffle_seeds]
    existing_shuffle_count = sum(1 for item in shuffled_json_map.values() if str(item["origin"]).startswith("existing"))
    generated_shuffle_count = sum(1 for item in shuffled_json_map.values() if not str(item["origin"]).startswith("existing"))

    suite_inputs: List[Dict[str, object]] = []
    if args.include_original:
        suite_inputs.append({"tag": "orig", "json": source_json, "shuffle_seed": None})
    for shuffle_seed in shuffle_seeds:
        shuffled_json = Path(str(shuffled_json_map[shuffle_seed]["path"]))
        suite_inputs.append(
            {
                "tag": f"seed{shuffle_seed:04d}",
                "json": shuffled_json,
                "shuffle_seed": shuffle_seed,
                "shuffle_origin": str(shuffled_json_map[shuffle_seed]["origin"]),
            }
        )

    print(f"[info] suite_root={suite_root}")
    print(f"[info] shared_cloud_root={shared_cloud_root}")
    print(f"[info] shared_artifact_cache_root={shared_artifact_cache_root}")
    print(f"[info] algorithm_script={algorithm_script}")
    print(f"[info] dataset_root={dataset_root}")
    print(f"[info] dataset_container_root={dataset_container_root}")
    print(f"[info] synthetic_dataset_prefix={synthetic_dataset_prefix}")
    print(f"[info] existing_shuffled_jsons={existing_shuffle_count}")
    print(f"[info] generated_shuffled_jsons={generated_shuffle_count}")
    print(f"[info] total_runs={len(suite_inputs)}")

    cloud_fullfiles_dir = shared_cloud_root / "cloud" / "storage" / "_fullfiles"
    cloud_prepared = path_has_entries(cloud_fullfiles_dir)
    # Restore may fall back to cloud full-file download for any placement that
    # leaves chunks off edge, so prepare cloud full files from dataset-root by
    # default unless the caller explicitly disables it.
    need_cloud_fullfiles = True
    if args.prepare_cloud_mode == "never" and need_cloud_fullfiles and not cloud_prepared:
        raise FileNotFoundError(
            "shared cloud full files are missing and --prepare-cloud-mode=never was requested: "
            f"{cloud_fullfiles_dir}"
        )

    run_records: List[Dict[str, object]] = []
    failed_runs: List[Dict[str, object]] = []
    metric_rows: List[Dict[str, Optional[float]]] = []

    for suite_item in suite_inputs:
        tag = str(suite_item["tag"])
        shuffled_json = Path(str(suite_item["json"]))
        shuffle_seed = suite_item["shuffle_seed"]
        shuffle_origin = str(suite_item.get("shuffle_origin", "original")) if shuffle_seed is not None else "original"
        run_root = suite_root / "runs" / tag
        store_root = run_root / "store"
        logs_dir = run_root / "logs"
        env_file = run_root / "compose.env"
        placement_host = store_root / "manager" / "placement" / f"{tag}.{algorithm_script.stem}.cap{cap_pct}.json"
        placement_container = f"/data/placement/{placement_host.name}"
        request_host = store_root / "manager" / "restore" / f"{tag}.requests.txt"
        request_container = to_container_restore_request_path(request_host, store_root)
        synthetic_fileinfo_host = store_root / "manager" / "fileinfo" / shuffled_json.name
        synthetic_fileinfo_container = f"/data/fileinfo/{shuffled_json.name}"

        write_env_file(
            base_env_file,
            env_file,
            {
                "STORE_ROOT": str(store_root),
                "CLOUD_STORE_ROOT": str(shared_cloud_root),
            },
        )

        stage_name = "setup"
        stage_log = logs_dir / "00_setup.log"
        stack_up = False
        try:
            stage_name = "algorithm"
            stage_log = logs_dir / "01_algorithm.log"
            placement_cache_key = build_placement_cache_key(
                shuffled_json=shuffled_json,
                algorithm_script=algorithm_script,
                env_file=env_file,
                capacity_ratio=args.capacity_ratio,
                protect_top_ratio=args.protect_top_ratio,
                k=args.k,
                alpha=args.alpha,
                server_num=args.server_num,
                parallel_edge_nodes=args.parallel_edge_nodes,
                workers=args.workers,
                restore_batch_size=args.restore_batch_size,
                algorithm_extra_args=list(args.algorithm_extra_arg),
            )
            placement_cache_path = (
                shared_artifact_cache_root / "placement" / algorithm_script.stem / f"{placement_cache_key}.json"
            )
            placement_cache_path.parent.mkdir(parents=True, exist_ok=True)
            if placement_cache_path.exists():
                materialize_cached_file(placement_cache_path, placement_host)
                write_log_message(
                    stage_log,
                    f"[cache-hit] placement reused from {placement_cache_path}\n[target] {placement_host}",
                )
            else:
                run_cmd(
                    build_algorithm_cmd(
                        algorithm_script,
                        input_json=shuffled_json,
                        env_file=env_file,
                        output_json=placement_cache_path,
                        capacity_ratio=args.capacity_ratio,
                        protect_top_ratio=args.protect_top_ratio,
                        k=args.k,
                        alpha=args.alpha,
                        server_num=args.server_num,
                        parallel_edge_nodes=args.parallel_edge_nodes,
                        workers=args.workers,
                        restore_batch_size=args.restore_batch_size,
                        algorithm_extra_args=list(args.algorithm_extra_arg),
                    ),
                    cwd=repo_root,
                    log_path=stage_log,
                )
                materialize_cached_file(placement_cache_path, placement_host)

            stage_name = "sync_configs"
            stage_log = logs_dir / "02_sync_configs.log"
            run_cmd(
                [
                    str(sync_script),
                    "--env-file",
                    str(env_file),
                    "--store-root",
                    str(store_root),
                    "--cloud-store-root",
                    str(shared_cloud_root),
                    "--edge-count",
                    str(args.server_num),
                    "--chunk-size",
                    str(chunk_size),
                    "--storage-granularity",
                    "block",
                    "--chunking-method",
                    "fastcdc",
                    "--placement-json",
                    placement_container,
                    "--placement-strategy",
                    "round_robin",
                    "--dedup-mode",
                    args.dedup_mode,
                    "--synthetic-fileinfo-json",
                    synthetic_fileinfo_container,
                    "--synthetic-dataset-prefix",
                    synthetic_dataset_prefix,
                    "--restore-cloud-full-threshold",
                    "1",
                    "--restore-discard-output",
                    "true",
                    "--load-previous-index",
                    "false",
                ],
                cwd=disdedup_dir,
                log_path=stage_log,
            )

            stage_name = "compose_up"
            stage_log = logs_dir / "03_compose_up.log"
            run_cmd(
                docker_compose_cmd(
                    compose_file,
                    env_file,
                    "up",
                    "-d",
                    "--force-recreate",
                    "cloud",
                    *[f"edge{i}" for i in range(1, args.server_num + 1)],
                    "manager",
                ),
                cwd=disdedup_dir,
                log_path=stage_log,
            )
            stack_up = True

            need_prepare_cloud = need_cloud_fullfiles and (
                args.prepare_cloud_mode == "always"
                or (args.prepare_cloud_mode == "auto" and not cloud_prepared)
            )
            if need_prepare_cloud:
                stage_name = "cloud_sync"
                stage_log = logs_dir / "04_cloud_sync.log"
                run_cmd(
                    docker_compose_cmd(
                        compose_file,
                        env_file,
                        "run",
                        "--rm",
                        "manager",
                        "manager",
                        "-config",
                        "/cfg/manager.yaml",
                        "cloud-sync",
                        dataset_container_root,
                    ),
                    cwd=disdedup_dir,
                    log_path=stage_log,
                )
                cloud_prepared = True

            stage_name = "dedup"
            stage_log = logs_dir / "05_dedup.log"
            if args.dedup_mode != "real":
                synthetic_fileinfo_host.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(shuffled_json, synthetic_fileinfo_host)
            run_cmd(
                docker_compose_cmd(
                    compose_file,
                    env_file,
                    "run",
                    "--rm",
                    "manager",
                    "manager",
                    "-config",
                    "/cfg/manager.yaml",
                    "-placement-json",
                    placement_container,
                    "dedup",
                    dataset_container_root,
                ),
                cwd=disdedup_dir,
                log_path=stage_log,
            )

            restore_cmd = [
                str(batch_restore_script),
                "--input-dir",
                str(dataset_root),
                "--env-file",
                str(env_file),
                "--fileinfo-json",
                str(shuffled_json),
                "--seed",
                str(args.request_seed),
                "--request-file-host",
                "",
                "--output-dir",
                "/output/restored/shared",
            ]
            if args.dedup_mode != "real":
                restore_cmd.append("--skip-missing")
            if args.request_count > 0:
                restore_cmd.extend(["--request-count", str(args.request_count)])
            else:
                restore_cmd.extend(["--request-multiplier", str(max(1, args.request_multiplier))])

            stage_name = "build_restore_requests"
            stage_log = logs_dir / "06_build_restore_requests.log"
            request_cache_key = build_request_cache_key(
                dataset_root=dataset_root,
                shuffled_json=shuffled_json,
                request_seed=args.request_seed,
                request_count=args.request_count,
                request_multiplier=max(1, args.request_multiplier),
                dedup_mode=args.dedup_mode,
            )
            request_cache_path = shared_artifact_cache_root / "restore_requests" / f"{request_cache_key}.txt"
            request_cache_path.parent.mkdir(parents=True, exist_ok=True)
            if request_cache_path.exists():
                materialize_cached_file(request_cache_path, request_host)
                write_log_message(
                    stage_log,
                    f"[cache-hit] restore requests reused from {request_cache_path}\n[target] {request_host}",
                )
            else:
                restore_cmd[restore_cmd.index("--request-file-host") + 1] = str(request_cache_path)
                run_cmd(
                    restore_cmd,
                    cwd=disdedup_dir,
                    log_path=stage_log,
                )
                materialize_cached_file(request_cache_path, request_host)

            stage_name = "restore_batch"
            stage_log = logs_dir / "07_restore_batch.log"
            run_cmd(
                docker_compose_cmd(
                    compose_file,
                    env_file,
                    "run",
                    "--rm",
                    "manager",
                    "manager",
                    "-config",
                    "/cfg/manager.yaml",
                    "-placement-json",
                    placement_container,
                    "restore-batch",
                    request_container,
                ),
                cwd=disdedup_dir,
                log_path=stage_log,
            )

            stage_name = "collect_stats"
            dedup_stats = store_root / "manager" / "stats" / "dedup_stats.json"
            restore_stats = store_root / "manager" / "stats" / "restore_stats.json"
            if not dedup_stats.exists():
                raise FileNotFoundError(f"dedup stats missing: {dedup_stats}")
            if not restore_stats.exists():
                raise FileNotFoundError(f"restore stats missing: {restore_stats}")

            metrics = extract_run_metrics(placement_host, dedup_stats, restore_stats)
            metric_rows.append(metrics)
            record = {
                "tag": tag,
                "status": "ok",
                "failure_stage": "",
                "failure_log": "",
                "failure_error": "",
                "failure_summary": "",
                "shuffle_seed": "" if shuffle_seed is None else shuffle_seed,
                "shuffle_origin": shuffle_origin,
                "fileinfo_json": str(shuffled_json),
                "algorithm_script": str(algorithm_script),
                "dedup_mode": args.dedup_mode,
                "store_root": str(store_root),
                "placement_json": str(placement_host),
                "request_file": str(request_host),
                "synthetic_fileinfo_json": str(synthetic_fileinfo_host) if args.dedup_mode != "real" else "",
            }
            record.update(metrics)
            run_records.append(record)
            dedup_ratio = metrics.get("dedup_ratio")
            restore_avg = metrics.get("restore_average_duration_seconds_per_file")
            restore_thr = metrics.get("restore_throughput_mb_s")
            parts = [f"[done] {tag}:"]
            if dedup_ratio is not None:
                parts.append(f"dedup_ratio={dedup_ratio:.6f}")
            if restore_avg is not None:
                parts.append(f"restore_avg_s={restore_avg:.6f}")
            if restore_thr is not None:
                parts.append(f"restore_thr={restore_thr:.2f} MB/s")
            print(" ".join(parts))
        except Exception as exc:
            failure_summary = summarize_failure(stage_log, exc)
            failure = {
                "tag": tag,
                "status": "failed",
                "failure_stage": stage_name,
                "failure_log": str(stage_log),
                "failure_error": f"{type(exc).__name__}: {exc}",
                "failure_summary": failure_summary,
                "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                "shuffle_seed": "" if shuffle_seed is None else shuffle_seed,
                "shuffle_origin": shuffle_origin,
                "fileinfo_json": str(shuffled_json),
                "algorithm_script": str(algorithm_script),
                "dedup_mode": args.dedup_mode,
                "store_root": str(store_root),
                "placement_json": str(placement_host),
                "request_file": str(request_host),
                "synthetic_fileinfo_json": str(synthetic_fileinfo_host) if args.dedup_mode != "real" else "",
            }
            failed_runs.append(failure)
            run_records.append(dict(failure))
            print(f"[failed] {tag}: stage={stage_name} log={stage_log} summary={failure_summary}", file=sys.stderr)
        finally:
            if stack_up:
                try:
                    run_cmd(
                        docker_compose_cmd(compose_file, env_file, "down"),
                        cwd=disdedup_dir,
                        log_path=logs_dir / "99_compose_down.log",
                    )
                except subprocess.CalledProcessError as exc:
                    print(f"[warn] docker compose down failed for {tag}: {exc}", file=sys.stderr)
            if args.cleanup_edge_data:
                cleanup_log = logs_dir / "98_cleanup_edge_data.log"
                cleanup_log.parent.mkdir(parents=True, exist_ok=True)
                try:
                    cleaned_dirs = cleanup_edge_data(store_root, args.server_num)
                    with cleanup_log.open("w", encoding="utf-8") as fh:
                        if cleaned_dirs:
                            for path in cleaned_dirs:
                                fh.write(f"cleaned {path}\n")
                        else:
                            fh.write("no edge directories found\n")
                except Exception as exc:
                    with cleanup_log.open("w", encoding="utf-8") as fh:
                        fh.write(f"cleanup failed: {type(exc).__name__}: {exc}\n")
                    print(f"[warn] edge data cleanup failed for {tag}: {exc}", file=sys.stderr)

    avg_metrics = average_metrics(metric_rows)
    suite_status = "ok"
    if failed_runs and metric_rows:
        suite_status = "partial"
    elif failed_runs and not metric_rows:
        suite_status = "failed"
    summary = {
        "status": suite_status,
        "source_json": str(source_json),
        "algorithm_script": str(algorithm_script),
        "dataset_root": str(dataset_root),
        "suite_root": str(suite_root),
        "shared_cloud_root": str(shared_cloud_root),
        "params": {
            "include_original": bool(args.include_original),
            "num_shuffles": len(shuffled_jsons),
            "total_runs": len(suite_inputs),
            "shuffle_seeds": shuffle_seeds,
            "existing_shuffle_count": existing_shuffle_count,
            "generated_shuffle_count": generated_shuffle_count,
            "capacity_ratio": float(args.capacity_ratio),
            "protect_top_ratio": float(args.protect_top_ratio),
            "k": int(args.k),
            "alpha": int(args.alpha),
            "server_num": int(args.server_num),
            "parallel_edge_nodes": int(args.parallel_edge_nodes),
            "workers": int(max(1, args.workers)),
            "restore_batch_size": int(args.restore_batch_size),
            "dedup_mode": args.dedup_mode,
            "synthetic_dataset_prefix": args.synthetic_dataset_prefix,
            "dataset_container_root": dataset_container_root,
            "request_count": int(args.request_count),
            "request_multiplier": int(max(1, args.request_multiplier)),
            "request_seed": int(args.request_seed),
            "prepare_cloud_mode": args.prepare_cloud_mode,
            "cleanup_edge_data": bool(args.cleanup_edge_data),
            "algorithm_extra_args": list(args.algorithm_extra_arg),
        },
        "success_count": len(metric_rows),
        "failure_count": len(failed_runs),
        "runs": run_records,
        "failed_runs": failed_runs,
        "average_metrics": avg_metrics,
    }

    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_json = summary_dir / "suite_summary.json"
    summary_csv = summary_dir / "suite_summary.csv"
    failed_json = summary_dir / "failed_runs.json"
    failed_csv = summary_dir / "failed_runs.csv"
    summary_json.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    failed_json.write_text(json.dumps(failed_runs, ensure_ascii=True, indent=2), encoding="utf-8")

    csv_rows = [dict(record) for record in run_records]
    if csv_rows:
        avg_row: Dict[str, object] = {"tag": "average", "shuffle_seed": "", "status": suite_status}
        for key in csv_rows[0].keys():
            if key in avg_metrics:
                avg_row[key] = avg_metrics[key]
            elif key not in avg_row:
                avg_row[key] = ""
        write_summary_csv(summary_csv, csv_rows, avg_row)
    write_rows_csv(failed_csv, failed_runs)

    print(f"[summary] json={summary_json}")
    print(f"[summary] csv={summary_csv}")
    print(f"[summary] failed_json={failed_json}")
    print(f"[summary] failed_csv={failed_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
