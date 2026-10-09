#!/usr/bin/env python3
"""
Run restore experiments for a fixed request batch and export a flat CSV.

Each CSV row corresponds to:
  one requested file x one restore scenario

Scenarios:
  - edge-all
  - cloud-full
  - hybrid with edge_ratio 0.95, 0.90, ..., 0.50
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class RestoreRequest:
    input_path: str
    output_path: str


@dataclass
class Scenario:
    mode: str
    edge_ratio: float


def parse_env_file(env_path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
            v = v[1:-1]
        out[k] = v
    return out


def parse_request_line(line: str) -> RestoreRequest:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        raise ValueError("skip")

    if "\t" in raw:
        parts = raw.split("\t")
        if len(parts) >= 2:
            first = parts[0].strip()
            try:
                float(first)
                input_path = parts[1].strip()
                output_path = "\t".join(parts[2:]).strip() if len(parts) >= 3 else ""
            except ValueError:
                input_path = first
                output_path = "\t".join(parts[1:]).strip()
        else:
            input_path = parts[0].strip()
            output_path = ""
    else:
        parts = raw.split()
        if len(parts) == 1:
            input_path = parts[0].strip()
            output_path = ""
        elif len(parts) >= 2:
            try:
                float(parts[0])
                input_path = parts[1].strip()
                output_path = parts[2].strip() if len(parts) >= 3 else ""
            except ValueError:
                input_path = parts[0].strip()
                output_path = parts[1].strip()
        else:
            raise ValueError("invalid request line")

    if not input_path:
        raise ValueError("input path cannot be empty")
    return RestoreRequest(input_path=input_path, output_path=output_path)


def load_requests(path: Path) -> list[RestoreRequest]:
    rows: list[RestoreRequest] = []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            req = parse_request_line(raw)
        except ValueError as exc:
            if str(exc) == "skip":
                continue
            raise ValueError(f"{path}:{lineno}: {exc}") from exc
        rows.append(req)
    if not rows:
        raise ValueError(f"no valid requests found in {path}")
    return rows


def detect_manager_container_id(compose_file: Path, env_file: Path, service: str) -> str:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "--env-file",
        str(env_file),
        "ps",
        "-q",
        service,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "docker compose ps failed").strip())
    cid = (proc.stdout or "").strip()
    if not cid:
        raise RuntimeError(
            f"manager service '{service}' is not running; start it first, e.g. "
            f"'docker compose -f {compose_file} --env-file {env_file} up -d {service}'"
        )
    return cid


def file_name_from_input(input_path: str) -> str:
    raw = input_path.strip()
    if raw.endswith(".recipe"):
        return Path(raw).name.removesuffix(".recipe")
    safe = raw.replace(":", "").replace("/", "-").replace("\\", "-")
    return safe


def build_scenarios(hybrid_min: float, hybrid_max: float, hybrid_step: float) -> list[Scenario]:
    scenarios = [
        Scenario(mode="edge-all", edge_ratio=1.0),
        Scenario(mode="cloud-full", edge_ratio=0.0),
    ]
    cur = hybrid_max
    guard = 0
    while cur >= hybrid_min - 1e-9:
        scenarios.append(Scenario(mode="hybrid", edge_ratio=cur))
        cur -= hybrid_step
        guard += 1
        if guard > 1000:
            raise RuntimeError("hybrid ratio generation overflow")
    return scenarios


def scenario_label(scenario: Scenario) -> str:
    ratio_pct = int(round(scenario.edge_ratio * 100))
    if scenario.mode == "edge-all":
        return f"edge-{ratio_pct}%"
    if scenario.mode == "cloud-full":
        return f"cloud-{ratio_pct}%"
    return f"hybrid-{ratio_pct}%"


def scenario_manager_command(scenario: Scenario, input_path: str, output_path: str) -> list[str]:
    if scenario.mode == "edge-all":
        return ["restore", input_path, output_path]
    if scenario.mode == "cloud-full":
        return ["restore-cloud", input_path, output_path]
    cmd = ["restore-exp", "hybrid", input_path, output_path, f"{scenario.edge_ratio:.2f}"]
    return cmd


def scenario_manager_batch_command(scenario: Scenario, request_file_container: str) -> list[str]:
    if scenario.mode == "edge-all":
        return ["restore-batch", request_file_container]
    if scenario.mode == "cloud-full":
        return ["restore-cloud-batch", request_file_container]
    return ["restore-exp-batch", "hybrid", request_file_container, f"{scenario.edge_ratio:.2f}"]


def scenario_output_path(base_dir: str, scenario: Scenario, index: int, req: RestoreRequest) -> str:
    suffix = Path(req.input_path).name or f"file_{index:06d}"
    ratio_tag = int(round(scenario.edge_ratio * 100))
    return f"{base_dir}/r{ratio_tag:03d}/{index:06d}_{suffix}"


def manager_container_path(host_path: Path, manager_host_dir: Path) -> str:
    rel = host_path.resolve().relative_to(manager_host_dir.resolve())
    return f"/data/{rel.as_posix()}"


def node_group_totals(nodes: list[dict[str, Any]], is_cloud: bool) -> dict[str, float]:
    out = {
        "bytes": 0.0,
        "chunks_requested": 0.0,
        "chunks_found": 0.0,
        "missing_chunks": 0.0,
        "batches": 0.0,
        "rpc_seconds": 0.0,
        "server_seconds": 0.0,
        "transfer_seconds": 0.0,
        "extract_seconds": 0.0,
    }
    for node in nodes:
        node_number = int(node.get("node_number", 0))
        node_id = str(node.get("node_id", "")).strip().lower()
        node_is_cloud = node_number == 0 or node_id == "cloud"
        if node_is_cloud != is_cloud:
            continue
        out["bytes"] += float(node.get("bytes_returned", 0))
        out["chunks_requested"] += float(node.get("chunks_requested", 0))
        out["chunks_found"] += float(node.get("chunks_found", 0))
        out["missing_chunks"] += float(node.get("missing_chunks", 0))
        out["batches"] += float(node.get("batches", 0))
        out["rpc_seconds"] += float(((node.get("rpc_round_trip") or {}) or {}).get("seconds", 0.0))
        out["server_seconds"] += float(((node.get("server_total") or {}) or {}).get("seconds", 0.0))
        out["transfer_seconds"] += float(((node.get("transfer_estimate") or {}) or {}).get("seconds", 0.0))
        out["extract_seconds"] += float(((node.get("extract") or {}) or {}).get("seconds", 0.0))
    return out


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent.parent

    parser = argparse.ArgumentParser(description="Run restore experiment batch and export flat CSV.")
    parser.add_argument("--request-file-host", required=True, help="host path of restore request file")
    parser.add_argument("--env-file", default=str(root_dir / "compose.paths.env"), help="compose env file")
    parser.add_argument("--compose-file", default=str(root_dir / "docker-compose.yml"), help="compose file")
    parser.add_argument("--manager-config", default="/cfg/manager.yaml", help="manager config path in container")
    parser.add_argument("--manager-service", default="manager", help="docker compose service name")
    parser.add_argument("--placement-json", default="", help="override manager -placement-json")
    parser.add_argument("--placement-dir", default="", help="override manager -placement-dir")
    parser.add_argument("--placement-strategy", default="", help="override manager -placement-strategy")
    parser.add_argument("--report-dir-host", default="", help="host dir for report outputs")
    parser.add_argument("--stats-file-host", default="", help="host restore stats json path override")
    parser.add_argument("--output-base-container", default="", help="container output base dir override")
    parser.add_argument("--hybrid-max", type=float, default=0.95, help="max hybrid edge ratio")
    parser.add_argument("--hybrid-min", type=float, default=0.50, help="min hybrid edge ratio")
    parser.add_argument("--hybrid-step", type=float, default=0.05, help="hybrid edge ratio step")
    parser.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    request_file = Path(args.request_file_host).resolve()
    env_file = Path(args.env_file).resolve()
    compose_file = Path(args.compose_file).resolve()
    if not request_file.exists():
        print(f"ERROR: request file not found: {request_file}", file=sys.stderr)
        return 2
    if not env_file.exists():
        print(f"ERROR: env file not found: {env_file}", file=sys.stderr)
        return 2
    if not compose_file.exists():
        print(f"ERROR: compose file not found: {compose_file}", file=sys.stderr)
        return 2
    if args.hybrid_step <= 0:
        print("ERROR: --hybrid-step must be > 0", file=sys.stderr)
        return 2
    if args.hybrid_max < args.hybrid_min:
        print("ERROR: --hybrid-max must be >= --hybrid-min", file=sys.stderr)
        return 2

    env_map = parse_env_file(env_file)
    store_root = env_map.get("STORE_ROOT", "").strip()
    if not store_root:
        print(f"ERROR: STORE_ROOT is empty in {env_file}", file=sys.stderr)
        return 2

    try:
        requests = load_requests(request_file)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    scenarios = build_scenarios(args.hybrid_min, args.hybrid_max, args.hybrid_step)
    manager_host_dir = Path(store_root).resolve() / "manager"
    now_tag = time.strftime("%Y%m%d_%H%M%S")
    report_dir = Path(args.report_dir_host).resolve() if args.report_dir_host else (manager_host_dir / f"restore_experiment_csv_{now_tag}")
    report_dir.mkdir(parents=True, exist_ok=True)
    stats_snapshots_dir = report_dir / "stats_snapshots"
    stats_snapshots_dir.mkdir(parents=True, exist_ok=True)

    csv_path = report_dir / "restore_experiment_rows.csv"
    json_path = report_dir / "restore_experiment_rows.json"
    requests_copy = report_dir / "requests_used.tsv"
    requests_copy.write_text(request_file.read_text(encoding="utf-8"), encoding="utf-8")

    stats_file_host = Path(args.stats_file_host).resolve() if args.stats_file_host else (manager_host_dir / "stats" / "restore_stats.json")
    output_base_container = args.output_base_container.strip() or "/output/restore_experiment_csv"

    try:
        manager_container_id = detect_manager_container_id(compose_file, env_file, args.manager_service)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    rows: list[dict[str, Any]] = []
    total_runs = len(requests) * len(scenarios)
    row_id = 1
    manager_stage_dir = manager_host_dir / f"restore_experiment_csv_stage_{now_tag}"
    manager_stage_dir.mkdir(parents=True, exist_ok=True)
    print(f"[export] manager container: {manager_container_id}")
    print(f"[export] requests={len(requests)} scenarios={len(scenarios)} total_runs={total_runs}")

    for scenario in scenarios:
        scenario_ratio_pct = int(round(scenario.edge_ratio * 100))
        scenario_dir = stats_snapshots_dir / f"r{scenario_ratio_pct:03d}"
        scenario_dir.mkdir(parents=True, exist_ok=True)
        label = scenario_label(scenario)
        print(f"[export] scenario={label}")

        scenario_requests: list[RestoreRequest] = []
        scenario_request_host = manager_stage_dir / f"requests_r{scenario_ratio_pct:03d}.tsv"
        with scenario_request_host.open("w", encoding="utf-8", newline="") as f:
            for idx, req in enumerate(requests, start=1):
                output_path = scenario_output_path(output_base_container, scenario, idx, req)
                scenario_requests.append(RestoreRequest(input_path=req.input_path, output_path=output_path))
                f.write(f"{req.input_path}\t{output_path}\n")
        scenario_request_container = manager_container_path(scenario_request_host, manager_host_dir)

        cmd = [
            "docker",
            "exec",
            manager_container_id,
            "manager",
            "-config",
            args.manager_config,
        ]
        if args.placement_json:
            cmd.extend(["-placement-json", args.placement_json])
        if args.placement_dir:
            cmd.extend(["-placement-dir", args.placement_dir])
        if args.placement_strategy:
            cmd.extend(["-placement-strategy", args.placement_strategy])
        cmd.extend(scenario_manager_batch_command(scenario, scenario_request_container))

        run_start = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True)
        run_end = time.time()

        stat_obj = None
        stat_snap_path = ""
        if proc.returncode == 0 and stats_file_host.exists():
            try:
                stat_obj = json.loads(stats_file_host.read_text(encoding="utf-8"))
                stat_path = scenario_dir / "restore_stats.json"
                stat_path.write_text(json.dumps(stat_obj, ensure_ascii=False, indent=2), encoding="utf-8")
                stat_snap_path = str(stat_path)
            except Exception:
                stat_obj = None

        scenario_runtime_seconds = round(run_end - run_start, 6)
        if stat_obj is None:
            rows.append(
                {
                    "id": row_id,
                    "file_name": "__scenario_failure__",
                    "experiment_output_path": "",
                    "edge_ratio": scenario_ratio_pct,
                    "exit_code": proc.returncode,
                    "runtime_seconds": scenario_runtime_seconds,
                    "end_to_end_latency_seconds": 0.0,
                    "data_path_latency_seconds": 0.0,
                    "throughput_mb_s": 0.0,
                    "file_size_bytes": 0,
                    "file_size_mb": 0.0,
                    "chunk_count": 0,
                    "cloud_full_download": False,
                    "edge_bytes": 0,
                    "edge_chunks_requested": 0,
                    "edge_chunks_found": 0,
                    "edge_batches": 0,
                    "edge_rpc_seconds": 0.0,
                    "edge_transfer_seconds": 0.0,
                    "cloud_bytes": 0,
                    "cloud_chunks_requested": 0,
                    "cloud_chunks_found": 0,
                    "cloud_batches": 0,
                    "cloud_rpc_seconds": 0.0,
                    "cloud_transfer_seconds": 0.0,
                    "edge_byte_ratio": 0.0,
                    "cloud_byte_ratio": 0.0,
                    "phase_setup_seconds": 0.0,
                    "phase_setup_pct": 0.0,
                    "phase_recipe_scan_seconds": 0.0,
                    "phase_recipe_scan_pct": 0.0,
                    "phase_window_prepare_seconds": 0.0,
                    "phase_window_prepare_pct": 0.0,
                    "phase_edge_fetch_seconds": 0.0,
                    "phase_edge_fetch_pct": 0.0,
                    "phase_cloud_fetch_seconds": 0.0,
                    "phase_cloud_fetch_pct": 0.0,
                    "phase_output_write_seconds": 0.0,
                    "phase_output_write_pct": 0.0,
                    "stats_snapshot": stat_snap_path,
                    "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-10:]),
                    "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-10:]),
                }
            )
            row_id += 1
            print(f"[export] scenario={label} failed exit={proc.returncode}")
            if not args.continue_on_error:
                print("[export] stopped due to failure (--no-continue-on-error).", file=sys.stderr)
                break
            continue

        stat_files = list(stat_obj.get("files", []) or [])
        if len(stat_files) != len(scenario_requests):
            print(
                f"[export] WARNING: scenario={label} stats files={len(stat_files)} requests={len(scenario_requests)}",
                file=sys.stderr,
            )
        n = min(len(stat_files), len(scenario_requests))
        for idx in range(n):
            req = scenario_requests[idx]
            file_stat = stat_files[idx] or {}
            phase_breakdown = {}
            for item in file_stat.get("phase_breakdown", []) or []:
                phase = str(item.get("phase", "")).strip()
                if phase:
                    phase_breakdown[phase] = float(item.get("duration_seconds", 0.0))
            nodes = list(file_stat.get("node_stats", []) or [])
            file_bytes = int(file_stat.get("bytes", 0))
            file_chunks = int(file_stat.get("chunks", 0))
            file_duration_seconds = float(file_stat.get("duration_seconds", 0.0))
            cloud_full_download = bool(file_stat.get("cloud_full_download", False))

            edge = node_group_totals(nodes, is_cloud=False)
            cloud = node_group_totals(nodes, is_cloud=True)
            total_node_bytes = edge["bytes"] + cloud["bytes"]
            phase_total = sum(float(v) for v in phase_breakdown.values())
            end_to_end_latency_seconds = phase_total if phase_total > 0.0 else file_duration_seconds
            data_path_latency_seconds = (
                float(phase_breakdown.get("edge_fetch", 0.0))
                + float(phase_breakdown.get("cloud_fallback_fetch", 0.0))
                + float(phase_breakdown.get("output_write", 0.0))
            )
            if data_path_latency_seconds <= 0.0:
                data_path_latency_seconds = file_duration_seconds
            throughput_mb_s = 0.0
            if end_to_end_latency_seconds > 0.0:
                throughput_mb_s = (float(file_bytes) / 1024.0 / 1024.0) / end_to_end_latency_seconds

            def phase_seconds(name: str) -> float:
                return float(phase_breakdown.get(name, 0.0))

            def phase_pct(name: str) -> float:
                sec = phase_seconds(name)
                if phase_total <= 0:
                    return 0.0
                return sec * 100.0 / phase_total

            rows.append(
                {
                    "id": row_id,
                    "file_name": file_name_from_input(req.input_path),
                    "experiment_output_path": req.output_path,
                    "edge_ratio": scenario_ratio_pct,
                    "exit_code": proc.returncode,
                    "runtime_seconds": scenario_runtime_seconds,
                    "end_to_end_latency_seconds": round(end_to_end_latency_seconds, 6),
                    "data_path_latency_seconds": round(data_path_latency_seconds, 6),
                    "throughput_mb_s": round(throughput_mb_s, 6),
                    "file_size_bytes": file_bytes,
                    "file_size_mb": round(float(file_bytes) / 1024.0 / 1024.0, 6),
                    "chunk_count": file_chunks,
                    "cloud_full_download": cloud_full_download,
                    "edge_bytes": int(edge["bytes"]),
                    "edge_chunks_requested": int(edge["chunks_requested"]),
                    "edge_chunks_found": int(edge["chunks_found"]),
                    "edge_batches": int(edge["batches"]),
                    "edge_rpc_seconds": round(edge["rpc_seconds"], 6),
                    "edge_transfer_seconds": round(edge["transfer_seconds"], 6),
                    "cloud_bytes": int(cloud["bytes"]),
                    "cloud_chunks_requested": int(cloud["chunks_requested"]),
                    "cloud_chunks_found": int(cloud["chunks_found"]),
                    "cloud_batches": int(cloud["batches"]),
                    "cloud_rpc_seconds": round(cloud["rpc_seconds"], 6),
                    "cloud_transfer_seconds": round(cloud["transfer_seconds"], 6),
                    "edge_byte_ratio": round((edge["bytes"] / total_node_bytes) if total_node_bytes > 0 else 0.0, 6),
                    "cloud_byte_ratio": round((cloud["bytes"] / total_node_bytes) if total_node_bytes > 0 else 0.0, 6),
                    "phase_setup_seconds": round(phase_seconds("setup"), 6),
                    "phase_setup_pct": round(phase_pct("setup"), 6),
                    "phase_recipe_scan_seconds": round(phase_seconds("recipe_scan"), 6),
                    "phase_recipe_scan_pct": round(phase_pct("recipe_scan"), 6),
                    "phase_window_prepare_seconds": round(phase_seconds("window_prepare"), 6),
                    "phase_window_prepare_pct": round(phase_pct("window_prepare"), 6),
                    "phase_edge_fetch_seconds": round(phase_seconds("edge_fetch"), 6),
                    "phase_edge_fetch_pct": round(phase_pct("edge_fetch"), 6),
                    "phase_cloud_fetch_seconds": round(phase_seconds("cloud_fallback_fetch"), 6),
                    "phase_cloud_fetch_pct": round(phase_pct("cloud_fallback_fetch"), 6),
                    "phase_output_write_seconds": round(phase_seconds("output_write"), 6),
                    "phase_output_write_pct": round(phase_pct("output_write"), 6),
                    "stats_snapshot": stat_snap_path,
                    "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-10:]),
                    "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-10:]),
                }
            )
            row_id += 1

        print(
            f"[export] scenario={label} completed exit={proc.returncode} "
            f"runtime={scenario_runtime_seconds:.6f}s files={n}"
        )
        if proc.returncode != 0 and not args.continue_on_error:
            print("[export] stopped due to failure (--no-continue-on-error).", file=sys.stderr)
            break

    fieldnames = [
        "id",
        "file_name",
        "experiment_output_path",
        "edge_ratio",
        "exit_code",
        "runtime_seconds",
        "end_to_end_latency_seconds",
        "data_path_latency_seconds",
        "throughput_mb_s",
        "file_size_bytes",
        "file_size_mb",
        "chunk_count",
        "cloud_full_download",
        "edge_bytes",
        "edge_chunks_requested",
        "edge_chunks_found",
        "edge_batches",
        "edge_rpc_seconds",
        "edge_transfer_seconds",
        "cloud_bytes",
        "cloud_chunks_requested",
        "cloud_chunks_found",
        "cloud_batches",
        "cloud_rpc_seconds",
        "cloud_transfer_seconds",
        "edge_byte_ratio",
        "cloud_byte_ratio",
        "phase_setup_seconds",
        "phase_setup_pct",
        "phase_recipe_scan_seconds",
        "phase_recipe_scan_pct",
        "phase_window_prepare_seconds",
        "phase_window_prepare_pct",
        "phase_edge_fetch_seconds",
        "phase_edge_fetch_pct",
        "phase_cloud_fetch_seconds",
        "phase_cloud_fetch_pct",
        "phase_output_write_seconds",
        "phase_output_write_pct",
        "stats_snapshot",
        "stdout_tail",
        "stderr_tail",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    json_path.write_text(json.dumps({"rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== Restore Experiment CSV Export ===")
    print(f"report_dir: {report_dir}")
    print(f"csv: {csv_path}")
    print(f"json: {json_path}")
    print(f"rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
