#!/usr/bin/env python3
"""
Replay restore requests by Poisson arrival trace and aggregate performance stats.

Arrival file format (TSV):
  # arrival_sec    input_path    output_path
  0.123456 /input/a /output/a
  0.456789 /input/b /output/b
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class Arrival:
    arrival_sec: float
    input_path: str
    output_path: str


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


def parse_arrivals(arrival_file: Path) -> list[Arrival]:
    rows: list[Arrival] = []
    for lineno, raw in enumerate(arrival_file.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            parts = line.split()
        if len(parts) < 3:
            raise ValueError(f"{arrival_file}:{lineno}: invalid line, need 3 fields")
        t = float(parts[0])
        rows.append(Arrival(arrival_sec=t, input_path=parts[1].strip(), output_path=parts[2].strip()))
    if not rows:
        raise ValueError(f"no valid arrivals found in {arrival_file}")
    rows.sort(key=lambda x: x.arrival_sec)
    return rows


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    return values[f] + (values[c] - values[f]) * (k - f)


def mbps(byte_count: int, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return float(byte_count) / 1024.0 / 1024.0 / seconds


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


def summarize(
    run_records: list[dict[str, Any]],
    arrival_rows: list[Arrival],
    wallclock_sec: float,
) -> dict[str, Any]:
    total_runs = len(run_records)
    success_runs = sum(1 for r in run_records if r["exit_code"] == 0)
    failed_runs = total_runs - success_runs

    total_files = 0
    total_chunks = 0
    total_bytes = 0
    per_file_durations: list[float] = []
    per_file_bytes: list[int] = []
    service_seconds_sum = 0.0
    cloud_full_files = 0
    service_latencies: list[float] = []
    orchestration_overheads: list[float] = []

    for r in run_records:
        runtime_sec = float(r.get("runtime_seconds", 0.0))
        run_service_sec = 0.0
        stat = r.get("stats")
        if not stat:
            continue
        files = stat.get("files", [])
        total_files += len(files)
        for f in files:
            b = int(f.get("bytes", 0))
            c = int(f.get("chunks", 0))
            d = float(f.get("duration_seconds", 0.0))
            total_bytes += b
            total_chunks += c
            per_file_bytes.append(b)
            per_file_durations.append(d)
            service_seconds_sum += d
            run_service_sec += d
            if bool(f.get("cloud_full_download", False)):
                cloud_full_files += 1
        if run_service_sec > 0:
            service_latencies.append(run_service_sec)
            orchestration_overheads.append(max(0.0, runtime_sec - run_service_sec))

    arrivals = [x.arrival_sec for x in arrival_rows]
    inter_arrivals = [arrivals[0]] + [arrivals[i] - arrivals[i - 1] for i in range(1, len(arrivals))]
    inter_arrivals = [max(0.0, x) for x in inter_arrivals]

    service_sorted = sorted(service_latencies)
    overhead_sorted = sorted(orchestration_overheads)
    orchestration_overhead_sum = float(sum(orchestration_overheads))

    return {
        "requests_total": total_runs,
        "requests_success": success_runs,
        "requests_failed": failed_runs,
        "files_total": total_files,
        "chunks_total": total_chunks,
        "bytes_total": total_bytes,
        "mb_total": float(total_bytes) / 1024.0 / 1024.0,
        "cloud_full_download_files": cloud_full_files,
        "wallclock_seconds": wallclock_sec,
        "effective_throughput_mb_s": mbps(total_bytes, wallclock_sec),
        "throughput_effective_mb_s": mbps(total_bytes, wallclock_sec),
        "restore_service_seconds_sum": service_seconds_sum,
        "service_latency_seconds_sum": service_seconds_sum,
        "service_latency_seconds_avg": (statistics.mean(service_latencies) if service_latencies else 0.0),
        "service_latency_seconds_p50": percentile(service_sorted, 0.50) if service_sorted else 0.0,
        "service_latency_seconds_p95": percentile(service_sorted, 0.95) if service_sorted else 0.0,
        "orchestration_overhead_seconds_sum": orchestration_overhead_sum,
        "orchestration_overhead_seconds_avg": (statistics.mean(orchestration_overheads) if orchestration_overheads else 0.0),
        "orchestration_overhead_seconds_p50": percentile(overhead_sorted, 0.50) if overhead_sorted else 0.0,
        "orchestration_overhead_seconds_p95": percentile(overhead_sorted, 0.95) if overhead_sorted else 0.0,
        "throughput_restore_service_mb_s": mbps(total_bytes, service_seconds_sum),
        "avg_bytes_per_file": (float(total_bytes) / total_files) if total_files > 0 else 0.0,
        "avg_chunks_per_file": (float(total_chunks) / total_files) if total_files > 0 else 0.0,
        "avg_duration_per_file_seconds": (statistics.mean(per_file_durations) if per_file_durations else 0.0),
        "p50_duration_per_file_seconds": percentile(sorted(per_file_durations), 0.50) if per_file_durations else 0.0,
        "p95_duration_per_file_seconds": percentile(sorted(per_file_durations), 0.95) if per_file_durations else 0.0,
        "arrival_count": len(arrival_rows),
        "arrival_last_sec": arrivals[-1] if arrivals else 0.0,
        "arrival_inter_sec_avg": (statistics.mean(inter_arrivals) if inter_arrivals else 0.0),
    }


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent.parent

    parser = argparse.ArgumentParser(description="Replay restore requests by Poisson arrival and aggregate stats.")
    parser.add_argument("--arrival-file-host", required=True, help="host path of arrival TSV")
    parser.add_argument("--env-file", default=str(root_dir / "compose.paths.env"), help="compose env file")
    parser.add_argument("--compose-file", default=str(root_dir / "docker-compose.yml"), help="compose file")
    parser.add_argument("--manager-config", default="/cfg/manager.yaml", help="manager config path in container")
    parser.add_argument("--manager-service", default="manager", help="docker compose service name")
    parser.add_argument("--placement-json", default="", help="override manager -placement-json in container")
    parser.add_argument("--placement-dir", default="", help="override manager -placement-dir in container")
    parser.add_argument("--placement-strategy", default="", help="override manager -placement-strategy (round_robin|random)")
    parser.add_argument("--cloud-only", action=argparse.BooleanOptionalAction, default=False, help="use restore-cloud-batch (cloud full restore only)")
    parser.add_argument("--time-scale", type=float, default=1.0, help="arrival speed scale (>1 faster, <1 slower)")
    parser.add_argument("--max-requests", type=int, default=0, help="limit replay count (0 means all)")
    parser.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--report-dir-host", default="", help="host dir for replay reports (default under STORE_ROOT/manager)")
    parser.add_argument("--stats-file-host", default="", help="host restore stats json path override")
    args = parser.parse_args()

    arrival_file = Path(args.arrival_file_host).resolve()
    env_file = Path(args.env_file).resolve()
    compose_file = Path(args.compose_file).resolve()

    if not arrival_file.exists():
        print(f"ERROR: arrival file not found: {arrival_file}", file=sys.stderr)
        return 2
    if not env_file.exists():
        print(f"ERROR: env file not found: {env_file}", file=sys.stderr)
        return 2
    if not compose_file.exists():
        print(f"ERROR: compose file not found: {compose_file}", file=sys.stderr)
        return 2
    if args.time_scale <= 0:
        print("ERROR: --time-scale must be > 0", file=sys.stderr)
        return 2

    env_map = parse_env_file(env_file)
    store_root = env_map.get("STORE_ROOT", "").strip()
    if not store_root:
        print(f"ERROR: STORE_ROOT is empty in {env_file}", file=sys.stderr)
        return 2
    store_root_path = Path(store_root).resolve()
    manager_host_dir = store_root_path / "manager"

    now_tag = time.strftime("%Y%m%d_%H%M%S")
    report_dir = Path(args.report_dir_host).resolve() if args.report_dir_host else (manager_host_dir / f"restore_poisson_replay_{now_tag}")
    req_dir = report_dir / "requests"
    stats_snap_dir = report_dir / "stats_snapshots"
    req_dir.mkdir(parents=True, exist_ok=True)
    stats_snap_dir.mkdir(parents=True, exist_ok=True)

    stats_file_host = Path(args.stats_file_host).resolve() if args.stats_file_host else (manager_host_dir / "stats" / "restore_stats.json")
    arrivals = parse_arrivals(arrival_file)
    if args.max_requests > 0:
        arrivals = arrivals[: args.max_requests]

    try:
        manager_container_id = detect_manager_container_id(compose_file, env_file, args.manager_service)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    print(f"[replay] manager container: {manager_container_id}")

    # request file path used by manager container (/data is mapped to STORE_ROOT/manager)
    manager_dir_resolved = manager_host_dir.resolve()
    run_records: list[dict[str, Any]] = []
    replay_start = time.time()
    prev_arrival = 0.0
    next_progress_pct = 10
    total_requests = len(arrivals)

    for i, row in enumerate(arrivals, start=1):
        dt = max(0.0, row.arrival_sec - prev_arrival) / args.time_scale
        prev_arrival = row.arrival_sec
        if dt > 0:
            time.sleep(dt)

        req_host = req_dir / f"req_{i:06d}.txt"
        req_host.write_text(f"{row.input_path}\t{row.output_path}\n", encoding="utf-8")
        req_host_abs = req_host.resolve()
        try:
            rel = req_host_abs.relative_to(manager_dir_resolved)
            req_container = f"/data/{rel.as_posix()}"
        except Exception:
            print(f"ERROR: request file must be under {manager_dir_resolved}: {req_host_abs}", file=sys.stderr)
            return 2

        manager_cmd = "restore-cloud-batch" if args.cloud_only else "restore-batch"
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
        cmd.extend([manager_cmd, req_container])
        run_start = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True)
        run_end = time.time()

        stat_obj = None
        stat_snap_path = None
        if stats_file_host.exists():
            try:
                stat_obj = json.loads(stats_file_host.read_text(encoding="utf-8"))
                stat_snap_path = stats_snap_dir / f"restore_stats_{i:06d}.json"
                stat_snap_path.write_text(json.dumps(stat_obj, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                stat_obj = None
                stat_snap_path = None

        record = {
            "index": i,
            "arrival_sec": row.arrival_sec,
            "sleep_seconds": dt,
            "request_file_host": str(req_host_abs),
            "request_file_container": req_container,
            "input_path": row.input_path,
            "output_path": row.output_path,
            "exit_code": proc.returncode,
            "runtime_seconds": run_end - run_start,
            "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-20:]),
            "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-20:]),
            "stats_snapshot": str(stat_snap_path) if stat_snap_path else "",
            "stats": stat_obj,
        }
        run_records.append(record)

        pct = int(i * 100.0 / total_requests) if total_requests > 0 else 100
        if pct >= next_progress_pct or i == total_requests:
            print(
                f"[replay] progress={pct}% ({i}/{total_requests}) "
                f"last_exit={proc.returncode} last_runtime={run_end-run_start:.3f}s"
            )
            while next_progress_pct <= pct:
                next_progress_pct += 10
        if proc.returncode != 0 and not args.continue_on_error:
            print("[replay] stopped due to failure (--no-continue-on-error).", file=sys.stderr)
            break

    replay_end = time.time()
    wallclock = replay_end - replay_start
    summary = summarize(run_records, arrivals[: len(run_records)], wallclock)

    detail_json = report_dir / "replay_detail.json"
    summary_json = report_dir / "summary.json"
    detail_json.write_text(json.dumps({"runs": run_records}, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== Replay Summary ===")
    print(f"report_dir: {report_dir}")
    print(f"requests: {summary['requests_success']}/{summary['requests_total']} success")
    print(f"files_total: {summary['files_total']}")
    print(f"mb_total: {summary['mb_total']:.2f}")
    print(f"wallclock_seconds: {summary['wallclock_seconds']:.3f}")
    print(f"service_latency_seconds: avg={summary['service_latency_seconds_avg']:.4f} p50={summary['service_latency_seconds_p50']:.4f} p95={summary['service_latency_seconds_p95']:.4f}")
    print(
        "orchestration_overhead_seconds: "
        f"avg={summary['orchestration_overhead_seconds_avg']:.4f} "
        f"p50={summary['orchestration_overhead_seconds_p50']:.4f} "
        f"p95={summary['orchestration_overhead_seconds_p95']:.4f}"
    )
    print(f"effective_throughput_mb_s: {summary['effective_throughput_mb_s']:.2f}")
    print(f"throughput_restore_service_mb_s: {summary['throughput_restore_service_mb_s']:.2f}")
    print(f"avg_duration_per_file_seconds: {summary['avg_duration_per_file_seconds']:.4f}")
    print(f"p50_duration_per_file_seconds: {summary['p50_duration_per_file_seconds']:.4f}")
    print(f"p95_duration_per_file_seconds: {summary['p95_duration_per_file_seconds']:.4f}")
    print(f"summary_json: {summary_json}")
    print(f"detail_json: {detail_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
