#!/usr/bin/env python3
"""
Run a matrix of placement experiments across:
- multiple algorithm scripts
- multiple source fileInfo JSONs
- multiple capacity ratios

Each matrix cell delegates to run_topfirst_shuffle_suite.py, which by default
means:
- original source JSON + 4 shuffled variants
- isolated STORE_ROOT per run
- shared fixed cloud root from compose.paths.env unless overridden
- per-cell averaging across the 5 runs
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def run_cmd(cmd: List[str], *, cwd: Path, log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[run] {' '.join(cmd)}")
    with log_path.open("w", encoding="utf-8") as fh:
        fh.write(f"$ {' '.join(cmd)}\n\n")
        fh.flush()
        subprocess.run(cmd, cwd=str(cwd), stdout=fh, stderr=subprocess.STDOUT, check=True)


def parse_caps(cap_values: str, cap_start: int, cap_end: int, cap_step: int) -> List[int]:
    if cap_values.strip():
        out: List[int] = []
        for item in cap_values.split(","):
            v = int(item.strip())
            if v <= 0 or v > 100:
                raise ValueError(f"invalid cap value: {v}")
            out.append(v)
        return out
    if cap_step <= 0:
        raise ValueError("--cap-step must be positive")
    if cap_end < cap_start:
        raise ValueError("--cap-end must be >= --cap-start")
    return list(range(cap_start, cap_end + 1, cap_step))


def load_summary(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def flatten_average_metrics(row: Dict[str, object], avg: Dict[str, object]) -> Dict[str, object]:
    out = dict(row)
    for key, value in avg.items():
        out[key] = value
    return out


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


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


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    disdedup_dir = repo_root / "disDedup"
    default_runner = disdedup_dir / "scripts" / "experiments" / "run_topfirst_shuffle_suite.py"

    parser = argparse.ArgumentParser(description="Run a matrix of algorithm/input/cap shuffled-heat experiment suites.")
    parser.add_argument(
        "--algorithm-script",
        action="append",
        required=True,
        help="algorithm script path under scripts/case1_cluster; repeat for multiple algorithms",
    )
    parser.add_argument(
        "--source-json",
        action="append",
        required=True,
        help="source fileInfo json path; repeat for multiple inputs",
    )
    parser.add_argument("--dataset-root", default="", help="host dataset root; forwarded to suite runner")
    parser.add_argument("--base-env-file", default="", help="base compose env file; forwarded to suite runner")
    parser.add_argument("--suite-root-base", default="", help="base directory for matrix outputs")
    parser.add_argument("--shared-cloud-root", default="", help="shared cloud root override; forwarded to suite runner")
    parser.add_argument("--suite-runner", default=str(default_runner), help="path to run_topfirst_shuffle_suite.py")
    parser.add_argument("--cap-values", default="", help="comma-separated capacity percentages, e.g. 20,25,30")
    parser.add_argument("--cap-start", type=int, default=20, help="capacity percentage start when --cap-values is empty")
    parser.add_argument("--cap-end", type=int, default=30, help="capacity percentage end when --cap-values is empty")
    parser.add_argument("--cap-step", type=int, default=5, help="capacity percentage step when --cap-values is empty")
    parser.add_argument("--num-shuffles", type=int, default=0, help="forwarded to suite runner")
    parser.add_argument(
        "--include-original",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="forwarded to suite runner",
    )
    parser.add_argument("--shuffle-seed-start", type=int, default=1, help="forwarded to suite runner")
    parser.add_argument("--k", type=int, default=1000, help="forwarded to suite runner")
    parser.add_argument("--alpha", type=int, default=5, help="forwarded to suite runner")
    parser.add_argument("--server-num", type=int, default=10, help="forwarded to suite runner")
    parser.add_argument("--parallel-edge-nodes", type=int, default=0, help="forwarded to suite runner")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 1) - 1), help="forwarded to suite runner")
    parser.add_argument("--restore-batch-size", type=int, default=128, help="forwarded to suite runner")
    parser.add_argument(
        "--dedup-mode",
        choices=["real", "synthetic", "synthetic_edge", "synthetic_manager"],
        default="synthetic_edge",
        help="forwarded to suite runner",
    )
    parser.add_argument(
        "--synthetic-dataset-prefix",
        default="",
        help="forwarded when --dedup-mode is a synthetic variant; default is inferred by the suite runner",
    )
    parser.add_argument("--protect-top-ratio", type=float, default=0.25, help="forwarded when the algorithm supports it")
    parser.add_argument("--request-count", type=int, default=0, help="forwarded to suite runner")
    parser.add_argument("--request-multiplier", type=int, default=2, help="forwarded to suite runner")
    parser.add_argument("--request-seed", type=int, default=42, help="forwarded to suite runner")
    parser.add_argument(
        "--prepare-cloud-mode",
        choices=["auto", "always", "never"],
        default="auto",
        help="forwarded to suite runner",
    )
    parser.add_argument(
        "--cleanup-edge-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="forwarded to suite runner; delete per-run edge node data after compose down",
    )
    parser.add_argument(
        "--algorithm-extra-arg",
        action="append",
        default=[],
        help="extra arg forwarded to each algorithm invocation inside the suite runner; repeat as needed",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    suite_runner = Path(args.suite_runner).resolve()
    if not suite_runner.exists():
        raise FileNotFoundError(f"suite runner not found: {suite_runner}")

    algorithm_scripts = [Path(x).resolve() for x in args.algorithm_script]
    source_jsons = [Path(x).resolve() for x in args.source_json]
    for path in algorithm_scripts + source_jsons:
        if not path.exists():
            raise FileNotFoundError(f"path not found: {path}")

    caps = parse_caps(args.cap_values, args.cap_start, args.cap_end, args.cap_step)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.suite_root_base:
        root_base = Path(args.suite_root_base).resolve()
    else:
        root_base = repo_root / "disDedup" / "scripts" / "data" / "output" / f"matrix_runs_{stamp}"
    root_base.mkdir(parents=True, exist_ok=True)

    summary_rows: List[Dict[str, object]] = []
    failed_cells: List[Dict[str, object]] = []

    for algorithm_script in algorithm_scripts:
        algo_stem = algorithm_script.stem
        for source_json in source_jsons:
            input_stem = source_json.stem
            for cap in caps:
                cap_ratio = float(cap) / 100.0
                suite_root = root_base / algo_stem / f"cap{cap}" / input_stem
                log_path = suite_root / "matrix_driver.log"

                cmd = [
                    "python3",
                    str(suite_runner),
                    "--algorithm-script",
                    str(algorithm_script),
                    "--source-json",
                    str(source_json),
                    "--suite-root",
                    str(suite_root),
                    "--capacity-ratio",
                    str(cap_ratio),
                    "--protect-top-ratio",
                    str(args.protect_top_ratio),
                    "--num-shuffles",
                    str(args.num_shuffles),
                    "--shuffle-seed-start",
                    str(args.shuffle_seed_start),
                    "--k",
                    str(args.k),
                    "--alpha",
                    str(args.alpha),
                    "--server-num",
                    str(args.server_num),
                    "--parallel-edge-nodes",
                    str(args.parallel_edge_nodes),
                    "--workers",
                    str(max(1, args.workers)),
                    "--restore-batch-size",
                    str(args.restore_batch_size),
                    "--dedup-mode",
                    str(args.dedup_mode),
                    "--synthetic-dataset-prefix",
                    str(args.synthetic_dataset_prefix),
                    "--request-count",
                    str(args.request_count),
                    "--request-multiplier",
                    str(args.request_multiplier),
                    "--request-seed",
                    str(args.request_seed),
                    "--prepare-cloud-mode",
                    str(args.prepare_cloud_mode),
                ]
                if args.cleanup_edge_data:
                    cmd.append("--cleanup-edge-data")
                else:
                    cmd.append("--no-cleanup-edge-data")
                if args.include_original:
                    cmd.append("--include-original")
                else:
                    cmd.append("--no-include-original")
                if args.dataset_root:
                    cmd.extend(["--dataset-root", args.dataset_root])
                if args.base_env_file:
                    cmd.extend(["--base-env-file", args.base_env_file])
                if args.shared_cloud_root:
                    cmd.extend(["--shared-cloud-root", args.shared_cloud_root])
                for extra_arg in args.algorithm_extra_arg:
                    cmd.extend(["--algorithm-extra-arg", extra_arg])

                try:
                    run_cmd(cmd, cwd=repo_root, log_path=log_path)

                    suite_summary_path = suite_root / "summary" / "suite_summary.json"
                    if not suite_summary_path.exists():
                        raise FileNotFoundError(f"suite summary missing: {suite_summary_path}")
                    suite_summary = load_summary(suite_summary_path)
                    row = {
                        "status": suite_summary.get("status", "ok"),
                        "algorithm_script": str(algorithm_script),
                        "algorithm_name": algo_stem,
                        "source_json": str(source_json),
                        "input_name": input_stem,
                        "cap_percent": cap,
                        "cap_ratio": cap_ratio,
                        "suite_root": str(suite_root),
                        "suite_summary_json": str(suite_summary_path),
                        "total_runs": suite_summary.get("params", {}).get("total_runs", ""),
                        "success_count": suite_summary.get("success_count", ""),
                        "failure_count": suite_summary.get("failure_count", ""),
                    }
                    row = flatten_average_metrics(row, suite_summary.get("average_metrics", {}))
                    summary_rows.append(row)
                    if row["status"] != "ok":
                        failed_cells.append(
                            {
                                "status": row["status"],
                                "algorithm_script": str(algorithm_script),
                                "algorithm_name": algo_stem,
                                "source_json": str(source_json),
                                "input_name": input_stem,
                                "cap_percent": cap,
                                "cap_ratio": cap_ratio,
                                "suite_root": str(suite_root),
                                "suite_summary_json": str(suite_summary_path),
                                "failure_error": f"suite_status={row['status']}",
                                "failure_log": str(log_path),
                                "failure_summary": f"see suite failed_runs under {suite_root / 'summary'}",
                            }
                        )
                    print(
                        f"[done] algo={algo_stem} input={input_stem} cap={cap}% "
                        f"status={row['status']} suite={suite_root}"
                    )
                except Exception as exc:
                    failure = {
                        "status": "failed",
                        "algorithm_script": str(algorithm_script),
                        "algorithm_name": algo_stem,
                        "source_json": str(source_json),
                        "input_name": input_stem,
                        "cap_percent": cap,
                        "cap_ratio": cap_ratio,
                        "suite_root": str(suite_root),
                        "suite_summary_json": "",
                        "failure_error": f"{type(exc).__name__}: {exc}",
                        "failure_log": str(log_path),
                        "failure_summary": summarize_failure(log_path, exc),
                        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                    }
                    failed_cells.append(failure)
                    summary_rows.append({k: v for k, v in failure.items() if k != "traceback"})
                    print(
                        f"[failed] algo={algo_stem} input={input_stem} cap={cap}% "
                        f"log={log_path} summary={failure['failure_summary']}"
                    )

    summary_json = root_base / "matrix_summary.json"
    summary_csv = root_base / "matrix_summary.csv"
    failed_json = root_base / "failed_cells.json"
    failed_csv = root_base / "failed_cells.csv"
    summary_json.write_text(json.dumps(summary_rows, ensure_ascii=True, indent=2), encoding="utf-8")
    failed_json.write_text(json.dumps(failed_cells, ensure_ascii=True, indent=2), encoding="utf-8")
    write_csv(summary_csv, summary_rows)
    write_csv(failed_csv, failed_cells)
    print(f"[summary] json={summary_json}")
    print(f"[summary] csv={summary_csv}")
    print(f"[summary] failed_json={failed_json}")
    print(f"[summary] failed_csv={failed_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
