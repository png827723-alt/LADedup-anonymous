#!/usr/bin/env python3
"""Generate placement.json from an existing fileInfo.json.

This is a small convenience wrapper for the placement algorithms under
disDedup/scripts/method. It resolves a user-facing method name, forwards
capacity and algorithm parameters, and writes the algorithm's placement output.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


FRIENDLY_ALIASES: Dict[str, str] = {
    "cloud_only": "run_case1_cluster_cloud_only.py",
    "edge_only": "run_case1_cluster_edge_only.py",
    "mean": "run_case1_cluster.py",
    "heat_only": "run_case1_cluster_heat_only_spread.py",
    "theoretical_weighted": "run_case1_cluster_theoretical_weighted.py",
    "theoretical_v2": "run_case1_cluster_theoretical_weighted_version2.py",
    "theoretical_v3": "run_case1_cluster_theoretical_weighted_version3.py",
    "theoretical_v4": "run_case1_cluster_theoretical_weighted_version4.py",
    "topfirst_v1": "run_case1_cluster_theoretical_weighted_topfirst_version1.0.py",
    "topfirst_v2": "run_case1_cluster_theoretical_weighted_topfirst_version2.0.py",
    "directprotect_v1": "run_case1_cluster_theoretical_weighted_directprotect_v1.py",
    "directprotect_v2": "run_case1_cluster_theoretical_weighted_directprotect_v2.py",
    "dedup_only": "Dedup_only.py",
    "greedy_benefit_size": "Greedy_benefit_size.py",
    "graph_community": "GraphCommunity_benefit_size.py",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def method_dir(root: Path) -> Path:
    return root / "disDedup" / "scripts" / "method"


def discover_methods(root: Path) -> Dict[str, Path]:
    base = method_dir(root)
    if not base.is_dir():
        raise FileNotFoundError(f"method directory not found: {base}")

    methods: Dict[str, Path] = {}
    for script in sorted(base.glob("*.py")):
        if script.name.startswith("__") or script.name == "analyze_edge_chunk_distribution.py":
            continue
        methods[script.name] = script.resolve()
        methods[script.stem] = script.resolve()
        if script.stem.startswith("run_case1_cluster_"):
            methods[script.stem.removeprefix("run_case1_cluster_")] = script.resolve()
    for alias, filename in FRIENDLY_ALIASES.items():
        script = base / filename
        if script.is_file():
            methods[alias] = script.resolve()
    return methods


def script_supports_flag(script_path: Path, flag: str) -> bool:
    text = script_path.read_text(encoding="utf-8")
    return f'"{flag}"' in text or f"'{flag}'" in text


def resolve_algorithm(root: Path, method: str) -> Path:
    methods = discover_methods(root)
    if method in methods:
        return methods[method]

    path = Path(method)
    if not path.is_absolute() and len(path.parts) == 1:
        path = method_dir(root) / path
    elif not path.is_absolute():
        path = root / path
    if path.suffix == "":
        path = path.with_suffix(".py")
    path = path.resolve()
    if not path.is_file():
        known = ", ".join(sorted(methods))
        raise FileNotFoundError(f"algorithm not found: {method} -> {path}\nknown methods: {known}")
    return path


def default_output_path(fileinfo_json: Path, method: str, capacity_ratio: float) -> Path:
    cap_pct = int(round(capacity_ratio * 100.0))
    safe_method = Path(method).stem if method else "placement"
    return fileinfo_json.with_name(f"{fileinfo_json.stem}.{safe_method}.cap{cap_pct}.placement.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate placement.json by running a selected placement algorithm on fileInfo.json."
    )
    parser.add_argument(
        "--method",
        required=False,
        default="",
        help="method name under disDedup/scripts/method, friendly alias, or algorithm script path",
    )
    parser.add_argument("--fileinfo-json", required=False, default="", help="input fileInfo.json")
    parser.add_argument(
        "--output-json",
        default="",
        help="output placement.json; default is next to fileInfo.json with method/cap suffix",
    )
    parser.add_argument("--capacity-ratio", type=float, default=0.0, help="edge capacity ratio, e.g. 0.2")
    parser.add_argument("--cap-percent", type=float, default=0.0, help="edge capacity percent, e.g. 20")
    parser.add_argument("--env-file", default="", help="optional env file forwarded if the algorithm supports --env-file")
    parser.add_argument("--k", type=int, default=1000, help="forwarded if supported")
    parser.add_argument("--alpha", type=int, default=5, help="forwarded if supported")
    parser.add_argument("--server-num", type=int, default=10, help="forwarded if supported")
    parser.add_argument("--parallel-edge-nodes", type=int, default=0, help="forwarded if supported")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (os.cpu_count() or 1) - 1),
        help="forwarded if supported",
    )
    parser.add_argument("--restore-batch-size", type=int, default=128, help="forwarded if supported")
    parser.add_argument("--protect-top-ratio", type=float, default=0.25, help="forwarded if supported")
    parser.add_argument(
        "--method-arg",
        action="append",
        default=[],
        help="extra raw argument forwarded to the algorithm; repeat as needed",
    )
    parser.add_argument("--list-methods", action="store_true", help="print known methods from disDedup/scripts/method and exit")
    parser.add_argument("--dry-run", action="store_true", help="print the algorithm command without running it")
    return parser.parse_args()


def build_algorithm_cmd(
    algorithm_script: Path,
    *,
    fileinfo_json: Path,
    output_json: Path,
    capacity_ratio: float,
    args: argparse.Namespace,
) -> List[str]:
    if not script_supports_flag(algorithm_script, "--input-json"):
        raise ValueError(f"algorithm does not support --input-json: {algorithm_script}")
    if not script_supports_flag(algorithm_script, "--output-json"):
        raise ValueError(f"algorithm does not support --output-json: {algorithm_script}")

    cmd: List[str] = [
        "python3",
        str(algorithm_script),
        "--input-json",
        str(fileinfo_json),
        "--output-json",
        str(output_json),
    ]
    if args.env_file and script_supports_flag(algorithm_script, "--env-file"):
        cmd.extend(["--env-file", str(Path(args.env_file).resolve())])
    if script_supports_flag(algorithm_script, "--capacity-ratio"):
        cmd.extend(["--capacity-ratio", str(capacity_ratio)])
    if script_supports_flag(algorithm_script, "--k"):
        cmd.extend(["--k", str(args.k)])
    if script_supports_flag(algorithm_script, "--alpha"):
        cmd.extend(["--alpha", str(args.alpha)])
    if script_supports_flag(algorithm_script, "--server-num"):
        cmd.extend(["--server-num", str(args.server_num)])
    if script_supports_flag(algorithm_script, "--parallel-edge-nodes"):
        cmd.extend(["--parallel-edge-nodes", str(args.parallel_edge_nodes)])
    if script_supports_flag(algorithm_script, "--workers"):
        cmd.extend(["--workers", str(max(1, args.workers))])
    if script_supports_flag(algorithm_script, "--restore-batch-size"):
        cmd.extend(["--restore-batch-size", str(args.restore_batch_size)])
    if script_supports_flag(algorithm_script, "--protect-top-ratio"):
        cmd.extend(["--protect-top-ratio", str(args.protect_top_ratio)])
    if script_supports_flag(algorithm_script, "--no-progress"):
        cmd.append("--no-progress")
    cmd.extend(args.method_arg)
    return cmd


def main() -> int:
    args = parse_args()
    root = repo_root()

    if args.list_methods:
        for name, path in sorted(discover_methods(root).items()):
            print(f"{name}\t{path.relative_to(root)}")
        return 0

    if not args.method:
        raise ValueError("--method is required unless --list-methods is used")
    if not args.fileinfo_json:
        raise ValueError("--fileinfo-json is required unless --list-methods is used")

    fileinfo_json = Path(args.fileinfo_json).resolve()
    if not fileinfo_json.is_file():
        raise FileNotFoundError(f"fileInfo JSON not found: {fileinfo_json}")

    if args.cap_percent > 0:
        capacity_ratio = float(args.cap_percent) / 100.0
    else:
        capacity_ratio = float(args.capacity_ratio)
    if not (0.0 < capacity_ratio <= 1.0):
        raise ValueError("set --capacity-ratio in (0, 1] or --cap-percent in (0, 100]")

    algorithm_script = resolve_algorithm(root, args.method)
    output_json = Path(args.output_json).resolve() if args.output_json else default_output_path(
        fileinfo_json,
        args.method,
        capacity_ratio,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)

    cmd = build_algorithm_cmd(
        algorithm_script,
        fileinfo_json=fileinfo_json,
        output_json=output_json,
        capacity_ratio=capacity_ratio,
        args=args,
    )
    print(f"[method] {args.method}")
    print(f"[algorithm] {algorithm_script}")
    print(f"[fileinfo] {fileinfo_json}")
    print(f"[capacity_ratio] {capacity_ratio:.6g}")
    print(f"[placement] {output_json}")
    print(f"[run] {' '.join(cmd)}")
    if args.dry_run:
        return 0

    subprocess.run(cmd, cwd=str(root), check=True)
    if not output_json.is_file():
        raise FileNotFoundError(f"algorithm completed but placement was not created: {output_json}")
    print(f"[ok] placement generated: {output_json}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
