#!/usr/bin/env python3
"""
Build fileInfo from a raw dataset, then launch the existing experiment matrix runner.

This script is a thin orchestration layer around:
1. scripts/fileinfo_builder/build_fileInfo_from_dataset.go
2. scripts/experiments/run_algorithm_input_cap_matrix.py

It is intended to provide one entrypoint for:
- raw dataset path
- file heat generation parameters (Zipf / CSV override)
- algorithm selection
- experiment parameters
"""

from __future__ import annotations

import argparse
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def run_cmd(cmd: List[str], *, cwd: Path) -> None:
    print(f"[run] {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(cwd), check=True)


def slugify_path_name(path: Path) -> str:
    parts = [p for p in path.resolve().parts if p not in ("/", "")]
    if not parts:
        return "dataset"
    tail = parts[-2:] if len(parts) >= 2 else parts
    text = "_".join(tail)
    out = []
    for ch in text:
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    slug = "".join(out)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_") or "dataset"


def format_chunk_label(chunk_bytes: int) -> str:
    if chunk_bytes % (1024 * 1024) == 0:
        return f"{chunk_bytes // (1024 * 1024)}MB"
    if chunk_bytes % 1024 == 0:
        return f"{chunk_bytes // 1024}KB"
    return f"{chunk_bytes}B"


def format_zipf_label(zipf_s: float) -> str:
    text = f"{zipf_s:.6g}"
    return text.replace(".", "p")


ALGORITHM_ALIASES: Dict[str, str] = {
    "cloud_only": "disDedup/scripts/case1_cluster/run_case1_cluster_cloud_only.py",
    "edge_only": "disDedup/scripts/case1_cluster/run_case1_cluster_edge_only.py",
    "mean": "disDedup/scripts/case1_cluster/run_case1_cluster.py",
    "heat_only": "disDedup/scripts/case1_cluster/run_case1_cluster_heat_only_spread.py",
    "theoretical_weighted": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted.py",
    "theoretical_v3": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted_version3.py",
    "theoretical_v4": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted_version4.py",
    "topfirst_v1": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted_topfirst_version1.0.py",
    "topfirst_v2": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted_topfirst_version2.0.py",
    "directprotect_v1": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted_directprotect_v1.py",
    "directprotect_v2": "disDedup/scripts/case1_cluster/run_case1_cluster_theoretical_weighted_directprotect_v2.py",
    "dedup_only": "disDedup/scripts/method/Dedup_only.py",
    "greedy_benefit_size": "disDedup/scripts/method/Greedy_benefit_size.py",
    "graph_community": "disDedup/scripts/method/GraphCommunity_benefit_size.py",
}


def resolve_algorithm(repo_root: Path, value: str) -> Path:
    candidate = ALGORITHM_ALIASES.get(value, value)
    path = Path(candidate)
    if not path.is_absolute():
        path = (repo_root / path).resolve()
    else:
        path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(f"algorithm script not found: {value} -> {path}")
    return path


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    default_builder = repo_root / "disDedup" / "scripts" / "fileinfo_builder"
    default_matrix_runner = repo_root / "disDedup" / "scripts" / "experiments" / "run_algorithm_input_cap_matrix.py"
    default_env_file = repo_root / "disDedup" / "compose.paths.env"

    parser = argparse.ArgumentParser(
        description="One-shot dataset -> fileInfo -> experiment matrix runner entrypoint."
    )
    parser.add_argument("--dataset-root", required=True, help="raw dataset root directory")
    parser.add_argument(
        "--method",
        action="append",
        required=True,
        help=(
            "algorithm alias or script path; repeat for multiple methods. "
            f"Known aliases: {', '.join(sorted(ALGORITHM_ALIASES.keys()))}"
        ),
    )
    parser.add_argument(
        "--fileinfo-json",
        default="",
        help="explicit output fileInfo JSON path; default is auto-generated under scripts/data/input/generated",
    )
    parser.add_argument(
        "--fileinfo-dir",
        default="",
        help="base directory for auto-generated fileInfo JSON when --fileinfo-json is empty",
    )
    parser.add_argument("--chunk-bytes", type=int, default=8192, help="FastCDC target chunk size")
    parser.add_argument("--zipf-s", type=float, default=0.7, help="Zipf exponent for default file heat")
    parser.add_argument("--zipf-random", action=argparse.BooleanOptionalAction, default=False, help="shuffle Zipf ranks")
    parser.add_argument("--zipf-seed", type=int, default=42, help="Zipf shuffle seed used with --zipf-random")
    parser.add_argument("--popularity-csv", default="", help="optional CSV with columns file,heat")
    parser.add_argument("--progress-every", type=int, default=500, help="builder progress print interval")
    parser.add_argument("--force-rebuild-fileinfo", action="store_true", help="rebuild fileInfo JSON even if it exists")
    parser.add_argument("--base-env-file", default=str(default_env_file), help="forwarded to matrix runner")
    parser.add_argument("--shared-cloud-root", default="", help="forwarded to matrix runner")
    parser.add_argument("--suite-root-base", default="", help="matrix output base dir")
    parser.add_argument("--run-label", default="", help="optional suffix for generated output root")
    parser.add_argument("--cap-values", default="25", help="comma-separated capacity percentages")
    parser.add_argument("--cap-start", type=int, default=20, help="forwarded when --cap-values is empty")
    parser.add_argument("--cap-end", type=int, default=30, help="forwarded when --cap-values is empty")
    parser.add_argument("--cap-step", type=int, default=5, help="forwarded when --cap-values is empty")
    parser.add_argument("--num-shuffles", type=int, default=0, help="forwarded to matrix runner")
    parser.add_argument(
        "--include-original",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="forwarded to matrix runner",
    )
    parser.add_argument("--shuffle-seed-start", type=int, default=1, help="forwarded to matrix runner")
    parser.add_argument("--k", type=int, default=1000, help="forwarded to matrix runner")
    parser.add_argument("--alpha", type=int, default=5, help="forwarded to matrix runner")
    parser.add_argument("--server-num", type=int, default=10, help="forwarded to matrix runner")
    parser.add_argument("--parallel-edge-nodes", type=int, default=0, help="forwarded to matrix runner")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (os.cpu_count() or 1) - 1),
        help="forwarded to matrix runner",
    )
    parser.add_argument("--restore-batch-size", type=int, default=128, help="forwarded to matrix runner")
    parser.add_argument(
        "--dedup-mode",
        choices=["real", "synthetic", "synthetic_edge", "synthetic_manager"],
        default="synthetic_edge",
        help="forwarded to matrix runner",
    )
    parser.add_argument(
        "--synthetic-dataset-prefix",
        default="",
        help="forwarded to matrix runner; default is inferred from dataset-root",
    )
    parser.add_argument("--protect-top-ratio", type=float, default=0.25, help="forwarded when supported by method")
    parser.add_argument("--request-count", type=int, default=0, help="forwarded to matrix runner")
    parser.add_argument("--request-multiplier", type=int, default=2, help="forwarded to matrix runner")
    parser.add_argument("--request-seed", type=int, default=42, help="forwarded to matrix runner")
    parser.add_argument(
        "--prepare-cloud-mode",
        choices=["auto", "always", "never"],
        default="auto",
        help="forwarded to matrix runner",
    )
    parser.add_argument(
        "--cleanup-edge-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="forwarded to matrix runner",
    )
    parser.add_argument(
        "--method-arg",
        action="append",
        default=[],
        help="extra raw arg forwarded to each algorithm invocation; repeat as needed",
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="only build fileInfo JSON and print its path; do not launch experiments",
    )
    parser.add_argument(
        "--fileinfo-builder-dir",
        default=str(default_builder),
        help="directory of build_fileInfo_from_dataset.go",
    )
    parser.add_argument(
        "--matrix-runner",
        default=str(default_matrix_runner),
        help="path to run_algorithm_input_cap_matrix.py",
    )
    return parser.parse_args()


def build_fileinfo_json_path(repo_root: Path, args: argparse.Namespace, dataset_root: Path) -> Path:
    if args.fileinfo_json:
        return Path(args.fileinfo_json).resolve()

    if args.fileinfo_dir:
        base_dir = Path(args.fileinfo_dir).resolve()
    else:
        base_dir = repo_root / "disDedup" / "scripts" / "data" / "input" / "generated"

    dataset_slug = slugify_path_name(dataset_root)
    chunk_label = format_chunk_label(args.chunk_bytes)
    heat_label = f"zipf-s{format_zipf_label(args.zipf_s)}"
    random_suffix = f"-seed{args.zipf_seed}" if args.zipf_random else ""
    filename = f"fileInfo-{dataset_slug}-{heat_label}-{chunk_label}{random_suffix}.json"
    return (base_dir / filename).resolve()


def maybe_build_fileinfo(
    args: argparse.Namespace,
    dataset_root: Path,
    out_json: Path,
) -> None:
    builder_dir = Path(args.fileinfo_builder_dir).resolve()
    if not builder_dir.exists():
        raise FileNotFoundError(f"fileinfo builder dir not found: {builder_dir}")

    if out_json.exists() and not args.force_rebuild_fileinfo:
        print(f"[info] reuse existing fileInfo JSON: {out_json}")
        return

    out_json.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go",
        "run",
        "build_fileInfo_from_dataset.go",
        "--dataset-root",
        str(dataset_root),
        "--output-json",
        str(out_json),
        "--chunk-bytes",
        str(args.chunk_bytes),
        "--zipf-s",
        str(args.zipf_s),
        "--zipf-seed",
        str(args.zipf_seed),
        "--progress-every",
        str(args.progress_every),
        "--pretty",
    ]
    if args.zipf_random:
        cmd.append("--zipf-random")
    if args.popularity_csv:
        cmd.extend(["--popularity-csv", str(Path(args.popularity_csv).resolve())])
    run_cmd(cmd, cwd=builder_dir)


def build_suite_root_base(repo_root: Path, args: argparse.Namespace, dataset_root: Path) -> Path:
    if args.suite_root_base:
        return Path(args.suite_root_base).resolve()

    base_dir = repo_root / "disDedup" / "scripts" / "data" / "output"
    dataset_slug = slugify_path_name(dataset_root)
    zipf_label = format_zipf_label(args.zipf_s)
    chunk_label = format_chunk_label(args.chunk_bytes).lower()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    label = args.run_label.strip()
    suffix = f"_{label}" if label else ""
    return (base_dir / f"auto_runs_{dataset_slug}_s{zipf_label}_{chunk_label}_{stamp}{suffix}").resolve()


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    dataset_root = Path(args.dataset_root).resolve()
    if not dataset_root.exists() or not dataset_root.is_dir():
        raise FileNotFoundError(f"dataset root not found or not a directory: {dataset_root}")

    matrix_runner = Path(args.matrix_runner).resolve()
    if not matrix_runner.exists():
        raise FileNotFoundError(f"matrix runner not found: {matrix_runner}")

    base_env_file = Path(args.base_env_file).resolve()
    if not base_env_file.exists():
        raise FileNotFoundError(f"base env file not found: {base_env_file}")

    fileinfo_json = build_fileinfo_json_path(repo_root, args, dataset_root)
    maybe_build_fileinfo(args, dataset_root, fileinfo_json)
    print(f"[info] fileinfo_json={fileinfo_json}")

    if args.build_only:
        return 0

    suite_root_base = build_suite_root_base(repo_root, args, dataset_root)
    suite_root_base.mkdir(parents=True, exist_ok=True)

    algorithm_paths = [resolve_algorithm(repo_root, item) for item in args.method]

    cmd = [
        "python3",
        str(matrix_runner),
        "--source-json",
        str(fileinfo_json),
        "--dataset-root",
        str(dataset_root),
        "--base-env-file",
        str(base_env_file),
        "--suite-root-base",
        str(suite_root_base),
        "--cap-values",
        args.cap_values,
        "--cap-start",
        str(args.cap_start),
        "--cap-end",
        str(args.cap_end),
        "--cap-step",
        str(args.cap_step),
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
        args.dedup_mode,
        "--synthetic-dataset-prefix",
        args.synthetic_dataset_prefix,
        "--protect-top-ratio",
        str(args.protect_top_ratio),
        "--request-count",
        str(args.request_count),
        "--request-multiplier",
        str(args.request_multiplier),
        "--request-seed",
        str(args.request_seed),
        "--prepare-cloud-mode",
        args.prepare_cloud_mode,
    ]
    if args.shared_cloud_root:
        cmd.extend(["--shared-cloud-root", str(Path(args.shared_cloud_root).resolve())])
    if args.include_original:
        cmd.append("--include-original")
    else:
        cmd.append("--no-include-original")
    if args.cleanup_edge_data:
        cmd.append("--cleanup-edge-data")
    else:
        cmd.append("--no-cleanup-edge-data")
    for algorithm_path in algorithm_paths:
        cmd.extend(["--algorithm-script", str(algorithm_path)])
    for extra_arg in args.method_arg:
        cmd.extend(["--algorithm-extra-arg", extra_arg])

    print(f"[info] suite_root_base={suite_root_base}")
    run_cmd(cmd, cwd=repo_root)

    summary_json = suite_root_base / "matrix_summary.json"
    summary_csv = suite_root_base / "matrix_summary.csv"
    failed_json = suite_root_base / "failed_cells.json"
    failed_csv = suite_root_base / "failed_cells.csv"
    print(f"[summary] suite_root_base={suite_root_base}")
    print(f"[summary] fileinfo_json={fileinfo_json}")
    print(f"[summary] matrix_summary_json={summary_json}")
    print(f"[summary] matrix_summary_csv={summary_csv}")
    print(f"[summary] failed_cells_json={failed_json}")
    print(f"[summary] failed_cells_csv={failed_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
