#!/usr/bin/env python3
"""
Plot low-capacity hotspot SLA metrics for directprotect_v2 protect sweeps.

This script aggregates only directprotect_v2 runs and focuses on low-capacity
settings by default (5%, 10%, 15%). It produces:

- a per-cell detail CSV for top-k hotspot cloud fallback metrics
- a protect-level summary CSV
- one evaluation figure in both PNG and SVG formats
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Sequence

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter


def parse_int_list(values: str) -> List[int]:
    out: List[int] = []
    for item in values.split(","):
        item = item.strip()
        if not item:
            continue
        out.append(int(item))
    return out


def normalize_restore_input_path(raw_path: str) -> str:
    path = Path(raw_path).as_posix()
    prefix = "/input/github_repo/"
    if path.startswith(prefix):
        return path[len(prefix) :]
    return path


def candidate_run_root(base_dir: Path, protect: int, cap: int, input_name: str) -> Path:
    outer = base_dir / f"run_case1_cluster_theoretical_weighted_directprotect_v2_protect{protect}"
    nested = outer / "run_case1_cluster_theoretical_weighted_directprotect_v2" / f"cap{cap}" / input_name
    if nested.exists():
        return nested
    direct = outer / f"cap{cap}" / input_name
    if direct.exists():
        return direct
    raise FileNotFoundError(
        f"unable to resolve run root for protect={protect}, cap={cap}, input={input_name} under {base_dir}"
    )


def load_cell_metrics(base_dir: Path) -> Dict[tuple[int, str, int], Dict[str, str]]:
    path = base_dir / "all_directprotect_v2_cell_metrics.csv"
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    out: Dict[tuple[int, str, int], Dict[str, str]] = {}
    for row in rows:
        suite_root = row.get("suite_root", "")
        if "run_case1_cluster_theoretical_weighted_directprotect_v2_protect" not in suite_root:
            continue
        key = (
            int(float(row["protect_percent"])),
            row["input_name"],
            int(float(row["cap_percent"])),
        )
        out[key] = row
    return out


def unique_restored_paths(stats_path: Path, valid_paths: Dict[str, float]) -> List[str]:
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    seen = set()
    ordered: List[str] = []
    for item in stats["files"]:
        path = normalize_restore_input_path(str(item["input_path"]))
        if path in valid_paths and path not in seen:
            seen.add(path)
            ordered.append(path)
    return ordered


def load_restore_stats_by_path(stats_path: Path, valid_paths: Dict[str, float]) -> Dict[str, dict]:
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    out: Dict[str, dict] = {}
    for item in stats["files"]:
        path = normalize_restore_input_path(str(item["input_path"]))
        if path in valid_paths and path not in out:
            out[path] = item
    return out


def average(values: Sequence[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


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
        writer.writerows(rows)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plot low-capacity hotspot SLA metrics for directprotect_v2 sweeps.")
    parser.add_argument(
        "--matrix-dir",
        default="disDedup/scripts/data/output/matrix_runs_20260313_231935",
        help="matrix output directory containing directprotect_v2 protect sweep results",
    )
    parser.add_argument(
        "--input-dir",
        default="disDedup/scripts/data/input/8KB",
        help="directory containing fileInfo input JSON files",
    )
    parser.add_argument(
        "--inputs",
        default="fileInfo-zipf-s0.5,fileInfo-zipf-s0.7,fileInfo-zipf-s1.0",
        help="comma-separated input stems to include",
    )
    parser.add_argument(
        "--protects",
        default="0,5,10,15,20,30",
        help="comma-separated protect percentages to include",
    )
    parser.add_argument(
        "--low-caps",
        default="5,10,15",
        help="comma-separated low-capacity percentages to aggregate",
    )
    parser.add_argument(
        "--top-ks",
        default="200,300",
        help="comma-separated top-k hotspot sets used for SLA metrics",
    )
    parser.add_argument(
        "--output-prefix",
        default="plots_directprotect_v2_lowcap_hotspot_sla",
        help="basename for generated figure files inside matrix-dir",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()

    matrix_dir = Path(args.matrix_dir).resolve()
    input_dir = Path(args.input_dir).resolve()
    inputs = [x.strip() for x in args.inputs.split(",") if x.strip()]
    protects = parse_int_list(args.protects)
    low_caps = parse_int_list(args.low_caps)
    top_ks = parse_int_list(args.top_ks)

    cell_metrics = load_cell_metrics(matrix_dir)

    input_heats: Dict[str, Dict[str, float]] = {}
    top_orders: Dict[tuple[str, int], List[str]] = {}
    for input_name in inputs:
        input_json = input_dir / f"{input_name}.json"
        data = json.loads(input_json.read_text(encoding="utf-8"))
        by_path = {str(item["path"]): float(item["heat"]) for item in data["files"]}
        input_heats[input_name] = by_path

        sample_root = candidate_run_root(matrix_dir, protect=15, cap=10, input_name=input_name)
        sample_stats = sample_root / "runs" / "orig" / "store" / "manager" / "stats" / "restore_stats.json"
        restored_paths = unique_restored_paths(sample_stats, by_path)
        restored_paths.sort(key=lambda path: (-by_path[path], path))
        for top_k in top_ks:
            top_orders[(input_name, top_k)] = restored_paths[:top_k]

    detail_rows: List[Dict[str, object]] = []
    summary_rows: List[Dict[str, object]] = []

    for protect in protects:
        row: Dict[str, object] = {"protect_percent": protect}
        restore_totals: List[float] = []
        seen_restore_keys = set()

        for top_k in top_ks:
            counts: List[float] = []
            heat_fracs: List[float] = []
            weighted_avgs: List[float] = []

            for input_name in inputs:
                heats_by_path = input_heats[input_name]
                hotset = top_orders[(input_name, top_k)]
                total_hotset_heat = sum(heats_by_path[path] for path in hotset)

                for cap in low_caps:
                    run_root = candidate_run_root(matrix_dir, protect=protect, cap=cap, input_name=input_name)
                    stats_path = run_root / "runs" / "orig" / "store" / "manager" / "stats" / "restore_stats.json"
                    restore_by_path = load_restore_stats_by_path(stats_path, heats_by_path)

                    cloud_count = sum(1 for path in hotset if restore_by_path[path]["cloud_full_download"])
                    cloud_heat = sum(
                        heats_by_path[path] for path in hotset if restore_by_path[path]["cloud_full_download"]
                    )
                    weighted_avg_duration = (
                        sum(
                            heats_by_path[path] * float(restore_by_path[path]["duration_seconds"])
                            for path in hotset
                        )
                        / total_hotset_heat
                    )

                    counts.append(float(cloud_count))
                    heat_fracs.append(cloud_heat / total_hotset_heat if total_hotset_heat > 0 else 0.0)
                    weighted_avgs.append(weighted_avg_duration)

                    detail_rows.append(
                        {
                            "protect_percent": protect,
                            "input_name": input_name,
                            "cap_percent": cap,
                            "top_k": top_k,
                            "cloud_fallback_count": cloud_count,
                            "cloud_fallback_heat_fraction": cloud_heat / total_hotset_heat if total_hotset_heat > 0 else 0.0,
                            "heat_weighted_avg_restore_seconds": weighted_avg_duration,
                        }
                    )

                    key = (protect, input_name, cap)
                    if key in cell_metrics and key not in seen_restore_keys:
                        restore_totals.append(float(cell_metrics[key]["restore_total_duration_seconds"]))
                        seen_restore_keys.add(key)

            row[f"top{top_k}_avg_cloud_fallback_count"] = average(counts)
            row[f"top{top_k}_max_cloud_fallback_count"] = max(counts) if counts else 0.0
            row[f"top{top_k}_avg_cloud_fallback_heat_fraction"] = average(heat_fracs)
            row[f"top{top_k}_avg_heat_weighted_restore_seconds"] = average(weighted_avgs)

        row["lowcap_avg_restore_total_duration_seconds"] = average(restore_totals)
        summary_rows.append(row)

    summary_csv = matrix_dir / f"{args.output_prefix}_summary.csv"
    detail_csv = matrix_dir / f"{args.output_prefix}_detail.csv"
    write_csv(summary_csv, summary_rows)
    write_csv(detail_csv, detail_rows)

    protects_x = [int(row["protect_percent"]) for row in summary_rows]
    top200_avg_cloud = [float(row["top200_avg_cloud_fallback_count"]) for row in summary_rows]
    top200_max_cloud = [float(row["top200_max_cloud_fallback_count"]) for row in summary_rows]
    top300_avg_cloud = [float(row["top300_avg_cloud_fallback_count"]) for row in summary_rows]
    top300_max_cloud = [float(row["top300_max_cloud_fallback_count"]) for row in summary_rows]
    top300_heat_frac = [float(row["top300_avg_cloud_fallback_heat_fraction"]) for row in summary_rows]
    restore_avg_total = [float(row["lowcap_avg_restore_total_duration_seconds"]) for row in summary_rows]

    plt.style.use("default")
    fig, axes = plt.subplots(2, 2, figsize=(13.5, 8.5), constrained_layout=True)
    fig.suptitle(
        "directprotect_v2 Low-Capacity Hotspot SLA\n"
        "Aggregate over cap=5/10/15 and inputs s=0.5/0.7/1.0",
        fontsize=15,
        fontweight="bold",
    )

    line_blue = "#1f77b4"
    line_orange = "#ff7f0e"
    line_red = "#d62728"
    line_green = "#2ca02c"

    ax = axes[0, 0]
    ax.plot(protects_x, top200_avg_cloud, marker="o", linewidth=2.4, color=line_blue, label="Average")
    ax.plot(protects_x, top200_max_cloud, marker="s", linewidth=2.0, linestyle="--", color=line_red, label="Worst Cell")
    ax.axvline(10, color="#666666", linestyle=":", linewidth=1.4)
    ax.set_title("Top-200 Hotspot SLA")
    ax.set_xlabel("protect_percent")
    ax.set_ylabel("Cloud Fallback Files")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    ax.annotate(
        "protect10+: zero cloud fallback",
        xy=(10, 0),
        xytext=(11.0, max(top200_max_cloud) * 0.35),
        arrowprops={"arrowstyle": "->", "color": "#444444"},
        fontsize=9,
    )

    ax = axes[0, 1]
    ax.plot(protects_x, top300_avg_cloud, marker="o", linewidth=2.4, color=line_orange, label="Average")
    ax.plot(protects_x, top300_max_cloud, marker="s", linewidth=2.0, linestyle="--", color=line_red, label="Worst Cell")
    ax.axvline(15, color="#666666", linestyle=":", linewidth=1.4)
    ax.set_title("Top-300 Hotspot SLA")
    ax.set_xlabel("protect_percent")
    ax.set_ylabel("Cloud Fallback Files")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    ax.annotate(
        "protect15+: strongest top300 coverage",
        xy=(15, top300_avg_cloud[protects_x.index(15)]),
        xytext=(15.7, max(top300_max_cloud) * 0.55),
        arrowprops={"arrowstyle": "->", "color": "#444444"},
        fontsize=9,
    )

    ax = axes[1, 0]
    ax.plot(protects_x, [v * 100.0 for v in top300_heat_frac], marker="o", linewidth=2.4, color=line_green)
    ax.axvline(10, color="#666666", linestyle=":", linewidth=1.2)
    ax.axvline(15, color="#666666", linestyle=":", linewidth=1.2)
    ax.set_title("Top-300 Hotset Heat Served by Cloud")
    ax.set_xlabel("protect_percent")
    ax.set_ylabel("Cloud-Exposed Heat Fraction")
    ax.yaxis.set_major_formatter(PercentFormatter())
    ax.grid(True, axis="y", alpha=0.25)
    ax.annotate(
        "8.77% -> 4.40% -> 1.10%",
        xy=(15, top300_heat_frac[protects_x.index(15)] * 100.0),
        xytext=(7.5, max(top300_heat_frac) * 100.0 * 0.7),
        arrowprops={"arrowstyle": "->", "color": "#444444"},
        fontsize=9,
    )

    ax = axes[1, 1]
    ax.plot(protects_x, restore_avg_total, marker="o", linewidth=2.4, color=line_blue)
    best_idx = min(range(len(restore_avg_total)), key=lambda idx: restore_avg_total[idx])
    ax.scatter(
        [protects_x[best_idx]],
        [restore_avg_total[best_idx]],
        s=75,
        color=line_red,
        zorder=5,
        label="Best Mean",
    )
    ax.set_title("Low-Cap Global Mean Restore")
    ax.set_xlabel("protect_percent")
    ax.set_ylabel("Average restore_total_duration_seconds")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False)
    ax.annotate(
        f"best mean at protect{protects_x[best_idx]}",
        xy=(protects_x[best_idx], restore_avg_total[best_idx]),
        xytext=(protects_x[best_idx] + 2.0, max(restore_avg_total) * 0.96),
        arrowprops={"arrowstyle": "->", "color": "#444444"},
        fontsize=9,
    )

    png_path = matrix_dir / f"{args.output_prefix}.png"
    svg_path = matrix_dir / f"{args.output_prefix}.svg"
    fig.savefig(png_path, dpi=220)
    fig.savefig(svg_path)
    plt.close(fig)

    print(f"Saved detail CSV: {detail_csv}")
    print(f"Saved summary CSV: {summary_csv}")
    print(f"Saved figure PNG: {png_path}")
    print(f"Saved figure SVG: {svg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
