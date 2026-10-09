#!/usr/bin/env python3
"""Run LADedup-two protect-top-ratio sweep using prepared fileInfo inputs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import run_multi_heat_method_capacity_fileinfo as base


RATIOS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
CAPS = [30, 25, 20, 15, 10, 5]
ZIPF_VALUES = [0.5, 0.7, 1.0]


def ratio_label(ratio: float) -> str:
    pct = int(round(ratio * 100))
    return f"{pct}pct"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exp-root", type=Path, default=Path("/mnt/test/exp_edgededup/multi_heat_method_capacity"))
    parser.add_argument("--data-root", type=Path, default=Path("/mnt/test/exp_edgededup"))
    parser.add_argument("--chunk-bytes", type=int, default=8192)
    parser.add_argument("--edge-count", type=int, default=10)
    parser.add_argument("--k", type=int, default=1000)
    parser.add_argument("--alpha", type=int, default=5)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 1) - 1))
    parser.add_argument("--restore-batch-size", type=int, default=128)
    parser.add_argument("--force-placement", action="store_true")
    parser.add_argument("--force-run", action="store_true")
    parser.add_argument("--cleanup-edge-data", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--stop-on-failure", action="store_true")
    parser.add_argument("--zipf", type=float, action="append", dest="zipfs", help="Zipf value to run; repeatable")
    parser.add_argument("--dataset", action="append", dest="datasets", help="Dataset name to run; repeatable")
    return parser.parse_args()


def prepared_paths(args: argparse.Namespace, dataset_name: str, zipf_s: float) -> Dict[str, Path]:
    dataset_exp = args.exp_root / dataset_name / f"zipf_{base.zipf_label(zipf_s)}"
    inputs_dir = dataset_exp / "inputs"
    fileinfo_json = inputs_dir / "fileinfo.json"
    request_file = inputs_dir / "requests.txt"
    cloud_store_root = inputs_dir / "cloud_store"
    missing = [p for p in (fileinfo_json, request_file, cloud_store_root) if not p.exists()]
    if missing:
        missing_text = ", ".join(str(p) for p in missing)
        raise FileNotFoundError(f"prepared input missing for {dataset_name} zipf={zipf_s}: {missing_text}")
    return {
        "dataset_exp": dataset_exp,
        "inputs_dir": inputs_dir,
        "fileinfo_json": fileinfo_json,
        "request_file": request_file,
        "cloud_store_root": cloud_store_root,
    }


def main() -> int:
    args = parse_args()
    args.exp_root = args.exp_root.resolve()
    args.data_root = args.data_root.resolve()
    args.exp_root.mkdir(parents=True, exist_ok=True)

    requested_zipfs = set(args.zipfs or ZIPF_VALUES)
    requested_datasets = set(args.datasets or [item[0] for item in base.DATASETS])
    datasets = [item for item in base.DATASETS if item[0] in requested_datasets]
    if not datasets:
        raise ValueError(f"no matching datasets: {sorted(requested_datasets)}")

    records: List[Dict[str, object]] = []
    started_at = datetime.now().isoformat(timespec="seconds")
    print(f"[sweep] started_at={started_at}", flush=True)
    print(f"[sweep] exp_root={args.exp_root}", flush=True)
    print(f"[sweep] ratios={', '.join(str(r) for r in RATIOS)}", flush=True)

    for zipf_s in ZIPF_VALUES:
        if zipf_s not in requested_zipfs:
            continue
        print(f"\n=== zipf-s={base.zipf_label(zipf_s)} ===", flush=True)
        for dataset_name, _, container_prefix in datasets:
            print(f"\n--- dataset={dataset_name} ---", flush=True)
            paths = prepared_paths(args, dataset_name, zipf_s)
            for ratio in RATIOS:
                method_name = f"ladedup_two_protect_{ratio_label(ratio)}"
                for cap in CAPS:
                    print(
                        f"\n[case] zipf={base.zipf_label(zipf_s)} dataset={dataset_name} "
                        f"method={method_name} ratio={ratio} cap={cap}",
                        flush=True,
                    )
                    placement = base.ensure_placement(
                        args,
                        method_name,
                        "disDedup/scripts/method/LADedup-two.py",
                        ratio,
                        cap,
                        paths,
                    )
                    rec = base.run_restore(args, dataset_name, container_prefix, zipf_s, method_name, cap, paths, placement)
                    rec.update(
                        {
                            "dataset": rec.get("dataset", dataset_name),
                            "zipf_s": rec.get("zipf_s", zipf_s),
                            "method": rec.get("method", method_name),
                            "cap_percent": rec.get("cap_percent", cap),
                            "protect_top_ratio": ratio,
                        }
                    )
                    records.append(rec)
                    base.append_records(args.exp_root, [rec])

    summary = {
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "total_cases": len(records),
        "ok": sum(1 for r in records if r.get("status") == "ok"),
        "skipped": sum(1 for r in records if r.get("status") == "skipped"),
        "failed": sum(1 for r in records if r.get("status") == "failed"),
        "ratios": RATIOS,
    }
    (args.exp_root / "ladedup_two_protect_ratio_sweep_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
