#!/usr/bin/env python3
"""Run only Greedy and GraphCommunity methods on existing fileInfo inputs."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import run_multi_heat_method_capacity_fileinfo as base


ZIPF_VALUES = [0.5, 0.7, 1.0]
METHODS = [
    ("greedy_benefit_size", "disDedup/scripts/method/Greedy_benefit_size.py", [30, 25, 20, 15, 10, 5], None),
    ("graph_community", "disDedup/scripts/method/GraphCommunity_benefit_size.py", [30, 25, 20, 15, 10, 5], None),
]


def main() -> int:
    args = base.parse_args()
    args.exp_root = args.exp_root.resolve()
    args.data_root = args.data_root.resolve()
    args.exp_root.mkdir(parents=True, exist_ok=True)

    all_records: List[Dict[str, object]] = []
    for zipf_s in ZIPF_VALUES:
        print(f"\n=== zipf-s={base.zipf_label(zipf_s)} ===", flush=True)
        for dataset_name, dataset_root_text, container_prefix in base.DATASETS:
            dataset_root = Path(dataset_root_text)
            if not dataset_root.exists():
                raise FileNotFoundError(f"dataset root missing: {dataset_root}")
            print(f"\n--- dataset={dataset_name} root={dataset_root} ---", flush=True)
            paths = base.ensure_inputs(args, dataset_name, dataset_root, container_prefix, zipf_s)
            batch_records: List[Dict[str, object]] = []
            for method_name, method_script, caps, protect_ratio in METHODS:
                for cap in caps:
                    print(
                        f"\n[case] zipf={base.zipf_label(zipf_s)} "
                        f"dataset={dataset_name} method={method_name} cap={cap}",
                        flush=True,
                    )
                    placement = base.ensure_placement(args, method_name, method_script, protect_ratio, cap, paths)
                    rec = base.run_restore(args, dataset_name, container_prefix, zipf_s, method_name, cap, paths, placement)
                    batch_records.append(rec)
                    base.append_records(args.exp_root, [rec])
            all_records.extend(batch_records)

    summary = {
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "total_cases": len(all_records),
        "ok": sum(1 for r in all_records if r.get("status") == "ok"),
        "skipped": sum(1 for r in all_records if r.get("status") == "skipped"),
        "failed": sum(1 for r in all_records if r.get("status") == "failed"),
    }
    (args.exp_root / "summary_greedy_graphcommunity.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
