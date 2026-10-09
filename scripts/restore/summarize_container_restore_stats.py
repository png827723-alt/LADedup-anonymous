#!/usr/bin/env python3
"""
Summarize container-restore efficiency stats from edge node snapshots.

Default input files:
  <STORE_ROOT>/edge*/stats/container_restore_stats.json

Optional baseline subtraction:
  --baseline-root <dir>
will subtract counters from:
  <baseline_root>/edge*/stats/container_restore_stats.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def parse_env_file(env_path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip("'").strip('"')
    return out


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_div(a: float, b: float) -> float:
    if b <= 0:
        return 0.0
    return a / b


def edge_name_from_path(path: Path) -> str:
    # .../<store_root>/<edgeX>/stats/container_restore_stats.json
    if len(path.parts) >= 3:
        return path.parts[-3]
    return path.parent.name


def subtract_counters(cur: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "cache_hits",
        "cache_misses",
        "useful_bytes",
        "container_loaded_bytes",
        "meta_read_bytes",
    ]
    out = dict(cur)
    for k in keys:
        out[k] = int(cur.get(k, 0)) - int(base.get(k, 0))
        if out[k] < 0:
            out[k] = 0
    return out


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent.parent

    parser = argparse.ArgumentParser(description="Summarize container restore efficiency from edge stats")
    parser.add_argument("--env-file", default=str(root_dir / "compose.paths.env"), help="compose env file")
    parser.add_argument("--store-root", default="", help="override STORE_ROOT")
    parser.add_argument("--edge-glob", default="edge*", help="edge directory glob under STORE_ROOT")
    parser.add_argument("--baseline-root", default="", help="optional baseline STORE_ROOT for delta counters")
    parser.add_argument("--output-json", default="", help="optional output JSON path")
    args = parser.parse_args()

    if args.store_root:
        store_root = Path(args.store_root).resolve()
    else:
        env_path = Path(args.env_file).resolve()
        if not env_path.exists():
            print(f"ERROR: env file not found: {env_path}", file=sys.stderr)
            return 2
        env_map = parse_env_file(env_path)
        val = env_map.get("STORE_ROOT", "").strip()
        if not val:
            print(f"ERROR: STORE_ROOT is empty in {env_path}", file=sys.stderr)
            return 2
        store_root = Path(val).resolve()

    pattern = f"{args.edge_glob}/stats/container_restore_stats.json"
    files = sorted(store_root.glob(pattern))
    if not files:
        print(f"ERROR: no stats files matched: {store_root}/{pattern}", file=sys.stderr)
        return 2

    baseline_root = Path(args.baseline_root).resolve() if args.baseline_root else None
    rows: list[dict[str, Any]] = []

    for path in files:
        edge = edge_name_from_path(path)
        cur = load_json(path)
        src = cur
        if baseline_root is not None:
            base_path = baseline_root / edge / "stats" / "container_restore_stats.json"
            if base_path.exists():
                src = subtract_counters(cur, load_json(base_path))

        cache_hits = int(src.get("cache_hits", 0))
        cache_misses = int(src.get("cache_misses", 0))
        useful = int(src.get("useful_bytes", 0))
        loaded = int(src.get("container_loaded_bytes", 0))
        meta = int(src.get("meta_read_bytes", 0))
        total_reads = loaded + meta

        rows.append(
            {
                "edge": edge,
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "cache_hit_rate": safe_div(cache_hits, cache_hits + cache_misses),
                "useful_bytes": useful,
                "container_loaded_bytes": loaded,
                "meta_read_bytes": meta,
                "total_read_bytes": total_reads,
                "useful_ratio": safe_div(useful, loaded),
                "read_amplification": safe_div(total_reads, useful),
            }
        )

    agg = {
        "cache_hits": sum(r["cache_hits"] for r in rows),
        "cache_misses": sum(r["cache_misses"] for r in rows),
        "useful_bytes": sum(r["useful_bytes"] for r in rows),
        "container_loaded_bytes": sum(r["container_loaded_bytes"] for r in rows),
        "meta_read_bytes": sum(r["meta_read_bytes"] for r in rows),
    }
    agg["total_read_bytes"] = agg["container_loaded_bytes"] + agg["meta_read_bytes"]
    agg["cache_hit_rate"] = safe_div(agg["cache_hits"], agg["cache_hits"] + agg["cache_misses"])
    agg["useful_ratio"] = safe_div(agg["useful_bytes"], agg["container_loaded_bytes"])
    agg["read_amplification"] = safe_div(agg["total_read_bytes"], agg["useful_bytes"])

    print("edge\tcache_hit_rate\tuseful_ratio\tread_amplification\tuseful_mb\tloaded_mb\tmeta_mb")
    for r in rows:
        print(
            f"{r['edge']}\t{r['cache_hit_rate']:.3f}\t{r['useful_ratio']:.3f}\t{r['read_amplification']:.3f}\t"
            f"{r['useful_bytes']/1024/1024:.2f}\t{r['container_loaded_bytes']/1024/1024:.2f}\t{r['meta_read_bytes']/1024/1024:.2f}"
        )
    print(
        "ALL\t"
        f"{agg['cache_hit_rate']:.3f}\t{agg['useful_ratio']:.3f}\t{agg['read_amplification']:.3f}\t"
        f"{agg['useful_bytes']/1024/1024:.2f}\t{agg['container_loaded_bytes']/1024/1024:.2f}\t{agg['meta_read_bytes']/1024/1024:.2f}"
    )

    payload = {"store_root": str(store_root), "rows": rows, "aggregate": agg}
    if args.output_json:
        out = Path(args.output_json).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"json: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

