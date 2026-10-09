#!/usr/bin/env python3
"""Run the multi heat/method/capacity fileInfo restore experiment matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


DATASETS = [
    ("wiki_cloud_full_file", "/mnt/test/exp_edgededup/wiki_cloud_full_file", "/input/wiki_cloud_full_file"),
    ("sina_news_depth1_15days", "/mnt/test/dedup_datasets/sina_news_depth1/15days", "/input/sina_news_depth1_15days"),
    ("github_repo_flat_files", "/mnt/test/dedup_datasets/github_repo_flat_files", "/input/github_repo_flat_files"),
]

METHODS = [
    ("cloud_only", "disDedup/scripts/method/Cloud_only.py", [30], None),
    ("edge_only", "disDedup/scripts/method/Edge_only.py", [30], None),
    ("ladedup_only", "disDedup/scripts/method/LADedup-only.py", [30, 25, 20, 15, 10, 5], None),
    ("ladedup_two", "disDedup/scripts/method/LADedup-two.py", [30, 25, 20, 15, 10, 5], 0.15),
    ("ladedup_two_protect_5pct", "disDedup/scripts/method/LADedup-two.py", [30, 25, 20, 15, 10, 5], 0.05),
    ("mean", "disDedup/scripts/method/MEAN.py", [30, 25, 20, 15, 10, 5], None),
    ("popularity_only", "disDedup/scripts/method/Popularity_only.py", [30, 25, 20, 15, 10, 5], None),
    ("dedup_only", "disDedup/scripts/method/Dedup_only.py", [30, 25, 20, 15, 10, 5], None),
]

ZIPF_VALUES = [0.5, 0.7, 0.9]
EDGE_SERVICES = [f"edge{i}" for i in range(1, 11)]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_cmd(cmd: Sequence[str], *, cwd: Path, log_path: Path | None = None) -> None:
    cmd_text = " ".join(str(x) for x in cmd)
    print(f"[run] {cmd_text}", flush=True)
    if log_path is None:
        subprocess.run(list(cmd), cwd=str(cwd), check=True)
        return

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as fh:
        fh.write(f"$ {cmd_text}\n")
        fh.flush()
        proc = subprocess.run(list(cmd), cwd=str(cwd), stdout=fh, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, list(cmd))


def write_log(log_path: Path, text: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(text, encoding="utf-8")


def container_path(host_path: Path, data_root: Path) -> str:
    host_path = host_path.resolve()
    data_root = data_root.resolve()
    rel = host_path.relative_to(data_root).as_posix()
    return f"/input/{rel}"


def zipf_label(value: float) -> str:
    return f"{value:.6g}"


def parse_float_list(text: str, default: Sequence[float]) -> List[float]:
    text = text.strip()
    if not text:
        return list(default)
    values: List[float] = []
    for raw in text.split(","):
        item = raw.strip()
        if item:
            values.append(float(item))
    if not values:
        raise ValueError("empty float list")
    return values


def parse_name_filter(text: str) -> set[str]:
    return {item.strip() for item in text.split(",") if item.strip()}


def parse_int_filter(text: str) -> set[int]:
    return {int(item.strip()) for item in text.split(",") if item.strip()}


def write_noports_compose(source: Path, target: Path) -> None:
    """Write a compose copy without host port publishing.

    The restore workflow only needs container-to-container networking. Avoiding
    host ports makes repeated matrix cells independent from stale host listeners.
    """
    lines = source.read_text(encoding="utf-8").splitlines()
    filtered = [line for line in lines if not line.lstrip().startswith("ports: [")]
    target.write_text("\n".join(filtered) + "\n", encoding="utf-8")


def project_name(dataset: str, zipf_s: float, method: str, cap: int) -> str:
    raw = f"{dataset}-{zipf_label(zipf_s)}-{method}-{cap}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"edgededup_{digest}"


def docker_compose_cmd(compose_file: Path, env_file: Path, *args: str) -> List[str]:
    return [
        "docker",
        "compose",
        "-p",
        env_file.parent.name.replace(".", "_").replace("-", "_")[:40],
        "--env-file",
        str(env_file),
        "-f",
        str(compose_file),
        *args,
    ]


def export_heat(fileinfo_json: Path, heat_txt: Path) -> None:
    data = json.loads(fileinfo_json.read_text(encoding="utf-8"))
    heat_txt.parent.mkdir(parents=True, exist_ok=True)
    with heat_txt.open("w", encoding="utf-8") as fh:
        for item in data.get("files", []):
            fh.write(f"{item.get('path', '')}\t{item.get('heat', '')}\n")


def ensure_inputs(args: argparse.Namespace, dataset_name: str, dataset_root: Path, container_prefix: str, zipf_s: float) -> Dict[str, Path]:
    repo = repo_root()
    dataset_exp = args.exp_root / dataset_name / f"zipf_{zipf_label(zipf_s)}"
    inputs_dir = dataset_exp / "inputs"
    fileinfo_json = inputs_dir / "fileinfo.json"
    request_file = inputs_dir / "requests.txt"
    heat_txt = inputs_dir / "heat.txt"
    cloud_store_root = inputs_dir / "cloud_store"
    cloud_fullfiles_dir = cloud_store_root / "cloud" / "storage" / "_fullfiles"
    cloud_fullfiles_link = inputs_dir / "cloud_fullfiles"

    ready = fileinfo_json.exists() and request_file.exists() and cloud_fullfiles_dir.exists()
    if not ready or args.force_inputs:
        if args.force_inputs and inputs_dir.exists():
            shutil.rmtree(inputs_dir)
        inputs_dir.mkdir(parents=True, exist_ok=True)
        run_cmd(
            [
                "python3",
                str(repo / "disDedup/scripts/experiments/prepare_fileinfo_restore_inputs.py"),
                "--dataset-root",
                str(dataset_root),
                "--container-dataset-prefix",
                container_prefix,
                "--fileinfo-json",
                str(fileinfo_json),
                "--request-file",
                str(request_file),
                "--cloud-fullfiles-dir",
                str(cloud_fullfiles_dir),
                "--request-count",
                str(args.request_count),
                "--seed",
                str(args.request_seed),
                "--zipf-s",
                str(zipf_s),
                "--zipf-seed",
                str(args.zipf_seed),
                "--chunk-bytes",
                str(args.chunk_bytes),
                "--copy-mode",
                args.copy_mode,
                "--pretty",
            ],
            cwd=repo,
            log_path=dataset_exp / "logs" / "00_prepare_inputs.log",
        )
    else:
        print(f"[skip] inputs ready: {dataset_exp}", flush=True)

    if not heat_txt.exists() or args.force_inputs:
        export_heat(fileinfo_json, heat_txt)
    if not cloud_fullfiles_link.exists():
        try:
            cloud_fullfiles_link.symlink_to(cloud_fullfiles_dir, target_is_directory=True)
        except FileExistsError:
            pass

    return {
        "dataset_exp": dataset_exp,
        "inputs_dir": inputs_dir,
        "fileinfo_json": fileinfo_json,
        "request_file": request_file,
        "heat_txt": heat_txt,
        "cloud_store_root": cloud_store_root,
    }


def ensure_placement(
    args: argparse.Namespace,
    method_name: str,
    method_script: str,
    protect_top_ratio: float | None,
    cap: int,
    paths: Dict[str, Path],
) -> Path:
    repo = repo_root()
    placement = paths["dataset_exp"] / "placements" / method_name / f"cap{cap}" / "placement.json"
    if placement.exists() and not args.force_placement:
        print(f"[skip] placement ready: {placement}", flush=True)
        return placement

    cmd = [
        "python3",
        str(repo / "disDedup/scripts/experiments/generate_placement_from_fileinfo.py"),
        "--method",
        str(repo / method_script),
        "--fileinfo-json",
        str(paths["fileinfo_json"]),
        "--output-json",
        str(placement),
        "--cap-percent",
        str(cap),
        "--server-num",
        str(args.edge_count),
        "--workers",
        str(args.workers),
        "--k",
        str(args.k),
        "--alpha",
        str(args.alpha),
        "--restore-batch-size",
        str(args.restore_batch_size),
        "--env-file",
        str(repo / "disDedup/compose.paths.env"),
    ]
    if protect_top_ratio is not None:
        cmd.extend(["--protect-top-ratio", str(protect_top_ratio)])
    run_cmd(cmd, cwd=repo, log_path=placement.parent / "generate_placement.log")
    return placement


def write_env_file(env_file: Path, data_root: Path, store_root: Path, cloud_store_root: Path) -> None:
    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_file.write_text(
        "\n".join(
            [
                f"DATA_ROOT={data_root}",
                f"STORE_ROOT={store_root}",
                f"CLOUD_STORE_ROOT={cloud_store_root}",
                f"PROJ={repo / 'disDedup'}",
                f"HOST_UID={os.getuid()}",
                f"HOST_GID={os.getgid()}",
                "NET_IFACE=eth0",
                "M2C_TARGETS=cloud",
                "M2C_RATE=100mbit",
                "M2C_DELAY=25ms",
                "M2C_JITTER=5ms",
                "M2C_LOSS=0%",
                "M2E_TARGETS=edge1 edge2 edge3 edge4 edge5 edge6 edge7 edge8 edge9 edge10",
                "M2E_RATE=1500mbit",
                "M2E_DELAY=1ms",
                "M2E_JITTER=0.4ms",
                "M2E_LOSS=0%",
                "MANAGER_NET_BURST=512kbit",
                "MANAGER_DEFAULT_RATE=10gbit",
                "CLOUD_NET_RATE=100mbit",
                "CLOUD_NET_DELAY=25ms",
                "CLOUD_NET_JITTER=5ms",
                "CLOUD_NET_LOSS=0%",
                "CLOUD_NET_BURST=256kbit",
                "CLOUD_NET_LATENCY=400ms",
                "EDGE_NET_RATE=1500mbit",
                "EDGE_NET_DELAY=1ms",
                "EDGE_NET_JITTER=0.4ms",
                "EDGE_NET_LOSS=0%",
                "EDGE_NET_BURST=32kbit",
                "EDGE_NET_LATENCY=400ms",
                "",
            ]
        ),
        encoding="utf-8",
    )


def cleanup_edge_storage(store_root: Path, edge_count: int) -> None:
    if not store_root.exists():
        return
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{store_root}:/target",
        "disdedup:latest",
        "sh",
        "-lc",
        "find /target -maxdepth 2 -type d \\( -name storage -o -name meta -o -name stats \\) -exec rm -rf {} +",
    ]
    proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"cleanup command failed: {' '.join(cmd)}")


def remove_runtime_dir(runtime_dir: Path) -> None:
    if not runtime_dir.exists():
        return
    try:
        shutil.rmtree(runtime_dir)
        return
    except PermissionError:
        pass
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{runtime_dir}:/target",
        "disdedup:latest",
        "sh",
        "-lc",
        "rm -rf /target/* /target/.[!.]* /target/..?* 2>/dev/null || true",
    ]
    proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"runtime cleanup failed: {' '.join(cmd)}")
    shutil.rmtree(runtime_dir, ignore_errors=True)


def run_restore(
    args: argparse.Namespace,
    dataset_name: str,
    container_prefix: str,
    zipf_s: float,
    method_name: str,
    cap: int,
    paths: Dict[str, Path],
    placement: Path,
) -> Dict[str, object]:
    repo = repo_root()
    disdedup = repo / "disDedup"
    result_dir = paths["dataset_exp"] / "results" / method_name / f"cap{cap}"
    runtime_dir = result_dir / "runtime"
    logs_dir = result_dir / "logs"
    store_root = runtime_dir / "store"
    env_file = runtime_dir / "compose.env"
    compose_file = runtime_dir / "docker-compose.noports.yml"
    status_file = result_dir / "status.json"
    restore_stats_out = result_dir / "restore_stats.json"

    if restore_stats_out.exists() and not args.force_run:
        print(f"[skip] restore ready: {restore_stats_out}", flush=True)
        return {"status": "skipped", "result_dir": str(result_dir)}

    if runtime_dir.exists():
        remove_runtime_dir(runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    write_env_file(env_file, args.data_root, store_root, paths["cloud_store_root"])
    write_noports_compose(disdedup / "docker-compose.yml", compose_file)

    fileinfo_container = container_path(paths["fileinfo_json"], args.data_root)
    request_container = container_path(paths["request_file"], args.data_root)
    placement_container = container_path(placement, args.data_root)

    start = time.time()
    record: Dict[str, object] = {
        "dataset": dataset_name,
        "zipf_s": zipf_s,
        "method": method_name,
        "cap_percent": cap,
        "result_dir": str(result_dir),
        "placement_json": str(placement),
        "fileinfo_json": str(paths["fileinfo_json"]),
        "request_file": str(paths["request_file"]),
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }

    try:
        run_cmd(
            [
                str(disdedup / "scripts/config/sync_cluster_configs.sh"),
                "--env-file",
                str(env_file),
                "--store-root",
                str(store_root),
                "--cloud-store-root",
                str(paths["cloud_store_root"]),
                "--edge-count",
                str(args.edge_count),
                "--chunk-size",
                str(args.chunk_bytes),
                "--storage-granularity",
                "block",
                "--chunking-method",
                "fastcdc",
                "--placement-json",
                placement_container,
                "--placement-strategy",
                "round_robin",
                "--dedup-mode",
                "synthetic_edge",
                "--synthetic-fileinfo-json",
                fileinfo_container,
                "--synthetic-dataset-prefix",
                container_prefix,
                "--restore-cloud-full-threshold",
                "1",
                "--restore-discard-output",
                "true",
                "--rpc-timeout-ms",
                str(args.rpc_timeout_ms),
                "--load-previous-index",
                "false",
            ],
            cwd=disdedup,
            log_path=logs_dir / "01_sync_configs.log",
        )

        compose_base = [
            "docker",
            "compose",
            "-p",
            project_name(dataset_name, zipf_s, method_name, cap),
            "--env-file",
            str(env_file),
            "-f",
            str(compose_file),
        ]
        compose_attempted = False
        try:
            compose_attempted = True
            run_cmd(
                [
                    *compose_base,
                    "up",
                    "-d",
                    "--force-recreate",
                    "cloud",
                    *EDGE_SERVICES[: args.edge_count],
                    "manager",
                ],
                cwd=disdedup,
                log_path=logs_dir / "02_compose_up.log",
            )
            run_cmd(
                [
                    *compose_base,
                    "run",
                    "--rm",
                    "manager",
                    "manager",
                    "-config",
                    "/cfg/manager.yaml",
                    "-placement-json",
                    placement_container,
                    "restore-fileinfo-batch",
                    fileinfo_container,
                    request_container,
                    container_prefix,
                ],
                cwd=disdedup,
                log_path=logs_dir / "03_restore_fileinfo_batch.log",
            )
        finally:
            if compose_attempted:
                try:
                    run_cmd([*compose_base, "down"], cwd=disdedup, log_path=logs_dir / "99_compose_down.log")
                except Exception as exc:
                    write_log(logs_dir / "99_compose_down.error.log", f"{type(exc).__name__}: {exc}\n")

        restore_stats = store_root / "manager" / "stats" / "restore_stats.json"
        if not restore_stats.exists():
            raise FileNotFoundError(f"restore stats missing: {restore_stats}")
        shutil.copy2(restore_stats, restore_stats_out)
        record["status"] = "ok"
        record["restore_stats_json"] = str(restore_stats_out)
        try:
            stats = json.loads(restore_stats_out.read_text(encoding="utf-8"))
            overall = stats.get("overall_average", {})
            record["file_count"] = overall.get("file_count")
            record["cloud_full_download_files"] = overall.get("cloud_full_download_files")
            record["total_duration_seconds"] = overall.get("total_duration_seconds")
            record["throughput_mb_s"] = overall.get("throughput_mb_s")
        except Exception:
            pass
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = f"{type(exc).__name__}: {exc}"
        print(f"[failed] {dataset_name} zipf={zipf_s} {method_name} cap{cap}: {record['error']}", file=sys.stderr, flush=True)
        if args.stop_on_failure:
            raise
    finally:
        record["finished_at"] = datetime.now().isoformat(timespec="seconds")
        record["elapsed_seconds"] = round(time.time() - start, 3)
        status_file.parent.mkdir(parents=True, exist_ok=True)
        status_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        if args.cleanup_edge_data:
            try:
                cleanup_edge_storage(store_root, args.edge_count)
            except Exception as exc:
                print(f"[warn] cleanup failed for {result_dir}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

    return record


def append_records(exp_root: Path, records: Iterable[Dict[str, object]]) -> None:
    records = list(records)
    if not records:
        return
    jsonl = exp_root / "records.jsonl"
    with jsonl.open("a", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    csv_path = exp_root / "records.csv"
    fieldnames = [
        "status",
        "dataset",
        "zipf_s",
        "method",
        "cap_percent",
        "file_count",
        "cloud_full_download_files",
        "total_duration_seconds",
        "throughput_mb_s",
        "elapsed_seconds",
        "result_dir",
        "error",
    ]
    existing = csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not existing:
            writer.writeheader()
        for rec in records:
            writer.writerow({k: rec.get(k, "") for k in fieldnames})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exp-root", type=Path, default=Path("/mnt/test/exp_edgededup/multi_heat_method_capacity"))
    parser.add_argument("--data-root", type=Path, default=Path("/mnt/test/exp_edgededup"))
    parser.add_argument("--chunk-bytes", type=int, default=8192)
    parser.add_argument("--request-count", type=int, default=1000)
    parser.add_argument("--request-seed", type=int, default=7)
    parser.add_argument("--zipf-seed", type=int, default=42)
    parser.add_argument("--copy-mode", choices=("copy", "hardlink"), default="hardlink")
    parser.add_argument("--edge-count", type=int, default=10)
    parser.add_argument("--k", type=int, default=1000)
    parser.add_argument("--alpha", type=int, default=5)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 1) - 1))
    parser.add_argument("--restore-batch-size", type=int, default=128)
    parser.add_argument("--rpc-timeout-ms", type=int, default=30000)
    parser.add_argument("--force-inputs", action="store_true")
    parser.add_argument("--force-placement", action="store_true")
    parser.add_argument("--force-run", action="store_true")
    parser.add_argument("--cleanup-edge-data", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--stop-on-failure", action="store_true")
    parser.add_argument(
        "--zipf-values",
        default="",
        help="comma-separated Zipf values to run; default uses the script matrix",
    )
    parser.add_argument(
        "--method-filter",
        default="",
        help="comma-separated method names to run; default runs all methods",
    )
    parser.add_argument(
        "--dataset-filter",
        default="",
        help="comma-separated dataset names to run; default runs all datasets",
    )
    parser.add_argument(
        "--cap-values",
        default="",
        help="comma-separated capacity percentages to run; default uses each method matrix",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.exp_root = args.exp_root.resolve()
    args.data_root = args.data_root.resolve()
    args.exp_root.mkdir(parents=True, exist_ok=True)
    zipf_values = parse_float_list(args.zipf_values, ZIPF_VALUES)
    method_filter = parse_name_filter(args.method_filter)
    dataset_filter = parse_name_filter(args.dataset_filter)
    cap_filter = parse_int_filter(args.cap_values)
    methods = [item for item in METHODS if not method_filter or item[0] in method_filter]
    if method_filter:
        known = {item[0] for item in METHODS}
        unknown = sorted(method_filter - known)
        if unknown:
            raise ValueError(f"unknown method-filter values: {', '.join(unknown)}")
    if not methods:
        raise ValueError("no methods selected")
    datasets = [item for item in DATASETS if not dataset_filter or item[0] in dataset_filter]
    if dataset_filter:
        known_datasets = {item[0] for item in DATASETS}
        unknown_datasets = sorted(dataset_filter - known_datasets)
        if unknown_datasets:
            raise ValueError(f"unknown dataset-filter values: {', '.join(unknown_datasets)}")
    if not datasets:
        raise ValueError("no datasets selected")

    all_records: List[Dict[str, object]] = []
    for zipf_s in zipf_values:
        print(f"\n=== zipf-s={zipf_label(zipf_s)} ===", flush=True)
        for dataset_name, dataset_root_text, container_prefix in datasets:
            dataset_root = Path(dataset_root_text)
            if not dataset_root.exists():
                raise FileNotFoundError(f"dataset root missing: {dataset_root}")
            print(f"\n--- dataset={dataset_name} root={dataset_root} ---", flush=True)
            paths = ensure_inputs(args, dataset_name, dataset_root, container_prefix, zipf_s)
            batch_records: List[Dict[str, object]] = []
            for method_name, method_script, caps, protect_ratio in methods:
                selected_caps = [cap for cap in caps if not cap_filter or cap in cap_filter]
                for cap in selected_caps:
                    print(f"\n[case] zipf={zipf_label(zipf_s)} dataset={dataset_name} method={method_name} cap={cap}", flush=True)
                    placement = ensure_placement(args, method_name, method_script, protect_ratio, cap, paths)
                    rec = run_restore(args, dataset_name, container_prefix, zipf_s, method_name, cap, paths, placement)
                    batch_records.append(rec)
                    append_records(args.exp_root, [rec])
            all_records.extend(batch_records)

    summary = {
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "total_cases": len(all_records),
        "ok": sum(1 for r in all_records if r.get("status") == "ok"),
        "skipped": sum(1 for r in all_records if r.get("status") == "skipped"),
        "failed": sum(1 for r in all_records if r.get("status") == "failed"),
    }
    (args.exp_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
