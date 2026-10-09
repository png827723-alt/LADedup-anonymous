#!/usr/bin/env python3
"""Prepare fileInfo, restore requests, and cloud full-file objects.

This script is for the fileInfo-driven restore path:

  manager -config /cfg/manager.yaml -placement-json <placement.json> \
    restore-fileinfo-batch <fileinfo.json> <requests.txt> <dataset_prefix>

It builds fileInfo from a dataset, samples restore requests by file heat, and
copies original files into the cloud node's _fullfiles directory using the same
object naming convention as manager cloud-sync.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def legacy_object_name(container_input_path: str) -> str:
    return container_input_path.replace(":", "").replace("/", "-").replace("\\", "-")


def normalize_rel(path: str) -> str:
    return path.strip().replace("\\", "/").lstrip("/")


def container_path(prefix: str, rel: str) -> str:
    prefix = prefix.rstrip("/")
    rel = normalize_rel(rel)
    return f"{prefix}/{rel}" if prefix else rel


def parse_args() -> argparse.Namespace:
    default_builder = repo_root() / "disDedup" / "scripts" / "fileinfo_builder" / "build_fileInfo_from_dataset.go"
    parser = argparse.ArgumentParser(
        description="Build fileInfo.json, heat-weighted requests.txt, and cloud _fullfiles copies."
    )
    parser.add_argument("--dataset-root", required=True, help="host dataset root directory")
    parser.add_argument(
        "--container-dataset-prefix",
        required=True,
        help="container-side dataset root, e.g. /input/wiki",
    )
    parser.add_argument("--heat-csv", default="", help="optional CSV with columns file,heat")
    parser.add_argument("--fileinfo-json", required=True, help="output fileInfo JSON path")
    parser.add_argument("--request-file", required=True, help="output restore request txt path")
    parser.add_argument(
        "--cloud-fullfiles-dir",
        required=True,
        help="cloud storage _fullfiles directory, e.g. <STORE_ROOT>/cloud/storage/_fullfiles",
    )
    parser.add_argument(
        "--builder",
        default=str(default_builder),
        help="Go fileInfo builder path",
    )
    parser.add_argument("--chunk-bytes", type=int, default=8192, help="target chunk size for fileInfo builder")
    parser.add_argument("--zipf-s", type=float, default=0.7, help="default Zipf heat exponent when heat CSV omits files")
    parser.add_argument(
        "--zipf-random",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="randomize default Zipf rank assignment (default: on)",
    )
    parser.add_argument("--zipf-seed", type=int, default=0, help="seed for --zipf-random")
    parser.add_argument("--request-count", type=int, default=0, help="number of requests to sample")
    parser.add_argument(
        "--request-multiplier",
        type=int,
        default=0,
        help="if request-count is 0, sample N * file_count requests",
    )
    parser.add_argument("--seed", type=int, default=1, help="request sampling seed")
    parser.add_argument(
        "--sample-without-replacement",
        action="store_true",
        help="sample each file at most once",
    )
    parser.add_argument(
        "--output-prefix",
        default="/output/restored",
        help="container output prefix written as request second column",
    )
    parser.add_argument("--overwrite-cloud", action="store_true", help="overwrite existing cloud full-file objects")
    parser.add_argument("--skip-cloud-copy", action="store_true", help="only build fileInfo and requests")
    parser.add_argument(
        "--copy-mode",
        choices=("copy", "hardlink"),
        default="copy",
        help="copy files into cloud storage, or hardlink when possible",
    )
    parser.add_argument("--pretty", action="store_true", help="pretty-print fileInfo JSON")
    return parser.parse_args()


def run_fileinfo_builder(args: argparse.Namespace, dataset_root: Path, fileinfo_json: Path) -> None:
    builder = Path(args.builder).resolve()
    if not builder.is_file():
        raise FileNotFoundError(f"fileInfo builder not found: {builder}")

    cmd = [
        "go",
        "run",
        str(builder),
        "--dataset-root",
        str(dataset_root),
        "--output-json",
        str(fileinfo_json),
        "--chunk-bytes",
        str(args.chunk_bytes),
        "--zipf-s",
        str(args.zipf_s),
    ]
    if args.heat_csv:
        cmd.extend(["--popularity-csv", str(Path(args.heat_csv).resolve())])
    if args.zipf_random:
        cmd.append("--zipf-random")
    if args.zipf_seed:
        cmd.extend(["--zipf-seed", str(args.zipf_seed)])
    if args.pretty:
        cmd.append("--pretty")

    fileinfo_json.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(cmd, cwd=str(builder.parent), check=True)


def load_fileinfo(fileinfo_json: Path) -> Dict[str, object]:
    with fileinfo_json.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    files = data.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError(f"fileInfo has no files: {fileinfo_json}")
    return data


def positive_heat(item: Dict[str, object]) -> float:
    try:
        heat = float(item.get("heat", 0.0))
    except (TypeError, ValueError):
        heat = 0.0
    return heat if heat > 0 else 0.0


def weighted_requests(files: Sequence[Dict[str, object]], count: int, seed: int, without_replacement: bool) -> List[Dict[str, object]]:
    if count <= 0:
        raise ValueError("effective request count must be positive")
    candidates = [f for f in files if normalize_rel(str(f.get("path", "")))]
    if not candidates:
        raise ValueError("fileInfo contains no requestable files")

    weights = [positive_heat(f) for f in candidates]
    if sum(weights) <= 0:
        weights = [1.0] * len(candidates)

    rng = random.Random(seed)
    if not without_replacement:
        return rng.choices(candidates, weights=weights, k=count)

    if count > len(candidates):
        raise ValueError(f"request-count={count} exceeds file count={len(candidates)} for without-replacement sampling")
    pool = list(candidates)
    pool_weights = list(weights)
    selected: List[Dict[str, object]] = []
    for _ in range(count):
        total = sum(pool_weights)
        if total <= 0:
            idx = rng.randrange(len(pool))
        else:
            target = rng.random() * total
            acc = 0.0
            idx = len(pool) - 1
            for i, w in enumerate(pool_weights):
                acc += w
                if target <= acc:
                    idx = i
                    break
        selected.append(pool.pop(idx))
        pool_weights.pop(idx)
    return selected


def write_requests(
    request_file: Path,
    selected: Iterable[Dict[str, object]],
    container_prefix: str,
    output_prefix: str,
) -> int:
    request_file.parent.mkdir(parents=True, exist_ok=True)
    output_prefix = output_prefix.rstrip("/")
    count = 0
    with request_file.open("w", encoding="utf-8") as fh:
        for item in selected:
            rel = normalize_rel(str(item.get("path", "")))
            in_path = container_path(container_prefix, rel)
            out_path = container_path(output_prefix, rel)
            fh.write(f"{in_path}\t{out_path}\n")
            count += 1
    return count


def iter_dataset_files(dataset_root: Path) -> Iterable[Path]:
    return sorted(p for p in dataset_root.rglob("*") if p.is_file())


def copy_cloud_fullfiles(
    dataset_root: Path,
    cloud_fullfiles_dir: Path,
    container_prefix: str,
    overwrite: bool,
    copy_mode: str,
) -> Tuple[int, int, int]:
    cloud_fullfiles_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    skipped = 0
    replaced = 0

    for src in iter_dataset_files(dataset_root):
        rel = src.relative_to(dataset_root).as_posix()
        object_name = legacy_object_name(container_path(container_prefix, rel))
        dst = cloud_fullfiles_dir / object_name

        if dst.exists() or dst.is_symlink():
            if not overwrite:
                skipped += 1
                continue
            dst.unlink()
            replaced += 1

        if copy_mode == "hardlink":
            try:
                dst.hardlink_to(src)
            except OSError:
                shutil.copy2(src, dst)
        else:
            shutil.copy2(src, dst)
        copied += 1

    return copied, replaced, skipped


def main() -> int:
    args = parse_args()
    dataset_root = Path(args.dataset_root).resolve()
    fileinfo_json = Path(args.fileinfo_json).resolve()
    request_file = Path(args.request_file).resolve()
    cloud_fullfiles_dir = Path(args.cloud_fullfiles_dir).resolve()

    if not dataset_root.is_dir():
        raise FileNotFoundError(f"dataset root not found or not a directory: {dataset_root}")
    if args.heat_csv and not Path(args.heat_csv).resolve().is_file():
        raise FileNotFoundError(f"heat CSV not found: {args.heat_csv}")
    if args.chunk_bytes <= 0:
        raise ValueError("--chunk-bytes must be positive")
    if args.request_count < 0 or args.request_multiplier < 0:
        raise ValueError("--request-count and --request-multiplier must be non-negative")
    if args.request_count > 0 and args.request_multiplier > 0:
        raise ValueError("use only one of --request-count or --request-multiplier")

    run_fileinfo_builder(args, dataset_root, fileinfo_json)
    data = load_fileinfo(fileinfo_json)
    files = data["files"]
    assert isinstance(files, list)

    request_count = args.request_count
    if request_count <= 0:
        request_count = args.request_multiplier * len(files)
    if request_count <= 0:
        raise ValueError("set --request-count > 0 or --request-multiplier > 0")

    selected = weighted_requests(
        files,
        count=request_count,
        seed=args.seed,
        without_replacement=bool(args.sample_without_replacement),
    )
    written = write_requests(request_file, selected, args.container_dataset_prefix, args.output_prefix)

    copied = replaced = skipped = 0
    if not args.skip_cloud_copy:
        copied, replaced, skipped = copy_cloud_fullfiles(
            dataset_root,
            cloud_fullfiles_dir,
            args.container_dataset_prefix,
            overwrite=bool(args.overwrite_cloud),
            copy_mode=str(args.copy_mode),
        )

    print(f"fileInfo: {fileinfo_json}")
    print(f"requests: {request_file} (count={written}, seed={args.seed})")
    if args.skip_cloud_copy:
        print("cloud fullfiles: skipped")
    else:
        print(
            "cloud fullfiles: "
            f"target={cloud_fullfiles_dir} copied={copied} replaced={replaced} skipped={skipped}"
        )
    print(f"container dataset prefix: {args.container_dataset_prefix.rstrip('/')}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
