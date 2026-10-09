#!/usr/bin/env python3
"""
Prepare cloud/storage/_fullfiles from a dataset using hard links.

The object names match the legacy cloud-sync naming used by manager/cloud_full.go:
  abs(input_path) with ":" removed and path separators replaced by "-"

Example:
  /input/github_repo/org/repo/file.txt
  -> -input-github_repo-org-repo-file.txt
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def legacy_object_name(container_input_path: str) -> str:
    safe = container_input_path.replace(":", "")
    safe = safe.replace("/", "-")
    safe = safe.replace("\\", "-")
    return safe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare cloud _fullfiles using hard links.")
    parser.add_argument("--dataset-root", required=True, help="host dataset root directory")
    parser.add_argument(
        "--container-dataset-prefix",
        required=True,
        help="container-side dataset root prefix, e.g. /input/github_repo",
    )
    parser.add_argument(
        "--cloud-fullfiles-dir",
        required=True,
        help="target cloud _fullfiles directory, e.g. /mnt/test/.../cloud/storage/_fullfiles",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace existing targets if present",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset_root = Path(args.dataset_root).resolve()
    cloud_fullfiles_dir = Path(args.cloud_fullfiles_dir).resolve()
    container_prefix = args.container_dataset_prefix.rstrip("/")

    if not dataset_root.is_dir():
        raise FileNotFoundError(f"dataset root not found or not a directory: {dataset_root}")

    cloud_fullfiles_dir.mkdir(parents=True, exist_ok=True)

    linked = 0
    skipped = 0
    replaced = 0
    for src in sorted(p for p in dataset_root.rglob("*") if p.is_file()):
        rel = src.relative_to(dataset_root).as_posix()
        object_name = legacy_object_name(f"{container_prefix}/{rel}")
        dst = cloud_fullfiles_dir / object_name

        if dst.exists() or dst.is_symlink():
            same_inode = False
            try:
                same_inode = os.path.samefile(src, dst)
            except FileNotFoundError:
                same_inode = False
            if same_inode:
                skipped += 1
                continue
            if not args.overwrite:
                skipped += 1
                continue
            dst.unlink()
            replaced += 1

        os.link(src, dst)
        linked += 1

    print(
        f"prepared cloud fullfiles: linked={linked} replaced={replaced} skipped={skipped} "
        f"target={cloud_fullfiles_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
