#!/usr/bin/env python3
"""
Thin wrapper used only to give matrix runs a distinct algorithm stem.

Supported flags forwarded to the real script:
"--input-json" "--output-json" "--env-file" "--k" "--alpha"
"--capacity-ratio" "--server-num" "--workers" "--restore-batch-size"
"--no-progress"
"""

from __future__ import annotations

from pathlib import Path
import runpy


def main() -> None:
    target = Path(__file__).with_name("run_case1_cluster_theoretical_weighted.py")
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
