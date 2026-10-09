#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


DEFAULT_TOPICS = [
    "azure",
    "aws",
    "docker",
    "kubernetes",
    "devops",
    "linux",
    "container",
]

DEFAULT_EXTENSIONS = [".apk", ".deb", ".rpm"]
GITHUB_API = "https://api.github.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a release-installer dataset from GitHub release assets."
    )
    parser.add_argument(
        "--output-root",
        default="/mnt/test/exp_edgededup/apk_cloud",
        help="dataset root directory",
    )
    parser.add_argument(
        "--topic",
        action="append",
        default=[],
        help="hot topic to search; repeatable",
    )
    parser.add_argument(
        "--query-mode",
        choices=["topic", "text"],
        default="text",
        help="GitHub repo search mode",
    )
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        help="asset extension to keep; repeatable",
    )
    parser.add_argument("--repos-per-topic", type=int, default=20, help="candidate repos fetched per topic")
    parser.add_argument("--sample-repos-per-topic", type=int, default=8, help="repos randomly sampled per topic")
    parser.add_argument("--releases-per-repo", type=int, default=10, help="max releases checked per repo")
    parser.add_argument("--assets-per-repo", type=int, default=20, help="max matching assets downloaded per repo")
    parser.add_argument("--max-downloads", type=int, default=100, help="global max asset downloads")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=0,
        help="global max bytes to download; 0 means unlimited",
    )
    parser.add_argument("--seed", type=int, default=42, help="sampling seed")
    parser.add_argument("--min-asset-size", type=int, default=1024, help="skip tiny assets")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="sleep between downloads")
    parser.add_argument(
        "--token-env",
        default="GITHUB_TOKEN",
        help="env var name that stores a GitHub token",
    )
    parser.add_argument(
        "--manifest-json",
        default="",
        help="output manifest path; default is <output-root>/manifest.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="discover assets without downloading them",
    )
    return parser.parse_args()


def build_headers(token_env: str) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "edgededup-rls-dataset-builder",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get(token_env, "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def http_json(url: str, headers: dict[str, str]) -> object:
    req = Request(url, headers=headers)
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def stream_download(url: str, headers: dict[str, str], dst: Path) -> int:
    req = Request(url, headers=headers)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(req, timeout=120) as resp, dst.open("wb") as fh:
        written = 0
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
            written += len(chunk)
    return written


def topic_query(topic: str, mode: str) -> str:
    if mode == "topic":
        return f"topic:{topic} archived:false is:public"
    return f"{topic} in:name,description,readme archived:false is:public"


def search_repos(topic: str, mode: str, per_page: int, headers: dict[str, str]) -> list[dict[str, object]]:
    params = urlencode(
        {
            "q": topic_query(topic, mode),
            "sort": "stars",
            "order": "desc",
            "per_page": per_page,
        }
    )
    data = http_json(f"{GITHUB_API}/search/repositories?{params}", headers)
    if not isinstance(data, dict):
        return []
    items = data.get("items", [])
    return items if isinstance(items, list) else []


def list_releases(full_name: str, per_page: int, headers: dict[str, str]) -> list[dict[str, object]]:
    data = http_json(f"{GITHUB_API}/repos/{full_name}/releases?per_page={per_page}", headers)
    return data if isinstance(data, list) else []


def iter_matching_assets(
    releases: list[dict[str, object]],
    extensions: tuple[str, ...],
    min_asset_size: int,
) -> Iterable[tuple[dict[str, object], dict[str, object]]]:
    for release in releases:
        assets = release.get("assets", [])
        if not isinstance(assets, list):
            continue
        for asset in assets:
            name = str(asset.get("name", ""))
            size = int(asset.get("size", 0) or 0)
            if size < min_asset_size:
                continue
            if not name.lower().endswith(extensions):
                continue
            yield release, asset


def safe_name(text: str) -> str:
    out = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out).strip("_") or "item"


def main() -> int:
    args = parse_args()
    headers = build_headers(args.token_env)
    topics = args.topic or DEFAULT_TOPICS
    extensions = tuple((args.extension or DEFAULT_EXTENSIONS))
    output_root = Path(args.output_root).resolve()
    manifest_path = Path(args.manifest_json).resolve() if args.manifest_json else output_root / "manifest.json"
    output_root.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    manifest: list[dict[str, object]] = []
    downloaded = 0
    downloaded_bytes = 0

    print(f"[info] output_root={output_root}")
    print(f"[info] topics={topics}")
    print(f"[info] extensions={extensions}")
    if "Authorization" not in headers:
        print("[warn] no GitHub token detected; rate limits will be low", file=sys.stderr)

    try:
        for topic in topics:
            print(f"[topic] {topic}")
            repos = search_repos(topic, args.query_mode, args.repos_per_topic, headers)
            if not repos:
                print(f"[warn] no repos found for topic={topic}")
                continue

            rng.shuffle(repos)
            chosen = repos[: min(args.sample_repos_per_topic, len(repos))]
            print(f"[info] selected {len(chosen)} repos from {len(repos)} candidates for topic={topic}")

            for repo in chosen:
                full_name = str(repo.get("full_name", ""))
                if not full_name:
                    continue
                print(f"[repo] {full_name}")
                try:
                    releases = list_releases(full_name, args.releases_per_repo, headers)
                except HTTPError as exc:
                    print(f"[skip] releases unavailable for {full_name}: HTTP {exc.code}")
                    continue

                matched_for_repo = 0
                for release, asset in iter_matching_assets(releases, extensions, args.min_asset_size):
                    if matched_for_repo >= args.assets_per_repo:
                        break
                    if downloaded >= args.max_downloads:
                        raise StopIteration

                    asset_name = str(asset.get("name", ""))
                    asset_size = int(asset.get("size", 0) or 0)
                    if args.max_bytes and downloaded_bytes + asset_size > args.max_bytes:
                        raise StopIteration

                    release_tag = safe_name(str(release.get("tag_name", "untagged")))
                    repo_dir = output_root / safe_name(topic) / safe_name(full_name.replace("/", "__")) / release_tag
                    dst = repo_dir / asset_name
                    record = {
                        "topic": topic,
                        "repo_full_name": full_name,
                        "repo_html_url": repo.get("html_url", ""),
                        "release_tag": str(release.get("tag_name", "")),
                        "asset_name": asset_name,
                        "asset_size": asset_size,
                        "asset_url": asset.get("browser_download_url", ""),
                        "download_path": str(dst),
                        "downloaded": False,
                    }

                    if dst.exists() and dst.stat().st_size == asset_size and asset_size > 0:
                        print(f"[reuse] {dst}")
                        record["downloaded"] = True
                        manifest.append(record)
                        downloaded += 1
                        downloaded_bytes += asset_size
                        matched_for_repo += 1
                        continue

                    print(f"[asset] {full_name} {asset_name} ({asset_size} bytes)")
                    if not args.dry_run:
                        tmp = dst.with_suffix(dst.suffix + ".part")
                        tmp.unlink(missing_ok=True)
                        written = stream_download(str(asset.get("browser_download_url", "")), headers, tmp)
                        tmp.rename(dst)
                        if written != asset_size and asset_size > 0:
                            print(f"[warn] size mismatch for {dst}: expected {asset_size}, got {written}")
                        record["downloaded"] = True
                        downloaded_bytes += written
                        time.sleep(args.sleep_seconds)
                    manifest.append(record)
                    downloaded += 1
                    matched_for_repo += 1

    except StopIteration:
        print("[info] reached configured download limit")
    except HTTPError as exc:
        print(f"[error] HTTP {exc.code}: {exc.reason}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"[error] network error: {exc}", file=sys.stderr)
        return 1

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] manifest={manifest_path}")
    print(f"[done] files={downloaded} bytes={downloaded_bytes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
