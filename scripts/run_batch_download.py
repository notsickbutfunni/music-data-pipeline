#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure project root is on sys.path so `src` can be imported when running the script directly
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.downloader import AudioDownloader, DownloadError

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloadJob:
    song_id: int
    title: str
    url: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Batch-download song previews from ingestor JSON output with checkpointing."
    )
    p.add_argument(
        "manifest",
        help="Path to JSON file produced by scripts/test_ingestor.py or scripts/run_ingest.py",
    )
    p.add_argument("--output-dir", default="downloads", help="Directory to save previews")
    p.add_argument("--checkpoint", default="logs/download_checkpoint.json", help="Checkpoint file path")
    p.add_argument("--start-sec", type=int, default=15, help="Preview start offset in seconds")
    p.add_argument("--duration", type=int, default=30, help="Preview duration in seconds")
    p.add_argument("--codec", default="mp3", help="Output codec/extension (default: mp3)")
    p.add_argument("--bitrate-kbps", type=int, default=64, help="Target bitrate (default: 64)")
    p.add_argument("--workers", type=int, default=6, help="Maximum concurrent downloads")
    p.add_argument("--timeout", type=int, default=15, help="Network timeout per download")
    p.add_argument("--retries", type=int, default=3, help="Retries per download")
    p.add_argument(
        "--cache-namespace",
        default="v1",
        help="Version namespace used in deterministic cache keys",
    )
    p.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="Optional cap for number of jobs in this run (0 = no cap)",
    )
    return p.parse_args()


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_checkpoint(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "completed": {},
            "failed": {},
        }

    raw = _load_json(path)
    completed = raw.get("completed") if isinstance(raw.get("completed"), dict) else {}
    failed = raw.get("failed") if isinstance(raw.get("failed"), dict) else {}
    return {"completed": completed, "failed": failed}


def _save_checkpoint(path: Path, checkpoint: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(checkpoint, indent=2, ensure_ascii=False), encoding="utf-8")


def _build_jobs(manifest: Dict[str, Any]) -> List[DownloadJob]:
    items = manifest.get("with_media")
    if not isinstance(items, list):
        return []

    jobs: List[DownloadJob] = []
    seen_song_ids = set()
    for row in items:
        if not isinstance(row, dict):
            continue
        song_id = row.get("vocadb_song_id")
        if not isinstance(song_id, int):
            continue
        if song_id in seen_song_ids:
            continue

        urls = row.get("source_urls")
        if not isinstance(urls, list) or not urls:
            continue
        first_url = next((u for u in urls if isinstance(u, str) and u.strip()), None)
        if not first_url:
            continue

        seen_song_ids.add(song_id)
        jobs.append(
            DownloadJob(
                song_id=song_id,
                title=str(row.get("title") or ""),
                url=first_url.strip(),
            )
        )
    return jobs


def main() -> int:
    args = parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Manifest file not found: {manifest_path}", file=sys.stderr)
        return 2

    checkpoint_path = Path(args.checkpoint)
    checkpoint = _load_checkpoint(checkpoint_path)

    manifest = _load_json(manifest_path)
    jobs = _build_jobs(manifest)
    if args.max_items > 0:
        jobs = jobs[: args.max_items]

    if not jobs:
        print("No download jobs found in manifest.")
        return 0

    downloader = AudioDownloader(
        download_dir=args.output_dir,
        timeout=args.timeout,
        retries=args.retries,
        cache_namespace=args.cache_namespace,
    )

    completed_map = checkpoint["completed"]
    failed_map = checkpoint["failed"]

    pending_jobs: List[DownloadJob] = []
    for job in jobs:
        song_key = str(job.song_id)
        if song_key in completed_map:
            continue
        pending_jobs.append(job)

    if not pending_jobs:
        print("All songs in this manifest are already completed in checkpoint.")
        return 0

    lock = threading.Lock()
    stats = {
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped_completed": len(jobs) - len(pending_jobs),
    }

    def worker(job: DownloadJob) -> Dict[str, Any]:
        cache_key = downloader.build_cache_key(
            url=job.url,
            song_id=job.song_id,
            duration_sec=args.duration,
            codec=args.codec,
            bitrate_kbps=args.bitrate_kbps,
        )
        try:
            out = downloader.download_preview(
                url=job.url,
                song_id=job.song_id,
                start_sec=args.start_sec,
                duration_sec=args.duration,
                codec=args.codec,
                bitrate_kbps=args.bitrate_kbps,
                cache_key=cache_key,
            )
            if out is None:
                return {
                    "song_id": job.song_id,
                    "ok": False,
                    "error": "download returned no output",
                }
            return {
                "song_id": job.song_id,
                "ok": True,
                "path": str(out),
                "cache_key": cache_key,
            }
        except DownloadError as exc:
            return {
                "song_id": job.song_id,
                "ok": False,
                "error": str(exc),
            }

    print(
        f"Starting batch: total={len(jobs)} pending={len(pending_jobs)} workers={args.workers} "
        f"duration={args.duration}s codec={args.codec} bitrate={args.bitrate_kbps}kbps"
    )

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures: List[Future[Dict[str, Any]]] = [pool.submit(worker, job) for job in pending_jobs]

        for future in as_completed(futures):
            result = future.result()
            song_key = str(result["song_id"])
            with lock:
                stats["processed"] += 1
                if result.get("ok"):
                    stats["succeeded"] += 1
                    completed_map[song_key] = {
                        "path": result.get("path"),
                        "cache_key": result.get("cache_key"),
                        "completed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    }
                    failed_map.pop(song_key, None)
                else:
                    stats["failed"] += 1
                    failed_map[song_key] = {
                        "error": result.get("error"),
                        "failed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    }

                # Persist often so interrupted runs can resume near-last item.
                _save_checkpoint(checkpoint_path, checkpoint)

                if stats["processed"] % 20 == 0:
                    LOGGER.info(
                        "Progress: processed=%s succeeded=%s failed=%s",
                        stats["processed"],
                        stats["succeeded"],
                        stats["failed"],
                    )

    summary = {
        "manifest": str(manifest_path),
        "checkpoint": str(checkpoint_path),
        "total_jobs": len(jobs),
        "pending_jobs": len(pending_jobs),
        "processed": stats["processed"],
        "succeeded": stats["succeeded"],
        "failed": stats["failed"],
        "skipped_completed": stats["skipped_completed"],
        "duration_seconds": args.duration,
        "start_seconds": args.start_sec,
        "codec": args.codec,
        "bitrate_kbps": args.bitrate_kbps,
        "workers": args.workers,
    }

    summary_path = Path("logs") / f"download_summary_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Summary written to: {summary_path}")
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
