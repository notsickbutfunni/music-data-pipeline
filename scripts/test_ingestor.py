#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure project root is on sys.path so `src` can be imported when running the script directly
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
	sys.path.insert(0, str(ROOT_DIR))

from src.ingestor import get_song_candidates_for_producer


def build_parser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(description="Quick test runner for VocaDB ingestor.")
	p.add_argument("producer_name", help="Producer name to search for")
	p.add_argument("--max-songs", type=int, default=200, help="Maximum songs to fetch")
	p.add_argument("--page-size", type=int, default=50, help="VocaDB page size")
	return p


def main() -> int:
	args = build_parser().parse_args()
	out_dir = Path("logs")
	out_dir.mkdir(parents=True, exist_ok=True)

	candidates = get_song_candidates_for_producer(
		args.producer_name, page_size=args.page_size, max_songs=args.max_songs
	)

	with_media = candidates.get("with_media", [])
	without_media = candidates.get("without_media", [])

	result = {
		"producer": args.producer_name,
		"with_media_count": len(with_media),
		"without_media_count": len(without_media),
		"with_media": [c.__dict__ for c in with_media],
		"without_media": [c.__dict__ for c in without_media],
	}

	filename = out_dir / f"test_ingestor_{args.producer_name.replace(' ', '_')}.json"
	filename.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
	print(f"Wrote {filename} — with_media={len(with_media)} without_media={len(without_media)}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
