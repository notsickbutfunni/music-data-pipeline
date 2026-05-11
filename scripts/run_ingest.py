from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
	sys.path.insert(0, str(ROOT_DIR))

from src.config import ConfigError, load_config
from src.ingestor import get_song_candidates_for_producer


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Fetch VocaDB song candidates for a producer.")
	parser.add_argument("producer_name", nargs="?", help="Producer name to search for in VocaDB.")
	parser.add_argument("--env-file", default=".env", help="Path to the environment file to load.")
	parser.add_argument("--max-songs", type=int, default=50, help="Maximum number of songs to return.")
	parser.add_argument("--page-size", type=int, default=50, help="VocaDB page size for song queries.")
	parser.add_argument("--json", action="store_true", help="Output the result as JSON.")
	parser.add_argument(
		"--validate-config",
		action="store_true",
		help="Only load and validate configuration without querying VocaDB.",
	)
	return parser


def main(argv: list[str] | None = None) -> int:
	parser = build_parser()
	args = parser.parse_args(argv)

	try:
		cfg = load_config(env_file=args.env_file)
	except ConfigError as exc:
		print(f"Configuration error: {exc}", file=sys.stderr)
		return 2

	if args.validate_config:
		message = {
			"status": "ok",
			"vocadb_base_url": cfg.vocadb_base_url,
			"vocadb_timeout_seconds": cfg.vocadb_timeout_seconds,
			"supabase_schema": cfg.supabase_schema,
			"r2_endpoint_url": cfg.r2_endpoint_url,
		}
		print(json.dumps(message, indent=2) if args.json else message)
		return 0

	if not args.producer_name:
		parser.error("producer_name is required unless --validate-config is used")

	candidates = get_song_candidates_for_producer(
		producer_name=args.producer_name,
		base_url=cfg.vocadb_base_url,
		timeout_seconds=cfg.vocadb_timeout_seconds,
		page_size=args.page_size,
		max_songs=args.max_songs,
	)

	with_media = candidates.get("with_media", [])
	without_media = candidates.get("without_media", [])

	result = {
		"producer_name": args.producer_name,
		"count": len(with_media) + len(without_media),
		"with_media_count": len(with_media),
		"without_media_count": len(without_media),
		"with_media": [asdict(song) for song in with_media],
		"without_media": [asdict(song) for song in without_media],
	}
	print(json.dumps(result, indent=2, ensure_ascii=False) if args.json else result)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
