import argparse
import logging
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.downloader import AudioDownloader

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def main():
    parser = argparse.ArgumentParser(description="Test downloading a song preview")
    parser.add_argument("url", help="YouTube or NicoNico URL")
    parser.add_argument("song_id", help="Unique ID for the song to name the file")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    parser.add_argument("--start-sec", type=int, default=15, help="Preview start offset in seconds")
    parser.add_argument("--output-dir", default="downloads", help="Directory to save the preview")
    parser.add_argument("--codec", default="mp3", help="Output codec/extension (default: mp3)")
    parser.add_argument(
        "--bitrate-kbps",
        type=int,
        default=64,
        help="Target bitrate in kbps for preview output (default: 64)",
    )
    parser.add_argument(
        "--cache-namespace",
        default="v1",
        help="Version namespace used in deterministic cache keys",
    )

    args = parser.parse_args()

    downloader = AudioDownloader(download_dir=args.output_dir, cache_namespace=args.cache_namespace)
    cache_key = downloader.build_cache_key(
        url=args.url,
        song_id=args.song_id,
        duration_sec=args.duration,
        codec=args.codec,
        bitrate_kbps=args.bitrate_kbps,
    )
    expected_name = f"{cache_key}.{args.codec.strip().lower()}"
    print(f"Downloading {args.url} -> {args.output_dir}/{expected_name} (max {args.duration}s)...")
    
    result = downloader.download_preview(
        args.url,
        args.song_id,
        start_sec=args.start_sec,
        duration_sec=args.duration,
        codec=args.codec,
        bitrate_kbps=args.bitrate_kbps,
        cache_key=cache_key,
    )
    
    if result:
        print(f"Success! Saved to {result}")
        return 0
    else:
        print("Failed to download.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
