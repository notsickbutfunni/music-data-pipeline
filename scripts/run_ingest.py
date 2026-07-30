from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict 
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.config import ConfigError, load_config
from src.ingestor import get_song_candidates_for_producer, get_top_producers, get_songs_by_artist_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Автоматический сбор треков популярных Voca-P из VocaDB.")
    parser.add_argument(
        "--producer-name", 
        type=str, 
        default=None, 
        help="Имя конкретного продюсера. Если не указано, автоматически берутся ТОП артисты."
    )
    parser.add_argument("--top-producers-count", type=int, default=100, help="Количество ТОП продюсеров для обработки (default: 100).")
    parser.add_argument("--max-songs-per-producer", type=int, default=10, help="Макс. треков на одного продюсера (default: 10).")
    parser.add_argument("--env-file", default=".env", help="Путь к .env файлу.")
    parser.add_argument("--json", action="store_true", help="Выводить результат в JSON.")
    parser.add_argument("--validate-config", action="store_true", help="Только проверить конфигурацию.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        cfg = load_config(env_file=args.env_file, strict=False)
    except ConfigError as exc:
        LOGGER.error("Ошибка конфигурации: %s", exc)
        return 2

    if args.validate_config:
        message = {
            "status": "ok",
            "vocadb_base_url": cfg.vocadb_base_url,
            "supabase_schema": cfg.supabase_schema,
            "r2_enabled": cfg.r2_enabled,
        }
        print(json.dumps(message, indent=2, ensure_ascii=False))
        return 0

    producers_to_process = []
    if args.producer_name:
        producers_to_process = [{"name": args.producer_name, "followers": None}]
    else:
        LOGGER.info("Загрузка списка TOP-%d продюсеров VocaDB по подписчикам...", args.top_producers_count)
        producers_to_process = get_top_producers(
            limit=args.top_producers_count, 
            base_url=cfg.vocadb_base_url
        )

    LOGGER.info("Найдено продюсеров для обработки: %d", len(producers_to_process))

    all_results = []

    for idx, producer in enumerate(producers_to_process, 1):
        p_name = producer["name"]
        p_id = producer.get("producer_id") or producer.get("id")
        
        LOGGER.info("[%d/%d] Обработка продюсера: %s (ID: %s)", idx, len(producers_to_process), p_name, p_id)
        
        try:
            songs = get_songs_by_artist_id(
                artist_id=p_id,
                max_songs=args.max_songs_per_producer,
                base_url=cfg.vocadb_base_url
            )
            
            candidates = get_song_candidates_for_producer(
                producer_name=p_name,
                max_songs=args.max_songs_per_producer,
            )
            with_media = candidates.get("with_media", [])
            
            all_results.append({
                "producer_id": p_id,
                "producer_name": p_name,
                "followers": producer.get("followers"),
                "found_songs_count": len(songs),
                "songs": songs,
            })
        except Exception as exc:
            LOGGER.error("Ошибка при сборе песен для %s: %s", p_name, exc)
            continue

    summary = {
        "total_producers_processed": len(all_results),
        "total_songs_found": sum(p["found_songs_count"] for p in all_results),
        "data": all_results,
    }

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        LOGGER.info("Сбор завершен! Всего продюсеров: %d, всего песен с видео: %d", 
                    summary["total_producers_processed"], summary["total_songs_found"])
    out_dir = ROOT_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ingest_output.json"

    # 2. Сохраняем итоговый JSON в файл
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    LOGGER.info("Данные успешно сохранены в %s", out_file)

    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        LOGGER.info(
            "Сбор завершен! Всего продюсеров: %d, всего песен с видео: %d",
            summary["total_producers_processed"],
            summary["total_songs_found"],
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())