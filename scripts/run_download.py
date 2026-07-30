import json
import logging
from pathlib import Path


from src.downloader import AudioDownloader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("downloader_runner")


def main():
    json_path = Path("data/ingest_output.json")
    
    if not json_path.exists():
        logger.error("Файл %s не найден! Сначала запусти инжестор.", json_path)
        return

    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    downloader = AudioDownloader(download_dir="data/downloads")

    producers = payload.get("data", [])
    total_songs = 0
    successful_downloads = 0

    logger.info("Начало скачивания сэмплов для %d продюсеров...", len(producers))

    # 3. Проходим по каждому продюсеру и его трекам
    for producer in producers:
        p_name = producer.get("producer_name", "Unknown")
        songs = producer.get("songs", [])
        
        logger.info("--- Продюсер: %s (%d песен) ---", p_name, len(songs))

        for song in songs:
            total_songs += 1
            song_id = song.get("song_id") or song.get("id")
            pv_url = song.get("pv_url")

            # Если ссылки нет в pv_url, проверяем массив pvs (если он есть)
            if not pv_url and song.get("pvs"):
                pvs = song.get("pvs", [])
                pv_url = next((pv.get("url") for pv in pvs if pv.get("service") == "YouTube"), None)
                if not pv_url and pvs:
                    pv_url = pvs[0].get("url")

            if not pv_url or not song_id:
                logger.warning("У песни %s (ID: %s) нет ссылки на PV. Пропускаем.", song.get("title") or song.get("name"), song_id)
                continue

            # Скачиваем 15 секунд, перехватывая ошибки недоступных видео
            try:
                sample_path = downloader.download_preview(
                    url=pv_url,
                    song_id=song_id,
                    start_sec=45,
                    duration_sec=15,
                    bitrate_kbps=128
                )
                if sample_path:
                    successful_downloads += 1
            except Exception as err:
                logger.warning("Пропуск трека %s (song_id=%s) из-за ошибки: %s", pv_url, song_id, err)
                continue
            

    logger.info(
        "Скачивание завершено! Успешно обработано: %d из %d сэмплов.",
        successful_downloads,
        total_songs
    )


if __name__ == "__main__":
    main()