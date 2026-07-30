import json
import logging
import os
from pathlib import Path
from typing import Any

import librosa
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import requests

# Если у тебя есть готовый класс AudioDownloader с yt-dlp, импортируй его:
# from src.downloader import AudioDownloader

# ------------------------------------------------------------------
# CONFIG & SETUP
# ------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("db_pipeline")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "music_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

AUDIO_DIR = Path("data/downloads")
AUDIO_DIR.mkdir(parents=True, exist_ok=True)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


# ------------------------------------------------------------------
# 1. AUDIO PROCESSING (Librosa)
# ------------------------------------------------------------------
def extract_audio_features(file_path: Path) -> dict[str, float] | None:
    """Извлекает базовые аудио-фичи из mp3/wav файла."""
    try:
        # Загружаем аудио (до 30 сек)
        y, sr = librosa.load(file_path, duration=30.0)

        # 1. BPM (Tempo)
        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
        bpm = float(tempo[0]) if isinstance(tempo, np.ndarray) else float(tempo)

        # 2. RMS Energy
        rms = librosa.feature.rms(y=y)
        rms_energy = float(np.mean(rms))

        # 3. Mean Pitch (Hz) via Pitch Tracking (piptrack)
        pitches, magnitudes = librosa.piptrack(y=y, sr=sr)
        pitch_values = pitches[magnitudes > np.median(magnitudes)]
        mean_pitch = (
            float(np.mean(pitch_values)) if len(pitch_values) > 0 else 0.0
        )

        return {
            "bpm": round(bpm, 2),
            "rms_energy": round(rms_energy, 4),
            "mean_pitch_hz": round(mean_pitch, 2),
        }
    except Exception as e:
        logger.error(f"Error extracting features for {file_path}: {e}")
        return None


# ------------------------------------------------------------------
# 2. PERSISTENCE LAYER (PostgreSQL)
# ------------------------------------------------------------------
def process_and_save_payload(json_data: dict[str, Any], conn):
    """Раскладывает новый формат JSON VocaDB по нормализованным таблицам Postgres."""
    cursor = conn.cursor()

    producers_data = json_data.get("data", [])
    logger.info(
        f"Processing batch: {len(producers_data)} producers, "
        f"{json_data.get('total_songs_found', 0)} total songs..."
    )

    # --- A. Save Staging Raw Payload ---
    cursor.execute(
        """
        INSERT INTO staging_songs_raw (producer_name, payload)
        VALUES (%s, %s);
    """,
        ("voca_batch_ingest", json.dumps(json_data)),
    )

    producers_batch = []
    songs_batch = []
    links_batch = []

    for producer in producers_data:
        p_id = producer["producer_id"]
        p_name = producer["producer_name"]
        followers = producer.get("followers", 0)

        producers_batch.append((p_id, p_name, followers))

        for song in producer.get("songs", []):
            song_id = song["song_id"]
            title = song["title"]
            published_at = song.get("publish_date")
            pv_url = song.get("pv_url")

            # В новом формате bpm/duration приходят позже после анализа librosa
            songs_batch.append((song_id, p_id, title, published_at))

            if pv_url:
                links_batch.append((song_id, pv_url, True, "YouTube"))

    # --- B. Upsert into dim_producers (если есть такая таблица) ---
    if producers_batch:
        producers_sql = """
            INSERT INTO dim_producers (producer_id, name, followers_count)
            VALUES %s
            ON CONFLICT (producer_id) DO UPDATE SET
                name = EXCLUDED.name,
                followers_count = EXCLUDED.followers_count;
        """
        # Закомментируй, если dim_producers еще не создана
        # execute_values(cursor, producers_sql, producers_batch)

    # --- C. Upsert into dim_songs ---
    if songs_batch:
        songs_sql = """
            INSERT INTO dim_songs (vocadb_song_id, producer_id, title, published_at)
            VALUES %s
            ON CONFLICT (vocadb_song_id) DO UPDATE SET
                title = EXCLUDED.title,
                published_at = EXCLUDED.published_at;
        """
        execute_values(cursor, songs_sql, songs_batch)

    # --- D. Upsert Links ---
    if links_batch:
        links_sql = """
            INSERT INTO song_links (song_id, url, is_official, category)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        execute_values(cursor, links_sql, links_batch)

    conn.commit()
    logger.info("Successfully populated relational tables (dim_songs & links)!")

    # --- E. Extract Audio Features for downloaded local MP3s ---
    logger.info("Starting Audio Feature Extraction pipeline...")
    pipeline_version = "v1.0"
    processed_count = 0

    # Проверяем локально скачанные файлы в data/downloads
    for producer in producers_data:
        for song in producer.get("songs", []):
            song_id = song["song_id"]

            # Ищем, скачан ли файл (например: 28_13351.mp3 или 13351_preview.mp3)
            possible_files = list(AUDIO_DIR.glob(f"*{song_id}*.mp3"))

            if not possible_files:
                continue

            audio_path = possible_files[0]
            features = extract_audio_features(audio_path)

            if features:
                cursor.execute(
                    """
                    INSERT INTO fact_audio_features (
                        song_id, pipeline_version, bpm, mean_pitch_hz, rms_energy
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id, pipeline_version) DO UPDATE SET
                        bpm = EXCLUDED.bpm,
                        mean_pitch_hz = EXCLUDED.mean_pitch_hz,
                        rms_energy = EXCLUDED.rms_energy;
                """,
                    (
                        song_id,
                        pipeline_version,
                        features["bpm"],
                        features["mean_pitch_hz"],
                        features["rms_energy"],
                    ),
                )
                processed_count += 1
                logger.info(
                    f"Extracted features for song ID {song_id}: {features}"
                )

    conn.commit()
    cursor.close()
    logger.info(
        f"Pipeline run finished! Total audio features computed: {processed_count}"
    )


# ------------------------------------------------------------------
# MAIN ENTRYPOINT
# ------------------------------------------------------------------
def main():
    # Путь к нашему новому сгенерированному JSON
    json_input_path = Path("data/ingest_output.json")

    if not json_input_path.exists():
        logger.error(
            f"Input file {json_input_path} not found. Run run_ingest.py first!"
        )
        return

    logger.info(f"Loading {json_input_path}...")
    with open(json_input_path, "r", encoding="utf-8") as f:
        json_payload = json.load(f)

    conn = get_db_connection()
    try:
        process_and_save_payload(json_payload, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()