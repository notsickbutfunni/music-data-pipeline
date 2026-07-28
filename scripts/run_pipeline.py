# Сохраняет сырой JSON в staging_songs_raw.
# Раскладывает данные по dim_songs, song_links, song_tags.
# Скачивает 5–10 аудио-превью (downloader.py).
# Считает базовые фичи (bpm, mean_pitch_hz, rms_energy) и пишет в fact_audio_features.

import json
import logging
import os
from pathlib import Path
import librosa
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import requests

# ------------------------------------------------------------------
# CONFIG & SETUP
# ------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("pipeline")

# Параметры БД (замените на свои или читайте из os.getenv)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "music_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

# Директория для временных аудио-файлов
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
def extract_audio_features(file_path: Path) -> dict:
    """Извлекает базовые аудио-фичи из mp3/wav файла."""
    try:
        # Загружаем первые 30 секунд аудио (sr=22050 по умолчанию)
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


def download_audio_preview(url: str, song_id: int) -> Path:
    """Скачивает превью файла во временную папку."""
    target_path = AUDIO_DIR / f"{song_id}_preview.mp3"
    if target_path.exists():
        return target_path

    try:
        response = requests.get(url, timeout=15, stream=True)
        response.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return target_path
    except Exception as e:
        logger.warning(f"Failed to download audio for song {song_id}: {e}")
        return None


# ------------------------------------------------------------------
# 2. PERSISTENCE LAYER (PostgreSQL)
# ------------------------------------------------------------------
def process_and_save_payload(json_data: dict, conn):
    """Раскладывает JSON по нормализованным таблицам Postgres."""
    cursor = conn.cursor()

    producer_name = json_data.get("producer", "Unknown")
    songs_with_media = json_data.get("with_media", [])
    songs_without_media = json_data.get("without_media", [])
    all_songs = songs_with_media + songs_without_media

    logger.info(
        f"Processing producer '{producer_name}' with {len(all_songs)} total songs..."
    )

    # --- A. Save Staging Raw Payload ---
    cursor.execute(
        """
        INSERT INTO staging_songs_raw (producer_name, payload)
        VALUES (%s, %s);
    """,
        (producer_name, json.dumps(json_data)),
    )

    # Собираем батчи для сохранения
    songs_batch = []
    links_batch = []
    tags_set = set()
    song_tags_batch = []

    for song in all_songs:
        song_id = song["vocadb_song_id"]
        title = song["title"]
        duration = song.get("duration_seconds")
        bpm = song.get("bpm")
        published_at = song.get("published_at")

        songs_batch.append((song_id, title, duration, bpm, published_at))

        # Собираем ссылки
        for link in song.get("classified_links", []):
            links_batch.append(
                (
                    song_id,
                    link["url"],
                    link.get("is_official", False),
                    link.get("raw", {}).get("category", "Official"),
                )
            )

        # Собираем теги
        for tag in song.get("tags", []):
            tags_set.add(tag)
            song_tags_batch.append((song_id, tag))

    # --- B. Upsert into dim_songs ---
    songs_sql = """
        INSERT INTO dim_songs (vocadb_song_id, title, duration_seconds, bpm, published_at)
        VALUES %s
        ON CONFLICT (vocadb_song_id) DO UPDATE SET
            title = EXCLUDED.title,
            duration_seconds = EXCLUDED.duration_seconds,
            bpm = EXCLUDED.bpm;
    """
    execute_values(cursor, songs_sql, songs_batch)

    # --- C. Upsert Tags ---
    if tags_set:
        tags_sql = """
            INSERT INTO dim_tags (name) VALUES %s
            ON CONFLICT (name) DO NOTHING;
        """
        execute_values(cursor, tags_sql, [(t,) for t in tags_set])

        # Map tag_name -> tag_id
        cursor.execute("SELECT name, tag_id FROM dim_tags;")
        tag_map = dict(cursor.fetchall())

        song_tag_ids = [
            (s_id, tag_map[t_name])
            for s_id, t_name in song_tags_batch
            if t_name in tag_map
        ]

        song_tags_sql = """
            INSERT INTO song_tags (song_id, tag_id) VALUES %s
            ON CONFLICT DO NOTHING;
        """
        execute_values(cursor, song_tags_sql, song_tag_ids)

    # --- D. Upsert Links ---
    if links_batch:
        links_sql = """
            INSERT INTO song_links (song_id, url, is_official, category)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        execute_values(cursor, links_sql, links_batch)

    conn.commit()
    logger.info("Successfully populated relational tables!")

    # --- E. Extract Audio Features for playable songs (Max 5 for test) ---
    logger.info("Starting Audio Feature Extraction pipeline...")
    pipeline_version = "v1.0"
    processed_count = 0

    for song in songs_with_media[:5]:  # Берем первые 5 треков для быстроты
        song_id = song["vocadb_song_id"]
        source_urls = song.get("source_urls", [])

        if not source_urls:
            continue

        audio_url = source_urls[0]
        audio_path = download_audio_preview(audio_url, song_id)

        if audio_path:
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
                logger.info(f"Extracted features for song ID {song_id}: {features}")

    conn.commit()
    cursor.close()
    logger.info(
        f"Pipeline run finished! Total audio features computed: {processed_count}"
    )


# ------------------------------------------------------------------
# MAIN ENTRYPOINT
# ------------------------------------------------------------------
def main():
    # Путь к файлу с результатами работы вашего инжестора
    json_input_path = Path("logs/test_ingestor_nashimoto.json")

    if not json_input_path.exists():
        logger.error(
            f"Input file {json_input_path} not found. Please run ingestor first!"
        )
        return

    logger.info(f"Loading {json_input_path}...")
    with open(json_input_path, "r", encoding="utf-8") as f:
        json_payload = json.load(f)

    # Подключение и запуск
    conn = get_db_connection()
    try:
        process_and_save_payload(json_payload, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()