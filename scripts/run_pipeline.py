import json
import logging
import os
from pathlib import Path
from typing import Any

import librosa
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

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


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def extract_audio_features(file_path: Path) -> dict[str, Any] | None:
    """Извлекает базовые аудио-фичи из mp3 файла."""
    try:
        y, sr = librosa.load(file_path, duration=30.0)

        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
        bpm = float(tempo[0]) if isinstance(tempo, np.ndarray) else float(tempo)

        rms = librosa.feature.rms(y=y)
        rms_energy = float(np.mean(rms))

        pitches, magnitudes = librosa.piptrack(y=y, sr=sr)
        pitch_values = pitches[magnitudes > np.median(magnitudes)]
        mean_pitch = (
            float(np.mean(pitch_values)) if len(pitch_values) > 0 else 0.0
        )

        return {
            "bpm": round(bpm, 2),
            "rms_energy": round(rms_energy, 4),
            "mean_pitch_hz": round(mean_pitch, 2),
            "sample_rate": sr,
        }
    except Exception as e:
        logger.error(f"Error extracting features for {file_path}: {e}")
        return None


# ------------------------------------------------------------------
# PERSISTENCE LAYER (Под твою реальную схему)
# ------------------------------------------------------------------
def process_and_save_payload(json_data: dict[str, Any], conn):
    cursor = conn.cursor()
    producers_data = json_data.get("data", [])

    logger.info(f"Начало обработки {len(producers_data)} продюсеров...")

    for producer in producers_data:
        voca_artist_id = producer["producer_id"]
        p_name = producer["producer_name"]

        # 1. Upsert в dim_producers
        cursor.execute(
            """
            INSERT INTO dim_producers (vocadb_artist_id, producer_name)
            VALUES (%s, %s)
            ON CONFLICT (vocadb_artist_id) DO UPDATE SET
                producer_name = EXCLUDED.producer_name,
                updated_at = CURRENT_TIMESTAMP
            RETURNING producer_id;
        """,
            (voca_artist_id, p_name),
        )

        producer_id = cursor.fetchone()[0]

        for song in producer.get("songs", []):
            voca_song_id = song["song_id"]
            title = song["title"]
            published_at = song.get("publish_date")
            pv_url = song.get("pv_url")

            # 2. Upsert в dim_songs
            cursor.execute(
                """
                INSERT INTO dim_songs (vocadb_song_id, producer_id, title, published_at, source_platform, source_url, raw_payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vocadb_song_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING song_id;
            """,
                (
                    voca_song_id,
                    producer_id,
                    title,
                    published_at,
                    "YouTube",
                    pv_url,
                    json.dumps(song),
                ),
            )

            db_song_id = cursor.fetchone()[0]

            # 3. Insert в song_links
            if pv_url:
                cursor.execute(
                    """
                    INSERT INTO song_links (song_id, url, platform, category, is_official, is_primary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """,
                    (db_song_id, pv_url, "YouTube", "Official", True, True),
                )

            # 4. Проверяем локальный скачанный MP3 и считаем Librosa фичи
            possible_files = list(AUDIO_DIR.glob(f"*{voca_song_id}*.mp3"))

            if possible_files:
                audio_path = possible_files[0]
                features = extract_audio_features(audio_path)

                if features:
                    cursor.execute(
                        """
                        INSERT INTO fact_audio_features (
                            song_id, producer_id, bpm, mean_pitch_hz, rms_energy, 
                            sample_rate, extraction_status, pipeline_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (song_id, pipeline_version) DO UPDATE SET
                            bpm = EXCLUDED.bpm,
                            mean_pitch_hz = EXCLUDED.mean_pitch_hz,
                            rms_energy = EXCLUDED.rms_energy,
                            extracted_at = CURRENT_TIMESTAMP;
                    """,
                        (
                            db_song_id,
                            producer_id,
                            features["bpm"],
                            features["mean_pitch_hz"],
                            features["rms_energy"],
                            features["sample_rate"],
                            "SUCCESS",
                            "v1.0",
                        ),
                    )
                    logger.info(
                        f"Успешно сохранены фичи для песни '{title}' (ID: {db_song_id})"
                    )

    conn.commit()
    cursor.close()
    logger.info("Всё успешно записано в БД!")


def main():
    json_input_path = Path("data/ingest_output.json")
    if not json_input_path.exists():
        logger.error(f"Файл {json_input_path} не найден!")
        return

    with open(json_input_path, "r", encoding="utf-8") as f:
        json_payload = json.load(f)

    conn = get_db_connection()
    try:
        process_and_save_payload(json_payload, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()