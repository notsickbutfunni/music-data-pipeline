import hashlib
import logging
from pathlib import Path
from typing import Optional

import yt_dlp

LOGGER = logging.getLogger(__name__)


class DownloadError(Exception):
    """Базовое исключение для ошибок скачивания."""


class AudioDownloader:
    """Скачивает короткие аудио-превью с YouTube/NicoNico с помощью yt-dlp."""

    def __init__(
        self,
        download_dir: str | Path,
        timeout: int = 15,
        retries: int = 3,
        cache_namespace: str = "v1",
    ) -> None:
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.retries = retries
        self.cache_namespace = cache_namespace

    def build_cache_key(
        self,
        url: str,
        song_id: int | str,
        start_sec: int,
        duration_sec: int,
        codec: str,
        bitrate_kbps: int,
    ) -> str:
        """Создает хеш-ключ для кэширования одинаковых запросов."""
        seed = "|".join(
            [
                str(song_id),
                url.strip().lower(),
                str(start_sec),
                str(duration_sec),
                codec.strip().lower(),
                str(bitrate_kbps),
                self.cache_namespace,
            ]
        )
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
        return f"{song_id}_{digest}"

    def download_preview(
        self,
        url: str,
        song_id: int | str,
        start_sec: int = 45,       # Начинаем с 45-й секунды (обычно там кульминация/припев)
        duration_sec: int = 15,    # Длительность ровно 15 секунд
        codec: str = "mp3",
        bitrate_kbps: int = 64,    # 64 kbps — идеальный баланс качества и веса (~100 KB на сэмпл)
        cache_key: Optional[str] = None,
    ) -> Optional[Path]:
        """
        Скачивает ТОЛЬКО указанный фрагмент аудио напрямик из сети без полной загрузки.
        """
        resolved_codec = codec.strip().lower()
        resolved_key = cache_key or self.build_cache_key(
            url=url,
            song_id=song_id,
            start_sec=start_sec,
            duration_sec=duration_sec,
            codec=resolved_codec,
            bitrate_kbps=bitrate_kbps,
        )

        expected_file = self.download_dir / f"{resolved_key}.{resolved_codec}"

        # Если файл уже в кэше — сразу возвращаем путь
        if expected_file.exists():
            LOGGER.info("Сэмпл %s уже существует. Пропускаем скачивание.", expected_file.name)
            return expected_file

        end_sec = start_sec + duration_sec
        time_section = f"*{start_sec}-{end_sec}"

        ydl_opts = {
            'format': 'ba/ba*',  # Берем только лучший аудиопоток
            'outtmpl': str(self.download_dir / f"{resolved_key}.%(ext)s"),
            'download_sections': [time_section],  # Стримит ТОЛЬКО нужный отрезок
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': self.timeout,
            'retries': self.retries,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': resolved_codec,
                'preferredquality': str(bitrate_kbps),
            }],
        }

        try:
            LOGGER.info("Скачивание 15-сек сэмпла для song_id=%s (URL: %s)", song_id, url)
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                error_code = ydl.download([url])
                if error_code != 0:
                    LOGGER.error("yt-dlp вернул ошибку %s для URL: %s", error_code, url)
                    return None

            if expected_file.exists():
                LOGGER.info("Успешно создан сэмпл: %s", expected_file.name)
                return expected_file

            LOGGER.error("Файл %s не был найден после обработки.", expected_file)
            return None

        except Exception as exc:
            LOGGER.exception("Ошибка при скачивании сэмпла %s: %s", url, exc)
            raise DownloadError(f"Не удалось скачать сэмпл для {url}") from exc