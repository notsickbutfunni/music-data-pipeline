import logging
import hashlib
from pathlib import Path
from typing import Optional

import yt_dlp

LOGGER = logging.getLogger(__name__)


class DownloadError(Exception):
    """Base exception for download failures."""


class AudioDownloader:
    """Downloads audio previews from URLs using yt-dlp."""

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
        """Build a deterministic key so reruns can skip unchanged download settings."""
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
        start_sec: int = 15,
        duration_sec: int = 30,
        codec: str = "mp3",
        bitrate_kbps: int = 64,
        cache_key: Optional[str] = None,
    ) -> Optional[Path]:
        """
        Downloads best audio and extracts a short preview using configured codec/bitrate.
        Returns the path to the downloaded file, or None if the download failed.
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
        output_template = str(self.download_dir / resolved_key)
        expected_file = self.download_dir / f"{resolved_key}.{resolved_codec}"

        # If we already have the preview, skip downloading
        if expected_file.exists():
            LOGGER.info("File %s already exists. Skipping download.", expected_file)
            return expected_file

        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': output_template + '.%(ext)s',
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': self.timeout,
            'retries': self.retries,
            'extract_audio': True,
            # Use postprocessors to extract as MP3 and clip duration
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': resolved_codec,
                'preferredquality': str(bitrate_kbps),
            }],
            'postprocessor_args': [
                '-ss', str(start_sec),
                '-t', str(duration_sec)
            ],
        }

        try:
            LOGGER.info("Starting download for %s (URL: %s)", song_id, url)
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                error_code = ydl.download([url])
                if error_code != 0:
                    LOGGER.error("yt-dlp returned error code %s for url %s", error_code, url)
                    return None

            if expected_file.exists():
                LOGGER.info("Successfully downloaded preview to %s", expected_file)
                return expected_file

            LOGGER.error("Expected output file %s not found after download.", expected_file)
            return None
        except Exception as exc:
            LOGGER.exception("Failed to download audio from %s: %s", url, exc)
            raise DownloadError(f"Download failed for {url}") from exc
