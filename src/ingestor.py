from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urlencode
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)


class IngestionError(RuntimeError):
	"""Base exception for ingestion failures."""


class ProducerNotFoundError(IngestionError):
	"""Raised when a producer cannot be resolved from VocaDB."""


@dataclass(frozen=True)
class Producer:
	vocadb_artist_id: int
	name: str


@dataclass(frozen=True)
class SongCandidate:
	vocadb_song_id: int
	title: str
	producer_vocadb_id: int
	producer_name: str
	source_urls: List[str] = field(default_factory=list)
	vocaloids: List[str] = field(default_factory=list)


def _safe_int(value: Any, default: int = 0) -> int:
	try:
		return int(value)
	except (TypeError, ValueError):
		return default


def _http_get_json(
	url: str,
	timeout_seconds: int,
	max_retries: int,
	retry_backoff_seconds: float,
) -> Dict[str, Any]:
	"""Perform a GET request with retry handling for transient failures."""
	last_error: Optional[Exception] = None

	for attempt in range(max_retries + 1):
		try:
			request = Request(url=url, headers={"Accept": "application/json"}, method="GET")
			with urlopen(request, timeout=timeout_seconds) as response:
				body = response.read().decode("utf-8")
				payload = json.loads(body)
				if isinstance(payload, dict):
					return payload
				return {"items": payload}
		except HTTPError as exc:
			last_error = exc
			# Retry rate-limited and server errors.
			if exc.code in {429, 500, 502, 503, 504} and attempt < max_retries:
				delay = retry_backoff_seconds * (2**attempt)
				LOGGER.warning("retryable_http_error code=%s url=%s delay=%.2fs", exc.code, url, delay)
				time.sleep(delay)
				continue
			raise IngestionError(f"HTTP error while fetching {url}: {exc.code}") from exc
		except (URLError, TimeoutError, json.JSONDecodeError) as exc:
			last_error = exc
			if attempt < max_retries:
				delay = retry_backoff_seconds * (2**attempt)
				LOGGER.warning("retryable_network_error url=%s delay=%.2fs", url, delay)
				time.sleep(delay)
				continue
			raise IngestionError(f"Failed to fetch/parse JSON from {url}") from exc

	raise IngestionError(f"Request failed after retries for {url}. last_error={last_error}")


def extract_media_urls(song_payload: Dict[str, Any]) -> List[str]:
	"""Extract usable media URLs from a song payload, with preference ordering."""
	links = song_payload.get("webLinks") or []
	candidates: List[str] = []

	for link in links:
		if not isinstance(link, dict):
			continue
		url = (link.get("url") or "").strip()
		if not url:
			continue
		lower = url.lower()
		if "youtube.com" in lower or "youtu.be" in lower:
			candidates.append(url)
			continue
		if "nicovideo.jp" in lower or "nico.ms" in lower:
			candidates.append(url)

	# Preserve order but remove duplicates.
	deduped: List[str] = []
	seen = set()
	for url in candidates:
		if url not in seen:
			seen.add(url)
			deduped.append(url)
	return deduped


def _extract_vocaloids(song_payload: Dict[str, Any]) -> List[str]:
	names: List[str] = []
	artists = song_payload.get("artists") or []

	for artist in artists:
		if not isinstance(artist, dict):
			continue
		categories = artist.get("categories")
		category_match = False
		if isinstance(categories, str) and "vocaloid" in categories.lower():
			category_match = True
		if isinstance(categories, list) and any(
			isinstance(item, str) and "vocaloid" in item.lower() for item in categories
		):
			category_match = True
		if not category_match:
			continue

		artist_name = ""
		artist_block = artist.get("artist")
		if isinstance(artist_block, dict):
			artist_name = (artist_block.get("name") or "").strip()
		if artist_name:
			names.append(artist_name)

	deduped: List[str] = []
	seen = set()
	for name in names:
		key = name.lower()
		if key not in seen:
			seen.add(key)
			deduped.append(name)
	return deduped


class VocaDBIngestor:
	"""Typed client for producer/song metadata ingestion from VocaDB."""

	def __init__(
		self,
		base_url: str = "https://vocadb.net/api",
		timeout_seconds: int = 20,
		max_retries: int = 3,
		retry_backoff_seconds: float = 1.0,
		page_size: int = 50,
	) -> None:
		self.base_url = base_url.rstrip("/")
		self.timeout_seconds = timeout_seconds
		self.max_retries = max_retries
		self.retry_backoff_seconds = retry_backoff_seconds
		self.page_size = page_size

	def search_producer(self, producer_name: str) -> Producer:
		"""Resolve a producer by name using VocaDB artist search."""
		query_name = producer_name.strip()
		if not query_name:
			raise ValueError("producer_name must not be empty.")

		query = urlencode(
			{
				"query": query_name,
				"nameMatchMode": "Auto",
				"maxResults": 10,
				"fields": "MainPicture",
			}
		)
		url = f"{self.base_url}/artists?{query}"
		payload = _http_get_json(
			url=url,
			timeout_seconds=self.timeout_seconds,
			max_retries=self.max_retries,
			retry_backoff_seconds=self.retry_backoff_seconds,
		)

		items = payload.get("items") or []
		if not items:
			LOGGER.error("producer_not_found producer_name=%s", producer_name)
			raise ProducerNotFoundError(f"Producer not found: {producer_name}")

		# Prefer exact case-insensitive match; fallback to first result.
		selected = items[0]
		for item in items:
			name = str(item.get("name") or "")
			if name.lower() == query_name.lower():
				selected = item
				break

		producer = Producer(
			vocadb_artist_id=_safe_int(selected.get("id")),
			name=str(selected.get("name") or query_name),
		)
		return producer

	def fetch_songs_for_producer(
		self,
		producer: Producer,
		max_songs: Optional[int] = None,
	) -> List[SongCandidate]:
		"""Fetch producer songs with pagination and normalize response payloads."""
		results: List[SongCandidate] = []
		seen_song_ids = set()
		start = 0

		while True:
			if max_songs is not None and len(results) >= max_songs:
				break

			url = self._build_song_search_url(producer_id=producer.vocadb_artist_id, start=start)
			payload = _http_get_json(
				url=url,
				timeout_seconds=self.timeout_seconds,
				max_retries=self.max_retries,
				retry_backoff_seconds=self.retry_backoff_seconds,
			)
			items = payload.get("items") or []
			if not items:
				break

			for item in items:
				if not isinstance(item, dict):
					continue
				song_id = _safe_int(item.get("id"))
				if song_id <= 0 or song_id in seen_song_ids:
					continue

				source_urls = extract_media_urls(item)
				if not source_urls:
					LOGGER.info("no_source_url vocadb_song_id=%s", song_id)
					continue

				candidate = SongCandidate(
					vocadb_song_id=song_id,
					title=str(item.get("name") or "Untitled"),
					producer_vocadb_id=producer.vocadb_artist_id,
					producer_name=producer.name,
					source_urls=source_urls,
					vocaloids=_extract_vocaloids(item),
				)
				seen_song_ids.add(song_id)
				results.append(candidate)

				if max_songs is not None and len(results) >= max_songs:
					break

			if len(items) < self.page_size:
				break
			start += self.page_size

		return results

	def _build_song_search_url(self, producer_id: int, start: int) -> str:
		fields = [
			"Artists",
			"WebLinks",
			"MainPicture",
		]
		params = {
			"artistId[]": str(producer_id),
			"start": str(start),
			"maxResults": str(self.page_size),
			"fields": ",".join(fields),
			"sort": "PublishDate",
			"lang": "Default",
			"nameMatchMode": "Auto",
			"songTypes": "Original,Remaster,Remix,Cover",
		}

		# urlencode does not preserve [] in keys by default; keep API-compatible key.
		encoded = urlencode(params)
		encoded = encoded.replace("artistId%5B%5D", quote_plus("artistId[]"))
		return f"{self.base_url}/songs?{encoded}"


def get_song_candidates_for_producer(
	producer_name: str,
	base_url: str = "https://vocadb.net/api",
	timeout_seconds: int = 20,
	max_retries: int = 3,
	retry_backoff_seconds: float = 1.0,
	page_size: int = 50,
	max_songs: Optional[int] = None,
) -> List[SongCandidate]:
	"""Convenience helper used by orchestration code."""
	ingestor = VocaDBIngestor(
		base_url=base_url,
		timeout_seconds=timeout_seconds,
		max_retries=max_retries,
		retry_backoff_seconds=retry_backoff_seconds,
		page_size=page_size,
	)
	producer = ingestor.search_producer(producer_name)
	return ingestor.fetch_songs_for_producer(producer=producer, max_songs=max_songs)
