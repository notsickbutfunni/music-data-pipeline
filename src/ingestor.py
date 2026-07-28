from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple
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
	# preserve all webLinks metadata from VocaDB for downstream classification
	all_weblinks: List[Dict[str, Any]] = field(default_factory=list)
	# derived classification of useful media links with an "is_official" flag
	classified_links: List[Dict[str, Any]] = field(default_factory=list)
	# enriched metadata fields
	duration_seconds: Optional[float] = None
	bpm: Optional[float] = None
	tags: List[str] = field(default_factory=list)
	languages: List[str] = field(default_factory=list)
	published_at: Optional[str] = None
	statistics: Dict[str, Any] = field(default_factory=dict)
	versions: List[Dict[str, Any]] = field(default_factory=list)


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


def extract_media_urls(song_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
	"""Extract usable media URLs from a song payload and classify links.

	Returns a list of dicts: {"url": str, "is_official": bool, "raw": <original link dict>}.
	For backward compatibility, callers that only need URLs can extract the `url` field.
	 """
	links = song_payload.get("webLinks") or []
	results: List[Dict[str, Any]] = []

	for link in links:
		if not isinstance(link, dict):
			continue
		url = (link.get("url") or "").strip()
		if not url:
			continue

		lower = url.lower()
		# simple host filtering for common media hosts
		if not ("youtube.com" in lower or "youtu.be" in lower or "nicovideo.jp" in lower or "nico.ms" in lower):
			continue

		# heuristic for official flag
		is_official = False
		if isinstance(link.get("isOfficial"), bool) and link.get("isOfficial"):
			is_official = True
		cat = link.get("category") or link.get("type") or ""
		if isinstance(cat, str) and "official" in cat.lower():
			is_official = True

		results.append({"url": url, "is_official": is_official, "raw": link})

	# Preserve order but remove duplicates by url
	deduped: List[Dict[str, Any]] = []
	seen = set()
	for item in results:
		u = item["url"]
		if u not in seen:
			seen.add(u)
			deduped.append(item)
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

		# Prefer an exact case-insensitive match, then probe the top results for one
		# that actually has songs. This avoids selecting artist groups like
		# "Kikuo Sound Works" when the intended producer is a narrower artist entry.
		selected = items[0]
		for item in items:
			name = str(item.get("name") or "")
			if name.lower() == query_name.lower():
				selected = item
				break
		else:
			for item in items[:5]:
				artist_id = _safe_int(item.get("id"))
				if artist_id <= 0:
					continue
				probe_url = self._build_song_search_url(producer_id=artist_id, start=0)
				probe_payload = _http_get_json(
					url=probe_url,
					timeout_seconds=self.timeout_seconds,
					max_retries=self.max_retries,
					retry_backoff_seconds=self.retry_backoff_seconds,
				)
				probe_items = probe_payload.get("items") or []
				if any(isinstance(song, dict) and song.get("webLinks") for song in probe_items):
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
	) -> Tuple[List[SongCandidate], List[SongCandidate]]:
		"""Fetch producer songs with pagination and normalize response payloads.

		Returns a tuple: (with_media, without_media).
		"""
		with_media: List[SongCandidate] = []
		without_media: List[SongCandidate] = []
		seen_song_ids = set()
		start = 0

		while True:
			# stop if we've collected enough overall (both lists combined)
			total_count = len(with_media) + len(without_media)
			if max_songs is not None and total_count >= max_songs:
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

				classified = extract_media_urls(item)
				urls = [c["url"] for c in classified]

				candidate = SongCandidate(
					vocadb_song_id=song_id,
					title=str(item.get("name") or "Untitled"),
					producer_vocadb_id=producer.vocadb_artist_id,
					producer_name=producer.name,
					source_urls=urls,
					vocaloids=_extract_vocaloids(item),
					all_weblinks=item.get("webLinks") or [],
					classified_links=classified,
				)
				candidate = normalize_song_metadata(candidate, item)
				seen_song_ids.add(song_id)

				if urls:
					with_media.append(candidate)
				else:
					without_media.append(candidate)

				total_count = len(with_media) + len(without_media)
				if max_songs is not None and total_count >= max_songs:
					break

			if len(items) < self.page_size:
				break
			start += self.page_size

		return with_media, without_media

	def _build_song_search_url(self, producer_id: int, start: int) -> str:
		fields = [
			"Artists",
			"WebLinks",
			"MainPicture",
			"Tags",
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
) -> Dict[str, List[SongCandidate]]:
	"""Convenience helper used by orchestration code.

	Returns a dict with keys `with_media` and `without_media`.
	"""
	ingestor = VocaDBIngestor(
		base_url=base_url,
		timeout_seconds=timeout_seconds,
		max_retries=max_retries,
		retry_backoff_seconds=retry_backoff_seconds,
		page_size=page_size,
	)
	producer = ingestor.search_producer(producer_name)
	with_media, without_media = ingestor.fetch_songs_for_producer(producer=producer, max_songs=max_songs)
	return {"with_media": with_media, "without_media": without_media}


def normalize_song_metadata(candidate: SongCandidate, song_payload: Dict[str, Any]) -> SongCandidate:
	"""Enrich a SongCandidate with additional metadata extracted from VocaDB song payload.

	This helper is conservative: it detects common fields when present and otherwise
	leaves defaults in place.
	 """
	# extract duration
	duration = None
	for key in ("lengthSeconds", "length", "durationSeconds", "duration"):
		v = song_payload.get(key)
		if isinstance(v, (int, float)):
			duration = float(v)
			break
		if isinstance(v, str) and v:
			# try parse mm:ss or h:mm:ss
			parts = v.split(":")
			try:
				parts = [int(p) for p in parts]
				if len(parts) == 2:
					duration = parts[0] * 60 + parts[1]
				elif len(parts) == 3:
					duration = parts[0] * 3600 + parts[1] * 60 + parts[2]
				break
			except Exception:
				pass

	# extract bpm
	bpm = None
	for key in ("bpm", "defaultBpm", "tempo"):
		v = song_payload.get(key)
		if isinstance(v, (int, float)):
			bpm = float(v)
			break
		if isinstance(v, str):
			try:
				bpm = float(v)
				break
			except Exception:
				pass

	# tags
	tags: List[str] = []
	raw_tags = song_payload.get("tags") or []
	if isinstance(raw_tags, list):
		for t in raw_tags:
			if isinstance(t, dict):
				tag_info = t.get("tag") if isinstance(t.get("tag"), dict) else t
				name = tag_info.get("name")
				if name:
					tags.append(str(name))
			elif isinstance(t, str):
				tags.append(t)

	# languages
	languages: List[str] = []
	lang = song_payload.get("language") or song_payload.get("languages") or song_payload.get("songLanguages")
	if isinstance(lang, list):
		languages = [str(x) for x in lang if x]
	elif isinstance(lang, str) and lang:
		languages = [lang]

	# published date
	published_at = None
	for key in ("publishDate", "published", "publish_date", "released"):
		v = song_payload.get(key)
		if isinstance(v, str) and v:
			published_at = v
			break

	# statistics (views, favorites, ratings)
	stats = {}
	raw_stats = song_payload.get("stats") or song_payload.get("statistics") or {}
	if isinstance(raw_stats, dict):
		stats = raw_stats

	# versions / other media
	versions: List[Dict[str, Any]] = []
	raw_versions = song_payload.get("otherVersions") or song_payload.get("versions") or []
	if isinstance(raw_versions, list):
		for v in raw_versions:
			if isinstance(v, dict):
				versions.append(v)

	# return a new SongCandidate (dataclass is frozen)
	return SongCandidate(
		vocadb_song_id=candidate.vocadb_song_id,
		title=candidate.title,
		producer_vocadb_id=candidate.producer_vocadb_id,
		producer_name=candidate.producer_name,
		source_urls=candidate.source_urls,
		vocaloids=candidate.vocaloids,
		all_weblinks=candidate.all_weblinks,
		classified_links=candidate.classified_links,
		duration_seconds=duration,
		bpm=bpm,
		tags=tags,
		languages=languages,
		published_at=published_at,
		statistics=stats,
		versions=versions,
	)
