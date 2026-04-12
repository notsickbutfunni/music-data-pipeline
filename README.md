# VocaP Archive & Pulse

An end-to-end data engineering project that transforms raw Vocaloid-related audio sources into a cloud-hosted analytics platform.

## Project Goal

The goal of this project is to prove full pipeline ownership, from ingesting unstructured audio data to presenting insights in a web dashboard.

This system is designed to:

- Collect song metadata from VocaDB.
- Process short audio previews locally to extract meaningful features.
- Store audio and structured metadata in cloud services.
- Power a public-facing dashboard with searchable and random song analytics.

## Why This Project

Most beginner data projects start from clean CSV files. This project focuses on realistic, messy inputs and production-style architecture:

- Unstructured data handling: audio extraction and signal analysis.
- Hybrid architecture: local compute plus cloud storage/database.
- Full-stack delivery: data pipeline, API layer, and frontend dashboard.

## High-Level Architecture

The system runs across three environments:

1. Local machine (engine)
	 - Python pipeline runs ingestion, download, and feature extraction.
	 - CPU/GPU-intensive work stays local.

2. Cloud storage and database (memory)
	 - Cloudflare R2 stores preview audio objects.
	 - Supabase Postgres stores metadata and extracted features.

3. Vercel-hosted app (showcase)
	 - Next.js frontend and API routes serve dashboards and random song selection.
	 - App reads metadata from Supabase and streams audio from R2.

## Core Tools and Technologies

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| Language | Python 3.10+ | Data ingestion and audio processing |
| Metadata Source | VocaDB API | Song and producer metadata retrieval |
| Downloader | yt-dlp | Pull short song previews from source URLs |
| Audio Analysis | librosa / pydub | Extract tempo, pitch, and energy features |
| Object Storage | Cloudflare R2 | Store processed audio previews |
| Database | Supabase (PostgreSQL) | Store dimensions, facts, and metadata |
| Full-Stack App | Next.js (React) | API routes + dashboard UI |
| Hosting | Vercel | Deploy frontend and backend routes |

## Data Model (Target)

Star-schema-inspired model for analytics:

- dim_producers: producer profiles and references.
- dim_vocaloids: vocaloid entities (for example Miku, Rin, Luka).
- dim_songs: song identity, source links, storage keys.
- fact_audio_features: tempo, mean pitch, RMS energy, and extraction metadata.

## Planned Repository Structure

This is the intended structure as development progresses:

```text
music-data-pipeline/
	README.md
	.env.example
	requirements.txt
	src/
		ingestor.py          # fetch metadata and source URLs
		downloader.py        # download and trim previews
		processor.py         # extract tempo, pitch, RMS features
		storage_writer.py    # upload previews to Cloudflare R2
		db_writer.py         # upsert dimensions/facts in Supabase
		orchestrator.py      # run end-to-end pipeline flow
		config.py            # environment and project settings
	sql/
		001_init_schema.sql
		002_fact_audio_features.sql
	logs/
	dashboard/
		(Next.js app)
```

## Execution Roadmap

### 1: Ingestion and Feature Engineering

- Integrate VocaDB API lookups.
- Download 30-second previews.
- Extract BPM, mean pitch, RMS energy.
- Upload preview audio to R2 and metadata/features to Supabase.

### 2: Data Modeling and Reliability

- Move to star schema dimensions and fact tables.
- Refactor into modular src components.
- Add error handling and quality gates for failed/corrupt audio.

### 3: Dashboard and Deployment

- Build Next.js API route for random song selection.
- Build frontend now-playing card + analytics charts.
- Deploy app to Vercel with environment-based cloud credentials.

## Success Criteria

By the end, the project should demonstrate:

- Reliable ingestion from external APIs and media sources.
- Reproducible audio feature extraction pipeline.
- Cloud-hosted, queryable analytics data model.
- User-facing dashboard that refreshes and visualizes insights.

## Notes

- This project is intended for education and portfolio demonstration.
- Source platform terms and content usage policies should be respected when downloading or serving previews.