## Pabulib Frontend

App to explore and download Participatory Budgeting (.pb) files.

## Start

Docker
- docker compose up --build
- App: http://localhost:${FLASK_PORT:-5050}
- Adminer (DB UI): http://localhost:${ADMINER_PORT:-8080}

## Services

- db: MySQL 8 for metadata and comments
- adminer: lightweight DB UI
- web: Flask app (ingests pb_files on start, serves UI)

## Data flow

- Source files live under pb_files/ (configurable). On startup, a refresh ingests all .pb files and stores their parsed metadata in MySQL.
- /search and aggregate pages use the stored, cached metadata (fast queries, no file I/O).
- File previews and detailed visualizations read the .pb file directly for accuracy.

## Database schema (brief)

- pb_files: one row per ingested version; current version per (country, unit, instance, subunit) group; key fields include file_name, path, year, counts, budget, vote_type, quality, timestamps.
- pb_comments: extracted from file metadata; active only for the current version.
- refresh_state: timestamps for incremental refresh.

## Configuration

- PB files path (host): set PB_FILES_DIR in your shell or .env to the local folder with .pb files. Docker binds it to /app/pb_files inside the container. Default is ./pb_files.
- Other common env: FLASK_PORT, REFRESH_FULL, MYSQL_*.

To re-ingest after adding files: docker compose exec web python -m scripts.db_refresh
