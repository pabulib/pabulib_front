## Pabulib Frontend

App to explore and download Participatory Budgeting (.pb) files.

## Start

Local development (`python run_locally.py` wraps Docker Compose for you):
- `cp config/.env.example config/.env`
- `python run_locally.py`
- App: http://localhost:${FLASK_PORT:-5050}
- Adminer (DB UI): http://localhost:${ADMINER_PORT:-8080}

`run_locally.py` forwards any extra arguments to Docker Compose (for example, `python run_locally.py down`). Use that if you need fine-grained control.

## Repository Layout

- `app/` — Flask application code
- `config/` — Environment templates (`.env.example`, `.env.production.example`) and your local `.env`
- `docker/` — Dockerfile and compose stacks
- `deployment/` — Production scripts, systemd service, logrotate, nginx config
- `docs/` — Project documentation (`CONTRIBUTING.md`, `bib.bib`, etc.)
- `pb_files/`, `pb_files_depreciated/`, `cache/`, `scripts/` — Data, archives, cache, and utility scripts
- Root contains `README.md`, `requirements.txt`, `run.py`, and other runtime essentials

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

- PB files path (host): set PB_FILES_DIR in your shell or `config/.env` to the local folder with .pb files. Docker binds it to /app/pb_files inside the container. Default is ./pb_files.
- Other common env: FLASK_PORT, REFRESH_FULL, MYSQL_*.

To re-ingest after adding files: docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml exec web python -m scripts.db_refresh

## PB File Validation (Checker)

This project uses the [pabulib/checker](https://github.com/pabulib/checker) library to validate .pb files. The checker is automatically installed during Docker build via `requirements.txt`.

To update the checker: `docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml build web && docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml --profile debug up -d`

Quick update (running container): `docker exec pabulib-web-1 pip install pabulib-checker==0.3.1` (replace version as needed)

Then you need to rebuild `./deployment/deploy.sh restart --build`

To validate files: `docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml exec web python scripts/validate_pb_files.py /app/pb_files 10`



## Deployment

To deploy changes to the server:

1. Push your changes to the `main` branch
2. SSH into the server and navigate to `/home/pabulib/pabulib_front` directory
3. Run `./deployment/deploy.sh` (fast - pulls code and restarts)
   - For full rebuild: `./deployment/deploy.sh rebuild`
4. Optionally, run `./deployment/deploy.sh monitor` to view logs
