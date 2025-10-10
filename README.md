## Pabulib frontend

[soon] https://pabulib.org/

## Quick start (macOS, zsh)

```bash
source venv/bin/activate
pip install -r requirements.txt
python run.py
```

Then open http://127.0.0.1:5000/

## Notes
- Files are read from `pb_files/`.
- Click the download icon on a tile to download the original `.pb` file.

## Performance and live updates

This app serves and aggregates metadata from `.pb` files under `pb_files/`. To keep it fast even with many files and frequent updates, the server uses an in-memory incremental cache:

- Tiles/metadata are computed per file and cached with the file's mtime and size.
- On each request we only stat files to detect changes and rebuild tiles for the files that changed, were added, or removed.
- Comments and statistics pages reuse the same cached tiles, avoiding re-reading files.

### Force rebuild caches

You can force the app to rebuild all caches when new files arrive via an admin endpoint:

- Endpoint: `GET /admin/refresh` (also accepts POST)
- Optional protection: set an env var `ADMIN_TOKEN` and pass it as `?token=...` or header `X-Admin-Token`.

Example:

```bash
export ADMIN_TOKEN=changeme
curl "http://localhost:5000/admin/refresh?token=changeme"
```

### Automatic rebuild on file changes (optional)

If you'd like the app to auto-refresh when `.pb` files are created/modified/deleted, enable the optional filesystem watcher. It uses the `watchdog` library when available.

- Install (optional): `pip install watchdog`
- Enable with env var (default is enabled): `PB_WATCH=1`
- Disable watching: `PB_WATCH=0`

Notes:

- Watching is best-effort. If `watchdog` isn't installed or fails, the app continues without watching.
- In production with multiple workers, each worker maintains its own in-process cache. You can use the `/admin/refresh` endpoint to invalidate all workers. For cross-process coordination or very large deployments, consider a shared cache (e.g., Redis) or a lightweight database index.

### Do we need a database?

For most use cases a database isn't necessary because files are the source of truth and the app incrementally caches parsed metadata in memory. Consider a DB if:

- You have tens of thousands of `.pb` files and want advanced queries or full-text search over metadata/comments.
- You run many processes/instances and need a shared, persistent index with coordinated invalidation.
- You want background jobs to precompute heavy analytics.

A pragmatic next step is a small SQLite index storing per-file metadata with `name`, `mtime`, `size`, and the computed tile fields, updated on change. The app would query SQLite for tiles/comments/statistics and update rows only for changed files. This keeps startup and requests fast while staying simple to operate.
