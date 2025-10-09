# PB UI (Flask)

Minimal Flask app that lists `.pb` files from `pb_files/` and renders tiles similar to the provided screenshot.

## Quick start (macOS, zsh)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Then open http://127.0.0.1:5000/

## Notes
- Files are read from `pb_files/`.
- Click the download icon on a tile to download the original `.pb` file.
