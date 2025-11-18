#!/usr/bin/env python3
"""WSGI entry point for Gunicorn.

Exposes the Flask application instance as ``application`` so that Gunicorn
can import and serve it. The script also loads environment overrides from the
new ``config/.env`` location when available.
"""

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

# Ensure the project root is importable when this module is executed directly.
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:  # Load local environment overrides when python-dotenv is installed.
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(BASE_DIR / "config" / ".env")
except Exception:
    pass

from app import create_app

# Gunicorn looks for this module-level variable by default.
application = create_app()

if __name__ == "__main__":
    # Allow ``python -m app.wsgi`` for quick manual testing.
    application.run(debug=False, host="0.0.0.0", port=8000)
