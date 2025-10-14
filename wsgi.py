#!/usr/bin/env python3
"""
WSGI entry point for Gunicorn
This creates the Flask app instance that Gunicorn will serve
"""

import os
import sys

# Add the app directory to Python path
sys.path.insert(0, "/app")

try:
    # Load environment variables from .env if present (local dev)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from app import create_app

# Create the Flask application instance
application = create_app()

if __name__ == "__main__":
    # This allows the script to be run directly for testing
    application.run(debug=False, host="0.0.0.0", port=8000)
