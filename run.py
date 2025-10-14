import os
import subprocess
import sys

try:
    # Load environment variables from .env if present (local dev)
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

from app import create_app


def is_production():
    """Detect if we're in production environment"""
    return (
        os.environ.get("ENVIRONMENT", "").strip().lower() == "production"
        or os.environ.get("FLASK_ENV", "").strip().lower() == "production"
        or os.path.exists("/etc/letsencrypt/live/pabulib.org/fullchain.pem")
    )


def start_http_redirect_server():
    """Start a simple HTTP->HTTPS redirect server on port 80"""
    import threading

    from flask import Flask, redirect, request

    redirect_app = Flask(__name__)

    @redirect_app.route("/", defaults={"path": ""})
    @redirect_app.route("/<path:path>")
    def redirect_to_https(path):
        return redirect(
            f"https://{request.host.split(':')[0]}{request.full_path}", code=301
        )

    def run_redirect():
        redirect_app.run(host="0.0.0.0", port=80, debug=False, use_reloader=False)

    thread = threading.Thread(target=run_redirect, daemon=True)
    thread.start()
    print("   ↗️  HTTP->HTTPS redirect server started on port 80")
    return thread


def start_gunicorn():
    """Start the Gunicorn WSGI server for production"""
    try:
        # Start HTTP redirect server first
        start_http_redirect_server()

        # Start Gunicorn with our configuration
        cmd = ["gunicorn", "--config", "gunicorn_config.py", "wsgi:application"]

        print("   🎯 Starting Gunicorn WSGI server...")
        print(f"   Command: {' '.join(cmd)}")

        # Use exec to replace current process (proper signal handling)
        os.execvp("gunicorn", cmd)

    except Exception as e:
        print(f"❌ Failed to start Gunicorn: {e}")
        return False


app = create_app()

if __name__ == "__main__":
    production_detected = is_production()

    print(f"🚀 Starting application...")
    print(
        f"   Environment: {'Production' if production_detected else 'Local Development'}"
    )

    if production_detected:
        # Production: Use Gunicorn WSGI server
        print(f"   🎯 Using Gunicorn for production-grade HTTPS server")
        start_gunicorn()
    else:
        # Local development: Standard Flask dev server
        port = int(os.environ.get("FLASK_PORT", "5050"))
        debug = os.environ.get("FLASK_DEBUG", "1").strip() not in {
            "0",
            "false",
            "False",
        }
        print(f"   📡 Starting Flask development server on http://localhost:{port}")

        # Use use_reloader=False to play nicer with VS Code tasks/background
        app.run(debug=debug, host="0.0.0.0", port=port, use_reloader=debug)
