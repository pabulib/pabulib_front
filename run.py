import os

try:
    # Load environment variables from .env if present (local dev)
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

from app import create_app
from app.server_config import get_server_config, start_production_servers

app = create_app()

if __name__ == "__main__":
    config = get_server_config()

    print(f"🚀 Starting application...")
    print(
        f"   Environment: {'Production' if config['is_production'] else 'Local Development'}"
    )
    print(f"   SSL Available: {'Yes' if config['ssl_available'] else 'No'}")

    if config["is_production"]:
        # Production: Start both HTTP redirect (port 80) and HTTPS main (port 443)
        print(f"   Starting production servers with HTTP->HTTPS redirect")

        if start_production_servers(app):
            print("✅ Production servers started successfully")
        else:
            print("⚠️  Failed to start HTTPS server, falling back to HTTP")
            # Fallback to HTTP if HTTPS fails
            port = config["http_port"]
            debug = False
            app.run(debug=debug, host=config["host"], port=port, use_reloader=False)
    else:
        # Local development: Standard HTTP server
        port = config["http_port"]
        debug = os.environ.get("FLASK_DEBUG", "1").strip() not in {
            "0",
            "false",
            "False",
        }
        print(f"   Starting development server on http://localhost:{port}")

        # Use use_reloader=False to play nicer with VS Code tasks/background
        app.run(debug=debug, host=config["host"], port=port, use_reloader=debug)
