#!/usr/bin/env python3
"""
Server configuration for HTTP to HTTPS redirect
- Detects local vs production environment
- Sets up SSL context for HTTPS when available
- Provides HTTP redirect functionality
"""

import os
import ssl
import threading

from flask import Flask, redirect, request


def is_production():
    """Check if we're running in production environment"""
    return os.environ.get("ENVIRONMENT", "").lower() == "production"


def get_ssl_context():
    """Get SSL context for HTTPS if certificates are available"""
    if not is_production():
        return None

    cert_path = "/etc/letsencrypt/live/pabulib.org/fullchain.pem"
    key_path = "/etc/letsencrypt/live/pabulib.org/privkey.pem"

    if not (os.path.exists(cert_path) and os.path.exists(key_path)):
        print(f"⚠️  SSL certificates not found at {cert_path} or {key_path}")
        return None

    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_path, key_path)
        print("✅ SSL certificates loaded successfully")
        return context
    except Exception as e:
        print(f"❌ Failed to load SSL certificates: {e}")
        return None


def create_http_redirect_app():
    """Create Flask app that only redirects HTTP to HTTPS"""
    app = Flask("http_redirect")

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def redirect_to_https(path):
        """Redirect all HTTP traffic to HTTPS"""
        https_url = f"https://{request.host}"
        if path:
            https_url += f"/{path}"
        if request.query_string:
            https_url += f"?{request.query_string.decode()}"

        return redirect(https_url, code=301)

    return app


def run_http_redirect_server():
    """Run HTTP server on port 80 for redirects only"""
    if not is_production():
        return  # No redirect server needed in local development

    redirect_app = create_http_redirect_app()
    print("🔄 Starting HTTP redirect server on port 80...")

    try:
        redirect_app.run(host="0.0.0.0", port=80, debug=False, threaded=True)
    except Exception as e:
        print(f"❌ Failed to start HTTP redirect server: {e}")


def run_https_server(app):
    """Run HTTPS server on port 443 with the main Flask app"""
    ssl_context = get_ssl_context()

    if not ssl_context:
        print("⚠️  No SSL context available, falling back to HTTP")
        return False

    print("🔐 Starting HTTPS server on port 443...")

    try:
        app.run(
            host="0.0.0.0",
            port=443,
            debug=False,
            ssl_context=ssl_context,
            threaded=True,
        )
        return True
    except Exception as e:
        print(f"❌ Failed to start HTTPS server: {e}")
        return False


def start_production_servers(main_app):
    """Start both HTTP redirect and HTTPS main servers for production"""
    if not is_production():
        return False

    # Start HTTP redirect server in background thread
    redirect_thread = threading.Thread(target=run_http_redirect_server)
    redirect_thread.daemon = True
    redirect_thread.start()

    # Start HTTPS main server (this will block)
    return run_https_server(main_app)


def get_server_config():
    """Get server configuration based on environment"""
    config = {
        "is_production": is_production(),
        "ssl_available": get_ssl_context() is not None,
        "should_redirect": is_production(),
    }

    if config["is_production"]:
        config["http_port"] = 80
        config["https_port"] = 443
        config["host"] = "0.0.0.0"
    else:
        config["http_port"] = int(os.environ.get("FLASK_PORT", 5050))
        config["host"] = "0.0.0.0"

    return config
