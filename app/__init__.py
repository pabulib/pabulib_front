import os
from datetime import datetime

from flask import Flask, render_template, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# Create limiter at import time so routes can use decorators; init with app later
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],
    storage_uri=os.environ.get("LIMITER_STORAGE_URI", "memory://"),
)


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ["SECRET_KEY"]

    # Global request body cap (security): default 10 MB unless overridden
    try:
        app.config["MAX_CONTENT_LENGTH"] = (
            int(os.environ.get("MAX_UPLOAD_MB", "10")) * 1024 * 1024
        )
    except Exception:
        app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024

    # In debug mode, auto-reload templates and avoid static caching
    debug_env = os.environ.get("FLASK_DEBUG", "0").strip() not in {
        "0",
        "false",
        "False",
    }
    if debug_env:
        app.config["TEMPLATES_AUTO_RELOAD"] = True
        app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0  # disable static cache in dev
        try:
            app.jinja_env.auto_reload = True
        except Exception:
            pass

    # Initialize rate limiter (used on public endpoints)
    try:
        limiter.init_app(app)
        app.extensions["limiter"] = limiter
    except Exception:
        # If limiter init fails, continue without global limits
        pass

    # Register routes
    from .routes import bp as main_bp
    from .routes_admin import bp as admin_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(admin_bp)

    # Error handlers
    @app.errorhandler(404)
    def not_found(e):  # pragma: no cover
        # Provide the requested path so the template can show what was missing
        return render_template("404.html", path=request.path), 404

    @app.errorhandler(500)
    def server_error(e):  # pragma: no cover
        return render_template("500.html"), 500

    @app.context_processor
    def inject_now():
        return {"now": datetime.now()}

    return app
