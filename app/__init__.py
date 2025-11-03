import os
from datetime import datetime

import sentry_sdk
from flask import Flask, render_template, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

# Create limiter at import time so routes can use decorators; init with app later
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],
    storage_uri=os.environ.get("LIMITER_STORAGE_URI", "memory://"),
)


def _enhance_sentry_event(event, hint):
    """Enhance Sentry events with additional context and tags."""
    # Add custom tags for better organization in Slack notifications
    event.setdefault("tags", {}).update(
        {
            "app": "pabulib-front",
            "component": "flask-app",
        }
    )

    # Add server information
    event.setdefault("server_name", os.environ.get("HOSTNAME", "localhost"))

    return event


def create_app():
    # Initialize Sentry for error tracking and performance monitoring
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[
                FlaskIntegration(),
                SqlalchemyIntegration(),
            ],
            # Performance monitoring: configurable sample rate (0.0 to 1.0)
            # Lower values recommended for production (0.01 = 1%)
            traces_sample_rate=float(
                os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1")
            ),
            # Profiling: configurable sample rate (0.0 to 1.0)
            # Lower values recommended for production (0.01 = 1%)
            profiles_sample_rate=float(
                os.environ.get("SENTRY_PROFILES_SAMPLE_RATE", "0.1")
            ),
            environment=os.environ.get("SENTRY_ENVIRONMENT", "development"),
            release=os.environ.get("SENTRY_RELEASE"),
            # Add default tags for better error organization
            before_send=lambda event, hint: _enhance_sentry_event(event, hint),
        )

    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ["SECRET_KEY"]

    # Create database tables if they don't exist
    from .db import Base, engine

    Base.metadata.create_all(engine)

    # Global request body cap (security): default 10 MB unless overridden.
    # Note: We provide a targeted handler to allow download-start endpoints
    # to proceed without needing to read large request bodies (see routes).
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
    @app.errorhandler(400)
    def bad_request(e):  # pragma: no cover
        # Provide the requested path and error details for bad request template
        return render_template("400.html", path=request.path, error=e), 400

    @app.errorhandler(404)
    def not_found(e):  # pragma: no cover
        # Provide the requested path so the template can show what was missing
        return render_template("404.html", path=request.path), 404

    @app.errorhandler(500)
    def server_error(e):  # pragma: no cover
        # Let Sentry capture the error before rendering the error page
        if sentry_dsn:
            with sentry_sdk.push_scope() as scope:
                scope.set_tag("error_handler", "500")
                scope.set_context(
                    "request",
                    {
                        "url": request.url,
                        "method": request.method,
                        "remote_addr": request.remote_addr,
                        "user_agent": request.headers.get("User-Agent"),
                    },
                )
                sentry_sdk.capture_exception()
        return render_template("500.html"), 500

    @app.context_processor
    def inject_now():
        return {"now": datetime.now()}

    @app.context_processor
    def inject_analytics():
        """Make analytics configuration available to templates."""
        return {"google_analytics_id": os.environ.get("GOOGLE_ANALYTICS_ID", "")}

    return app
