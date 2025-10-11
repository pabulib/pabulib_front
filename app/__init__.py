import os
from datetime import datetime

from flask import Flask, render_template, request


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ["SECRET_KEY"]

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
