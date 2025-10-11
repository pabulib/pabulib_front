import os

from flask import Flask, render_template


def create_app():
    app = Flask(__name__)
    app.config.setdefault("SECRET_KEY", "change-me")

    # In debug mode, auto-reload templates and avoid static caching
    debug_env = os.environ.get("FLASK_DEBUG", "0").strip() not in {"0", "false", "False"}
    if debug_env:
        app.config["TEMPLATES_AUTO_RELOAD"] = True
        app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0  # disable static cache in dev
        try:
            app.jinja_env.auto_reload = True
        except Exception:
            pass

    # Register routes
    from .routes import bp as main_bp

    app.register_blueprint(main_bp)

    # Error handlers
    @app.errorhandler(404)
    def not_found(e):  # pragma: no cover
        return render_template("404.html"), 404

    @app.errorhandler(500)
    def server_error(e):  # pragma: no cover
        return render_template("500.html"), 500

    return app
