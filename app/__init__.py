from flask import Flask, render_template


def create_app():
    app = Flask(__name__)
    app.config.setdefault("SECRET_KEY", "change-me")

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
