import os

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

    # Optional: watch pb_files folder for changes and auto-invalidate caches
    # Enable when PB_WATCH env var is truthy (default on). If watchdog isn't installed,
    # this is silently skipped.
    try:
        enabled = os.environ.get("PB_WATCH", "1").strip().lower() not in {
            "0",
            "false",
            "no",
        }
        # Start watcher only in the active reloader process (or when no reloader)
        is_main = os.environ.get("WERKZEUG_RUN_MAIN", "true").lower() == "true"
        if enabled and is_main and not app.config.get("PB_WATCH_STARTED"):
            # Avoid double start with reloader
            app.config["PB_WATCH_STARTED"] = True
            from .routes import (
                _aggregate_comments_cached,
                _aggregate_statistics_cached,
                _get_tiles_cached,
                _invalidate_all_caches,
                _pb_folder,
            )

            try:
                import importlib

                events_mod = importlib.import_module("watchdog.events")
                observers_mod = importlib.import_module("watchdog.observers")
                FileSystemEventHandler = getattr(events_mod, "FileSystemEventHandler")
                Observer = getattr(observers_mod, "Observer")

                class _PBFolderHandler(FileSystemEventHandler):
                    def on_any_event(self, event):  # type: ignore
                        try:
                            if getattr(event, "is_directory", False):
                                return
                            path = str(getattr(event, "src_path", ""))
                            if path.endswith(".pb"):
                                _invalidate_all_caches()
                                # Pre-warm in background-ish context
                                try:
                                    _get_tiles_cached()
                                    _aggregate_comments_cached()
                                    _aggregate_statistics_cached()
                                except Exception:
                                    pass
                        except Exception:
                            pass

                observer = Observer()
                observer.schedule(
                    _PBFolderHandler(), str(_pb_folder()), recursive=False
                )
                observer.daemon = True
                observer.start()
                app.config["PB_WATCHER"] = observer
            except Exception:
                # watchdog not available or failed; skip watching
                pass
    except Exception:
        pass

    return app
