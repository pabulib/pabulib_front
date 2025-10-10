import os

from app import create_app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "1").strip() not in {"0", "false", "False"}
    # Use use_reloader=False to play nicer with VS Code tasks/background
    app.run(debug=debug, host="0.0.0.0", port=port, use_reloader=debug)
