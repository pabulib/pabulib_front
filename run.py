import os
import argparse

from app import create_app

app = create_app()

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run the Flask application')
    parser.add_argument('-p', '--port', type=int, default=None,
                        help='Port number to run the server on (overrides environment variable)')
    args = parser.parse_args()
    
    # Use command-line argument if provided, otherwise use environment variable or default
    if args.port:
        port = args.port
    else:
        port = int(os.environ.get("PORT", "5000"))
    
    debug = os.environ.get("FLASK_DEBUG", "1").strip() not in {"0", "false", "False"}
    # Use use_reloader=False to play nicer with VS Code tasks/background
    app.run(debug=debug, host="0.0.0.0", port=port, use_reloader=debug)
