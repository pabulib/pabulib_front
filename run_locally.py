#!/usr/bin/env python3
"""Helper to start the local Docker stack with the shared env file."""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
ENV_FILE = REPO_ROOT / "config" / ".env"
# Compose arguments that ensure the env file is loaded before parsing manifests.
COMPOSE_PREFIX = [
    "docker",
    "compose",
    "--env-file",
    str(ENV_FILE),
    "-f",
    str(REPO_ROOT / "docker" / "docker-compose.yml"),
    "-f",
    str(REPO_ROOT / "docker" / "docker-compose.override.yml"),
    "--profile",
    "debug",
]


def main() -> None:
    if not ENV_FILE.exists():
        print(
            "config/.env is missing. Copy config/.env.example to config/.env and try again."
        )
        sys.exit(1)

    compose_args = sys.argv[1:] or ["up", "--build"]
    cmd = COMPOSE_PREFIX + compose_args

    print("Running:", " ".join(cmd))

    try:
        subprocess.run(cmd, check=True, cwd=REPO_ROOT)
    except FileNotFoundError:
        print(
            "docker compose is not available. Install Docker Desktop or ensure docker is on PATH."
        )
        sys.exit(1)
    except subprocess.CalledProcessError as exc:
        print(f"Docker Compose exited with status {exc.returncode}.")
        sys.exit(exc.returncode)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
