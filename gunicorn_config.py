# Production-ready WSGI server configuration for Gunicorn
# This replaces Flask's development server with a robust, multi-process server

bind = "0.0.0.0:443"
workers = 3  # Adjust based on CPU cores (2 x cores + 1)
worker_class = "sync"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
timeout = 60
keepalive = 5

# Process naming
proc_name = "pabulib_app"

# Logging
accesslog = "-"  # Log to stdout
errorlog = "-"  # Log to stderr
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# SSL Configuration
keyfile = "/etc/letsencrypt/live/pabulib.org/privkey.pem"
certfile = "/etc/letsencrypt/live/pabulib.org/fullchain.pem"
ssl_version = 2  # TLS
ciphers = "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"

# Worker lifecycle
preload_app = True
max_worker_memory = 200  # MB - restart worker if memory usage exceeds this

# Graceful restart
graceful_timeout = 30


def when_ready(server):
    """Called just after the server is started."""
    server.log.info("🚀 Pabulib HTTPS server ready")


def worker_int(worker):
    """Called just after a worker has been killed by a signal."""
    worker.log.info("🔄 Worker received INT or QUIT signal")


def on_exit(server):
    """Called just before exiting."""
    server.log.info("🛑 Pabulib server shutting down")


def post_fork(server, worker):
    """Dispose DB connections inherited from preloaded master.

    With preload_app=True, any DB connections opened during app import (e.g.,
    metadata.create_all(engine)) may be inherited by worker processes. MySQL
    connections are not fork-safe and can lead to packet sequence / framing
    errors under load. Disposing the SQLAlchemy engine here ensures each worker
    opens fresh, independent connections.
    """
    try:
        from app.db import engine  # type: ignore

        engine.dispose()
        server.log.info(
            "🔌 Disposed SQLAlchemy engine in worker PID %s after fork", worker.pid
        )
    except Exception as e:  # pragma: no cover
        server.log.warning("Engine dispose after fork failed: %s", e)
