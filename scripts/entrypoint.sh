#!/usr/bin/env sh
set -e

DB_HOST=${MYSQL_HOST:-db}
DB_PORT=${MYSQL_PORT:-3306}
DB_USER=${MYSQL_USER:-pabulib}
DB_NAME=${MYSQL_DATABASE:-pabulib}
MAX_TRIES=${WAIT_FOR_DB_MAX_TRIES:-60}
SLEEP_SECS=${WAIT_FOR_DB_SLEEP:-2}
export DB_HOST DB_PORT

echo "[DB-WAIT] Waiting for database ${DB_HOST}:${DB_PORT} (user=${DB_USER}, db=${DB_NAME}) ..."
TRIES=0
while true; do
  TRIES=$((TRIES+1))
  echo "[DB-WAIT] Attempt ${TRIES}/${MAX_TRIES}..."
  if python - <<'PY'
import os, sys
try:
    import pymysql
except Exception as e:
    print(f"[DB-WAIT] pymysql import failed: {e}", flush=True)
    sys.exit(1)

host=os.environ.get('DB_HOST','db')
port=int(os.environ.get('DB_PORT','3306'))
user=os.environ.get('MYSQL_USER','pabulib')
pwd=os.environ.get('MYSQL_PASSWORD','pabulib')
db=os.environ.get('MYSQL_DATABASE','pabulib')

try:
    conn=pymysql.connect(host=host, port=port, user=user, password=pwd, database=db, connect_timeout=2)
    conn.close()
    print(f"[DB-WAIT] Connected to {host}:{port}", flush=True)
    sys.exit(0)
except Exception as e:
    print(f"[DB-WAIT] Connection failed: {e.__class__.__name__}: {e}", flush=True)
    sys.exit(1)
PY
  then
    echo "[DB-WAIT] Database is reachable."
    break
  fi
  if [ "$TRIES" -ge "$MAX_TRIES" ]; then
    echo "[DB-WAIT] DB not reachable after ${MAX_TRIES} tries; exiting"
    exit 1
  fi
  sleep "$SLEEP_SECS"
done

GRACE=${WAIT_FOR_DB_GRACE:-0}
if [ "$GRACE" -gt 0 ]; then
  echo "[DB-WAIT] Grace period after readiness: sleeping ${GRACE}s..."
  sleep "$GRACE"
fi

echo "Running database refresh..."
echo "[DEBUG] PB files directory check:"
ls -la /app/pb_files/ 2>/dev/null || echo "[DEBUG] /app/pb_files/ not accessible"
echo "[DEBUG] Current working directory: $(pwd)"
echo "[DEBUG] Python path check:"
python -c "from app.utils.pb_utils import pb_folder; print(f'pb_folder() returns: {pb_folder()}')" 2>/dev/null || echo "[DEBUG] pb_folder() check failed"

if [ "${REFRESH_FULL:-0}" = "1" ]; then
  echo "[REFRESH] Mode: full (--full)"
  python -m scripts.db_refresh --full || echo "Refresh failed (continuing to start app)"
else
  echo "[REFRESH] Mode: incremental"
  python -m scripts.db_refresh || echo "Refresh failed (continuing to start app)"
fi

# Seed admin user from environment if provided
if [ -n "${ADMIN_USERNAME}" ] && [ -n "${ADMIN_PASSWORD}" ]; then
  echo "[ADMIN] Ensuring admin user exists for '${ADMIN_USERNAME}'..."
  python - <<'PY'
import os
from werkzeug.security import generate_password_hash
from sqlalchemy.exc import OperationalError
from app.db import Base, engine, get_session
from app.models import AdminUser

def ensure_schema():
    try:
        Base.metadata.create_all(bind=engine)
    except OperationalError:
        pass

ensure_schema()
username = os.environ.get('ADMIN_USERNAME')
password = os.environ.get('ADMIN_PASSWORD')
if username and password:
    with get_session() as s:
        user = s.query(AdminUser).filter(AdminUser.username==username).one_or_none()
        if user:
            # Update password on every start for convenience in dev; comment out if undesired
            user.password_hash = generate_password_hash(password)
            user.is_active = True
            print(f"[ADMIN] Updated password for '{username}'.", flush=True)
        else:
            s.add(AdminUser(username=username, password_hash=generate_password_hash(password), is_active=True))
            print(f"[ADMIN] Created admin user '{username}'.", flush=True)
else:
    print("[ADMIN] ADMIN_USERNAME or ADMIN_PASSWORD missing; skipping.", flush=True)
PY
fi

echo "Starting Flask app on port ${FLASK_PORT:-${APP_PORT:-${PORT:-5050}}}"
exec python run.py
