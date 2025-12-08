#!/bin/bash

# Configuration
BACKUP_DIR="/home/pabulib/backups/daily_db_backup"
PROJECT_DIR="/home/pabulib/pabulib_front"
ENV_FILE="$PROJECT_DIR/config/.env"
COMPOSE_FILE="$PROJECT_DIR/docker/docker-compose.yml"
COMPOSE_PROD_FILE="$PROJECT_DIR/docker/docker-compose.prod.yml"
PROJECT_NAME="pabulib"

DATE=$(date +"%Y%m%d_%H%M%S")
FILENAME="db_backup_$DATE.sql.gz"

# Load environment variables
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Run backup
echo "Starting backup..."

# We use docker compose with explicit files and project name
# -T is crucial for redirection
# We use sh -c inside the container to handle the password variable which is available in the container environment
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_PROD_FILE" -p "$PROJECT_NAME" exec -T db sh -c 'mysqldump -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE"' | gzip > "$BACKUP_DIR/$FILENAME"

# Check exit status of the first command in the pipe (docker compose)
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "Backup created successfully: $BACKUP_DIR/$FILENAME"
else
    echo "Backup failed!"
    rm -f "$BACKUP_DIR/$FILENAME"
    exit 1
fi

# Delete backups older than 7 days
echo "Cleaning up old backups..."
find "$BACKUP_DIR" -name "db_backup_*.sql.gz" -mtime +7 -delete

echo "Done."
