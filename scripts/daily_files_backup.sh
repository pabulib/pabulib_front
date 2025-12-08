#!/bin/bash

# Configuration
BACKUP_DIR="/home/pabulib/backups/daily_files_backup"
PROJECT_DIR="/home/pabulib/pabulib_front"
ENV_FILE="$PROJECT_DIR/config/.env"

DATE=$(date +"%Y%m%d_%H%M%S")
FILENAME="files_backup_$DATE.tar.gz"

# Load environment variables to get file paths
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

# Default paths if not set in env (fallback)
PB_FILES="${PB_FILES_DIR:-$PROJECT_DIR/pb_files}"
PB_FILES_DEPRECIATED="${PB_FILES_DEPRECIATED_DIR:-$PROJECT_DIR/pb_files_depreciated}"

# Create backup directory
mkdir -p "$BACKUP_DIR"

echo "Starting files backup..."
echo "Backing up:"
echo "  - $PB_FILES"
echo "  - $PB_FILES_DEPRECIATED"

# Create tarball using gzip compression
# We verify directories exist before backing up to avoid tar errors
if [ ! -d "$PB_FILES" ] && [ ! -d "$PB_FILES_DEPRECIATED" ]; then
    echo "Error: Neither directory exists to backup."
    exit 1
fi

tar -czf "$BACKUP_DIR/$FILENAME" "$PB_FILES" "$PB_FILES_DEPRECIATED" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "Backup created successfully: $BACKUP_DIR/$FILENAME"
else
    echo "Backup failed! (Check if directories exist and are readable)"
    # Don't delete immediately if it failed, maybe partial backup is better than nothing? 
    # But usually we want atomic success.
    # If tar fails (e.g. file changed while reading), it returns non-zero.
    # Let's keep it simple.
    rm -f "$BACKUP_DIR/$FILENAME"
    exit 1
fi

# Cleanup old backups (7 days)
echo "Cleaning up old backups..."
find "$BACKUP_DIR" -name "files_backup_*.tar.gz" -mtime +7 -delete

echo "Done."
