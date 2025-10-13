#!/bin/bash

#################################################
# Update and Fix Permissions Script            #
# Pulls latest changes and fixes file perms    #
#################################################

set -euo pipefail  # Exit on error, undefined vars, pipe failures

echo "=== Pabulib Update and Permissions Fix ==="

# Navigate to project directory
cd /home/pabulib/pabulib_front

# Pull latest changes from git
echo "Pulling latest changes from git..."
git pull

echo "Fixing file permissions..."

# Make shell scripts executable
chmod +x deploy.sh
chmod +x setup-server.sh
chmod +x scripts/entrypoint.sh

# Ensure run.py is executable (if needed)
chmod +x run.py

# Fix ownership if needed (uncomment if running as root)
# chown -R pabulib:pabulib /home/pabulib/pabulib_front

# Make cache directory writable
chmod 755 cache/

# Ensure pb_files directory is readable
chmod 755 pb_files/

echo "✅ Permissions fixed successfully!"
echo ""
echo "Shell scripts that are now executable:"
ls -la *.sh scripts/*.sh
echo ""
echo "You can now run: ./deploy.sh"