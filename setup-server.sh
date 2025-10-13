#!/bin/bash

##################################################
# Pabulib Server Setup Script                   #
# Run this once on the server to set up         #
# the production environment                     #
##################################################

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" >&2; }
success() { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] SUCCESS:${NC} $1"; }

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   error "This script should not be run as root. Please run as the pabulib user."
   exit 1
fi

log "🚀 Setting up Pabulib production environment..."

# Create user directories
log "Creating directory structure..."
mkdir -p /home/pabulib/{logs,backups,pb_files,pb_files_depreciated}

# Copy files to proper locations
log "Setting up deployment files..."

# Make deploy script executable
chmod +x /home/pabulib/pabulib_front/deploy.sh

# Create symlink for easy access
ln -sf /home/pabulib/pabulib_front/deploy.sh /home/pabulib/deploy.sh

# Setup environment file
if [ ! -f /home/pabulib/pabulib_front/.env ]; then
    log "Creating .env file from template..."
    cp /home/pabulib/pabulib_front/.env.production.example /home/pabulib/pabulib_front/.env
    
    echo -e "${YELLOW}"
    cat << 'EOF'
⚠️  IMPORTANT: Please edit /home/pabulib/pabulib_front/.env file with your production values:

   • SECRET_KEY=your-production-secret-key
   • ADMIN_PASSWORD=your-secure-password
   • MYSQL_ROOT_PASSWORD=your-root-password
   • MYSQL_PASSWORD=your-db-password

Run: nano /home/pabulib/pabulib_front/.env
EOF
    echo -e "${NC}"
fi

# Setup systemd service (requires sudo)
log "Setting up systemd service..."
echo "You may be prompted for sudo password to install systemd service:"

sudo cp /home/pabulib/pabulib_front/pabulib.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable pabulib.service

# Setup log rotation (requires sudo)
log "Setting up log rotation..."
sudo cp /home/pabulib/pabulib_front/pabulib.logrotate /etc/logrotate.d/pabulib

# Set proper permissions
sudo chown -R pabulib:pabulib /home/pabulib/
chmod 755 /home/pabulib/{logs,backups,pb_files,pb_files_depreciated}

success "🎉 Setup completed successfully!"

log ""
log "📋 Next Steps:"
log "1. Edit .env file with your production values:"
log "   nano /home/pabulib/pabulib_front/.env"
log ""
log "2. Deploy the application:"
log "   /home/pabulib/deploy.sh"
log ""
log "3. Enable auto-start on boot:"
log "   sudo systemctl start pabulib"
log ""
log "📖 Useful Commands:"
log "   • Deploy:           /home/pabulib/deploy.sh"
log "   • Check status:     /home/pabulib/deploy.sh status"
log "   • View logs:        /home/pabulib/deploy.sh logs"
log "   • Monitor:          /home/pabulib/deploy.sh monitor"
log "   • Systemd status:   sudo systemctl status pabulib"
log "   • Systemd logs:     sudo journalctl -u pabulib -f"