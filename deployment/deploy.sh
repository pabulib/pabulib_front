#!/bin/bash

###########################################
# Pabulib Production Deployment Script   #
# Location: /home/pabulib/pabulib_front/ #
###########################################

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
PROJECT_DIR="/home/pabulib/pabulib_front"
LOG_DIR="/home/pabulib/logs"
BACKUP_DIR="/home/pabulib/backups"
COMPOSE_PROJECT_NAME="pabulib"
CONFIG_DIR="$PROJECT_DIR/config"
ENV_FILE="$CONFIG_DIR/.env"

# Detect docker compose command and provide a helper wrapper
COMPOSE_CMD=""
dc() {
    # Wrapper around docker compose/docker-compose with project files and name
    if [ -z "${COMPOSE_CMD}" ]; then
        if docker compose version >/dev/null 2>&1; then
            COMPOSE_CMD="docker compose"
        elif command -v docker-compose >/dev/null 2>&1; then
            COMPOSE_CMD="docker-compose"
        else
            error "Docker Compose is not installed or not in PATH"
            exit 1
        fi
    fi
    ${COMPOSE_CMD} \
        -f "${PROJECT_DIR}/docker/docker-compose.yml" \
        -f "${PROJECT_DIR}/docker/docker-compose.prod.yml" \
        -p "${COMPOSE_PROJECT_NAME}" "$@"
}

# Load environment variables
if [ -f "$ENV_FILE" ]; then
    set -a  # automatically export all variables
    source "$ENV_FILE"
    set +a  # disable automatic export
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" >&2
}

warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] SUCCESS:${NC} $1"
}

# Create necessary directories
setup_directories() {
    log "Setting up directories..."
    mkdir -p "$LOG_DIR"
    mkdir -p "$BACKUP_DIR"
    mkdir -p "$PROJECT_DIR/pb_files"
    mkdir -p "$PROJECT_DIR/pb_files_depreciated"
    
    # Set permissions only if we have permission to do so (non-fatal)
    chmod 755 "$LOG_DIR" "$BACKUP_DIR" 2>/dev/null || true
    chmod 755 "$PROJECT_DIR/pb_files" "$PROJECT_DIR/pb_files_depreciated" 2>/dev/null || true
    
    # Try to set ownership, but don't fail if we can't (non-fatal)
    chown -R $(whoami):$(whoami) "$PROJECT_DIR/pb_files" "$PROJECT_DIR/pb_files_depreciated" 2>/dev/null || true
    
    # Check if critical directories are writable
    for d in "$LOG_DIR" "$BACKUP_DIR" "$PROJECT_DIR/pb_files" "$PROJECT_DIR/pb_files_depreciated"; do
        if [ ! -w "$d" ]; then
            warning "Directory $d is not writable by $(whoami). You may need to adjust permissions or ownership."
        fi
    done
    
    success "Directories verified successfully"
}

# Check for port conflicts
check_port_conflicts() {
    log "Checking for port conflicts..."
    
    # Check if critical ports are in use
    # NOTE: We now use ports 19080/19443 instead of 80/443 for CBIP integration
    # Port 80/443 should be handled by Apache as reverse proxy
    for port in 19080 19443; do
        if ss -tln | grep -q ":${port} "; then
            warning "Port ${port} is currently in use:"
            ss -tlnp | grep ":${port}" || true
        fi
    done
    
    # Check if Apache is running (it should be for proxying)
    if pgrep apache2 >/dev/null; then
        log "Apache2 detected - this is expected for CBIP integration"
    else
        warning "Apache2 not running - ensure it's configured for reverse proxy to pabulib.org"
    fi
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check that Docker is usable without sudo (user in docker group)
    if ! docker info >/dev/null 2>&1; then
        error "Docker is not accessible without sudo. Ensure your user is in the 'docker' group and re-login."
        echo "Hint: sudo usermod -aG docker $(whoami) && newgrp docker"
        exit 1
    fi

    # Initialize compose command via wrapper; will error out with helpful message if missing
    dc version >/dev/null 2>&1 || true
    
    if [ ! -f "$ENV_FILE" ]; then
        error ".env file not found at $ENV_FILE"
        echo "Please create it from $CONFIG_DIR/.env.production.example"
        exit 1
    fi
    
    if [ ! -f "/etc/letsencrypt/live/pabulib.org/fullchain.pem" ]; then
        warning "SSL certificates not found. HTTPS will not be available."
    fi
    
    check_port_conflicts
    
    success "Prerequisites check completed"
}

# Backup current deployment (if exists)
backup_deployment() {
    if containers=$(dc ps -q 2>/dev/null) && [ -n "${containers}" ]; then
        log "Creating backup of current deployment..."
        
        BACKUP_FILE="$BACKUP_DIR/deployment_backup_$(date +'%Y%m%d_%H%M%S').tar.gz"
        
        # Export database using application user credentials
        dc exec -T db sh -c "MYSQL_PWD=\"$MYSQL_PASSWORD\" mysqldump -u \"$MYSQL_USER\" --databases \"$MYSQL_DATABASE\"" > "$BACKUP_DIR/db_backup_$(date +'%Y%m%d_%H%M%S').sql" 2>/dev/null || warning "Database backup failed"
        
        # Backup pb_files
        tar -czf "$BACKUP_FILE" -C "/home/pabulib" pb_files pb_files_depreciated 2>/dev/null || warning "Files backup failed"
        
        success "Backup created: $BACKUP_FILE"
    fi
}

# Stop existing services
stop_services() {
    log "Stopping existing services..."
    
    cd "$PROJECT_DIR"
    
    # Stop Docker Compose services first
    dc down --remove-orphans 2>/dev/null || true
    
    # NOTE: We NO LONGER stop Apache2 as it's needed for reverse proxy
    # The old port conflict check is changed to prevent stopping Apache
    # Check if our custom ports are free
    if ss -tln | grep -q ":19080\|:19443"; then
        warning "Pabulib Docker ports (19080/19443) are still in use after stopping containers:"
        ss -tlnp | grep -E ":19080|:19443" || true
    fi
    
    success "Services stopped"
}

# Pull latest images and build
update_images() {
    log "Updating Docker images..."
    
    cd "$PROJECT_DIR"
    
    # Pull latest base images
    dc pull 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"
    
    # Build application image
    dc build --no-cache web 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"
    
    success "Images updated successfully"
}

# Start services
start_services() {
    log "Starting production services..."
    
    cd "$PROJECT_DIR"
    
    # Start services in production mode
    dc up -d 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 10
    
    # Check if services are running
    if dc ps | grep -q "Up"; then
        success "Services started successfully"
        
        # Show running services
        log "Running services:"
        dc ps
        
        # Test internal ports accessibility (not HTTP redirect since it's handled by Apache)
        log "Testing internal Pabulib ports..."
        if curl -sk https://localhost:19443/ | grep -q "pabulib\|Pabulib" 2>/dev/null; then
            success "Internal HTTPS port (19443) is responding"
        else
            warning "Internal HTTPS port may not be responding properly - check container logs"
        fi
        
    else
        error "Some services failed to start"
        dc ps
        exit 1
    fi
}

# Show logs
show_logs() {
    log "Showing recent logs..."
    cd "$PROJECT_DIR"
    dc logs --tail=50
}

# Monitor function
monitor() {
    log "Monitoring services (Ctrl+C to stop)..."
    cd "$PROJECT_DIR"
    dc logs -f
}

# Cleanup old logs and backups
cleanup() {
    log "Cleaning up old logs and backups..."
    
    # Keep logs for 30 days
    find "$LOG_DIR" -name "*.log" -mtime +30 -delete 2>/dev/null || true
    
    # Keep backups for 7 days
    find "$BACKUP_DIR" -name "*.tar.gz" -mtime +7 -delete 2>/dev/null || true
    find "$BACKUP_DIR" -name "*.sql" -mtime +7 -delete 2>/dev/null || true
    
    # Clean up unused Docker images
    docker image prune -f &>/dev/null || true
    
    success "Cleanup completed"
}

# Main deployment function
deploy() {
    local rebuild_flag="${1:-}"
    log "🚀 Starting Pabulib production deployment..."
    
    # Pull latest code from git
    log "Pulling latest code from git..."
    cd "$PROJECT_DIR"
    if git pull 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"; then
        success "Git pull completed successfully"
    else
        error "Git pull failed. Check if there are local changes or conflicts."
        exit 1
    fi
    
    setup_directories
    check_prerequisites
    backup_deployment
    stop_services
    
    # Only rebuild images if explicitly requested
    if [[ "$rebuild_flag" == "rebuild" || "$rebuild_flag" == "--rebuild" ]]; then
        log "Rebuild flag detected - rebuilding Docker images..."
        update_images
    else
        log "Skipping image rebuild (use 'deploy rebuild' to rebuild images)"
    fi
    
    start_services
    cleanup
    
    success "🎉 Deployment completed successfully!"
    
    log "📋 Deployment Summary:"
    log "   • Application: https://pabulib.org (via Apache proxy)"
    log "   • Internal HTTPS: https://localhost:19443"
    log "   • Internal HTTP: http://localhost:19080" 
    log "   • Logs: $LOG_DIR"
    log "   • Backups: $BACKUP_DIR"
    log ""
    log "⚙️  CBIP Integration Notes:"
    log "   • Apache should proxy pabulib.org → localhost:19443"
    log "   • Docker containers use ports 19080/19443 internally"
    log "   • Ensure Apache virtual host is configured for pabulib.org"
    log ""
    log "📖 Useful commands:"
    log "   • View logs: $0 logs"
    log "   • Monitor: $0 monitor"
    log "   • Status: $0 status"
    log "   • Stop: $0 stop"
}

# Status check
status() {
    log "📊 Service Status:"
    cd "$PROJECT_DIR"
    dc ps
    
    log ""
    log "🌐 Port Status:"
    # Check both Apache ports and our internal Docker ports
    log "   Apache ports (should be in use):"
    ss -tln | grep -E ":80\s|:443\s" || log "   No Apache services on standard ports"
    log "   Pabulib internal ports:"
    ss -tln | grep -E ":19080\s|:19443\s|:3306\s" || log "   No Pabulib services on internal ports"
    
    log ""
    log "💾 Disk Usage:"
    df -h /home/pabulib
    
    log ""
    log "🐳 Docker Status:"
    docker system df
}

# Help function
show_help() {
    cat << EOF
Pabulib Production Deployment Script

Usage: $0 [COMMAND]

Commands:
    deploy      Full deployment (default)
    status      Show service status
    logs        Show recent logs
    monitor     Monitor logs in real-time
    stop        Stop all services
    restart     Restart all services
    backup      Create manual backup
    cleanup     Clean old logs and backups
    help        Show this help

Examples:
    $0                  # Full deployment
    $0 status           # Check status
    $0 logs             # View recent logs
    $0 monitor          # Monitor in real-time
    $0 restart --build  # Restart services and force rebuild

Notes:
    - This script does not require sudo. Ensure your user is in the 'docker' group.
      Add with: sudo usermod -aG docker $(whoami) && newgrp docker

Logs are saved to: $LOG_DIR
Backups are saved to: $BACKUP_DIR
EOF
}

# Main script logic
main() {
    case "${1:-deploy}" in
        deploy)
            deploy "${2:-}"
            ;;
        status)
            status
            ;;
        logs)
            show_logs
            ;;
        monitor)
            monitor
            ;;
        stop)
            stop_services
            ;;
        restart)
            log "Restarting services..."
            stop_services
            # If second argument requests a rebuild, rebuild images before starting
            if [[ "${2:-}" == "--build" || "${2:-}" == "-b" || "${FORCE_REBUILD:-0}" == "1" ]]; then
                log "Rebuilding images before restart (requested via --build/-b or FORCE_REBUILD=1)..."
                update_images
            fi
            start_services
            ;;
        backup)
            setup_directories
            backup_deployment
            ;;
        cleanup)
            cleanup
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
