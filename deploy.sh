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

# Load environment variables
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a  # automatically export all variables
    source "$PROJECT_DIR/.env"
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
    
    # Set proper permissions and ownership
    chmod 755 "$LOG_DIR" "$BACKUP_DIR"
    chmod 755 "$PROJECT_DIR/pb_files" "$PROJECT_DIR/pb_files_depreciated"
    
    # Ensure current user owns the directories (fixes tee permission issues)
    sudo chown -R $(whoami):$(whoami) "$LOG_DIR" "$BACKUP_DIR"
    chown -R $(whoami):$(whoami) "$PROJECT_DIR/pb_files" "$PROJECT_DIR/pb_files_depreciated" 2>/dev/null || true
    
    success "Directories created successfully"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v sudo docker compose &> /dev/null; then
        error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    if [ ! -f "$PROJECT_DIR/.env" ]; then
        error ".env file not found in $PROJECT_DIR"
        echo "Please create .env file from .env.production.example"
        exit 1
    fi
    
    if [ ! -f "/etc/letsencrypt/live/pabulib.org/fullchain.pem" ]; then
        warning "SSL certificates not found. HTTPS will not be available."
    fi
    
    success "Prerequisites check completed"
}

# Backup current deployment (if exists)
backup_deployment() {
    if sudo docker compose -f "$PROJECT_DIR/docker-compose.yml" -f "$PROJECT_DIR/docker-compose.prod.yml" -p "$COMPOSE_PROJECT_NAME" ps -q &> /dev/null; then
        log "Creating backup of current deployment..."
        
        BACKUP_FILE="$BACKUP_DIR/deployment_backup_$(date +'%Y%m%d_%H%M%S').tar.gz"
        
        # Export database using application user credentials
        sudo docker compose -f "$PROJECT_DIR/docker-compose.yml" -f "$PROJECT_DIR/docker-compose.prod.yml" -p "$COMPOSE_PROJECT_NAME" exec -T db sh -c "MYSQL_PWD=\"$MYSQL_PASSWORD\" mysqldump -u \"$MYSQL_USER\" --databases \"$MYSQL_DATABASE\"" > "$BACKUP_DIR/db_backup_$(date +'%Y%m%d_%H%M%S').sql" 2>/dev/null || warning "Database backup failed"
        
        # Backup pb_files
        tar -czf "$BACKUP_FILE" -C "/home/pabulib" pb_files pb_files_depreciated 2>/dev/null || warning "Files backup failed"
        
        success "Backup created: $BACKUP_FILE"
    fi
}

# Stop existing services
stop_services() {
    log "Stopping existing services..."
    
    cd "$PROJECT_DIR"
    
    # Stop with both compose files to ensure everything stops
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" down --remove-orphans 2>/dev/null || true
    
    success "Services stopped"
}

# Pull latest images and build
update_images() {
    log "Updating Docker images..."
    
    cd "$PROJECT_DIR"
    
    # Pull latest base images
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml pull 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"
    
    # Build application image
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml build --no-cache web 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"
    
    success "Images updated successfully"
}

# Start services
start_services() {
    log "Starting production services..."
    
    cd "$PROJECT_DIR"
    
    # Start services in production mode
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" up -d 2>&1 | tee -a "$LOG_DIR/deploy_$(date +'%Y%m%d').log"
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 10
    
    # Check if services are running
    if sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" ps | grep -q "Up"; then
        success "Services started successfully"
        
        # Show running services
        log "Running services:"
        sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" ps
        
        # Test HTTP redirect
        log "Testing HTTP to HTTPS redirect..."
        if curl -sI http://localhost/ | grep -q "301\|302"; then
            success "HTTP redirect is working"
        else
            warning "HTTP redirect may not be working properly"
        fi
        
    else
        error "Some services failed to start"
        sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" ps
        exit 1
    fi
}

# Show logs
show_logs() {
    log "Showing recent logs..."
    cd "$PROJECT_DIR"
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" logs --tail=50
}

# Monitor function
monitor() {
    log "Monitoring services (Ctrl+C to stop)..."
    cd "$PROJECT_DIR"
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" logs -f
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
    log "🚀 Starting Pabulib production deployment..."
    
    setup_directories
    check_prerequisites
    backup_deployment
    stop_services
    update_images
    start_services
    cleanup
    
    success "🎉 Deployment completed successfully!"
    
    log "📋 Deployment Summary:"
    log "   • Application: https://pabulib.org"
    log "   • Logs: $LOG_DIR"
    log "   • Backups: $BACKUP_DIR"
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
    sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml -p "$COMPOSE_PROJECT_NAME" ps
    
    log ""
    log "🌐 Port Status:"
    ss -tlnp | grep -E ":80\s|:443\s|:3306\s" || log "No services listening on expected ports"
    
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

Logs are saved to: $LOG_DIR
Backups are saved to: $BACKUP_DIR
EOF
}

# Main script logic
main() {
    case "${1:-deploy}" in
        deploy)
            deploy
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