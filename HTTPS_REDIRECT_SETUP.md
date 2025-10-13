# HTTP to HTTPS Redirect Setup

This document explains the HTTP to HTTPS redirect functionality that has been added to the pabulib-front application.

## Overview

The application now supports automatic HTTP to HTTPS redirect in production environments while maintaining compatibility with local development.

## How It Works

### Local Development
- Environment variable `ENVIRONMENT=development` (default)
- Runs standard HTTP server on the configured port (e.g., 5052)
- No SSL certificates required
- No HTTP redirect functionality

### Production Server
- Environment variable `ENVIRONMENT=production`
- Automatically detects SSL certificates at `/etc/letsencrypt/live/pabulib.org/`
- Runs two servers simultaneously:
  - **HTTP server (port 80)**: Redirects all traffic to HTTPS
  - **HTTPS server (port 443)**: Serves the main application with SSL

## Configuration

### Local Development (.env)
```bash
ENVIRONMENT=development
FLASK_PORT=5052
FLASK_DEBUG=1
SECRET_KEY=dev-secret-change-me
# ... other development settings
```

### Production Server (.env)
```bash
ENVIRONMENT=production
FLASK_DEBUG=0
SECRET_KEY=your-production-secret-key
# ... other production settings
```

## Docker Compose Setup

### Local Development
Use the standard docker-compose.yml:
```bash
docker-compose up
```

### Production Server
Use the production override:
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

The production override includes:
- Maps ports 80 and 443 to the container
- Mounts SSL certificates from Let's Encrypt
- Sets proper restart policies
- Disables Adminer for security

## SSL Certificate Requirements

The application expects Let's Encrypt certificates at:
- Certificate: `/etc/letsencrypt/live/pabulib.org/fullchain.pem`
- Private Key: `/etc/letsencrypt/live/pabulib.org/privkey.pem`

If certificates are not found, the application will:
1. Log a warning
2. Fall back to HTTP-only mode

## File Structure

```
app/
├── __init__.py              # Main Flask app factory
├── server_config.py         # New: HTTP/HTTPS redirect logic
├── routes.py               # Application routes
└── ...

run.py                      # Updated: Uses server_config for smart startup
docker-compose.yml          # Development configuration
docker-compose.prod.yml     # Production override
.env.example               # Development environment template
.env.production.example    # Production environment template
```

## Server Startup Logs

### Local Development
```
🚀 Starting application...
   Environment: Local Development
   SSL Available: No
   Starting development server on http://localhost:5052
```

### Production
```
🚀 Starting application...
   Environment: Production
   SSL Available: Yes
   Starting production servers with HTTP->HTTPS redirect
✅ SSL certificates loaded successfully
🔄 Starting HTTP redirect server on port 80...
🔐 Starting HTTPS server on port 443...
```

## Implementation Details

### server_config.py
- `is_production()`: Detects environment from `ENVIRONMENT` variable
- `get_ssl_context()`: Loads SSL certificates if available
- `create_http_redirect_app()`: Creates Flask app for HTTP redirects
- `start_production_servers()`: Manages both HTTP and HTTPS servers

### run.py
- Auto-detects environment and configures accordingly
- Provides informative startup logging
- Graceful fallback if HTTPS setup fails

## Migration from Old Setup

The previous separate `app_redirect.py` functionality is now integrated into the main application. No manual server management is required - the application automatically handles:

1. Environment detection
2. SSL certificate loading
3. HTTP redirect server setup
4. HTTPS main server startup

## Troubleshooting

### SSL Certificate Issues
- Ensure certificates exist at the expected paths
- Check certificate permissions
- Verify certificate validity with `openssl x509 -in /etc/letsencrypt/live/pabulib.org/fullchain.pem -text -noout`

### Port Conflicts
- Ensure ports 80 and 443 are available in production
- Check for other services using these ports with `netstat -tlnp | grep :80` and `netstat -tlnp | grep :443`

### Environment Detection
- Verify `ENVIRONMENT=production` is set correctly
- Check environment variables are loaded properly

## Security Considerations

1. **Certificate Security**: SSL certificates are mounted read-only
2. **Admin Interface**: Adminer is disabled in production
3. **Debug Mode**: Automatically disabled in production
4. **HTTP Redirect**: All HTTP traffic is permanently redirected (301) to HTTPS