# HTTPS Redirect Setup

This repository includes an example nginx configuration for redirecting HTTP traffic to HTTPS. The file lives at `deployment/nginx_redirect.conf`.

## Usage

1. Copy the configuration to your server (e.g., `/etc/nginx/sites-available/pabulib.conf`).
2. Adjust the `server_name` directive if you are serving a different domain.
3. Enable the site and reload nginx:
   ```bash
   sudo ln -s /etc/nginx/sites-available/pabulib.conf /etc/nginx/sites-enabled/pabulib.conf
   sudo nginx -t
   sudo systemctl reload nginx
   ```
4. (Optional) Uncomment the HTTPS reverse-proxy block in the file if you want nginx to proxy traffic to the Dockerized app directly. Make sure to update certificate paths and upstream host/port to match your environment.

## Related Files

- `deployment/nginx_redirect.conf` - nginx configuration described above
- `deployment/deploy.sh` - production deployment helper script
- `deployment/pabulib.service` - systemd unit that orchestrates Docker deployment
