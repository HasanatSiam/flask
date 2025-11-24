# Deployment Guide

This guide outlines the recommended steps for deploying the **ProcG Blueprint** application to a production environment.

## Production Requirements

-   **WSGI Server**: Do not use the built-in Flask development server. Use **Gunicorn**.
-   **Reverse Proxy**: Use **Nginx** or **Apache** as a reverse proxy to handle static files, SSL termination, and request buffering.
-   **Process Manager**: Use **Supervisor** or **Systemd** to keep your application and Celery workers running.

## Deployment Steps

### 1. Set up Gunicorn

Install Gunicorn:
```bash
pip install gunicorn
```

Run the application with Gunicorn:
```bash
gunicorn -w 4 -b 0.0.0.0:5000 "app:create_app()"
```
*Adjust the number of workers (`-w`) based on your server's CPU cores (usually 2-4 x cores).*

### 2. Configure Nginx

Create an Nginx server block to proxy requests to Gunicorn:

```nginx
server {
    listen 80;
    server_name example.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

### 3. Set up Supervisor (Recommended)

Use Supervisor to manage the Gunicorn and Celery processes.

**`/etc/supervisor/conf.d/procg.conf`**

```ini
[program:procg_web]
command=/path/to/venv/bin/gunicorn -w 4 -b 127.0.0.1:5000 "app:create_app()"
directory=/path/to/procg_blueprint
user=www-data
autostart=true
autorestart=true

[program:procg_worker]
command=/path/to/venv/bin/celery -A executors.celery worker --loglevel=info
directory=/path/to/procg_blueprint
user=www-data
autostart=true
autorestart=true

[program:procg_beat]
command=/path/to/venv/bin/celery -A executors.celery beat --loglevel=info
directory=/path/to/procg_blueprint
user=www-data
autostart=true
autorestart=true
```

## Security Checklist

-   [ ] **SSL/TLS**: Ensure your site is served over HTTPS (e.g., using Let's Encrypt).
-   [ ] **Secrets**: Ensure `JWT_SECRET_ACCESS_TOKEN` and `CRYPTO_SECRET_KEY` are strong, random strings.
-   [ ] **Debug Mode**: Ensure `DEBUG` is set to `False` in production.
-   [ ] **Firewall**: Restrict access to ports (e.g., close 5000, 6379, 5432 to the public internet).
