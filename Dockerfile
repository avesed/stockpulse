# Stage 1: Frontend builder
FROM node:20-alpine AS frontend-builder
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --no-audit
COPY frontend/ ./
RUN npm run build

# Stage 2: Backend + Production
FROM python:3.11-slim AS production
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl nginx supervisor dumb-init \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app

# Install Python dependencies
COPY backend/pyproject.toml ./
RUN pip install --no-cache-dir .

# Copy backend code
COPY backend/ ./

# Copy built frontend to nginx html
COPY --from=frontend-builder /build/dist /usr/share/nginx/html

# Copy Docker config
COPY docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY docker/supervisord.conf /etc/supervisor/conf.d/stockpulse.conf
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Remove default nginx config
RUN rm -f /etc/nginx/sites-enabled/default

EXPOSE 80 8010

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/entrypoint.sh"]
