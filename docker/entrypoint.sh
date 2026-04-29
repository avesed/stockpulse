#!/bin/bash
set -e

echo "Running database migrations..."
cd /app && alembic upgrade head

# Seed data from GitHub release on first deployment (empty DB)
if [ -x /usr/local/bin/seed-data.sh ]; then
  /usr/local/bin/seed-data.sh || echo "[entrypoint] seed-data failed (non-fatal), continuing..."
fi

echo "Starting StockPulse..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/stockpulse.conf
