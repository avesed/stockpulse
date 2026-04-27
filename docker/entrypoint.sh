#!/bin/bash
set -e

echo "Running database migrations..."
cd /app && alembic upgrade head

echo "Starting StockPulse..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/stockpulse.conf
