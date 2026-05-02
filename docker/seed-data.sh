#!/bin/bash
# First-deployment data seeding from GitHub releases.
#
# Skips if DB already has data. Otherwise downloads CSV files from
# the data-2026-04-29 release and bulk-loads via psql COPY.
#
# Set SP_SEED_RELEASE=skip to disable.
# Set SP_SEED_RELEASE=<tag> to use a different release.

set -e

RELEASE_TAG="${SP_SEED_RELEASE:-data-2026-05-02}"
RELEASE_REPO="${SP_SEED_REPO:-avesed/stockpulse}"
BASE_URL="https://github.com/${RELEASE_REPO}/releases/download/${RELEASE_TAG}"

if [ "$RELEASE_TAG" = "skip" ]; then
  echo "[seed] SP_SEED_RELEASE=skip — skipping data seeding"
  exit 0
fi

if [ -z "$DATABASE_URL" ]; then
  echo "[seed] DATABASE_URL not set — skipping"
  exit 0
fi

# Detect existing data — skip if any of the main tables has rows
EXISTING=$(psql "$DATABASE_URL" -t -A -c "SELECT (SELECT COUNT(*) FROM stock_daily_bars LIMIT 1) + (SELECT COUNT(*) FROM stock_symbols LIMIT 1);" 2>/dev/null || echo "0")
EXISTING=$(echo "$EXISTING" | tr -d '[:space:]')

if [ "$EXISTING" != "0" ] && [ -n "$EXISTING" ]; then
  echo "[seed] DB already has data (rows=${EXISTING}) — skipping seed"
  exit 0
fi

echo "[seed] Empty DB detected — downloading data from ${RELEASE_TAG}"

# (filename → table) — order matters: symbols before bars (FK-like dependencies)
FILES=(
  "us-stock-symbols.csv.gz:stock_symbols"
  "hk-stock-symbols.csv.gz:stock_symbols"
  "cn-stock-symbols.csv.gz:stock_symbols"
  "metal-stock-symbols.csv.gz:stock_symbols"
  "us-stock-profiles.csv.gz:stock_profiles"
  "hk-stock-profiles.csv.gz:stock_profiles"
  "cn-stock-profiles.csv.gz:stock_profiles"
  "us-stock-daily-bars.csv.gz:stock_daily_bars"
  "hk-stock-daily-bars.csv.gz:stock_daily_bars"
  "cn-stock-daily-bars.csv.gz:stock_daily_bars"
  "metal-stock-daily-bars.csv.gz:stock_daily_bars"
  "us-stock-fundamentals.csv.gz:stock_fundamentals"
  "hk-stock-fundamentals.csv.gz:stock_fundamentals"
  "cn-stock-fundamentals.csv.gz:stock_fundamentals"
  "us-analyst-ratings.csv.gz:analyst_ratings"
  "hk-analyst-ratings.csv.gz:analyst_ratings"
  "us-institutional-holders.csv.gz:institutional_holders"
  "hk-institutional-holders.csv.gz:institutional_holders"
  "cn-fund-holdings.csv.gz:fund_holdings"
  "cn-northbound-holdings.csv.gz:northbound_holdings"
)

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

for entry in "${FILES[@]}"; do
  fname="${entry%%:*}"
  table="${entry##*:}"
  url="${BASE_URL}/${fname}"

  echo "[seed] Downloading ${fname}..."
  if ! curl -fsSL --retry 3 -o "${TMPDIR}/${fname}" "${url}"; then
    echo "[seed] WARN: failed to download ${fname}, skipping"
    continue
  fi

  # Get column list from CSV header (excluding 'id' which is auto-generated)
  HEADER=$(gunzip -c "${TMPDIR}/${fname}" | head -1)
  # Build column list, dropping 'id' column for tables with auto-increment PK
  COLS=$(echo "$HEADER" | python3 -c "
import sys
cols = [c.strip() for c in sys.stdin.read().strip().split(',')]
cols = [c for c in cols if c != 'id']
print(','.join(cols))
")

  echo "[seed] Loading ${fname} into ${table} (cols: ${COLS})..."
  # Use awk to drop the id column from each line
  ID_COL_IDX=$(echo "$HEADER" | python3 -c "
import sys
cols = [c.strip() for c in sys.stdin.read().strip().split(',')]
print(cols.index('id') + 1 if 'id' in cols else 0)
")

  if [ "$ID_COL_IDX" -gt 0 ]; then
    gunzip -c "${TMPDIR}/${fname}" | tail -n +2 | \
      awk -F',' -v idx="$ID_COL_IDX" 'BEGIN{OFS=","} {for(i=idx;i<NF;i++)$i=$(i+1); NF--; print}' | \
      psql "$DATABASE_URL" -c "COPY ${table} (${COLS}) FROM STDIN WITH CSV" 2>&1 | tail -1
  else
    gunzip -c "${TMPDIR}/${fname}" | \
      psql "$DATABASE_URL" -c "COPY ${table} (${COLS}) FROM STDIN WITH CSV HEADER" 2>&1 | tail -1
  fi

  rm -f "${TMPDIR}/${fname}"
done

# Reset sequences for tables with auto-increment PK
echo "[seed] Resetting sequences..."
psql "$DATABASE_URL" -c "
SELECT setval(pg_get_serial_sequence('stock_daily_bars', 'id'), COALESCE((SELECT MAX(id) FROM stock_daily_bars), 1));
SELECT setval(pg_get_serial_sequence('stock_fundamentals', 'id'), COALESCE((SELECT MAX(id) FROM stock_fundamentals), 1));
SELECT setval(pg_get_serial_sequence('analyst_ratings', 'id'), COALESCE((SELECT MAX(id) FROM analyst_ratings), 1));
SELECT setval(pg_get_serial_sequence('institutional_holders', 'id'), COALESCE((SELECT MAX(id) FROM institutional_holders), 1));
SELECT setval(pg_get_serial_sequence('fund_holdings', 'id'), COALESCE((SELECT MAX(id) FROM fund_holdings), 1));
SELECT setval(pg_get_serial_sequence('northbound_holdings', 'id'), COALESCE((SELECT MAX(id) FROM northbound_holdings), 1));
" >/dev/null 2>&1 || true

echo "[seed] Done — data seeded from ${RELEASE_TAG}"
