#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${ROOT_DIR}/artifacts/backups/${STAMP}"
mkdir -p "${OUT_DIR}"

docker exec uni-tracker-db-1 pg_dump -U uni_tracker -d uni_tracker > "${OUT_DIR}/db.sql"
tar -C "${ROOT_DIR}/artifacts" -czf "${OUT_DIR}/runtime_artifacts.tar.gz" runtime

echo "Backup written to ${OUT_DIR}"
