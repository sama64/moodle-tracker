#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <backup-dir>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="$1"

if [[ ! -f "${BACKUP_DIR}/db.sql" ]]; then
  echo "missing ${BACKUP_DIR}/db.sql" >&2
  exit 1
fi

docker exec -i uni-tracker-db-1 psql -U uni_tracker -d uni_tracker < "${BACKUP_DIR}/db.sql"

if [[ -f "${BACKUP_DIR}/runtime_artifacts.tar.gz" ]]; then
  tar -C "${ROOT_DIR}/artifacts" -xzf "${BACKUP_DIR}/runtime_artifacts.tar.gz"
fi

echo "Restore completed from ${BACKUP_DIR}"
