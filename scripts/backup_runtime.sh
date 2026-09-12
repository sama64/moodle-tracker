#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ENV_FILE="${ROOT_DIR}/.env"
RESTIC_PASSWORD_FILE="${RESTIC_PASSWORD_FILE:-${HOME}/.config/moodle-tracker/restic-password}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "missing ${ENV_FILE}" >&2
  exit 1
fi
if [[ ! -f "${RESTIC_PASSWORD_FILE}" ]]; then
  echo "missing ${RESTIC_PASSWORD_FILE}" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID is required}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY is required}"
export RESTIC_PASSWORD_FILE
export RESTIC_REPOSITORY="${RESTIC_REPOSITORY:-s3:${S3_ENDPOINT_URL}/${S3_BUCKET}/backups/production-v2}"

cd "${ROOT_DIR}"
docker compose exec -T db pg_dump \
  --clean --if-exists --format=custom \
  -U "${POSTGRES_USER:-uni_tracker}" \
  -d "${POSTGRES_DB:-uni_tracker}" > "${TMP_DIR}/uni_tracker.dump"
cp "${ENV_FILE}" "${TMP_DIR}/deployment.env"
chmod 600 "${TMP_DIR}/deployment.env" "${TMP_DIR}/uni_tracker.dump"

if ! restic snapshots --tag moodle-tracker >/dev/null 2>&1; then
  restic init
fi
restic backup "${TMP_DIR}" --tag moodle-tracker --tag "${STAMP}"
restic forget --tag moodle-tracker --keep-daily 7 --keep-weekly 4 --keep-monthly 6 --prune

echo "Encrypted Moodle tracker backup completed at ${STAMP}"
