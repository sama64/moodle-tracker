#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
RESTIC_PASSWORD_FILE="${RESTIC_PASSWORD_FILE:-${HOME}/.config/moodle-tracker/restic-password}"
SNAPSHOT="${1:-latest}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID is required}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY is required}"
export RESTIC_PASSWORD_FILE
export RESTIC_REPOSITORY="${RESTIC_REPOSITORY:-s3:${S3_ENDPOINT_URL}/${S3_BUCKET}/backups/production-v2}"

restic restore "${SNAPSHOT}" --tag moodle-tracker --target "${TMP_DIR}"
DUMP_PATH="$(find "${TMP_DIR}" -type f -name uni_tracker.dump -print -quit)"
if [[ -z "${DUMP_PATH}" ]]; then
  echo "backup does not contain uni_tracker.dump" >&2
  exit 1
fi

cd "${ROOT_DIR}"
docker compose exec -T db pg_restore \
  --clean --if-exists --no-owner --no-privileges \
  -U "${POSTGRES_USER:-uni_tracker}" \
  -d "${POSTGRES_DB:-uni_tracker}" < "${DUMP_PATH}"

CALENDAR_STATE="$(find "${TMP_DIR}" -type f -path '*/exam-calendar/academic-state.yaml' -print -quit)"
if [[ -n "${CALENDAR_STATE}" ]]; then
  install -D -m 0644 "${CALENDAR_STATE}" "${HOME}/.config/exam-calendar/academic-state.yaml"
fi

echo "Restore completed from encrypted snapshot ${SNAPSHOT}"
