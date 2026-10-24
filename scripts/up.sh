#!/usr/bin/env bash
# Start the whole stack (creates the persistent volumes first). Extra arguments go to `docker compose up`.
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/volumes.sh
docker compose up -d --build "$@"
