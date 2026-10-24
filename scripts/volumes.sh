#!/usr/bin/env bash
# Creates the Docker volumes that hold the Redpanda data.
# They are declared `external` in docker-compose.yml, so `docker compose down`, `down -v` and even removing the
# compose project never touch them. Delete them deliberately with: docker volume rm signalr-redpanda-1 -2 -3
set -euo pipefail
for n in 1 2 3; do
  docker volume inspect "signalr-redpanda-$n" >/dev/null 2>&1 || docker volume create "signalr-redpanda-$n" >/dev/null
  echo "volume signalr-redpanda-$n ready"
done
