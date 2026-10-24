# Windows PowerShell version of scripts/volumes.sh: creates the Docker volumes that hold the Redpanda data.
# They are declared `external` in docker-compose.yml, so `docker compose down -v` never deletes them.
foreach ($n in 1..3) {
    docker volume inspect "signalr-redpanda-$n" *> $null
    if ($LASTEXITCODE -ne 0) { docker volume create "signalr-redpanda-$n" | Out-Null }
    Write-Host "volume signalr-redpanda-$n ready"
}
