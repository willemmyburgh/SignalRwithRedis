# Windows PowerShell version of scripts/up.sh. Extra arguments go to `docker compose up`.
Set-Location (Join-Path $PSScriptRoot "..")
& "$PSScriptRoot\volumes.ps1"
docker compose up -d --build @args
