Write-Host "Stopping and removing containers, volumes, and networks..."
docker compose down -v --remove-orphans

Write-Host "Ensuring containers are removed..."
docker rm -f falkordb falkordb-browser pgvector-db 2>$null

Write-Host "Removing named volumes..."
docker volume rm -f pgvector_data falkor_data 2>$null

Write-Host "Pruning dangling volumes (if any)..."
docker volume prune -f

Write-Host "Clearing extracted directory..."
Remove-Item -Recurse -Force "extracted" -ErrorAction SilentlyContinue

Write-Host "Rebuilding containers..."
docker compose up -d --force-recreate

Write-Host "Reset complete."
