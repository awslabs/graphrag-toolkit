@echo off
echo Stopping and removing containers, volumes, and networks...
docker compose down -v --remove-orphans

echo Ensuring containers are removed...
docker rm -f falkordb falkordb-browser pgvector-db 2>nul

echo Removing named volumes...
docker volume rm -f pgvector_data falkor_data 2>nul

echo Pruning dangling volumes (if any)...
docker volume prune -f

echo Clearing extracted directory...
rmdir /s /q extracted

echo Rebuilding containers...
docker compose up -d --force-recreate

echo Reset complete.
