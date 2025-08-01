#!/bin/bash

echo "Stopping and removing development containers, volumes, and networks..."
docker compose -f docker-compose-dev.yml up -d --build --force-recreate

echo "Ensuring development containers are removed..."
docker rm -f lg-falkordb-dev lg-pgvector-db-dev lg-jupyter-dev 2>/dev/null

echo "Removing development volumes..."
docker volume rm -f lg_pgvector_data_dev lg_falkor_data_dev lg_jupyter_data_dev 2>/dev/null

echo "Clearing extracted directory..."
rm -rf extracted

echo "Rebuilding development containers..."
docker compose -f docker-compose-dev.yml up -d --force-recreate

echo "Development environment reset complete."
echo ""
echo "Jupyter Lab is available at: http://localhost:8889 (no password required)"
echo "Source code is mounted for live development"