#!/bin/bash

echo "Stopping and removing development containers, volumes, and networks..."
docker compose -f docker-compose-dev.yml down -v --remove-orphans

echo "Ensuring development containers are removed..."
docker rm -f neo4j-hybrid-dev pgvector-hybrid-dev jupyter-hybrid-dev mysql-hybrid-dev 2>/dev/null

echo "Removing development volumes..."
docker volume rm -f neo4j_hybrid_data_dev neo4j_hybrid_logs_dev pgvector_hybrid_data_dev jupyter_hybrid_data_dev mysql_hybrid_data_dev 2>/dev/null

echo "Clearing extracted directory..."
rm -rf extracted

echo "Rebuilding development containers..."
docker compose -f docker-compose-dev.yml up -d --force-recreate

echo "Development environment reset complete."
echo ""
echo "Jupyter Lab is available at: http://localhost:8889 (no password required)"
echo "Source code is mounted for live development"