#!/bin/bash

# Default to standard docker-compose file
COMPOSE_FILE="docker-compose.yml"

# Check for Mac/ARM flag
for arg in "$@"; do
    case $arg in
        --mac)
            COMPOSE_FILE="docker-compose.arm.yml"
            echo "Using ARM/Mac-specific configuration"
            ;;
    esac
done

echo "Stopping and removing containers, volumes, and networks..."
docker compose -f $COMPOSE_FILE down -v --remove-orphans

echo "Ensuring containers are removed..."
docker rm -f neo4j jupyter-notebook pgvector-db 2>/dev/null

echo "Removing named volumes..."
docker volume rm -f pgvector_data neo4j_data neo4j_logs jupyter_data 2>/dev/null

echo "Pruning dangling volumes (if any)..."
docker volume prune -f

echo "Clearing extracted directory..."
rm -rf extracted

echo "Rebuilding containers..."
docker compose -f $COMPOSE_FILE up -d --force-recreate

echo "Reset complete."