#!/bin/bash
set -e

echo "Setting up Lexical-Graph Local Development Environment..."

# Check for --reset flag
if [ "$1" = "--reset" ]; then
    echo "Resetting all data..."
    docker compose down -v
    docker volume prune -f
    rm -rf data
    echo "All data deleted."
fi

# Create data directories
echo "Creating data directories..."
mkdir -p data/falkordb
chmod -R 755 data/

# Check if network exists
if ! docker network inspect graphrag_network >/dev/null 2>&1; then
  echo "Creating Docker network: graphrag_network"
  docker network create graphrag_network
else
  echo "Docker network 'graphrag_network' already exists"
fi

# Start containers
echo "Starting Docker containers..."
docker compose up -d

echo "Waiting for services to be ready..."
sleep 10

# Check service health
echo "Checking service health..."
if curl -f http://localhost:8889 >/dev/null 2>&1; then
    echo " Jupyter is ready at http://localhost:8889 (token: lexical-graph)"
else
    echo "  Jupyter may still be starting up"
fi

echo " Lexical-Graph environment is ready!"
echo "   Jupyter Lab: http://localhost:8889 (token: lexical-graph)"
echo "   FalkorDB: localhost:6379"
echo "   PostgreSQL: localhost:5433"