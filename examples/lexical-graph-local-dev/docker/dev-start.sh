#!/bin/bash

echo "Building and starting development containers..."
docker compose -f docker-compose-dev.yml up -d --build
echo "Development environment startup complete."
echo ""
echo "Jupyter Lab is available at: http://localhost:8889 (no password required)"
echo "Source code is mounted for live development"