#!/bin/bash

echo "Building and starting containers..."
docker compose up -d --build
echo "Build and startup complete."
echo ""
echo "Jupyter Lab is available at: http://localhost:8889"
echo "Waiting for Jupyter to start..."
sleep 5
echo "Jupyter token:"
docker logs lg-jupyter 2>&1 | grep -E "(token=|127.0.0.1:8888)" | tail -1