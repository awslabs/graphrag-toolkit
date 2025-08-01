#!/bin/bash

COMPOSE_FILE="docker-compose.yml"
DEV_MODE=false
RESET_MODE=false

for arg in "$@"; do
    case $arg in
        --mac)
            COMPOSE_FILE="docker-compose.arm.yml"
            echo "Using ARM/Mac-specific configuration"
            ;;
        --dev)
            DEV_MODE=true
            echo "Enabling development mode with hot-code-injection"
            ;;
        --reset)
            RESET_MODE=true
            echo "Reset mode enabled - will rebuild containers and reset data"
            ;;
    esac
done

if [ "$RESET_MODE" = true ]; then
    echo "Resetting containers and data..."
    docker compose -f $COMPOSE_FILE down -v
    echo "Building and starting containers..."
    BUILD_FLAG="--build"
else
    echo "Starting containers (preserving data)..."
    BUILD_FLAG=""
fi

if [ "$DEV_MODE" = true ]; then
    export LEXICAL_GRAPH_DEV_MOUNT="../../../lexical-graph:/home/jovyan/lexical-graph-src"
    echo "Development mode: Mounting lexical-graph source code"
fi

docker compose -f $COMPOSE_FILE up -d $BUILD_FLAG

echo ""
if [ "$RESET_MODE" = true ]; then
    echo "Reset and startup complete!"
else
    echo "Startup complete!"
fi
echo ""
echo "Services available at:"
echo "  Jupyter Lab:     http://localhost:8889 (no password required)"
echo "  Neo4j Browser:   http://localhost:7476 (neo4j/password)"
echo ""
echo "IMPORTANT: All notebook execution must happen in Jupyter Lab."
echo "   Open http://localhost:8889 to access the development environment."
echo "   Navigate to the 'work' folder to find the notebooks."
if [ "$DEV_MODE" = true ]; then
    echo ""
    echo "Development mode enabled - lexical-graph source code mounted for hot-code-injection"
    echo "   Changes to lexical-graph source will be reflected immediately in notebooks"
fi
if [ "$RESET_MODE" = false ]; then
    echo ""
    echo "Data preserved from previous runs. Use --reset to start fresh."
fi