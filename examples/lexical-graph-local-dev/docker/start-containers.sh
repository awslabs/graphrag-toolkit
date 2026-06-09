#!/bin/bash

COMPOSE_FILE="docker-compose.yml"
DEV_MODE=false
RESET_MODE=false

for arg in "$@"; do
    case $arg in
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

if [ "$DEV_MODE" = true ]; then
    COMPOSE_FILE="docker-compose-dev.yml"
    echo "Development mode: Using docker-compose-dev.yml with hot-code-injection"
fi

if [ "$RESET_MODE" = true ]; then
    echo "Resetting containers and data..."
    docker compose -f $COMPOSE_FILE down -v
    rm -rf extracted
    if [ "$DEV_MODE" = false ]; then
        echo "NOTE: This resets standard mode containers. Use --dev --reset to reset dev containers."
    fi
    echo "Building and starting containers..."
    BUILD_FLAG="--build"
else
    echo "Starting containers (preserving data)..."
    BUILD_FLAG=""
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
if [ "$DEV_MODE" = true ]; then
    echo "  Jupyter Lab:     http://localhost:8890 (no password required)"
    echo "  Neo4j Browser:   http://localhost:7477 (neo4j/password)"
else
    echo "  Jupyter Lab:     http://localhost:8889 (no password required)"
    echo "  Neo4j Browser:   http://localhost:7476 (neo4j/password)"
fi
echo ""
echo "IMPORTANT: All notebook execution must happen in Jupyter Lab."
if [ "$DEV_MODE" = true ]; then
    echo "   Open http://localhost:8890 to access the development environment."
else
    echo "   Open http://localhost:8889 to access the development environment."
fi
echo "   Navigate to the 'notebooks' folder to find the notebooks."
if [ "$DEV_MODE" = true ]; then
    echo ""
    echo "Development mode enabled - lexical-graph source code mounted for hot-code-injection"
    echo "   Changes to lexical-graph source will be reflected immediately in notebooks"
fi
if [ "$RESET_MODE" = false ]; then
    echo ""
    echo "Data preserved from previous runs. Use --reset to start fresh."
fi
