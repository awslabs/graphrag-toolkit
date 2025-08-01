param(
    [switch]$Mac,
    [switch]$Dev,
    [switch]$Reset
)

# Default to standard docker-compose file
$COMPOSE_FILE = "docker-compose.yml"
$DEV_MODE = $false
$RESET_MODE = $false

# Check for flags
if ($Mac) {
    $COMPOSE_FILE = "docker-compose.arm.yml"
    Write-Host "Using ARM/Mac-specific configuration"
}

if ($Dev) {
    $DEV_MODE = $true
    Write-Host "Enabling development mode with hot-code-injection"
}

if ($Reset) {
    $RESET_MODE = $true
    Write-Host "Reset mode enabled - will rebuild containers and reset data"
}

if ($RESET_MODE) {
    Write-Host "Resetting containers and data..."
    docker compose -f $COMPOSE_FILE down -v
    Write-Host "Building and starting containers..."
    $BUILD_FLAG = "--build"
} else {
    Write-Host "Starting containers (preserving data)..."
    $BUILD_FLAG = ""
}

if ($DEV_MODE) {
    $env:LEXICAL_GRAPH_DEV_MOUNT = "../../../lexical-graph:/home/jovyan/lexical-graph-src"
    Write-Host "Development mode: Mounting lexical-graph source code"
}

if ($BUILD_FLAG) {
    docker compose -f $COMPOSE_FILE up -d --build
} else {
    docker compose -f $COMPOSE_FILE up -d
}

Write-Host ""
if ($RESET_MODE) {
    Write-Host "Reset and startup complete!"
} else {
    Write-Host "Startup complete!"
}
Write-Host ""
Write-Host "Services available at:"
Write-Host "  Jupyter Lab:     http://localhost:8889 (no password required)"
Write-Host "  Neo4j Browser:   http://localhost:7476 (neo4j/password)"
Write-Host ""
Write-Host "IMPORTANT: All notebook execution must happen in Jupyter Lab."
Write-Host "   Open http://localhost:8889 to access the development environment."
Write-Host "   Navigate to the 'work' folder to find the notebooks."
if ($DEV_MODE) {
    Write-Host ""
    Write-Host "Development mode enabled - lexical-graph source code mounted for hot-code-injection"
    Write-Host "   Changes to lexical-graph source will be reflected immediately in notebooks"
}
if (-not $RESET_MODE) {
    Write-Host ""
    Write-Host "Data preserved from previous runs. Use -Reset to start fresh."
}