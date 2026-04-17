param(
    [switch]$Mac,
    [switch]$Dev,
    [switch]$Reset
)

$ComposeFile = "docker-compose.yml"

if ($Mac) {
    $ComposeFile = "docker-compose.arm.yml"
    Write-Host "Using ARM/Mac-specific configuration"
}

if ($Dev) {
    $ComposeFile = "docker-compose-dev.yml"
    Write-Host "Development mode: Using docker-compose-dev.yml with hot-code-injection"
}

if ($Reset) {
    Write-Host "Resetting containers and data..."
    docker compose -f $ComposeFile down -v
    Remove-Item -Recurse -Force extracted -ErrorAction SilentlyContinue
    Write-Host "Building and starting containers..."
    $BuildFlag = "--build"
} else {
    Write-Host "Starting containers (preserving data)..."
    $BuildFlag = ""
}

if ($BuildFlag) {
    docker compose -f $ComposeFile up -d --build
} else {
    docker compose -f $ComposeFile up -d
}

Write-Host ""
if ($Reset) {
    Write-Host "Reset and startup complete!"
} else {
    Write-Host "Startup complete!"
}
Write-Host ""
Write-Host "Services available at:"
if ($Dev) {
    Write-Host "  Jupyter Lab:     http://localhost:8890 (no password required)"
    Write-Host "  Neo4j Browser:   http://localhost:7477 (neo4j/password)"
} else {
    Write-Host "  Jupyter Lab:     http://localhost:8889 (no password required)"
    Write-Host "  Neo4j Browser:   http://localhost:7476 (neo4j/password)"
}
Write-Host ""
Write-Host "IMPORTANT: All notebook execution must happen in Jupyter Lab."
if ($Dev) {
    Write-Host "   Open http://localhost:8890 to access the development environment."
} else {
    Write-Host "   Open http://localhost:8889 to access the development environment."
}
Write-Host "   Navigate to the 'work' folder to find the notebooks."
if ($Dev) {
    Write-Host ""
    Write-Host "Development mode enabled - lexical-graph source code mounted for hot-code-injection"
    Write-Host "   Changes to lexical-graph source will be reflected immediately in notebooks"
}
if (-not $Reset) {
    Write-Host ""
    Write-Host "Data preserved from previous runs. Use -Reset to start fresh."
}
