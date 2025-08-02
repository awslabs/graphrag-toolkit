param(
    [switch]$Mac
)

$ComposeFile = "docker-compose.yml"

if ($Mac) {
    $ComposeFile = "docker-compose.arm.yml"
    Write-Host "Using ARM/Mac-specific configuration"
}

Write-Host "Building and starting containers..."
docker compose -f $ComposeFile up -d --build
Write-Host "Build and startup complete."
