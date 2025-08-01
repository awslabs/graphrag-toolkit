@echo off
setlocal enabledelayedexpansion

REM Default to standard docker-compose file
set COMPOSE_FILE=docker-compose.yml
set DEV_MODE=false
set RESET_MODE=false

REM Check for flags
for %%i in (%*) do (
    if "%%i"=="--mac" (
        set COMPOSE_FILE=docker-compose.arm.yml
        echo Using ARM/Mac-specific configuration
    )
    if "%%i"=="--dev" (
        set DEV_MODE=true
        echo Enabling development mode with hot-code-injection
    )
    if "%%i"=="--reset" (
        set RESET_MODE=true
        echo Reset mode enabled - will rebuild containers and reset data
    )
)

if "%RESET_MODE%"=="true" (
    echo Resetting containers and data...
    docker compose -f %COMPOSE_FILE% down -v
    echo Building and starting containers...
    set BUILD_FLAG=--build
) else (
    echo Starting containers (preserving data)...
    set BUILD_FLAG=
)

if "%DEV_MODE%"=="true" (
    set LEXICAL_GRAPH_DEV_MOUNT=../../../lexical-graph:/home/jovyan/lexical-graph-src
    echo Development mode: Mounting lexical-graph source code
)

docker compose -f %COMPOSE_FILE% up -d %BUILD_FLAG%

echo.
if "%RESET_MODE%"=="true" (
    echo Reset and startup complete!
) else (
    echo Startup complete!
)
echo.
echo Services available at:
echo   Jupyter Lab:     http://localhost:8889 (no password required)
echo   Neo4j Browser:   http://localhost:7476 (neo4j/password)
echo.
echo IMPORTANT: All notebook execution must happen in Jupyter Lab.
echo    Open http://localhost:8889 to access the development environment.
echo    Navigate to the 'work' folder to find the notebooks.
if "%DEV_MODE%"=="true" (
    echo.
    echo Development mode enabled - lexical-graph source code mounted for hot-code-injection
    echo    Changes to lexical-graph source will be reflected immediately in notebooks
)
if "%RESET_MODE%"=="false" (
    echo.
    echo Data preserved from previous runs. Use --reset to start fresh.
)