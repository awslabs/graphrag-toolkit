@echo off

echo Setting up Lexical-Graph Local Development Environment...

REM Check for --reset flag
IF "%1"=="--reset" (
    echo Resetting all data...
    docker compose down -v
    docker volume prune -f
    rmdir /s /q data 2>nul
    echo All data deleted.
)

REM Create data directories
echo Creating data directories...
mkdir data\falkordb 2>nul

REM Check if Docker network exists
docker network inspect graphrag_network >nul 2>&1

IF %ERRORLEVEL% NEQ 0 (
    echo Creating Docker network: graphrag_network
    docker network create graphrag_network
) ELSE (
    echo Docker network 'graphrag_network' already exists
)

REM Start services
echo Starting Docker containers...
docker compose up -d

echo Waiting for services to be ready...
timeout /t 10 /nobreak >nul

echo Lexical-Graph environment is ready!
echo    Jupyter Lab: http://localhost:8889 (token: lexical-graph)
echo    FalkorDB: localhost:6379
echo    PostgreSQL: localhost:5433