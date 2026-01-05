#!/bin/bash
#
# Reset Docker state for Crypto Streaming Pipeline
# Usage: ./script/reset-docker.sh [--hard]
#
# Options:
#   --hard    Remove images (full rebuild)
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

HARD_RESET=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --hard)
            HARD_RESET=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--hard]"
            echo ""
            echo "Options:"
            echo "  --hard    Remove Docker images (full rebuild)"
            echo "  -h        Show help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  RESET DOCKER - Crypto Streaming      ${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

echo -e "${RED}WARNING: This will delete all data!${NC}"
echo "Including:"
echo "  - All containers"
echo "  - All volumes (PostgreSQL, Redis, Grafana, Prometheus)"
echo "  - Airflow metadata and logs"
echo "  - Spark checkpoints"
if [ "$HARD_RESET" = true ]; then
    echo -e "  ${RED}- All Docker images (will rebuild)${NC}"
fi
echo ""
read -p "Are you sure? (y/N): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}[1/6] Stopping containers...${NC}"
docker compose down --remove-orphans 2>/dev/null || true

echo ""
echo -e "${GREEN}[2/6] Removing Docker volumes...${NC}"
docker volume rm -f \
    $(docker volume ls -q --filter name=postgres-db-volume) \
    $(docker volume ls -q --filter name=postgres-data-volume) \
    $(docker volume ls -q --filter name=grafana-storage) \
    $(docker volume ls -q --filter name=prometheus-data) \
    $(docker volume ls -q --filter name=spark-checkpoints) \
    2>/dev/null || true

PROJECT_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
docker volume ls -q --filter "name=${PROJECT_NAME}" | xargs -r docker volume rm 2>/dev/null || true

echo ""
echo -e "${GREEN}[3/6] Removing Airflow logs...${NC}"
rm -rf dags/logs 2>/dev/null || true
mkdir -p dags/logs
echo "  Cleared dags/logs/"

echo ""
echo -e "${GREEN}[4/6] Spark checkpoints in Docker volume (auto-cleaned)${NC}"
echo "  Volume: spark-checkpoints"

echo ""
echo -e "${GREEN}[5/6] Removing local data files...${NC}"
rm -rf data/parquet 2>/dev/null || true
mkdir -p data/parquet
find data -type f -name "*.parquet" -delete 2>/dev/null || true
find data -type f -name "*.json" -delete 2>/dev/null || true
echo "  Cleared data files"

if [ "$HARD_RESET" = true ]; then
    echo ""
    echo -e "${GREEN}[6/6] Removing Docker images...${NC}"
    docker compose down --rmi local 2>/dev/null || true
    docker image prune -f 2>/dev/null || true
    echo "  Removed local images"
else
    echo ""
    echo -e "${GREEN}[6/6] Keeping Docker images (use --hard to remove)${NC}"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RESET COMPLETE!                      ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "To start the system:"
echo "  docker compose up -d"
echo ""
echo "To rebuild images:"
echo "  docker compose up -d --build"
echo ""
