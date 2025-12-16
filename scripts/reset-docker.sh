#!/bin/bash
#
# Script reset toàn bộ trạng thái Docker cho hệ thống Crypto Streaming
# Sử dụng: ./scripts/reset-docker.sh [--hard]
#
# Options:
#   --hard    Xóa cả images (rebuild từ đầu)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

HARD_RESET=false

# Parse arguments
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
            echo "  --hard    Xóa cả Docker images (rebuild từ đầu)"
            echo "  -h        Hiển thị help"
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

# Confirm before proceeding
echo -e "${RED}CẢNH BÁO: Script này sẽ xóa toàn bộ dữ liệu!${NC}"
echo "Bao gồm:"
echo "  - Tất cả containers"
echo "  - Tất cả volumes (PostgreSQL, Redis, MinIO, Grafana, Prometheus)"
echo "  - Airflow metadata và logs"
echo "  - Spark checkpoints"
if [ "$HARD_RESET" = true ]; then
    echo -e "  ${RED}- Tất cả Docker images (sẽ rebuild)${NC}"
fi
echo ""
read -p "Bạn có chắc chắn muốn tiếp tục? (y/N): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Đã hủy."
    exit 0
fi

echo ""
echo -e "${GREEN}[1/6] Dừng tất cả containers...${NC}"
docker compose down --remove-orphans 2>/dev/null || true

echo ""
echo -e "${GREEN}[2/6] Xóa Docker volumes...${NC}"
# Xóa named volumes được định nghĩa trong docker-compose.yml
docker volume rm -f \
    $(docker volume ls -q --filter name=postgres-db-volume) \
    $(docker volume ls -q --filter name=postgres-data-volume) \
    $(docker volume ls -q --filter name=minio-data) \
    $(docker volume ls -q --filter name=grafana-storage) \
    $(docker volume ls -q --filter name=prometheus-data) \
    2>/dev/null || true

# Xóa volumes với prefix của project (nếu có)
PROJECT_NAME=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
docker volume ls -q --filter "name=${PROJECT_NAME}" | xargs -r docker volume rm 2>/dev/null || true

echo ""
echo -e "${GREEN}[3/6] Xóa Airflow logs...${NC}"
rm -rf dags/logs/dag_id=* 2>/dev/null || true
echo "  Đã xóa dags/logs/"

echo ""
echo -e "${GREEN}[4/6] Xóa Spark checkpoints...${NC}"
rm -rf data/spark-checkpoints/* 2>/dev/null || true
echo "  Đã xóa data/spark-checkpoints/"

echo ""
echo -e "${GREEN}[5/6] Xóa local data files...${NC}"
# Xóa các file parquet nếu có
rm -rf data/parquet/* 2>/dev/null || true
# Giữ lại thư mục nhưng xóa nội dung
find data -type f -name "*.parquet" -delete 2>/dev/null || true
find data -type f -name "*.json" -delete 2>/dev/null || true
echo "  Đã xóa data files"

if [ "$HARD_RESET" = true ]; then
    echo ""
    echo -e "${GREEN}[6/6] Xóa Docker images...${NC}"
    # Xóa images được build bởi project
    docker compose down --rmi local 2>/dev/null || true
    # Prune dangling images
    docker image prune -f 2>/dev/null || true
    echo "  Đã xóa local images"
else
    echo ""
    echo -e "${GREEN}[6/6] Giữ lại Docker images (dùng --hard để xóa)${NC}"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RESET HOÀN TẤT!                      ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Để khởi động lại hệ thống:"
echo "  docker compose up -d"
echo ""
echo "Hoặc khởi động với build lại images:"
echo "  docker compose up -d --build"
echo ""
