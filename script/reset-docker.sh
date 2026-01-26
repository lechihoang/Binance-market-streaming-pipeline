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

echo ""
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  RESET DOCKER - Binance Market Pipeline${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

echo -e "${RED}WARNING: This will delete all data!${NC}"
echo "Including:"
echo "  - All containers"
echo "  - All volumes (PostgreSQL, Redis, Grafana, Prometheus, Spark)"
echo "  - Kafka topics (raw_trades, raw_tickers)"
echo "  - Redis cache"
echo "  - Airflow metadata and logs"
echo "  - Spark checkpoints"
echo "  - Local data files"
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
echo -e "${GREEN}[1/7] Stopping containers...${NC}"
docker compose down --remove-orphans 2>/dev/null || true

echo ""
echo -e "${GREEN}[2/7] Removing Docker volumes...${NC}"
docker volume rm -f \
    binance-market-streaming-pipeline_postgres-data \
    binance-market-streaming-pipeline_grafana-storage \
    binance-market-streaming-pipeline_prometheus-data \
    binance-market-streaming-pipeline_spark-checkpoints \
    2>/dev/null || true
echo "  Removed all volumes"

echo ""
echo -e "${GREEN}[3/7] Clearing Kafka topics...${NC}"
if docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
    docker exec kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic raw_trades 2>/dev/null || true
    docker exec kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic raw_tickers 2>/dev/null || true
    echo "  Cleared Kafka topics: raw_trades, raw_tickers"
else
    echo "  Kafka not running, skipping topic cleanup"
fi

echo ""
echo -e "${GREEN}[4/7] Clearing Redis cache...${NC}"
if docker ps --format '{{.Names}}' | grep -q '^redis$'; then
    docker exec redis redis-cli FLUSHALL 2>/dev/null || true
    echo "  Flushed all Redis data"
else
    echo "  Redis not running, skipping"
fi

echo ""
echo -e "${GREEN}[5/7] Removing Airflow logs...${NC}"
rm -rf log/* 2>/dev/null || true
mkdir -p log
echo "  Cleared log/"

echo ""
echo -e "${GREEN}[6/7] Removing local data files...${NC}"
rm -rf data/parquet 2>/dev/null || true
rm -rf data/spark-checkpoints/* 2>/dev/null || true
mkdir -p data/parquet data/spark-checkpoints
find data -type f -name "*.parquet" -delete 2>/dev/null || true
find data -type f -name "*.json" -delete 2>/dev/null || true
echo "  Cleared data files and Spark checkpoints"

if [ "$HARD_RESET" = true ]; then
    echo ""
    echo -e "${GREEN}[7/7] Removing Docker images...${NC}"
    docker compose down --rmi local 2>/dev/null || true
    docker image prune -f 2>/dev/null || true
    echo "  Removed local images"
else
    echo ""
    echo -e "${GREEN}[7/7] Keeping Docker images (use --hard to remove)${NC}"
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
