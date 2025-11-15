#!/bin/bash

echo "========================================"
echo "Zillow Real Estate Data Pipeline"
echo "Nhom 14 - Lop 154050"
echo "========================================"
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "[ERROR] Virtual environment not found!"
    echo "Please run: python -m venv .venv"
    echo "Then activate it and install dependencies: pip install -r requirements.txt"
    exit 1
fi

# Activate virtual environment
echo "[1/6] Activating virtual environment..."
source .venv/bin/activate

# Check if Docker is running
echo ""
echo "[2/6] Checking Docker services..."
if ! docker ps >/dev/null 2>&1; then
    echo "[ERROR] Docker is not running! Please start Docker."
    exit 1
fi

# Start Docker services
echo ""
echo "[3/6] Starting Docker Compose services..."
docker-compose up -d

# Wait for services to be ready
echo ""
echo "[4/6] Waiting for services to start (60 seconds)..."
sleep 60

# Check services status
echo ""
echo "[5/6] Checking service status..."
docker-compose ps

# Start all components
echo ""
echo "[6/6] Starting Kafka Producer, Batch Consumer, and Spark Streaming..."
echo ""
echo "========================================"
echo "Opening 3 new terminals:"
echo "  - Terminal 1: Kafka Producer"
echo "  - Terminal 2: Batch Consumer (HDFS)"
echo "  - Terminal 3: Spark Streaming"
echo "========================================"
echo ""

# Detect terminal emulator
if command -v gnome-terminal &> /dev/null; then
    TERM_CMD="gnome-terminal --"
elif command -v xterm &> /dev/null; then
    TERM_CMD="xterm -e"
elif command -v konsole &> /dev/null; then
    TERM_CMD="konsole -e"
else
    echo "[WARNING] No supported terminal emulator found."
    echo "Please run the following commands manually in separate terminals:"
    echo ""
    echo "Terminal 1: python kafka/producer.py"
    echo "Terminal 2: python kafka/consumer_batch.py"
    echo "Terminal 3: python kafka/consumer_structured_stream.py"
    echo ""
    exit 0
fi

# Start components in new terminals
$TERM_CMD bash -c "source .venv/bin/activate && python kafka/producer.py; exec bash" &
sleep 5

$TERM_CMD bash -c "source .venv/bin/activate && python kafka/consumer_batch.py; exec bash" &
sleep 5

$TERM_CMD bash -c "source .venv/bin/activate && python kafka/consumer_structured_stream.py; exec bash" &

echo ""
echo "========================================"
echo "All components started successfully!"
echo "========================================"
echo ""
echo "Web UIs available at:"
echo "  - Spark Master: http://localhost:8080"
echo "  - Spark Worker: http://localhost:8081"
echo "  - HDFS NameNode: http://localhost:9870"
echo "  - Elasticsearch: http://localhost:9200"
echo "  - Kibana: http://localhost:5601"
echo ""
echo "To stop all services:"
echo "  1. Close all terminal windows (Ctrl+C)"
echo "  2. Run: docker-compose down"
echo ""
