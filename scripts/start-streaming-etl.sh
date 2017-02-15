#!/bin/bash

# Start Streaming ETL Pipeline
# This script orchestrates the complete streaming ETL setup

set -e

echo "🌊 Starting Streaming ETL Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

print_status "Starting infrastructure services..."

# Start infrastructure services
docker-compose up -d zookeeper kafka postgres minio grafana

print_status "Waiting for services to be ready..."

# Wait for Kafka to be ready
print_status "Waiting for Kafka to be ready..."
timeout=60
counter=0
while ! docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    if [ $counter -eq $timeout ]; then
        print_error "Kafka failed to start within $timeout seconds"
        exit 1
    fi
    sleep 1
    counter=$((counter + 1))
done

print_success "Kafka is ready!"

# Create Kafka topics
print_status "Creating Kafka topics..."
docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic orders --if-not-exists
docker-compose exec -T kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic customers --if-not-exists

print_success "Kafka topics created!"

# Wait for PostgreSQL to be ready
print_status "Waiting for PostgreSQL to be ready..."
timeout=60
counter=0
while ! docker-compose exec -T postgres pg_isready -U etl_user -d streaming_etl > /dev/null 2>&1; do
    if [ $counter -eq $timeout ]; then
        print_error "PostgreSQL failed to start within $timeout seconds"
        exit 1
    fi
    sleep 1
    counter=$((counter + 1))
done

print_success "PostgreSQL is ready!"

# Start Spark services
print_status "Starting Spark services..."
docker-compose up -d spark-master spark-worker

# Wait for Spark to be ready
print_status "Waiting for Spark to be ready..."
sleep 10

print_success "Spark cluster is ready!"

# Install Python dependencies
print_status "Installing Python dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    print_success "Python dependencies installed!"
else
    print_warning "requirements.txt not found. Please install dependencies manually."
fi

# Create necessary directories
print_status "Creating data directories..."
mkdir -p spark-data/checkpoints
mkdir -p spark-data/delta
mkdir -p spark-data/logs

print_success "Data directories created!"

# Start the data generator in background
print_status "Starting data generator..."
if [ -f "data-generator/order_producer.py" ]; then
    nohup python data-generator/order_producer.py --duration 1440 --rate 5 > logs/data_generator.log 2>&1 &
    DATA_GENERATOR_PID=$!
    echo $DATA_GENERATOR_PID > data_generator.pid
    print_success "Data generator started (PID: $DATA_GENERATOR_PID)"
else
    print_warning "Data generator not found. Please start it manually."
fi

# Submit Spark streaming job
print_status "Submitting Spark streaming job..."
if [ -f "spark-apps/streaming_etl.py" ]; then
    docker-compose exec -d spark-master spark-submit \
        --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        /opt/spark-apps/streaming_etl.py
    
    print_success "Spark streaming job submitted!"
else
    print_warning "Spark streaming application not found. Please submit it manually."
fi

# Wait a bit for streaming to start
sleep 5

# Start the dashboard
print_status "Starting dashboard..."
if [ -f "dashboard/streaming_dashboard.py" ]; then
    nohup streamlit run dashboard/streaming_dashboard.py --server.port 8501 > logs/dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    echo $DASHBOARD_PID > dashboard.pid
    print_success "Dashboard started (PID: $DASHBOARD_PID)"
    print_status "Dashboard available at: http://localhost:8501"
else
    print_warning "Dashboard not found. Please start it manually."
fi

# Display service URLs
echo ""
print_success "🎉 Streaming ETL Pipeline is now running!"
echo ""
echo "📊 Service URLs:"
echo "  • Spark Master UI:    http://localhost:8080"
echo "  • Kafka UI:           http://localhost:9021 (if available)"
echo "  • Grafana:            http://localhost:3000 (admin/admin)"
echo "  • MinIO:              http://localhost:9001 (minioadmin/minioadmin)"
echo "  • Streaming Dashboard: http://localhost:8501"
echo ""
echo "📝 Logs:"
echo "  • Data Generator:     tail -f logs/data_generator.log"
echo "  • Dashboard:          tail -f logs/dashboard.log"
echo "  • Docker Compose:     docker-compose logs -f"
echo ""
echo "🛑 To stop the pipeline:"
echo "  ./scripts/stop-streaming-etl.sh"
echo ""

# Create logs directory if it doesn't exist
mkdir -p logs

# Save service information
cat > service_info.txt << EOF
Streaming ETL Pipeline - Service Information
Generated: $(date)

Service URLs:
- Spark Master UI: http://localhost:8080
- Grafana: http://localhost:3000 (admin/admin)
- MinIO: http://localhost:9001 (minioadmin/minioadmin)
- Streaming Dashboard: http://localhost:8501

Process IDs:
- Data Generator: $(cat data_generator.pid 2>/dev/null || echo "Not started")
- Dashboard: $(cat dashboard.pid 2>/dev/null || echo "Not started")

Docker Services:
$(docker-compose ps)
EOF

print_success "Service information saved to service_info.txt"

# Monitor initial startup
print_status "Monitoring initial data flow..."
sleep 30

# Check if data is flowing
KAFKA_MESSAGES=$(docker-compose exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --timeout-ms 5000 --max-messages 1 2>/dev/null | wc -l || echo "0")

if [ "$KAFKA_MESSAGES" -gt 0 ]; then
    print_success "✅ Data is flowing through Kafka!"
else
    print_warning "⚠️  No data detected in Kafka. Check data generator logs."
fi

print_success "🚀 Streaming ETL Pipeline startup complete!"
print_status "Monitor the dashboard at http://localhost:8501 to see real-time metrics."