#!/bin/bash

# Stop Streaming ETL Pipeline
# This script gracefully shuts down all components

set -e

echo "🛑 Stopping Streaming ETL Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Stop background processes
print_status "Stopping background processes..."

# Stop data generator
if [ -f "data_generator.pid" ]; then
    DATA_GENERATOR_PID=$(cat data_generator.pid)
    if kill -0 $DATA_GENERATOR_PID 2>/dev/null; then
        kill $DATA_GENERATOR_PID
        print_success "Data generator stopped (PID: $DATA_GENERATOR_PID)"
    else
        print_warning "Data generator process not found"
    fi
    rm -f data_generator.pid
fi

# Stop dashboard
if [ -f "dashboard.pid" ]; then
    DASHBOARD_PID=$(cat dashboard.pid)
    if kill -0 $DASHBOARD_PID 2>/dev/null; then
        kill $DASHBOARD_PID
        print_success "Dashboard stopped (PID: $DASHBOARD_PID)"
    else
        print_warning "Dashboard process not found"
    fi
    rm -f dashboard.pid
fi

# Stop any other Python processes related to the project
print_status "Stopping any remaining Python processes..."
pkill -f "streaming_etl.py" 2>/dev/null || true
pkill -f "order_producer.py" 2>/dev/null || true
pkill -f "streaming_dashboard.py" 2>/dev/null || true

# Stop Docker services
print_status "Stopping Docker services..."
docker-compose down

print_success "Docker services stopped!"

# Optional: Clean up data (ask user)
echo ""
read -p "Do you want to clean up data volumes? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Cleaning up data volumes..."
    docker-compose down -v
    docker volume prune -f
    print_success "Data volumes cleaned!"
else
    print_status "Data volumes preserved for next startup"
fi

# Clean up temporary files
print_status "Cleaning up temporary files..."
rm -f service_info.txt
rm -f *.pid

# Optional: Clean up logs
echo ""
read -p "Do you want to clean up log files? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Cleaning up log files..."
    rm -rf logs/
    print_success "Log files cleaned!"
else
    print_status "Log files preserved in logs/ directory"
fi

print_success "🎉 Streaming ETL Pipeline stopped successfully!"

echo ""
echo "📝 Summary:"
echo "  • All Docker services stopped"
echo "  • Background processes terminated"
echo "  • Temporary files cleaned up"
echo ""
echo "🚀 To restart the pipeline:"
echo "  ./scripts/start-streaming-etl.sh"
echo ""