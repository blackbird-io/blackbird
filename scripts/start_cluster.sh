#!/bin/bash

# Blackbird Local Cluster Startup Script
# This script starts etcd, keystone, and worker services on localhost for testing

set -e

# Configuration
ETCD_PORT=2379
KEYSTONE_PORT=9090
KEYSTONE_HTTP_PORT=9091
WORKER_MEMORY_SIZE="2147483648"  # 2GB
CLUSTER_ID="blackbird_local"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[BLACKBIRD]${NC} $1"
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

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Function to wait for service to be ready
wait_for_service() {
    local port=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1
    
    print_status "Waiting for $service_name to start on port $port..."
    
    while ! nc -z localhost $port 2>/dev/null; do
        if [ $attempt -ge $max_attempts ]; then
            print_error "$service_name failed to start after $max_attempts attempts"
            return 1
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    
    print_success "$service_name is ready on port $port"
    return 0
}

# Function to cleanup processes on exit
cleanup() {
    print_status "Shutting down services..."
    
    # Kill background processes
    for pid in "${PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            print_status "Stopping process $pid"
            kill $pid 2>/dev/null || true
        fi
    done
    
    # Wait a bit for graceful shutdown
    sleep 2
    
    # Force kill if still running
    for pid in "${PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            print_warning "Force killing process $pid"
            kill -9 $pid 2>/dev/null || true
        fi
    done
    
    print_status "Cleanup complete"
}

# Set up cleanup trap
PIDS=()
trap cleanup EXIT INT TERM

# Change to script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$(dirname "$SCRIPT_DIR")/build"

if [ ! -d "$BUILD_DIR" ]; then
    print_error "Build directory not found: $BUILD_DIR"
    print_error "Please run 'cd blackbird && mkdir build && cd build && cmake .. && make' first"
    exit 1
fi

cd "$BUILD_DIR"

# Check if binaries exist
if [ ! -f "examples/keystone_example" ]; then
    print_error "keystone_example binary not found. Please build the project first."
    exit 1
fi

if [ ! -f "examples/worker_example" ]; then
    print_error "worker_example binary not found. Please build the project first."
    exit 1
fi

print_status "Starting Blackbird Local Cluster..."
print_status "Cluster ID: $CLUSTER_ID"
print_status "Keystone RPC: localhost:$KEYSTONE_PORT"
print_status "Keystone HTTP: localhost:$KEYSTONE_HTTP_PORT"
print_status "Worker Memory: $(($WORKER_MEMORY_SIZE / 1024 / 1024))MB"

# 1. Start etcd if not already running
if check_port $ETCD_PORT; then
    print_warning "etcd is already running on port $ETCD_PORT, using existing instance"
else
    print_status "Starting etcd on port $ETCD_PORT..."
    
    # Check if etcd is installed
    if ! command -v etcd &> /dev/null; then
        print_error "etcd is not installed. Please install etcd first:"
        print_error "  Ubuntu: sudo apt install etcd"
        print_error "  macOS: brew install etcd"
        exit 1
    fi
    
    etcd --name=blackbird-etcd \
         --data-dir=/tmp/blackbird-etcd \
         --listen-client-urls="http://localhost:$ETCD_PORT" \
         --advertise-client-urls="http://localhost:$ETCD_PORT" \
         --listen-peer-urls="http://localhost:2380" \
         --initial-advertise-peer-urls="http://localhost:2380" \
         --initial-cluster="blackbird-etcd=http://localhost:2380" \
         --initial-cluster-token="blackbird-cluster" \
         --initial-cluster-state="new" \
         > /tmp/blackbird-etcd.log 2>&1 &
    
    ETCD_PID=$!
    PIDS+=($ETCD_PID)
    
    if ! wait_for_service $ETCD_PORT "etcd"; then
        exit 1
    fi
fi

# 2. Start Keystone
print_status "Starting Keystone service..."

if check_port $KEYSTONE_PORT; then
    print_error "Port $KEYSTONE_PORT is already in use. Please stop the existing service."
    exit 1
fi

./examples/keystone_example \
    --etcd-endpoints="localhost:$ETCD_PORT" \
    --listen-address="0.0.0.0:$KEYSTONE_PORT" \
    --http-port="$KEYSTONE_HTTP_PORT" \
    --cluster-id="$CLUSTER_ID" \
    > /tmp/blackbird-keystone.log 2>&1 &

KEYSTONE_PID=$!
PIDS+=($KEYSTONE_PID)

if ! wait_for_service $KEYSTONE_PORT "Keystone"; then
    exit 1
fi

# 3. Start Worker
print_status "Starting Worker service..."

./examples/worker_example \
    --worker-id="localhost-worker-1" \
    --node-id="localhost" \
    --etcd-endpoints="localhost:$ETCD_PORT" \
    --cluster-id="$CLUSTER_ID" \
    --memory-size="$WORKER_MEMORY_SIZE" \
    --storage-class="RAM_CPU" \
    > /tmp/blackbird-worker.log 2>&1 &

WORKER_PID=$!
PIDS+=($WORKER_PID)

# Give worker a moment to start
sleep 2

# 4. Verify cluster status
print_status "Verifying cluster status..."

# Test connectivity to keystone
if ./examples/simple_client_test --host=localhost --rpc-port=$KEYSTONE_PORT >/dev/null 2>&1; then
    print_success "Keystone connectivity test passed"
else
    print_warning "Keystone connectivity test failed (this might be normal if no RPC client is implemented yet)"
fi

print_success "Blackbird cluster is running!"
echo
print_status "Services:"
print_status "  etcd:     http://localhost:$ETCD_PORT"
print_status "  Keystone: http://localhost:$KEYSTONE_PORT (RPC), http://localhost:$KEYSTONE_HTTP_PORT (HTTP)"
print_status "  Worker:   localhost-worker-1"
echo
print_status "Logs:"
print_status "  etcd:     tail -f /tmp/blackbird-etcd.log"
print_status "  Keystone: tail -f /tmp/blackbird-keystone.log"
print_status "  Worker:   tail -f /tmp/blackbird-worker.log"
echo
print_status "Press Ctrl+C to stop all services"

# Keep script running and show live logs
while true; do
    # Check if any process died
    for i in "${!PIDS[@]}"; do
        pid=${PIDS[$i]}
        if ! kill -0 $pid 2>/dev/null; then
            print_error "Process $pid died unexpectedly"
            exit 1
        fi
    done
    
    sleep 5
done 