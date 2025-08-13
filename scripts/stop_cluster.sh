#!/bin/bash

# Blackbird Local Cluster Stop Script
# This script stops all blackbird-related processes

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

print_status "Stopping Blackbird cluster..."

# Stop processes by name (including etcd started by our script)
processes=("keystone_example" "worker_example" "etcd")
stopped_count=0

# First, try to stop processes gracefully
for process in "${processes[@]}"; do
    pids=$(pgrep -f "$process" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        print_status "Stopping $process (PIDs: $pids)"
        echo "$pids" | xargs kill -TERM 2>/dev/null || true
        stopped_count=$((stopped_count + 1))
    fi
done

# Wait a bit for graceful shutdown
if [ $stopped_count -gt 0 ]; then
    print_status "Waiting for graceful shutdown..."
    sleep 3
    
    # Force kill any remaining processes
    for process in "${processes[@]}"; do
        pids=$(pgrep -f "$process" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            print_warning "Force killing $process (PIDs: $pids)"
            echo "$pids" | xargs kill -9 2>/dev/null || true
        fi
    done
fi

# Clean up temporary files and directories
print_status "Cleaning up temporary files..."

if [ -f "/tmp/blackbird-etcd.log" ]; then
    rm -f /tmp/blackbird-etcd.log
fi

if [ -f "/tmp/blackbird-keystone.log" ]; then
    rm -f /tmp/blackbird-keystone.log
fi

if [ -f "/tmp/blackbird-worker.log" ]; then
    rm -f /tmp/blackbird-worker.log
fi

if [ -d "/tmp/blackbird-etcd" ]; then
    print_status "Removing etcd data directory..."
    rm -rf /tmp/blackbird-etcd
fi

if [ $stopped_count -gt 0 ]; then
    print_success "Blackbird cluster stopped"
else
    print_warning "No Blackbird processes found running"
fi 