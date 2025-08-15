# Blackbird Examples

This directory contains example applications and test clients for the Blackbird distributed object store.

## Building Examples

```bash
cd blackbird/build
make -j4
```

## Example Programs

### System Components

#### worker_example.cpp
Demonstrates how to start a standalone Worker service with memory pools, etcd registration, and TTL-based heartbeat mechanism.

```bash
./examples/worker_example [config_file]
```

#### keystone_example.cpp
Demonstrates how to start a standalone Keystone (master) service for cluster coordination, worker discovery, and object placement decisions.

```bash
./examples/keystone_example [config_file]
```

#### simple_client_test.cpp
Basic connectivity test for Keystone RPC service.

```bash
./examples/simple_client_test --host <keystone_host> --port <keystone_port>
```

### Data Transfer Clients

#### ucx_client.cpp
Full-featured UCX data transfer demonstration with object placement, data verification, and performance metrics.

```bash
./examples/ucx_client \
    --keystone 127.0.0.1:9090 \
    --etcd localhost:2379 \
    --cluster blackbird_cluster \
    --key "my-object-$(date +%s)" \
    --size 1048576 \
    --replicas 1
```

#### benchmark_client.cpp
High-throughput UCX performance benchmarking with random data generation and comprehensive statistics.

```bash
./examples/benchmark_client \
    --size 104857600 \
    --iterations 10 \
    --key-prefix "bench" \
    --replicas 1
```

## Quick Start Guide

### 1. Start the Cluster
```bash
cd blackbird/scripts
./start_cluster.sh
```

### 2. Verify Services
```bash
# Check etcd registration
ETCDCTL_API=3 etcdctl get --prefix "/blackbird"

# Test Keystone connectivity  
./examples/simple_client_test --host 127.0.0.1 --port 9090
```

### 3. Transfer Data
```bash
# Small demo transfer
./examples/ucx_client \
    --keystone 127.0.0.1:9090 \
    --etcd localhost:2379 \
    --cluster blackbird_cluster \
    --key "demo-$(date +%s)" \
    --size 64

# Performance benchmark
./examples/benchmark_client --size 10485760 --iterations 5
```

## Configuration

### Command Line Arguments

| Argument | Description | Default | Example |
|----------|-------------|---------|---------|
| `--keystone` | Keystone RPC endpoint | `127.0.0.1:9090` | `192.168.1.100:9090` |
| `--etcd` | etcd endpoint | `localhost:2379` | `etcd-cluster:2379` |
| `--cluster` | Cluster name | `blackbird_cluster` | `prod_cluster` |
| `--key` | Object key name | Required | `my-data-123` |
| `--size` | Data size in bytes | `4096` | `1048576` |
| `--replicas` | Number of replicas | `1` | `3` |
| `--iterations` | Benchmark iterations | `1` | `10` |

## Troubleshooting

### Common Issues

**put_start returned error: 5001**  
Object key already exists. Use unique keys with `$(date +%s)` or delete existing objects.

**Failed to fetch any pool from etcd**  
Worker not registered or etcd connection issue. Verify worker is running and etcd is accessible.

**UCX connection failed**  
Network connectivity or UCX protocol mismatch. Check firewall rules and UCX endpoints.

### Debug Commands
```bash
# Check cluster state
ETCDCTL_API=3 etcdctl get --prefix "/blackbird"

# Monitor logs
tail -f /tmp/blackbird_*.log

# Network connectivity
telnet 127.0.0.1 9090  # Keystone RPC
telnet 127.0.0.1 2379  # etcd
```

## Usage Recommendations

For development and testing, use `ucx_client` with small data sizes and single replicas. For performance evaluation, use `benchmark_client` with production-sized data and multiple iterations. Build custom clients using these examples as implementation patterns.