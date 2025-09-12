# Blackbird: High-Performance Distributed Storage System

Blackbird is a high-performance, multi-tiered distributed storage cache system designed for modern applications requiring fast, scalable data access. Built on UCX (Unified Communication X) for optimal RDMA performance, Blackbird provides intelligent data placement across memory and storage tiers.

## Key Features

- **High Performance**: Leverages UCX for low-latency RDMA communication
- **Multi-Tiered Storage**: Intelligent placement across GPU memory, CPU memory, and NVMe storage
- **High Availability**: Keystone election and failover support
- **Worker Placement**: Sophisticated placement algorithms for optimal performance
- **Etcd Integration**: Distributed coordination and service discovery
- **Batch Operations**: High-throughput batch API support

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Blackbird     │    │   Blackbird     │    │   Blackbird     │
│   Client        │    │   Client        │    │   Client        │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ UCX Memory  │ │    │ │ UCX Memory  │ │    │ │ UCX Memory  │ │
│ │ Pool        │ │    │ │ Pool        │ │    │ │ Pool        │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                   ┌─────────────┴───────────┐
                   │   Blackbird Keystone    │
                   │                         │
                   │ ┌─────────────────────┐ │
                   │ │ Object Metadata     │ │
                   │ │ Manager             │ │
                   │ └─────────────────────┘ │
                   │ ┌─────────────────────┐ │
                   │ │ Worker Placement    │ │
                   │ │ Engine              │ │
                   │ └─────────────────────┘ │
                   │ ┌─────────────────────┐ │
                   │ │ Client Health       │ │
                   │ │ Monitor             │ │
                   │ └─────────────────────┘ │
                   └─────────────┬───────────┘
                                 │
                    ┌────────────┴────────────┐
                    │      etcd Cluster       │
                    │   (Service Discovery    │
                    │   & Coordination)       │
                    └─────────────────────────┘
```

## Core Components

### 1. Keystone Service
- **Object Metadata Management**: Tracks object locations and worker status
- **Worker Placement**: Intelligent placement of workers across nodes
- **Client Health Monitoring**: Monitors client health and handles failures
- **Garbage Collection**: Automatic cleanup of expired objects
- **Load Balancing**: Distributes load across available nodes

### 2. Etcd Integration
- **Service Discovery**: Automatic discovery of cluster nodes
- **Leader Election**: High availability through master election
- **Configuration Management**: Distributed configuration storage
- **Health Monitoring**: Cluster-wide health status tracking

### 3. UCX Communication Layer
- **RDMA Support**: Direct memory access for ultra-low latency
- **Multiple Transports**: TCP, InfiniBand, RoCE, and more
- **Zero-copy Operations**: Minimal CPU overhead for data transfers
- **Scalable**: Supports large-scale deployments

## Getting Started

### Prerequisites

- **C++20 compatible compiler** (GCC 10+, Clang 12+)
- **CMake 3.20+**
- **UCX library** (1.12+)
- **etcd cluster** (3.4+)
- **glog** for logging
- **nlohmann/json** for JSON support
- **yaLanTingLibs (YLT)** for RPC framework

### Installation

1. **Install Dependencies**:
   ```bash
   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install cmake build-essential libgoogle-glog-dev nlohmann-json3-dev
   
   # Install UCX (from source or package manager)
   # Install etcd
   # Install YLT (yaLanTingLibs)
   ```

2. **Clone and Build**:
   ```bash
   git clone <Blackbird-repo>
   cd Blackbird
   mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..
   make -j$(nproc)
   ```

3. **Install** (optional):
   ```bash
   sudo make install
   ```

### Quick Start

1. **Start etcd** (if not already running):
   ```bash
   etcd --listen-client-urls http://localhost:2379 \
        --advertise-client-urls http://localhost:2379
   ```

2. **Start Keystone**:
```bash
./examples/keystone_example --etcd-endpoints localhost:2379
```

3. **The keystone will**:
   - Connect to etcd for coordination
   - Start RPC server on port 9090
   - Start HTTP metrics server on port 9091
   - Begin accepting worker and client connections

### Keystone Configuration

```json
{
  "cluster_id": "Blackbird_cluster",
  "etcd_endpoints": "localhost:2379",
  "listen_address": "0.0.0.0:9090",
  "http_metrics_port": "9091",
  "enable_gc": true,
  "enable_ha": true,
  "eviction_ratio": 0.1,
  "high_watermark": 0.9,
  "client_ttl_sec": 10
}
```

### Client Configuration

```json
{
  "node_id": "client-001",
  "keystone_address": "localhost:9090",
  "local_address": "0.0.0.0:0",
  "memory_pool_size": 1073741824,
  "storage_path": "/tmp/Blackbird"
}
```

## API Overview

### Core Operations
```cpp
// Check if object exists
auto exists = keystone_service->object_exists("my_key");

// Get worker placements for an object
auto workers = keystone_service->get_workers("my_key");

// Start a put operation
auto allocated_workers = keystone_service->put_start("my_key", data_size, worker_config);

// Complete the put operation
auto result = keystone_service->put_complete("my_key");

// Remove an object
auto result2 = keystone_service->remove_object("my_key");

### Batch Operations

```cpp
// Batch existence check
auto results = keystone_service->batch_object_exists(keys);

// Batch get workers
auto worker_results = keystone_service->batch_get_workers(keys);

// Batch put operations
auto put_results = keystone_service->batch_put_start(keys, sizes, config);
```

## Data Models

### Object Storage
- **Keys**: String-based object identifiers
- **Workers**: One or more placements per key with configurable policies
- **TTL**: Automatic expiration support
- **Soft Pinning**: Protection from eviction

### UCX Integration
- **Remote Memory Access**: Direct RDMA operations
- **Worker Addresses**: UCX endpoint identification
- **Remote Keys**: RDMA memory registration keys
- **Zero-copy Transfers**: Efficient data movement

## Monitoring

### Metrics Endpoint
Access cluster metrics via HTTP:
```bash
curl http://localhost:9091/metrics
```

### Cluster Statistics
```cpp
auto stats = keystone_service->get_cluster_stats();
if (is_ok(stats)) {
    auto cluster_stats = get_value(stats);
    std::cout << "Active clients: " << cluster_stats.active_clients << std::endl;
    std::cout << "Total objects: " << cluster_stats.total_objects << std::endl;
    std::cout << "Utilization: " << (cluster_stats.utilization * 100) << "%" << std::endl;
}
```

### Health Checks
- **Client Heartbeats**: Regular ping operations
- **Chunk Monitoring**: Memory chunk health
- **Automatic Recovery**: Failed client cleanup

## Development

## Project Structure

```
Blackbird/
├── include/Blackbird/          # Public headers
│   ├── types.h                 # Core types and configuration
│   ├── keystone_service.h      # Keystone service
│   ├── rpc_service.h           # RPC service wrapper
│   └── etcd_service.h          # Etcd integration
├── src/                        # Implementation files
│   ├── types.cpp
│   ├── keystone_service.cpp
│   ├── rpc_service.cpp
│   ├── etcd_service.cpp
│   └── error/                  # Error handling
├── examples/                   # Usage examples
│   └── setup_example.cpp
├── proto/                      # Protocol buffer definitions
└── CMakeLists.txt
```

### Building Tests
```bash
cmake -DBUILD_TESTS=ON ..
make -j$(nproc)
ctest
```

## Performance Characteristics

- **Latency**: Sub-microsecond for RDMA operations
- **Throughput**: Limited by network bandwidth
- **Scalability**: Supports 100+ node clusters
- **Memory Efficiency**: Zero-copy operations
- **CPU Overhead**: Minimal due to RDMA offload

## Roadmap

- [ ] **Phase 1**: Complete keystone service implementation
- [ ] **Phase 2**: Client library with UCX integration
- [ ] **Phase 3**: Storage backend implementation
- [ ] **Phase 4**: Advanced features (compression, encryption)
- [ ] **Phase 5**: Performance optimizations and benchmarks

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Comparison with Other Systems

| Feature | Blackbird | Redis Cluster | Memcached | Alluxio |
|---------|-----------|---------------|-----------|---------|
| RDMA Support | ✅ Native | ❌ | ❌ | ⚠️ Limited |
| Multi-tier | ✅ | ❌ | ❌ | ✅ |
| Service Discovery | ✅ etcd | ⚠️ Manual | ❌ | ✅ |
| High Availability | ✅ | ✅ | ❌ | ✅ |
| Language | C++20 | C | C | Java/Scala |
| Memory Efficiency | ✅ Zero-copy | ❌ | ⚠️ | ❌ |

## Contact

For questions, issues, or contributions, please use the project's issue tracker or contact the maintainers. 
