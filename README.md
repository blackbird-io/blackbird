<br>
<p align="center">
  <img src="assets/logo.svg" alt="Blackbird Logo" width="2000"/>
</p>
<br>

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]() [![License: MIT](https://img.shields.io/badge/license-MIT-blue)]() [![C++20](https://img.shields.io/badge/language-C++20-lightgrey)]()

# Blackbird

**A High-Performance RDMA based Distributed Storage System.**
<br>

Blackbird draws inspiration from [Microsoft/FARM](https://www.microsoft.com/en-us/research/project/farm/) and RDMA based KV store among other projects. It also takes cues from Redis for its simplicity and ubiquity. It delivers intelligent data placement, enabling applications to seamlessly offload data to a high-performance tiered system managing placement and latency for you.

**Use cases:** HPC & ML training/inference pipelines, realtime analytics, feature stores, and metadata-heavy services where Redis/Memcached lack tiering or RDMA, and Alluxio is too heavyweight.

---

## ✨ Key Features

- **RDMA-first performance:** UCX (RoCE/InfiniBand) with TCP fallback; zero-copy fast path  
- **Tiered caching:** GPU memory → CPU DRAM → NVMe; policy-driven placement and eviction  
- **High availability:** Keystone control-plane with leader election & failover (etcd)  
- **Placement engine:** Topology-aware worker selection & load balancing  
- **Batch APIs:** High-throughput batched puts/gets/exists  
- **Observability:** Prometheus-style `/metrics`, health, and cluster stats


### Run Example
```bash
# 1) Start etcd
etcd --listen-client-urls http://localhost:2379 \
     --advertise-client-urls http://localhost:2379

# 2) Start Keystone (example)
./examples/keystone_example --etcd-endpoints localhost:2379
# Keystone:
#  - RPC: :9090
#  - Metrics (Prometheus): :9091/metrics
```

---

## ⚙️ Configuration

### Keystone (JSON)
```json
{
  "cluster_id": "blackbird_cluster",
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

### Client (JSON)
```json
{
  "node_id": "client-001",
  "keystone_address": "localhost:9090",
  "local_address": "0.0.0.0:0",
  "memory_pool_size": 1073741824,
  "storage_path": "/tmp/blackbird"
}
```

---

## 🧪 API Overview (C++)

```cpp
// Existence
auto exists = keystone_service->object_exists("my_key");

// Worker lookup
auto workers = keystone_service->get_workers("my_key");

// Put workflow
auto placements = keystone_service->put_start("my_key", data_size, worker_config);
// ... perform UCX transfers to placements ...
auto ok = keystone_service->put_complete("my_key");

// Remove
auto removed = keystone_service->remove_object("my_key");

// Batch ops
auto ex = keystone_service->batch_object_exists(keys);
auto w  = keystone_service->batch_get_workers(keys);
auto ps = keystone_service->batch_put_start(keys, sizes, config);
```

**Data model basics**
- **Key:** string identifier  
- **Placements:** one-or-more workers per key (policy-driven)  
- **TTL:** optional expiry; **Soft pin:** opt-out of eviction  
- **UCX fields:** endpoint addresses, rkeys, region descriptors

---

## 📊 Monitoring & Health

```bash
# Prometheus metrics
curl -s http://localhost:9091/metrics | head -n 50
```

Programmatic stats:
```cpp
auto stats = keystone_service->get_cluster_stats();
if (is_ok(stats)) {
  auto s = get_value(stats);
  std::cout << "Active clients: " << s.active_clients << "\n";
  std::cout << "Total objects:  " << s.total_objects << "\n";
  std::cout << "Utilization:    " << (s.utilization * 100) << "%\n";
}
```

Health signals:
- Client heartbeats & TTL expiry  
- Worker liveness & chunk health  
- Automatic recovery & cleanup of orphaned placements

---

## 📦 Core Components

### Keystone (control plane)
- Object metadata & locations; worker liveness/status  
- Placement & load balancing; admission control  
- Client/session tracking; automatic failure handling  
- TTL/GC of objects; eviction coordination

### Clients/Workers (data plane)
- UCX endpoints; registered memory for RDMA  
- Local tier managers (GPU/DRAM/NVMe) with pluggable policies  
- Background compaction/defragmentation (future)

### etcd Integration
- Service discovery & registration  
- Leader election for Keystone HA  
- Distributed configuration and health registry

## ⚡ Quick Start

### Prerequisites
- **C++20** compiler (GCC ≥10 or Clang ≥12)  
- **CMake ≥3.20**  
- **UCX ≥1.12**  
- **etcd ≥3.4**  
- Libraries: `glog`, `nlohmann/json`, [yaLanTingLibs](https://github.com/alibaba/yalantinglibs)

### Build from Source
```bash
git clone https://github.com/blackbird-io/blackbird.git
cd blackbird
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j"$(nproc)"
sudo make install # optional
```


### Build & Run Tests
```bash
cmake -S . -B build -DBUILD_TESTS=ON
cmake --build build -j"$(nproc)"
cd build && ctest --output-on-failure
```

## 🔭 Roadmap

- [ ] **v0.1:** Keystone MVP, basic client SDK, Prometheus metrics  
- [ ] **v0.2:** UCX client library GA, placement policies, benchmark suite  
- [ ] **v0.3:** Tier managers (GPU/DRAM/NVMe) + compaction/defrag  
- [ ] **v0.4:** Security (mTLS), ACLs, encryption-at-rest/in-flight  
- [ ] **v1.0:** Stability, perf tuning, operability hardening

---

## 🔍 Comparison

| Feature            | Blackbird | Redis Cluster | Memcached | Alluxio |
|--------------------|-----------|---------------|-----------|---------|
| RDMA Support       | ✅ Native | ❌            | ❌        | ⚠️ Limited |
| Multi-tier Caching | ✅        | ❌            | ❌        | ✅       |
| Service Discovery  | ✅ etcd   | ⚠️ Manual     | ❌        | ✅       |
| High Availability  | ✅        | ✅            | ❌        | ✅       |
| Language           | C++20     | C             | C         | Java/Scala |

## 🤝 Contributing

We welcome issues and PRs.

1. Fork the repo  
2. Create a branch: `git checkout -b feature/awesome`  
3. Add your feature.
4. `clang-format`/`cppcheck` if available  
5. Open a PR

See `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md` (coming soon).

---

## 📜 License

MIT — see [LICENSE](LICENSE).
