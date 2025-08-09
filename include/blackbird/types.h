#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <iostream>

#include <glog/logging.h>
#include <nlohmann/json.hpp>

#include "blackbird/error/error_codes.h"

namespace blackbird {

// Type aliases for improved readability and type safety
using ObjectKey = std::string;
using Version = uint64_t;
using SegmentId = std::string;  // Use string for better readability
using NodeId = std::string;
using UUID = std::pair<uint64_t, uint64_t>;

// UCX-specific types
using UcxAddress = std::vector<uint8_t>;  // UCX worker address
using UcxRkey = uint64_t;                 // UCX remote key

// Etcd types
using EtcdRevisionId = int64_t;
using ViewVersionId = EtcdRevisionId;
using EtcdLeaseId = int64_t;

// Constants
static constexpr uint64_t DEFAULT_WORKER_COUNT = 1;
static constexpr uint64_t DEFAULT_KV_TTL_MS = 30 * 60 * 1000;  // 30 minutes
static constexpr double DEFAULT_EVICTION_RATIO = 0.1;
static constexpr double DEFAULT_HIGH_WATERMARK = 0.9;
static constexpr int64_t DEFAULT_CLIENT_TTL_SEC = 10;
static const std::string DEFAULT_CLUSTER_ID = "blackbird_cluster";

// Utility function for UUID generation
UUID generate_uuid();

inline std::ostream& operator<<(std::ostream& os, const UUID& uuid) noexcept {
    os << uuid.first << "-" << uuid.second;
    return os;
}

/**
 * @brief Status of a worker-held object placement
 */
enum class WorkerStatus {
    UNDEFINED = 0,
    ALLOCATED,     // Memory allocated, waiting for data
    WRITING,       // Data transfer in progress
    COMPLETE,      // Data transfer complete, worker ready
    FAILED,        // Transfer failed or data corrupted
    REMOVED,       // Data has been deleted from worker
};

inline std::ostream& operator<<(std::ostream& os, const WorkerStatus& status) noexcept {
    static const std::unordered_map<WorkerStatus, std::string_view> status_strings{
        {WorkerStatus::UNDEFINED, "UNDEFINED"},
        {WorkerStatus::ALLOCATED, "ALLOCATED"},
        {WorkerStatus::WRITING, "WRITING"},
        {WorkerStatus::COMPLETE, "COMPLETE"},
        {WorkerStatus::FAILED, "FAILED"},
        {WorkerStatus::REMOVED, "REMOVED"}
    };
    auto it = status_strings.find(status);
    os << (it != status_strings.end() ? it->second : "UNKNOWN");
    return os;
}

/**
 * @brief Client status from keystone's perspective
 */
enum class ClientStatus {
    UNDEFINED = 0,
    ACTIVE,        // Client is active and healthy
    NEED_REFRESH,  // Client needs to refresh its view
    STALE,         // Client hasn't pinged recently
};

inline std::ostream& operator<<(std::ostream& os, const ClientStatus& status) noexcept {
    static const std::unordered_map<ClientStatus, std::string_view> status_strings{
        {ClientStatus::UNDEFINED, "UNDEFINED"},
        {ClientStatus::ACTIVE, "ACTIVE"},
        {ClientStatus::NEED_REFRESH, "NEED_REFRESH"},
        {ClientStatus::STALE, "STALE"}
    };
    auto it = status_strings.find(status);
    os << (it != status_strings.end() ? it->second : "UNKNOWN");
    return os;
}

/**
 * @brief Represents a memory slice for data operations
 */
struct Slice {
    void* ptr{nullptr};
    size_t size{0};

    Slice() = default;
    Slice(void* p, size_t s) : ptr(p), size(s) {}
};

/**
 * @brief Configuration for worker placement and management
 */
struct WorkerConfig {
    size_t worker_count{DEFAULT_WORKER_COUNT};
    bool enable_soft_pin{false};
    std::string preferred_node{};  // Preferred node for primary placement
    uint64_t ttl_ms{DEFAULT_KV_TTL_MS};

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(WorkerConfig, worker_count, enable_soft_pin, preferred_node, ttl_ms)

    friend std::ostream& operator<<(std::ostream& os, const WorkerConfig& config) noexcept {
        return os << "WorkerConfig{worker_count=" << config.worker_count
                  << ", enable_soft_pin=" << config.enable_soft_pin
                  << ", preferred_node=" << config.preferred_node
                  << ", ttl_ms=" << config.ttl_ms << "}";
    }
};

// Placement model types

/**
 * @brief UCX region for remote memory access
 */
struct UcxRegion {
    UcxAddress worker_address;  // UCX worker address
    UcxRkey remote_key;         // UCX remote key for RDMA access
    uintptr_t remote_addr;      // Remote memory address
    size_t size;                // Region size

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(UcxRegion, worker_address, remote_key, remote_addr, size)
};

enum class MemoryKind { UNSPECIFIED = 0, CPU_DRAM = 1, GPU_HBM = 2 };
enum class DiskKind { UNSPECIFIED = 0, NVME = 1, SSD = 2, HDD = 3 };

/**
 * @brief Memory placement details (CPU/GPU)
 */
struct MemoryPlacement {
    MemoryKind kind{MemoryKind::UNSPECIFIED};
    std::vector<UcxRegion> regions;  // RDMA-accessible regions
    int32_t device_id{-1};           // GPU device id if GPU memory, else -1
    int32_t numa_node{-1};           // NUMA node for CPU memory, else -1

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MemoryPlacement, kind, regions, device_id, numa_node)
};

/**
 * @brief Disk placement details
 */
struct DiskPlacement {
    DiskKind kind{DiskKind::UNSPECIFIED};
    std::string mount_path;     // mount point or base path
    size_t capacity{0};         // bytes reserved for this placement
    std::string node_id;        // node hosting this disk placement

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DiskPlacement, kind, mount_path, capacity, node_id)
};

/**
 * @brief Unified worker placement supporting both memory and disk storage
 */
struct WorkerPlacement {
    std::variant<MemoryPlacement, DiskPlacement> storage;
    WorkerStatus status{WorkerStatus::UNDEFINED};
    std::string node_id;  // Node hosting this placement

    bool is_memory_placement() const noexcept {
        return std::holds_alternative<MemoryPlacement>(storage);
    }

    bool is_disk_placement() const noexcept {
        return std::holds_alternative<DiskPlacement>(storage);
    }

    const MemoryPlacement& get_memory_placement() const {
        return std::get<MemoryPlacement>(storage);
    }

    const DiskPlacement& get_disk_placement() const {
        return std::get<DiskPlacement>(storage);
    }

    MemoryPlacement& get_memory_placement() {
        return std::get<MemoryPlacement>(storage);
    }

    DiskPlacement& get_disk_placement() {
        return std::get<DiskPlacement>(storage);
    }
};

/**
 * @brief Represents a memory segment (chunk) in the distributed cache
 */
struct Segment {
    SegmentId id;
    NodeId node_id;         // Node that owns this segment
    uintptr_t base_addr{0}; // Base address of the segment
    size_t size{0};         // Total size of the segment
    size_t used{0};         // Currently used space
    UcxAddress ucx_address; // UCX worker address for this segment

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Segment, id, node_id, base_addr, size, used, ucx_address)

    double utilization() const noexcept {
        return size > 0 ? static_cast<double>(used) / size : 0.0;
    }

    size_t available() const noexcept {
        return size > used ? size - used : 0;
    }
};

/**
 * @brief Response structure for ping operations
 */
struct PingResponse {
    ViewVersionId view_version;
    ClientStatus client_status;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(PingResponse, view_version, client_status)

    friend std::ostream& operator<<(std::ostream& os, const PingResponse& response) noexcept {
        return os << "PingResponse{view_version=" << response.view_version
                  << ", client_status=" << response.client_status << "}";
    }
};

/**
 * @brief Configuration for the keystone service
 */
struct KeystoneConfig {
    std::string cluster_id{DEFAULT_CLUSTER_ID};
    std::string etcd_endpoints;  // Comma-separated etcd endpoints
    std::string listen_address{"0.0.0.0:9090"};
    std::string http_metrics_port{"9091"};

    bool enable_gc{true};
    bool enable_ha{false};  // High availability mode
    double eviction_ratio{DEFAULT_EVICTION_RATIO};
    double high_watermark{DEFAULT_HIGH_WATERMARK};
    int64_t client_ttl_sec{DEFAULT_CLIENT_TTL_SEC};
    int64_t worker_heartbeat_ttl_sec{30};  // Worker considered stale after 30s without heartbeat

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(KeystoneConfig, cluster_id, etcd_endpoints, listen_address,
                                   http_metrics_port, enable_gc, enable_ha, eviction_ratio,
                                   high_watermark, client_ttl_sec, worker_heartbeat_ttl_sec)
};

/**
 * @brief Configuration for client nodes
 */
struct ClientConfig {
    std::string node_id;
    std::string keystone_address;
    std::string local_address{"0.0.0.0:0"};  // Let UCX choose port

    size_t memory_pool_size{1ULL << 30};  // 1GB default
    std::string storage_path;  // Optional disk storage path

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ClientConfig, node_id, keystone_address, local_address,
                                   memory_pool_size, storage_path)
};

}  // namespace blackbird

namespace std {

template<>
struct hash<blackbird::UUID> {
    size_t operator()(const blackbird::UUID& id) const noexcept {
        uint64_t a = id.first;
        uint64_t b = id.second;
        a ^= b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2);
        a ^= a >> 33;
        a *= 0xff51afd7ed558ccdULL;
        a ^= a >> 33;
        a *= 0xc4ceb9fe1a85ec53ULL;
        a ^= a >> 33;
        return static_cast<size_t>(a);
    }
};

} // namespace std 