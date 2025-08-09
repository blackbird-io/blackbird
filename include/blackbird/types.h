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
static constexpr uint64_t DEFAULT_REPLICA_COUNT = 1;
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
 * @brief Status of a replica in the system
 */
enum class ReplicaStatus {
    UNDEFINED = 0,
    ALLOCATED,     // Memory allocated, waiting for data
    WRITING,       // Data transfer in progress
    COMPLETE,      // Data transfer complete, replica ready
    FAILED,        // Transfer failed or replica corrupted
    REMOVED,       // Replica has been deleted
};

inline std::ostream& operator<<(std::ostream& os, const ReplicaStatus& status) noexcept {
    static const std::unordered_map<ReplicaStatus, std::string_view> status_strings{
        {ReplicaStatus::UNDEFINED, "UNDEFINED"},
        {ReplicaStatus::ALLOCATED, "ALLOCATED"},
        {ReplicaStatus::WRITING, "WRITING"},
        {ReplicaStatus::COMPLETE, "COMPLETE"},
        {ReplicaStatus::FAILED, "FAILED"},
        {ReplicaStatus::REMOVED, "REMOVED"}
    };
    
    auto it = status_strings.find(status);
    os << (it != status_strings.end() ? it->second : "UNKNOWN");
    return os;
}

/**
 * @brief Client status from master's perspective
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
 * @brief Configuration for replica placement and management
 */
struct ReplicaConfig {
    size_t replica_count{DEFAULT_REPLICA_COUNT};
    bool enable_soft_pin{false};
    std::string preferred_node{};  // Preferred node for primary replica
    uint64_t ttl_ms{DEFAULT_KV_TTL_MS};
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ReplicaConfig, replica_count, enable_soft_pin, preferred_node, ttl_ms)
    
    friend std::ostream& operator<<(std::ostream& os, const ReplicaConfig& config) noexcept {
        return os << "ReplicaConfig{replica_count=" << config.replica_count 
                  << ", enable_soft_pin=" << config.enable_soft_pin
                  << ", preferred_node=" << config.preferred_node
                  << ", ttl_ms=" << config.ttl_ms << "}";
    }
};

/**
 * @brief UCX-specific buffer descriptor for remote memory access
 */
struct UcxBufferDescriptor {
    UcxAddress worker_address;  // UCX worker address
    UcxRkey remote_key;         // UCX remote key for RDMA access
    uintptr_t remote_addr;      // Remote memory address
    size_t size;                // Buffer size
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(UcxBufferDescriptor, worker_address, remote_key, remote_addr, size)
};

/**
 * @brief Memory-based replica descriptor
 */
struct MemoryDescriptor {
    std::vector<UcxBufferDescriptor> buffers;
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MemoryDescriptor, buffers)
};

/**
 * @brief Disk-based replica descriptor
 */
struct DiskDescriptor {
    std::string file_path;
    size_t file_size{0};
    std::string node_id;  // Node where the file is stored
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DiskDescriptor, file_path, file_size, node_id)
};

/**
 * @brief Unified replica descriptor supporting both memory and disk storage
 */
struct ReplicaDescriptor {
    std::variant<MemoryDescriptor, DiskDescriptor> storage;
    ReplicaStatus status{ReplicaStatus::UNDEFINED};
    std::string node_id;  // Node hosting this replica
    
    // Helper methods
    bool is_memory_replica() const noexcept {
        return std::holds_alternative<MemoryDescriptor>(storage);
    }
    
    bool is_disk_replica() const noexcept {
        return std::holds_alternative<DiskDescriptor>(storage);
    }
    
    const MemoryDescriptor& get_memory_descriptor() const {
        return std::get<MemoryDescriptor>(storage);
    }
    
    const DiskDescriptor& get_disk_descriptor() const {
        return std::get<DiskDescriptor>(storage);
    }
    
    MemoryDescriptor& get_memory_descriptor() {
        return std::get<MemoryDescriptor>(storage);
    }
    
    DiskDescriptor& get_disk_descriptor() {
        return std::get<DiskDescriptor>(storage);
    }
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ReplicaDescriptor, storage, status, node_id)
};

/**
 * @brief Represents a memory segment in the distributed cache
 */
struct Segment {
    SegmentId id;
    NodeId node_id;         // Node that owns this segment
    uintptr_t base_addr{0}; // Base address of the segment
    size_t size{0};         // Total size of the segment
    size_t used{0};         // Currently used space
    UcxAddress ucx_address; // UCX worker address for this segment
    
    // JSON serialization support
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
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(PingResponse, view_version, client_status)
    
    friend std::ostream& operator<<(std::ostream& os, const PingResponse& response) noexcept {
        return os << "PingResponse{view_version=" << response.view_version
                  << ", client_status=" << response.client_status << "}";
    }
};

/**
 * @brief Configuration for the master service
 */
struct MasterConfig {
    std::string cluster_id{DEFAULT_CLUSTER_ID};
    std::string etcd_endpoints;  // Comma-separated etcd endpoints
    std::string listen_address{"0.0.0.0:9090"};
    std::string http_metrics_port{"9091"};
    
    bool enable_gc{true};
    bool enable_ha{false};  // High availability mode
    double eviction_ratio{DEFAULT_EVICTION_RATIO};
    double high_watermark{DEFAULT_HIGH_WATERMARK};
    int64_t client_ttl_sec{DEFAULT_CLIENT_TTL_SEC};
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MasterConfig, cluster_id, etcd_endpoints, listen_address, 
                                   http_metrics_port, enable_gc, enable_ha, eviction_ratio, 
                                   high_watermark, client_ttl_sec)
};

/**
 * @brief Configuration for client nodes
 */
struct ClientConfig {
    std::string node_id;
    std::string master_address;
    std::string local_address{"0.0.0.0:0"};  // Let UCX choose port
    
    size_t memory_pool_size{1ULL << 30};  // 1GB default
    std::string storage_path;  // Optional disk storage path
    
    // JSON serialization support
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ClientConfig, node_id, master_address, local_address,
                                   memory_pool_size, storage_path)
};

}  // namespace blackbird 