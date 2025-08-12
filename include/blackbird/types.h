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

// YLT struct_pack support - included but not required for basic serialization
// YLT struct_pack works with POD types by default
#ifdef YLT_ENABLE_STRUCT_PACK
#include <ylt/struct_pack.hpp>
#endif

#include "blackbird/error/error_codes.h"

namespace blackbird {

/**
 * @brief Result type for operations that can fail
 */
template<typename T>
using Result = std::variant<T, ErrorCode>;

/**
 * @brief Simple expected-like helper for Result
 */
template<typename T>
bool is_ok(const Result<T>& result) {
    return std::holds_alternative<T>(result);
}

template<typename T>
T get_value(const Result<T>& result) {
    return std::get<T>(result);
}

template<typename T>
ErrorCode get_error(const Result<T>& result) {
    return std::get<ErrorCode>(result);
}

// === Type Aliases (Must be defined first) ===
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
static constexpr const char* DEFAULT_CLUSTER_ID = "blackbird_cluster";
static constexpr double DEFAULT_HIGH_WATERMARK = 0.9;
static constexpr int64_t DEFAULT_CLIENT_TTL_SEC = 10;
static constexpr size_t DEFAULT_REPLICATION_FACTOR = 3;
static constexpr size_t DEFAULT_MAX_WORKERS_PER_COPY = 4;

// === Core Types for YLT struct_pack ===

/**
 * @brief Storage class enumeration for YLT serialization
 */
enum class StorageClass : uint32_t {
    STORAGE_UNSPECIFIED = 0,
    RAM_CPU = 1,
    RAM_GPU = 2, 
    NVME = 3,
    SSD = 4,
    HDD = 5,
    CUSTOM = 999
};

/**
 * @brief UCX endpoint information for RDMA communication
 */
struct UcxEndpoint {
    std::string ip;
    uint32_t port;
    std::vector<uint8_t> worker_key;
    std::vector<uint8_t> reserved;
};

/**
 * @brief Memory location for RDMA access
 */
struct MemoryLocation {
    uint64_t remote_addr;
    uint32_t rkey;
    uint64_t size;
};

/**
 * @brief File location for disk storage
 */
struct FileLocation {
    std::string file_path;
    uint64_t file_offset;
};

/**
 * @brief Location detail using variant for type safety
 */
using LocationDetail = std::variant<MemoryLocation, FileLocation>;

/**
 * @brief Shard placement information
 */
struct ShardPlacement {
    std::string pool_id;
    std::string worker_id;
    UcxEndpoint endpoint;
    StorageClass storage_class;
    uint64_t length;
    LocationDetail location;
};

/**
 * @brief Copy placement containing multiple shards
 */
struct CopyPlacement {
    uint32_t copy_index;
    std::vector<ShardPlacement> shards;
    
    size_t shards_size() const noexcept { return shards.size(); }
};

/**
 * @brief Configuration for worker placement
 */
struct WorkerConfig {
    size_t replication_factor{DEFAULT_REPLICATION_FACTOR};  // Number of copies of data (fault tolerance)
    size_t max_workers_per_copy{DEFAULT_MAX_WORKERS_PER_COPY};                  // Max workers to shard each copy across
    bool enable_soft_pin{false};                     // Enable soft pinning
    std::string preferred_node{};                    // Preferred node for primary copy
    std::vector<StorageClass> preferred_classes{};   // Preferred storage classes
    uint64_t ttl_ms{30 * 60 * 1000};             // Time-to-live in milliseconds
    
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(WorkerConfig, replication_factor, max_workers_per_copy, enable_soft_pin, preferred_node, preferred_classes, ttl_ms)

    friend std::ostream& operator<<(std::ostream& os, const WorkerConfig& config) noexcept {
        return os << "WorkerConfig{replication_factor=" << config.replication_factor
                  << ", max_workers_per_copy=" << config.max_workers_per_copy
                  << ", enable_soft_pin=" << config.enable_soft_pin
                  << ", preferred_node=" << config.preferred_node
                  << ", preferred_classes.size=" << config.preferred_classes.size()
                  << ", ttl_ms=" << config.ttl_ms << "}";
    }
};

/**
 * @brief Cluster statistics
 */
struct ClusterStats {
    size_t total_workers{0};
    size_t active_workers{0};
    size_t total_segments{0};
    size_t total_capacity{0};     // Total storage capacity in bytes
    size_t used_capacity{0};      // Used storage capacity in bytes
    size_t total_objects{0};      // Number of stored objects
    double utilization{0.0};      // Overall cluster utilization (0.0-1.0)
};

// === RPC Request/Response Types (Direct KeystoneService Mapping) ===

/**
 * @brief Request to check if object exists
 * Maps to: Result<bool> object_exists(const ObjectKey& key)
 */
struct ObjectExistsRequest {
    ObjectKey key;
};

struct ObjectExistsResponse {
    bool exists;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request to get worker locations for an object  
 * Maps to: Result<std::vector<CopyPlacement>> get_workers(const ObjectKey& key)
 */
struct GetWorkersRequest {
    ObjectKey key;
};

struct GetWorkersResponse {
    std::vector<CopyPlacement> copies;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request to start a put operation
 * Maps to: Result<std::vector<CopyPlacement>> put_start(const ObjectKey& key, size_t size, const WorkerConfig& config)
 */
struct PutStartRequest {
    ObjectKey key;
    size_t data_size;
    WorkerConfig config;
};

struct PutStartResponse {
    std::vector<CopyPlacement> copies;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request to complete a put operation
 * Maps to: ErrorCode put_complete(const ObjectKey& key)
 */
struct PutCompleteRequest {
    ObjectKey key;
};

struct PutCompleteResponse {
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request to cancel a put operation
 * Maps to: ErrorCode put_cancel(const ObjectKey& key)
 */
struct PutCancelRequest {
    ObjectKey key;
};

struct PutCancelResponse {
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request to remove an object
 * Maps to: ErrorCode remove_object(const ObjectKey& key)
 */
struct RemoveObjectRequest {
    ObjectKey key;
};

struct RemoveObjectResponse {
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request to remove all objects
 * Maps to: Result<size_t> remove_all_objects()
 */
struct RemoveAllObjectsRequest {
    int32_t dummy{0};  // YLT struct_pack requires non-empty structs
};

struct RemoveAllObjectsResponse {
    size_t objects_removed;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request for cluster statistics
 * Maps to: Result<ClusterStats> get_cluster_stats() const
 */
struct GetClusterStatsRequest {
    int32_t dummy{0};  // YLT struct_pack requires non-empty structs
};

struct GetClusterStatsResponse {
    ClusterStats stats;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Request for view version
 * Maps to: ViewVersionId get_view_version() const noexcept
 */
struct GetViewVersionRequest {
    int32_t dummy{0};  // YLT struct_pack requires non-empty structs
};

struct GetViewVersionResponse {
    ViewVersionId view_version;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Batch request to check object existence
 * Maps to: std::vector<Result<bool>> batch_object_exists(const std::vector<ObjectKey>& keys)
 */
struct BatchObjectExistsRequest {
    std::vector<ObjectKey> keys;
};

struct BatchObjectExistsResponse {
    std::vector<Result<bool>> results;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Batch request to get worker placements
 * Maps to: std::vector<Result<std::vector<CopyPlacement>>> batch_get_workers(const std::vector<ObjectKey>& keys)
 */
struct BatchGetWorkersRequest {
    std::vector<ObjectKey> keys;
};

struct BatchGetWorkersResponse {
    std::vector<Result<std::vector<CopyPlacement>>> results;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Batch request to start put operations
 * Maps to: std::vector<Result<std::vector<CopyPlacement>>> batch_put_start(const std::vector<std::pair<ObjectKey, std::pair<size_t, WorkerConfig>>>& requests)
 */
struct BatchPutStartRequest {
    std::vector<std::pair<ObjectKey, std::pair<size_t, WorkerConfig>>> requests;
};

struct BatchPutStartResponse {
    std::vector<Result<std::vector<CopyPlacement>>> results;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Batch request to complete put operations
 * Maps to: std::vector<ErrorCode> batch_put_complete(const std::vector<ObjectKey>& keys)
 */
struct BatchPutCompleteRequest {
    std::vector<ObjectKey> keys;
};

struct BatchPutCompleteResponse {
    std::vector<ErrorCode> results;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Batch request to cancel put operations
 * Maps to: std::vector<ErrorCode> batch_put_cancel(const std::vector<ObjectKey>& keys)
 */
struct BatchPutCancelRequest {
    std::vector<ObjectKey> keys;
};

struct BatchPutCancelResponse {
    std::vector<ErrorCode> results;
    ErrorCode error_code{ErrorCode::OK};
};

/**
 * @brief Response structure for ping operations
 */
struct PingResponse {
    ViewVersionId view_version;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(PingResponse, view_version)

    friend std::ostream& operator<<(std::ostream& os, const PingResponse& response) noexcept {
        return os << "PingResponse{view_version=" << response.view_version << "}";
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
    double eviction_ratio{0.1};
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