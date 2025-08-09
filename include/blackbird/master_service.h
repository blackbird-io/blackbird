#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>

#include "blackbird/types.h"
#include "blackbird/etcd_service.h"

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

/**
 * @brief Information about a connected client
 */
struct ClientInfo {
    UUID client_id;
    NodeId node_id;
    std::string endpoint;
    std::chrono::steady_clock::time_point last_ping;
    std::vector<SegmentId> segments;  // Segments owned by this client
    
    bool is_stale(std::chrono::seconds ttl) const {
        auto now = std::chrono::steady_clock::now();
        return (now - last_ping) > ttl;
    }
};

/**
 * @brief Information about an object stored in the cache
 */
struct ObjectInfo {
    ObjectKey key;
    std::vector<WorkerPlacement> workers;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_accessed;
    bool soft_pinned{false};
    uint64_t ttl_ms{DEFAULT_KV_TTL_MS};
    
    bool is_expired() const {
        if (ttl_ms == 0) return false;  // No expiration
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::milliseconds>(now - created_at);
        return age.count() > static_cast<int64_t>(ttl_ms);
    }
    
    void touch() {
        last_accessed = std::chrono::steady_clock::now();
    }
};

/**
 * @brief Keystone service that coordinates the distributed cache
 * - Managing client registrations and health
 * - Coordinating worker placement
 * - Handling object metadata
 * - Performing garbage collection and eviction
 * - Providing service discovery through etcd
 */
class MasterService {
public:
    /**
     * @brief Constructor
     * @param config Master service configuration
     */
    explicit MasterService(const MasterConfig& config);
    
    /**
     * @brief Destructor
     */
    ~MasterService();
    
    // Non-copyable, non-movable
    MasterService(const MasterService&) = delete;
    MasterService& operator=(const MasterService&) = delete;
    MasterService(MasterService&&) = delete;
    MasterService& operator=(MasterService&&) = delete;
    
    /**
     * @brief Initialize the master service
     * @return ErrorCode::OK on success
     */
    ErrorCode initialize();
    
    /**
     * @brief Start the master service
     * @return ErrorCode::OK on success
     */
    ErrorCode start();
    
    /**
     * @brief Stop the master service
     */
    void stop();
    
    /**
     * @brief Check if the service is running
     */
    bool is_running() const noexcept { return running_.load(); }
    
    // === Client Management ===
    
    /**
     * @brief Register a client with the master
     * @param client_id Unique client identifier
     * @param node_id Node identifier where client is running
     * @param endpoint Client's network endpoint
     * @return ErrorCode::OK on success
     */
    ErrorCode register_client(const UUID& client_id, const NodeId& node_id, const std::string& endpoint);
    
    /**
     * @brief Handle client ping (heartbeat)
     * @param client_id Client identifier
     * @return PingResponse with current view version and client status
     */
    Result<PingResponse> ping_client(const UUID& client_id);
    
    /**
     * @brief Unregister a client
     * @param client_id Client identifier
     * @return ErrorCode::OK on success
     */
    ErrorCode unregister_client(const UUID& client_id);
    
    /**
     * @brief Get list of active clients
     * @param clients Output parameter for client information
     * @return ErrorCode::OK on success
     */
    ErrorCode get_active_clients(std::vector<ClientInfo>& clients) const;
    
    // === Segment Management ===
    
    /**
     * @brief Register a memory segment with the master
     * @param segment Segment information
     * @param client_id Client that owns the segment
     * @return ErrorCode::OK on success
     */
    ErrorCode register_segment(const Segment& segment, const UUID& client_id);
    
    /**
     * @brief Unregister a memory segment
     * @param segment_id Segment identifier
     * @param client_id Client that owns the segment
     * @return ErrorCode::OK on success
     */
    ErrorCode unregister_segment(const SegmentId& segment_id, const UUID& client_id);
    
    /**
     * @brief Get information about available segments
     * @param segments Output parameter for segment information
     * @return ErrorCode::OK on success
     */
    ErrorCode get_segments(std::vector<Segment>& segments) const;
    
    // === Object Management ===
    
    /**
     * @brief Check if an object exists
     * @param key Object key
     * @return true if object exists, false otherwise
     */
    Result<bool> object_exists(const ObjectKey& key);
    
    /**
     * @brief Get worker placements for an object
     * @param key Object key
     * @return Vector of worker placements
     */
    Result<std::vector<WorkerPlacement>> get_workers(const ObjectKey& key);
    
    /**
     * @brief Start a put operation (allocate workers)
     * @param key Object key
     * @param data_size Total size of the object data
     * @param config Worker configuration
     * @return Vector of allocated worker placements
     */
    Result<std::vector<WorkerPlacement>> put_start(const ObjectKey& key, 
                                                     size_t data_size, 
                                                     const WorkerConfig& config);
    
    /**
     * @brief Complete a put operation (mark workers as complete)
     * @param key Object key
     * @return ErrorCode::OK on success
     */
    ErrorCode put_complete(const ObjectKey& key);
    
    /**
     * @brief Cancel a put operation (cleanup allocated workers)
     * @param key Object key
     * @return ErrorCode::OK on success
     */
    ErrorCode put_cancel(const ObjectKey& key);
    
    /**
     * @brief Remove an object from the cache
     * @param key Object key
     * @return ErrorCode::OK on success
     */
    ErrorCode remove_object(const ObjectKey& key);
    
    /**
     * @brief Remove all objects (for testing/maintenance)
     * @return Number of objects removed
     */
    Result<size_t> remove_all_objects();
    
    // === Batch Operations ===
    
    /**
     * @brief Batch check if objects exist
     * @param keys Vector of object keys
     * @return Vector of existence results
     */
    std::vector<Result<bool>> batch_object_exists(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch get worker placements
     * @param keys Vector of object keys
     * @return Vector of worker placement results
     */
    std::vector<Result<std::vector<WorkerPlacement>>> batch_get_workers(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch start put operations
     * @param keys Vector of object keys
     * @param data_sizes Vector of data sizes
     * @param config Worker configuration (applied to all)
     * @return Vector of allocated worker placement results
     */
    std::vector<Result<std::vector<WorkerPlacement>>> batch_put_start(
        const std::vector<ObjectKey>& keys,
        const std::vector<size_t>& data_sizes,
        const WorkerConfig& config);
    
    /**
     * @brief Batch complete put operations
     * @param keys Vector of object keys
     * @return Vector of completion results
     */
    std::vector<ErrorCode> batch_put_complete(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch cancel put operations
     * @param keys Vector of object keys
     * @return Vector of cancellation results
     */
    std::vector<ErrorCode> batch_put_cancel(const std::vector<ObjectKey>& keys);
    
    // === Metrics and Status ===
    
    /**
     * @brief Get cluster statistics
     */
    struct ClusterStats {
        size_t total_clients{0};
        size_t active_clients{0};
        size_t total_segments{0};
        size_t total_capacity{0};
        size_t used_capacity{0};
        size_t total_objects{0};
        double utilization{0.0};
        
        // JSON serialization support
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(ClusterStats, total_clients, active_clients, total_segments,
                                       total_capacity, used_capacity, total_objects, utilization)
    };
    
    /**
     * @brief Get current cluster statistics
     * @return Cluster statistics
     */
    Result<ClusterStats> get_cluster_stats() const;
    
    /**
     * @brief Get current view version
     * @return Current view version ID
     */
    ViewVersionId get_view_version() const noexcept { return view_version_.load(); }

private:
    // Configuration
    MasterConfig config_;
    
    // Etcd integration
    std::unique_ptr<EtcdService> etcd_;
    EtcdLeaseId master_lease_id_{0};
    
    // State management
    std::atomic<bool> running_{false};
    std::atomic<ViewVersionId> view_version_{0};
    
    // Client management
    mutable std::shared_mutex clients_mutex_;
    std::unordered_map<UUID, ClientInfo> clients_;
    
    // Segment management
    mutable std::shared_mutex segments_mutex_;
    std::unordered_map<SegmentId, Segment> segments_;
    
    // Object management
    mutable std::shared_mutex objects_mutex_;
    std::unordered_map<ObjectKey, ObjectInfo> objects_;
    
    // Background threads
    std::thread gc_thread_;
    std::thread health_check_thread_;
    std::thread etcd_keepalive_thread_;
    
    // Internal methods
    ErrorCode setup_etcd_integration();
    void run_garbage_collection();
    void run_health_checks();
    void run_etcd_keepalive();
    
    ErrorCode allocate_workers(const ObjectKey& key, size_t data_size, 
                               const WorkerConfig& config,
                               std::vector<WorkerPlacement>& workers);
    
    void cleanup_stale_clients();
    void evict_objects_if_needed();
    
    // Helper methods
    ViewVersionId increment_view_version();
    std::string make_etcd_key(const std::string& suffix) const;
};

}  // namespace blackbird 