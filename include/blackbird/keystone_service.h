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
 * @brief Information about a worker node
 */
struct WorkerInfo {
    std::string worker_id;           // Worker identifier
    NodeId node_id;                  // Physical node ID
    std::string endpoint;            // Network endpoint
    std::unordered_map<std::string, Segment> chunks;  // chunk_id -> Chunk metadata
    std::chrono::steady_clock::time_point last_heartbeat;
    bool is_healthy{true};
    
    bool is_stale(std::chrono::seconds ttl) const {
        auto now = std::chrono::steady_clock::now();
        return (now - last_heartbeat) > ttl;
    }
};

/**
 * @brief Cluster statistics for monitoring
 */
struct ClusterStats {
    size_t total_workers{0};
    size_t active_workers{0};
    size_t total_clients{0};      // Total number of clients
    size_t active_clients{0};     // Active clients (non-stale)
    size_t total_segments{0};
    size_t total_capacity{0};     // Total storage capacity in bytes
    size_t used_capacity{0};      // Used storage capacity in bytes
    size_t total_objects{0};      // Number of stored objects
    double utilization{0.0};      // Overall cluster utilization (0.0-1.0)
};

/**
 * @brief Keystone service that coordinates the distributed cache
 * - Managing client registrations and health
 * - Coordinating worker placement
 * - Handling object metadata
 * - Performing garbage collection and eviction
 * - Providing service discovery through etcd
 */
class KeystoneService {
public:
    /**
     * @brief Constructor
     * @param config Keystone service configuration
     */
    explicit KeystoneService(const KeystoneConfig& config);
    
    /**
     * @brief Destructor
     */
    ~KeystoneService();
    
    // Non-copyable, non-movable
    KeystoneService(const KeystoneService&) = delete;
    KeystoneService& operator=(const KeystoneService&) = delete;
    KeystoneService(KeystoneService&&) = delete;
    KeystoneService& operator=(KeystoneService&&) = delete;
    
    /**
     * @brief Initialize the keystone service
     * @return ErrorCode::OK on success
     */
    ErrorCode initialize();
    
    /**
     * @brief Start the keystone service
     * @return ErrorCode::OK on success
     */
    ErrorCode start();
    
    /**
     * @brief Stop the keystone service
     */
    void stop();
    
    /**
     * @brief Check if the service is running
     */
    bool is_running() const noexcept { return running_.load(); }
    
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
     * @param data_size Total data size
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
     * @brief Handle client ping (heartbeat) - for future compatibility
     * @param client_id Client identifier
     * @return PingResponse with current view version and client status
     */
    Result<PingResponse> ping_client(const UUID& client_id);
    
    /**
     * @brief Get active clients (read-only view from ETCD)
     * @param clients Output parameter for client information
     * @return ErrorCode::OK on success
     */
    ErrorCode get_active_clients(std::vector<ClientInfo>& clients) const;
    
    /**
     * @brief Get available chunks (read-only view from ETCD) 
     * @param chunks Output parameter for chunk information
     * @return ErrorCode::OK on success
     */
    ErrorCode get_chunks(std::vector<Segment>& chunks) const;
    
    /**
     * @brief Remove worker from cluster (emergency management)
     * @param worker_id Worker identifier to remove
     * @return ErrorCode::OK on success
     */
    ErrorCode remove_worker(const std::string& worker_id);
    
    /**
     * @brief Get worker information (read-only view from ETCD)
     * @param workers Output parameter for worker information
     * @return ErrorCode::OK on success
     */
    ErrorCode get_workers_info(std::vector<WorkerInfo>& workers) const;
    
    // === Batch RPC Methods ===
    
    /**
     * @brief Batch check object existence
     * @param keys Vector of object keys
     * @return Vector of boolean results
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
     * @param config Worker configuration
     * @return Vector of worker placement results
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
    
    // === Monitoring and Statistics ===
    
    /**
     * @brief Get cluster statistics
     * @return Cluster statistics including worker capacity and utilization
     */
    Result<ClusterStats> get_cluster_stats() const;
    
    /**
     * @brief Get current view version for cache invalidation
     * @return Current view version
     */
    ViewVersionId get_view_version() const noexcept { return view_version_.load(); }
    
private:
    // Configuration
    KeystoneConfig config_;
    
    // Etcd integration
    std::unique_ptr<EtcdService> etcd_;
    
    // State management
    std::atomic<bool> running_{false};
    std::atomic<ViewVersionId> view_version_{0};
    
    // Chunk information from ETCD (read-only view) - using 'Segment' type for compatibility
    mutable std::shared_mutex chunks_mutex_;
    std::unordered_map<SegmentId, Segment> chunks_;   // Aggregated view of chunks from all workers
    
    // Client information from ETCD (read-only view)
    mutable std::shared_mutex clients_mutex_;
    std::unordered_map<UUID, ClientInfo> clients_;      // client_id -> ClientInfo
    
    // Object metadata
    mutable std::shared_mutex objects_mutex_;
    std::unordered_map<ObjectKey, ObjectInfo> objects_;
    
    // Background threads
    std::thread gc_thread_;
    std::thread health_check_thread_;
    std::thread etcd_keepalive_thread_;
    
    // ETCD lease management - CRITICAL for fault tolerance and service discovery
    // The lease ensures automatic cleanup if keystone crashes (TTL-based expiration)
    // and provides service discovery for other nodes to find active keystones
    EtcdLeaseId keystone_lease_id_{0};
    
    // Worker registry tracking from ETCD
    mutable std::shared_mutex worker_registry_mutex_;
    std::unordered_map<std::string, WorkerInfo> workers_;        // worker_id -> WorkerInfo
    mutable std::shared_mutex heartbeat_mutex_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> worker_heartbeats_;
    
    // Helper methods
    ErrorCode allocate_workers(const ObjectKey& key, size_t data_size, 
                              const WorkerConfig& config,
                              std::vector<WorkerPlacement>& workers);
    
    void cleanup_stale_workers();
    void cleanup_stale_clients();
    void evict_objects_if_needed();
    ViewVersionId increment_view_version();
    
    // ETCD integration helpers
    ErrorCode setup_etcd_integration();
    void load_existing_state_from_etcd();
    void setup_watcher_with_error_handling(const std::string& watcher_name, const std::function<void()>& watcher_func);
    void run_garbage_collection();
    void run_health_checks();
    void run_etcd_keepalive();
    
    // Helper methods
    std::string make_etcd_key(const std::string& suffix) const;

    // Small helpers
    std::string chunks_prefix() const { return make_etcd_key("chunks/"); }
    std::string workers_prefix() const { return make_etcd_key("workers/"); }
    std::string clients_prefix() const { return make_etcd_key("clients/"); }
    std::string heartbeat_prefix() const { return make_etcd_key("heartbeat/"); }

    // Etcd key builders for new worker layout
    std::string make_worker_key(const std::string& worker_id) const;
    std::string make_worker_chunk_key(const std::string& worker_id, const std::string& chunk_id) const;
    std::string make_heartbeat_key(const std::string& worker_id) const;

    // Parsing helpers (best-effort, minimal schema)
    void upsert_chunk_from_json(const std::string& key, const std::string& json_value);
    void remove_chunk_by_key(const std::string& key);

    // New worker management via etcd
    void watch_worker_registry_namespace();
    void watch_heartbeat_namespace();
    void upsert_worker_from_json(const std::string& key, const std::string& json_value);
    void remove_worker_by_key(const std::string& key);
    void upsert_worker_chunk_from_json(const std::string& key, const std::string& json_value);
    void remove_worker_chunk_by_key(const std::string& key);
    void update_worker_heartbeat(const std::string& key, const std::string& json_value);
    
    // Client management helpers for ETCD parsing
    void upsert_client_from_json(const std::string& key, const std::string& json_value);
    void remove_client_by_key(const std::string& key);
    
    // Client and chunk watchers for etcd-driven state management
    void watch_clients_namespace();
    void watch_chunks_namespace();
};

}  // namespace blackbird 