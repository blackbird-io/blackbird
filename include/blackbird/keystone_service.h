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
 * @brief Information about an object stored in the cache
 */
struct ObjectInfo {
    ObjectKey key;
    size_t size{0};                               // Object size in bytes
    std::chrono::system_clock::time_point created;
    std::chrono::system_clock::time_point last_accessed;
    WorkerConfig config;                          // Configuration used for this object
    std::vector<CopyPlacement> copies;           // Data copies (replication factor)
    
    bool is_expired() const {
        if (config.ttl_ms == 0) return false;  // No expiration
        auto now = std::chrono::system_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::milliseconds>(now - created);
        return age.count() > static_cast<int64_t>(config.ttl_ms);
    }
    
    void touch() {
        last_accessed = std::chrono::system_clock::now();
    }
    
    bool has_complete_copy() const noexcept {
        return std::any_of(copies.begin(), copies.end(), [](const CopyPlacement& copy) {
            return !copy.shards.empty(); // Simple check - has shards means complete
        });
    }
    
    size_t replication_factor() const noexcept {
        return copies.size();
    }
    
    size_t total_worker_count() const noexcept {
        size_t count = 0;
        for (const auto& copy : copies) {
            count += copy.shards_size();
        }
        return count;
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
     * @brief Get worker placement information for an object
     * @param key Object key to retrieve
     * @return Result containing placement information or error
     */
    Result<std::vector<CopyPlacement>> get_workers(const ObjectKey& key);
    
    /**
     * @brief Start a put operation for an object
     * @param key Object key
     * @param size Object size in bytes
     * @param config Worker configuration
     * @return Result containing placement information or error
     */
    Result<std::vector<CopyPlacement>> put_start(const ObjectKey& key,
                                           size_t size,
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
    
    /**
     * Readonly snapshot of all objects/storage systems mounted on the cluster.
     */
    ErrorCode get_chunks(std::vector<Segment>& chunks) const;
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
     * @brief Batch version of get_workers
     * @param keys Vector of object keys
     * @return Vector of results for each key
     */
    std::vector<Result<std::vector<CopyPlacement>>> batch_get_workers(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch version of put_start
     * @param requests Vector of put start requests
     * @return Vector of results for each request
     */
    std::vector<Result<std::vector<CopyPlacement>>> batch_put_start(
        const std::vector<std::pair<ObjectKey, std::pair<size_t, WorkerConfig>>>& requests);
    
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
    ErrorCode allocate_data_copies(const ObjectKey& key, size_t data_size, 
                                  const WorkerConfig& config,
                                  std::vector<CopyPlacement>& copies);
                                  
    ErrorCode allocate_shards_for_copy(const ObjectKey& key, size_t data_size,
                                       size_t copy_id, size_t max_workers,
                                       std::vector<ShardPlacement>& shards);
    
    void cleanup_stale_workers();
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
    std::string heartbeat_prefix() const { return make_etcd_key("heartbeat/"); }

    // Etcd key builders for new worker layout
    std::string make_worker_key(const std::string& worker_id) const;
    std::string make_worker_chunk_key(const std::string& worker_id, const std::string& chunk_id) const;
    std::string make_heartbeat_key(const std::string& worker_id) const;

    // Parsing helpers
    void upsert_chunk_from_json(const std::string& key, const std::string& json_value);
    void remove_chunk_by_key(const std::string& key);

    // Worker management via etcd
    void watch_worker_registry_namespace();
    void watch_heartbeat_namespace();
    void upsert_worker_from_json(const std::string& key, const std::string& json_value);
    void remove_worker_by_key(const std::string& key);
    void upsert_worker_chunk_from_json(const std::string& key, const std::string& json_value);
    void remove_worker_chunk_by_key(const std::string& key);
    void update_worker_heartbeat(const std::string& key, const std::string& json_value);
    
    // Chunk watchers
    void watch_chunks_namespace();
};

}  // namespace blackbird 