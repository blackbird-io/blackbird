#pragma once

#include <memory>
#include <atomic>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_http/coro_http_server.hpp>

#include "blackbird/common/types.h"
#include "blackbird/keystone/keystone_service.h"

namespace blackbird {

/**
 * @brief RPC service wrapper for KeystoneService
 * 
 * This class provides the RPC interface for the Keystone using the YLT framework.
 * It handles:
 * - RPC method registration and routing  
 * - Error handling and response formatting
 * - HTTP metrics endpoint
 * - Server lifecycle management
 *
 * Uses struct_pack serialization for maximum performance.
 * All network endpoints use structured request/response types for type safety.
 */
class RpcService {
public:
    /**
     * @brief Constructor
     * @param keystone_service The keystone service instance to wrap
     * @param config Keystone configuration (for server settings)
     */
    explicit RpcService(std::shared_ptr<KeystoneService> keystone_service, const KeystoneConfig& config);
    
    /**
     * @brief Destructor
     */
    ~RpcService();
    
    // Non-copyable, non-movable
    RpcService(const RpcService&) = delete;
    RpcService& operator=(const RpcService&) = delete;
    RpcService(RpcService&&) = delete;
    RpcService& operator=(RpcService&&) = delete;
    
    /**
     * @brief Start the RPC server
     * @return ErrorCode::OK on success
     */
    ErrorCode start();
    
    /**
     * @brief Stop the RPC server
     */
    void stop();
    
    /**
     * @brief Check if the server is running
     */
    bool is_running() const noexcept { return running_.load(); }
    
    /**
     * @brief Check if an object exists
     * @param key Object key
     * @return Boolean result
     */
    Result<bool> object_exists(const ObjectKey& key);
    
    /**
     * @brief Get worker placements for an object
     * @param key Object key
     * @return Result containing placement information
     */
    Result<std::vector<CopyPlacement>> get_workers(const ObjectKey& key);
    
    /**
     * @brief Start a put operation
     * @param key Object key
     * @param data_size Data size in bytes
     * @param config Worker configuration
     * @return Result containing placement information
     */
    Result<std::vector<CopyPlacement>> put_start(const ObjectKey& key, 
                                           size_t data_size, 
                                           const WorkerConfig& config);
    
    /**
     * @brief Complete a put operation
     * @param key Object key
     * @return ErrorCode result
     */
    ErrorCode put_complete(const ObjectKey& key);
    
    /**
     * @brief Cancel a put operation
     * @param key Object key
     * @return ErrorCode result
     */
    ErrorCode put_cancel(const ObjectKey& key);
    
    /**
     * @brief Remove an object
     * @param key Object key
     * @return ErrorCode result
     */
    ErrorCode remove_object(const ObjectKey& key);
    
    /**
     * @brief Remove all objects
     * @return Number of objects removed
     */
    Result<size_t> remove_all_objects();
    
    // === Batch domain methods ===
    
    /**
     * @brief Batch check object existence
     * @param keys Vector of object keys
     * @return Vector of boolean results
     */
    std::vector<Result<bool>> batch_object_exists(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch get worker placements
     * @param keys Vector of object keys
     * @return Vector of results
     */
    std::vector<Result<std::vector<CopyPlacement>>> batch_get_workers(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch put start operations
     * @param keys Vector of object keys
     * @param data_sizes Vector of data sizes
     * @param config Worker configuration
     * @return Vector of results
     */
    std::vector<Result<std::vector<CopyPlacement>>> batch_put_start(
        const std::vector<ObjectKey>& keys,
        const std::vector<size_t>& data_sizes,
        const WorkerConfig& config);
    
    /**
     * @brief Batch complete put operations
     * @param keys Vector of object keys
     * @return Vector of error codes
     */
    std::vector<ErrorCode> batch_put_complete(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch cancel put operations
     * @param keys Vector of object keys
     * @return Vector of error codes
     */
    std::vector<ErrorCode> batch_put_cancel(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Get cluster statistics
     */
    Result<ClusterStats> get_cluster_stats();
    
    /**
     * @brief Get current view version
     * @return Current view version
     */
    ViewVersionId get_view_version();

private:
    // Core services
    std::shared_ptr<KeystoneService> keystone_service_;
    KeystoneConfig config_;
    
    // RPC server
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    std::thread rpc_server_thread_;
    
    // HTTP server for metrics
    std::unique_ptr<coro_http::coro_http_server> http_server_;
    std::thread http_thread_;
    
    // State
    std::atomic<bool> running_{false};
    
    // Internal methods
    ErrorCode setup_rpc_server();
    ErrorCode setup_http_server();
    void register_rpc_methods();
    void setup_metrics_endpoint();
    void run_http_server();
    
    // Helper methods for error handling
    template<typename T>
    Result<T> handle_service_call(std::function<Result<T>()> service_call);
    
    ErrorCode handle_service_call(std::function<ErrorCode()> service_call);

public:
    // Expose RPC handlers as public so clients can reference them
    ObjectExistsResponse rpc_object_exists(ObjectExistsRequest request);
    
    /**
     * @brief Get worker placements | Direct: get_workers(ObjectKey) -> Result<vector<CopyPlacement>>
     */
    GetWorkersResponse rpc_get_workers(GetWorkersRequest request);
    
    /**
     * @brief Start put operation | Direct: put_start(ObjectKey, size_t, WorkerConfig) -> Result<vector<CopyPlacement>>
     */
    PutStartResponse rpc_put_start(PutStartRequest request);
    
    /**
     * @brief Complete put operation | Direct: put_complete(ObjectKey) -> ErrorCode
     */
    PutCompleteResponse rpc_put_complete(PutCompleteRequest request);
    
    /**
     * @brief Cancel put operation | Direct: put_cancel(ObjectKey) -> ErrorCode
     */
    PutCancelResponse rpc_put_cancel(PutCancelRequest request);
    
    /**
     * @brief Remove object | Direct: remove_object(ObjectKey) -> ErrorCode
     */
    RemoveObjectResponse rpc_remove_object(RemoveObjectRequest request);
    
    /**
     * @brief Remove all objects | Direct: remove_all_objects() -> Result<size_t>
     */
    RemoveAllObjectsResponse rpc_remove_all_objects(RemoveAllObjectsRequest request);
    
    /**
     * @brief Get cluster statistics | Direct: get_cluster_stats() -> Result<ClusterStats>
     */
    GetClusterStatsResponse rpc_get_cluster_stats(GetClusterStatsRequest request);
    
    /**
     * @brief Get view version | Direct: get_view_version() -> ViewVersionId
     */
    GetViewVersionResponse rpc_get_view_version(GetViewVersionRequest request);
    
    /**
     * @brief Batch check object existence | Direct: batch_object_exists(vector<ObjectKey>) -> vector<Result<bool>>
     */
    BatchObjectExistsResponse rpc_batch_object_exists(BatchObjectExistsRequest request);
    
    /**
     * @brief Batch get worker placements | Direct: batch_get_workers(vector<ObjectKey>) -> vector<Result<vector<CopyPlacement>>>
     */
    BatchGetWorkersResponse rpc_batch_get_workers(BatchGetWorkersRequest request);
    
    /**
     * @brief Batch put start operations | Direct: batch_put_start(...) -> vector<Result<vector<CopyPlacement>>>
     */
    BatchPutStartResponse rpc_batch_put_start(BatchPutStartRequest request);
    
    /**
     * @brief Batch complete put operations | Direct: batch_put_complete(vector<ObjectKey>) -> vector<ErrorCode>
     */
    BatchPutCompleteResponse rpc_batch_put_complete(BatchPutCompleteRequest request);
    
    /**
     * @brief Batch cancel put operations | Direct: batch_put_cancel(vector<ObjectKey>) -> vector<ErrorCode>
     */
    BatchPutCancelResponse rpc_batch_put_cancel(BatchPutCancelRequest request);
    
};

/**
 * @brief Helper function to create and start a complete blackbird keystone
 * @param config Keystone configuration
 * @return Shared pointer to the running RPC service, or nullptr on error
 */
std::shared_ptr<RpcService> create_and_start_keystone(const KeystoneConfig& config);

}  // namespace blackbird 