#pragma once

#include <memory>
#include <atomic>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_http/coro_http_server.hpp>

#include "blackbird/types.h"
#include "blackbird/master_service.h"

namespace blackbird {

/**
 * @brief RPC service wrapper for MasterService
 * 
 * This class provides the RPC interface for the master service using the YLT framework.
 * It handles:
 * - RPC method registration and routing
 * - Error handling and response formatting
 * - HTTP metrics endpoint
 * - Server lifecycle management
 */
class RpcService {
public:
    /**
     * @brief Constructor
     * @param master_service The master service instance to wrap
     * @param config Master configuration (for server settings)
     */
    explicit RpcService(std::shared_ptr<MasterService> master_service, const MasterConfig& config);
    
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
    
    // === RPC Methods (exposed to clients) ===
    
    /**
     * @brief Ping method for client heartbeat
     * @param client_id Client UUID
     * @return PingResponse with view version and client status
     */
    Result<PingResponse> ping(const UUID& client_id);
    
    /**
     * @brief Register a client
     * @param client_id Client UUID
     * @param node_id Node identifier
     * @param endpoint Client endpoint
     * @return ErrorCode result
     */
    ErrorCode register_client(const UUID& client_id, const NodeId& node_id, const std::string& endpoint);
    
    /**
     * @brief Register a memory segment
     * @param segment Segment information
     * @param client_id Client UUID
     * @return ErrorCode result
     */
    ErrorCode register_segment(const Segment& segment, const UUID& client_id);
    
    /**
     * @brief Unregister a memory segment
     * @param segment_id Segment identifier
     * @param client_id Client UUID
     * @return ErrorCode result
     */
    ErrorCode unregister_segment(const SegmentId& segment_id, const UUID& client_id);
    
    /**
     * @brief Check if an object exists
     * @param key Object key
     * @return Boolean result
     */
    Result<bool> object_exists(const ObjectKey& key);
    
    /**
     * @brief Get worker placements for an object
     * @param key Object key
     * @return Vector of worker placements
     */
    Result<std::vector<WorkerPlacement>> get_workers(const ObjectKey& key);
    
    /**
     * @brief Start a put operation
     * @param key Object key
     * @param data_size Total data size
     * @param config Worker configuration
     * @return Vector of allocated worker placements
     */
    Result<std::vector<WorkerPlacement>> put_start(const ObjectKey& key, 
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
     * @return Vector of error codes
     */
    std::vector<ErrorCode> batch_put_complete(const std::vector<ObjectKey>& keys);
    
    /**
     * @brief Batch cancel put operations
     * @param keys Vector of object keys
     * @return Vector of error codes
     */
    std::vector<ErrorCode> batch_put_cancel(const std::vector<ObjectKey>& keys);
    
    // === Admin/Monitoring Methods ===
    
    /**
     * @brief Get cluster statistics
     * @return Cluster statistics
     */
    Result<MasterService::ClusterStats> get_cluster_stats();
    
    /**
     * @brief Get current view version
     * @return Current view version
     */
    ViewVersionId get_view_version();

private:
    // Core services
    std::shared_ptr<MasterService> master_service_;
    MasterConfig config_;
    
    // RPC server
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    
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
};

/**
 * @brief Helper function to register all RPC methods
 * @param server The RPC server instance
 * @param rpc_service The RPC service instance
 */
void register_rpc_methods(coro_rpc::coro_rpc_server& server, RpcService& rpc_service);

/**
 * @brief Helper function to create and start a complete blackbird master
 * @param config Master configuration
 * @return Shared pointer to the running RPC service, or nullptr on error
 */
std::shared_ptr<RpcService> create_and_start_master(const MasterConfig& config);

}  // namespace blackbird 