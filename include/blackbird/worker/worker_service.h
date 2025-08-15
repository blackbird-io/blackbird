#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <chrono>

#include "blackbird/common/types.h"
#include "blackbird/etcd/etcd_service.h"
#include "blackbird/worker/storage/storage_backend.h"
#include "blackbird/transport/ucx_engine.h"

namespace blackbird {

/**
 * @brief Configuration for worker service
 */
struct WorkerServiceConfig {
	std::string cluster_id{DEFAULT_CLUSTER_ID};
	std::string worker_id;                      
	std::string node_id;                        
	std::string etcd_endpoints;                 
	std::string rpc_endpoint; // Must be configured via YAML     
	std::string ucx_endpoint;                  
	std::vector<std::string> interconnects; // Must be configured via YAML: ["tcp", "rdma", "nvlink"] 

	std::vector<StorageClass> storage_classes; // Must be configured via YAML
	double max_bw_gbps; // Must be configured via YAML
	int numa_node{0};
	std::string version; // Must be configured via YAML

	int64_t lease_ttl_sec{10};          
	int64_t heartbeat_interval_sec{5};  
	int64_t allocation_poll_interval_ms{100}; 
	
	struct PoolConfig {
		std::string pool_id;
		StorageClass storage_class;
		uint64_t size_bytes;
		std::string mount_path; 
		int gpu_device_id{-1}; 
	};
	std::vector<PoolConfig> storage_pools;
	
	NLOHMANN_DEFINE_TYPE_INTRUSIVE(WorkerServiceConfig, cluster_id, worker_id, node_id, etcd_endpoints,
	                               rpc_endpoint, ucx_endpoint, interconnects, storage_classes,
	                               max_bw_gbps, numa_node, version, lease_ttl_sec, 
	                               heartbeat_interval_sec, allocation_poll_interval_ms)
};

ErrorCode load_worker_config_from_file(const std::string& config_file, WorkerServiceConfig& config);

/**
 * @brief Worker service that advertises storage pools to etcd and handles allocations
 */
class WorkerService {
public:
	/**
	 * @brief Constructor
	 * @param config Worker configuration
	 */
	explicit WorkerService(const WorkerServiceConfig& config);
	
	/**
	 * @brief Destructor
	 */
	~WorkerService();
	
	WorkerService(const WorkerService&) = delete;
	WorkerService& operator=(const WorkerService&) = delete;
	WorkerService(WorkerService&&) = delete;
	WorkerService& operator=(WorkerService&&) = delete;
	
	/**
	 * @brief Initialize the worker service
	 * @return ErrorCode::OK on success
	 */
	ErrorCode initialize();
	
	/**
	 * @brief Start the worker service
	 * @return ErrorCode::OK on success
	 */
	ErrorCode start();
	
	/**
	 * @brief Stop the worker service
	 */
	void stop();
	
	/**
	 * @brief Check if the service is running
	 */
	bool is_running() const noexcept { return running_.load(); }
	
	/**
	 * @brief Add a storage backend (memory pool)
	 * @param pool_id Unique pool identifier
	 * @param backend Storage backend implementation
	 * @return ErrorCode::OK on success
	 */
	ErrorCode add_storage_pool(const std::string& pool_id, 
	                          std::unique_ptr<StorageBackend> backend);
	
	/**
	 * @brief Create storage pools from configuration
	 * @return ErrorCode::OK on success
	 */
	ErrorCode create_storage_pools_from_config();
	
	/**
	 * @brief Get worker statistics
	 * @return Worker stats including pool utilization
	 */
	Result<nlohmann::json> get_stats() const;

private:
	// Configuration
	WorkerServiceConfig config_;
	
	// State management
	std::atomic<bool> running_{false};
	std::unique_ptr<EtcdService> etcd_;
	
	// Storage backends
	std::unordered_map<std::string, std::unique_ptr<StorageBackend>> storage_pools_;
	mutable std::shared_mutex storage_pools_mutex_;
	
	// UCX engine and per-pool registration state
	std::unique_ptr<UcxEngine> ucx_engine_;
	std::unordered_map<std::string, UcxRegInfo> pool_ucx_reg_;
	
	// Background threads
	std::thread heartbeat_thread_;
	
	// Private methods
	ErrorCode setup_etcd_connection();
	ErrorCode register_worker();
	ErrorCode register_storage_pools();
	void run_heartbeat_loop();
	
	// Etcd key helpers
	std::string make_etcd_key(const std::string& suffix) const;
	std::string workers_key() const;
	std::string worker_pool_key(const std::string& pool_id) const;
	std::string heartbeat_key() const;
	
	// JSON serialization helpers
	nlohmann::json worker_info_to_json() const;
	nlohmann::json pool_to_json(const std::string& pool_id, const StorageBackend& backend) const;
};

} // namespace blackbird 