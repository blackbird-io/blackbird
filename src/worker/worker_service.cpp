#include "blackbird/worker/worker_service.h"

#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <thread>
#include <fstream>

#include <yaml-cpp/yaml.h>

#include "blackbird/worker/storage/ram_backend.h"

namespace blackbird {

static StorageClass parse_storage_class(const std::string& s) {
	if (s == "RAM_CPU") return StorageClass::RAM_CPU;
	if (s == "RAM_GPU") return StorageClass::RAM_GPU;
	if (s == "NVME") return StorageClass::NVME;
	if (s == "SSD") return StorageClass::SSD;
	if (s == "HDD") return StorageClass::HDD;
	return StorageClass::RAM_CPU;
}

ErrorCode load_worker_config_from_file(const std::string& config_file, WorkerServiceConfig& config) {
	try {
		YAML::Node root = YAML::LoadFile(config_file);
		
		if (!root || !root.IsMap()) {
			LOG(ERROR) << "Invalid YAML root in " << config_file;
			return ErrorCode::INVALID_CONFIGURATION;
		}
		
		// Worker section
		if (root["worker"]) {
			auto w = root["worker"];
			if (w["worker_id"]) config.worker_id = w["worker_id"].as<std::string>();
			if (w["node_id"]) config.node_id = w["node_id"].as<std::string>();
			if (w["cluster_id"]) config.cluster_id = w["cluster_id"].as<std::string>();
			
			if (w["etcd_endpoints"]) {
				const auto& eps = w["etcd_endpoints"];
				std::string combined;
				if (eps.IsSequence()) {
					for (size_t i = 0; i < eps.size(); ++i) {
						if (i > 0) combined += ",";
						combined += eps[i].as<std::string>();
					}
				} else if (eps.IsScalar()) {
					combined = eps.as<std::string>();
				}
				if (!combined.empty()) config.etcd_endpoints = combined;
			}
			
			if (w["rpc_endpoint"]) config.rpc_endpoint = w["rpc_endpoint"].as<std::string>();
			if (w["ucx_endpoint"]) config.ucx_endpoint = w["ucx_endpoint"].as<std::string>();
			if (w["interconnects"]) {
				config.interconnects.clear();
				for (const auto& v : w["interconnects"]) {
					config.interconnects.push_back(v.as<std::string>());
				}
			}
			if (w["max_bw_gbps"]) config.max_bw_gbps = w["max_bw_gbps"].as<double>();
			if (w["numa_node"]) config.numa_node = w["numa_node"].as<int>();
			if (w["version"]) config.version = w["version"].as<std::string>();
			if (w["lease_ttl_sec"]) config.lease_ttl_sec = w["lease_ttl_sec"].as<int64_t>();
			if (w["heartbeat_interval_sec"]) config.heartbeat_interval_sec = w["heartbeat_interval_sec"].as<int64_t>();
		}
		
		// Storage pools
		config.storage_pools.clear();
		if (root["storage_pools"]) {
			const auto& pools = root["storage_pools"];
			if (!pools.IsSequence()) {
				LOG(ERROR) << "storage_pools must be a sequence";
				return ErrorCode::INVALID_CONFIGURATION;
			}
			for (const auto& p : pools) {
				WorkerServiceConfig::PoolConfig pc;
				if (p["pool_id"]) pc.pool_id = p["pool_id"].as<std::string>();
				if (p["storage_class"]) pc.storage_class = parse_storage_class(p["storage_class"].as<std::string>());
				if (p["size_bytes"]) pc.size_bytes = p["size_bytes"].as<uint64_t>();
				if (p["mount_path"]) pc.mount_path = p["mount_path"].as<std::string>();
				if (p["gpu_device_id"]) pc.gpu_device_id = p["gpu_device_id"].as<int>();
				
				if (pc.pool_id.empty() || pc.size_bytes == 0) {
					LOG(ERROR) << "Invalid pool config (pool_id/size_bytes missing)";
					return ErrorCode::INVALID_CONFIGURATION;
				}
				config.storage_pools.push_back(std::move(pc));
			}
		}
		
		// Infer storage_classes summary
		config.storage_classes.clear();
		for (const auto& pc : config.storage_pools) {
			if (std::find(config.storage_classes.begin(), config.storage_classes.end(), pc.storage_class) ==
				config.storage_classes.end()) {
				config.storage_classes.push_back(pc.storage_class);
			}
		}
		
		return ErrorCode::OK;
	} catch (const std::exception& e) {
		LOG(ERROR) << "Error parsing YAML config: " << e.what();
		return ErrorCode::INVALID_CONFIGURATION;
	}
}

WorkerService::WorkerService(const WorkerServiceConfig& config) 
	: config_(config) {
	LOG(INFO) << "Creating WorkerService with worker_id: " << config_.worker_id;
}

WorkerService::~WorkerService() {
	if (running_.load()) {
		stop();
	}
	
	// Manually cleanup persistent worker and storage pool keys
	if (etcd_) {
		// Delete worker info
		std::string worker_key = workers_key();
		etcd_->del(worker_key);
		
		// Delete storage pool keys
		{
			std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
			for (const auto& [pool_id, backend] : storage_pools_) {
				std::string pool_key = worker_pool_key(pool_id);
				etcd_->del(pool_key);
			}
		}
		
		// Revoke heartbeat lease (this will remove heartbeat key)
		if (worker_lease_id_ != 0) {
			etcd_->revoke_lease(worker_lease_id_);
		}
	}
}

ErrorCode WorkerService::initialize() {
	LOG(INFO) << "Initializing WorkerService...";
	
	if (config_.worker_id.empty()) {
		LOG(ERROR) << "Worker ID cannot be empty";
		return ErrorCode::INVALID_PARAMETERS;
	}
	
	if (config_.node_id.empty()) {
		LOG(ERROR) << "Node ID cannot be empty";
		return ErrorCode::INVALID_PARAMETERS;
	}
	
	auto err = setup_etcd_connection();
	if (err != ErrorCode::OK) {
		LOG(ERROR) << "Failed to setup etcd connection: " << error::to_string(err);
		return err;
	}
	
	// Initialize storage backends
	{
		std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
		for (const auto& [pool_id, backend] : storage_pools_) {
			err = backend->initialize();
			if (err != ErrorCode::OK) {
				LOG(ERROR) << "Failed to initialize storage pool " << pool_id 
					<< ": " << error::to_string(err);
				return err;
			}
		}
	}
	
	// Initialize UCX
	ucx_engine_ = std::make_unique<UcxEngine>();
	if (!ucx_engine_->init()) {
		LOG(ERROR) << "UCX init failed";
		return ErrorCode::INTERNAL_ERROR;
	}
	// Start UCX listener
	{
		std::string host = "0.0.0.0";
		uint16_t port = 0;
		if (!config_.ucx_endpoint.empty()) {
			auto pos = config_.ucx_endpoint.find(':');
			if (pos != std::string::npos) {
				host = config_.ucx_endpoint.substr(0, pos);
				port = static_cast<uint16_t>(std::stoi(config_.ucx_endpoint.substr(pos + 1)));
			}
		}
		std::string advertised = ucx_engine_->start_listener(host, port);
		if (advertised.empty()) {
			LOG(ERROR) << "Failed to start UCX listener";
			return ErrorCode::INTERNAL_ERROR;
		}
		config_.ucx_endpoint = advertised;
		LOG(INFO) << "UCX listener started at " << config_.ucx_endpoint;
	}
	
	// Register each pool's memory with UCX
	{
		std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
		for (const auto& [pool_id, backend] : storage_pools_) {
			void* base_ptr = reinterpret_cast<void*>(backend->get_base_address());
			size_t size = backend->get_total_capacity();
			if (base_ptr == nullptr || size == 0) {
				LOG(ERROR) << "Pool " << pool_id << " has invalid base/size for UCX registration";
				return ErrorCode::INVALID_STATE;
			}
			UcxRegInfo reg;
			if (!ucx_engine_->register_memory(base_ptr, size, reg)) {
				LOG(ERROR) << "UCX memory registration failed for pool " << pool_id;
				return ErrorCode::INTERNAL_ERROR;
			}
			pool_ucx_reg_[pool_id] = reg;
			LOG(INFO) << "UCX registered pool " << pool_id << ", remote_addr=0x" 
				      << std::hex << reg.remote_addr << std::dec;
		}
	}
	
	LOG(INFO) << "WorkerService initialized successfully";
	return ErrorCode::OK;
}

ErrorCode WorkerService::start() {
	LOG(INFO) << "Starting WorkerService...";
	
	if (running_.load()) {
		LOG(WARNING) << "WorkerService is already running";
		return ErrorCode::OK;
	}
	
	auto err = register_worker();
	if (err != ErrorCode::OK) {
		LOG(ERROR) << "Failed to register worker: " << error::to_string(err);
		return err;
	}
	
	err = register_storage_pools();
	if (err != ErrorCode::OK) {
		LOG(ERROR) << "Failed to register storage pools: " << error::to_string(err);
		return err;
	}
	
	running_.store(true);
	
	// Start background threads
	heartbeat_thread_ = std::thread(&WorkerService::run_heartbeat_loop, this);
	
	LOG(INFO) << "WorkerService started successfully";
	return ErrorCode::OK;
}

void WorkerService::stop() {
	LOG(INFO) << "Stopping WorkerService...";
	
	running_.store(false);
	
	// Join background threads
	if (heartbeat_thread_.joinable()) {
		heartbeat_thread_.join();
	}
	
	// UCX cleanup happens in UcxEngine destructor when unique_ptr resets below
	ucx_engine_.reset();
	pool_ucx_reg_.clear();
	
	// Shutdown storage backends
	{
		std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
		for (const auto& [pool_id, backend] : storage_pools_) {
			backend->shutdown();
		}
	}
	
	// Manually cleanup persistent worker and storage pool keys
	if (etcd_) {
		// Delete worker info
		std::string worker_key = workers_key();
		etcd_->del(worker_key);
		
		// Delete storage pool keys
		{
			std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
			for (const auto& [pool_id, backend] : storage_pools_) {
				std::string pool_key = worker_pool_key(pool_id);
				etcd_->del(pool_key);
			}
		}
		
		// Revoke heartbeat lease (this will remove heartbeat key)
		if (worker_lease_id_ != 0) {
			etcd_->revoke_lease(worker_lease_id_);
			worker_lease_id_ = 0;
		}
	}
	
	LOG(INFO) << "WorkerService stopped";
}

ErrorCode WorkerService::add_storage_pool(const std::string& pool_id, 
                                          std::unique_ptr<StorageBackend> backend) {
	if (pool_id.empty()) {
		return ErrorCode::INVALID_PARAMETERS;
	}
	
	std::unique_lock<std::shared_mutex> lock(storage_pools_mutex_);
	if (storage_pools_.find(pool_id) != storage_pools_.end()) {
		LOG(WARNING) << "Storage pool " << pool_id << " already exists";
		return ErrorCode::OBJECT_ALREADY_EXISTS;
	}
	
	storage_pools_[pool_id] = std::move(backend);
	LOG(INFO) << "Added storage pool: " << pool_id;
	
	return ErrorCode::OK;
}

ErrorCode WorkerService::create_storage_pools_from_config() {
	LOG(INFO) << "Creating storage pools from configuration...";
	
	for (const auto& pool_config : config_.storage_pools) {
		LOG(INFO) << "Creating pool: " << pool_config.pool_id 
		          << " (" << static_cast<int>(pool_config.storage_class) 
		          << ", " << pool_config.size_bytes << " bytes)";
		
		// Create storage backend based on storage class
		std::unique_ptr<StorageBackend> backend;
		
		switch (pool_config.storage_class) {
			case StorageClass::RAM_CPU:
			case StorageClass::RAM_GPU:
				backend = create_storage_backend(pool_config.storage_class, pool_config.size_bytes);
				break;
				
			// TODO: Add support for other storage classes
			// case StorageClass::NVME:
			// case StorageClass::SSD:
			// case StorageClass::HDD:
			//     backend = create_disk_storage_backend(pool_config.storage_class, 
			//                                          pool_config.size_bytes, 
			//                                          pool_config.mount_path);
			//     break;
				
			default:
				LOG(ERROR) << "Unsupported storage class: " << static_cast<int>(pool_config.storage_class);
				return ErrorCode::INVALID_CONFIGURATION;
		}
		
		if (!backend) {
			LOG(ERROR) << "Failed to create storage backend for pool: " << pool_config.pool_id;
			return ErrorCode::ALLOCATION_FAILED;
		}
		
		auto err = add_storage_pool(pool_config.pool_id, std::move(backend));
		if (err != ErrorCode::OK) {
			LOG(ERROR) << "Failed to add storage pool: " << pool_config.pool_id;
			return err;
		}
	}
	
	LOG(INFO) << "Created " << config_.storage_pools.size() << " storage pools";
	return ErrorCode::OK;
}

Result<nlohmann::json> WorkerService::get_stats() const {
	nlohmann::json stats;
	stats["worker_id"] = config_.worker_id;
	stats["node_id"] = config_.node_id;
	stats["running"] = running_.load();
	
	nlohmann::json pools = nlohmann::json::array();
	std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
	for (const auto& [pool_id, backend] : storage_pools_) {
		nlohmann::json pool_stats;
		pool_stats["pool_id"] = pool_id;
		pool_stats["storage_class"] = static_cast<uint32_t>(backend->get_storage_class());
		pool_stats["stats"] = backend->get_stats();
		pools.push_back(pool_stats);
	}
	stats["storage_pools"] = pools;
	
	return stats;
}

ErrorCode WorkerService::setup_etcd_connection() {
	if (config_.etcd_endpoints.empty()) {
		LOG(ERROR) << "No etcd endpoints configured";
		return ErrorCode::INVALID_CONFIGURATION;
	}
	
	etcd_ = std::make_unique<EtcdService>(config_.etcd_endpoints);
	auto err = etcd_->connect();
	if (err != ErrorCode::OK) {
		LOG(ERROR) << "Failed to connect to etcd: " << error::to_string(err);
		return err;
	}
	
	LOG(INFO) << "Connected to etcd successfully";
	return ErrorCode::OK;
}

ErrorCode WorkerService::register_worker() {
	// Register worker info without lease (persistent)
	std::string worker_key = workers_key();
	std::string worker_info = worker_info_to_json().dump();
	
	auto err = etcd_->put(worker_key, worker_info);
	if (err != ErrorCode::OK) {
		LOG(ERROR) << "Failed to register worker: " << error::to_string(err);
		return err;
	}
	
	// Grant a lease only for heartbeat
	err = etcd_->grant_lease(30, worker_lease_id_);
	if (err != ErrorCode::OK) {
		LOG(ERROR) << "Failed to grant lease for heartbeat: " << error::to_string(err);
		return err;
	}
	
	LOG(INFO) << "Registered worker " << config_.worker_id << " (lease " << worker_lease_id_ << " for heartbeat only)";
	return ErrorCode::OK;
}

ErrorCode WorkerService::register_storage_pools() {
	std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
	
	for (const auto& [pool_id, backend] : storage_pools_) {
		std::string pool_key = worker_pool_key(pool_id);
		std::string pool_info = pool_to_json(pool_id, *backend).dump();
		
		auto err = etcd_->put(pool_key, pool_info);
		if (err != ErrorCode::OK) {
			LOG(ERROR) << "Failed to register storage pool " << pool_id 
			          << ": " << error::to_string(err);
			return err;
		}
		
		LOG(INFO) << "Registered storage pool: " << pool_id;
	}
	
	return ErrorCode::OK;
}

void WorkerService::run_heartbeat_loop() {
    LOG(INFO) << "Starting heartbeat loop";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.heartbeat_interval_sec));
        if (!running_.load()) break;
        
        try {
            // Renew the lease for heartbeat
            auto err = etcd_->keep_alive(worker_lease_id_);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to renew heartbeat lease: " << error::to_string(err);
                // Try to re-grant lease and re-register heartbeat
                err = etcd_->grant_lease(30, worker_lease_id_);
                if (err != ErrorCode::OK) {
                    LOG(WARNING) << "Failed to re-grant heartbeat lease: " << error::to_string(err);
                    continue;
                }
            }
            
            // Update heartbeat key with lease
            std::string hb_key = heartbeat_key();
            std::string timestamp = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
            err = etcd_->put_with_lease(hb_key, timestamp, worker_lease_id_);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to update heartbeat: " << error::to_string(err);
                continue;
            }
            
            LOG(DEBUG) << "Heartbeat sent successfully with lease " << worker_lease_id_;
            
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception in heartbeat loop: " << e.what();
        }
    }
    
    LOG(INFO) << "Heartbeat loop stopped";
}

// Etcd key helpers
std::string WorkerService::make_etcd_key(const std::string& suffix) const {
	return "/blackbird/clusters/" + config_.cluster_id + "/" + suffix;
}

std::string WorkerService::workers_key() const {
	return make_etcd_key("workers/" + config_.worker_id);
}

std::string WorkerService::worker_pool_key(const std::string& pool_id) const {
	return make_etcd_key("workers/" + config_.worker_id + "/memory_pools/" + pool_id);
}

std::string WorkerService::heartbeat_key() const {
	return make_etcd_key("heartbeat/" + config_.worker_id);
}

// JSON serialization helpers
nlohmann::json WorkerService::worker_info_to_json() const {
	nlohmann::json info;
	info["node_id"] = config_.node_id;
	info["rpc_endpoint"] = config_.rpc_endpoint;
	info["ucx_endpoint"] = config_.ucx_endpoint;
	info["interconnects"] = config_.interconnects;
	info["capabilities"] = {
		{"storage_classes", config_.storage_classes},
		{"max_bw_gbps", config_.max_bw_gbps},
		{"numa_node", config_.numa_node}
	};
	info["version"] = config_.version;
	return info;
}

nlohmann::json WorkerService::pool_to_json(const std::string& pool_id, 
                                          const StorageBackend& backend) const {
	nlohmann::json pool;
	pool["id"] = pool_id;
	pool["node_id"] = config_.node_id;
	pool["base_addr"] = backend.get_base_address();
	pool["size"] = backend.get_total_capacity();
	pool["used"] = backend.get_used_capacity();
	pool["storage_class"] = static_cast<uint32_t>(backend.get_storage_class());
	
	// UCX advertisement
	pool["ucx_endpoint"] = config_.ucx_endpoint;
	auto it = pool_ucx_reg_.find(pool_id);
	if (it != pool_ucx_reg_.end()) {
		pool["ucx_rkey_hex"] = it->second.rkey_hex;
		pool["ucx_remote_addr"] = it->second.remote_addr;
	} else {
		pool["ucx_rkey_hex"] = "";
		pool["ucx_remote_addr"] = 0;
	}
	
	return pool;
}

// Helper function to convert string to StorageClass
StorageClass string_to_storage_class(const std::string& str) {
	if (str == "RAM_CPU") return StorageClass::RAM_CPU;
	if (str == "RAM_GPU") return StorageClass::RAM_GPU;
	if (str == "NVME") return StorageClass::NVME;
	if (str == "SSD") return StorageClass::SSD;
	if (str == "HDD") return StorageClass::HDD;
	return StorageClass::RAM_CPU; // default
}

} // namespace blackbird 