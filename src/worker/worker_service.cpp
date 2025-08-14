#include "blackbird/worker/worker_service.h"

#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <thread>

#include "blackbird/worker/storage/ram_backend.h"

namespace blackbird {

WorkerService::WorkerService(const WorkerServiceConfig& config) 
    : config_(config) {
    LOG(INFO) << "Creating WorkerService with worker_id: " << config_.worker_id;
}

WorkerService::~WorkerService() {
    if (running_.load()) {
        stop();
    }
    
    // Cleanup etcd resources
    if (etcd_ && worker_lease_id_ != 0) {
        etcd_->revoke_lease(worker_lease_id_);
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
    std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
    for (const auto& [pool_id, backend] : storage_pools_) {
        err = backend->initialize();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize storage pool " << pool_id 
                      << ": " << error::to_string(err);
            return err;
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
    
    // Shutdown storage backends
    std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
    for (const auto& [pool_id, backend] : storage_pools_) {
        backend->shutdown();
    }
    
    // Revoke etcd lease (this will automatically remove our keys)
    if (etcd_ && worker_lease_id_ != 0) {
        etcd_->revoke_lease(worker_lease_id_);
        worker_lease_id_ = 0;
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
    // Grant lease for worker registration
    auto err = etcd_->grant_lease(config_.lease_ttl_sec, worker_lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to grant lease: " << error::to_string(err);
        return err;
    }
    
    // Register worker with lease
    std::string worker_key = workers_key();
    std::string worker_info = worker_info_to_json().dump();
    
    err = etcd_->put_with_lease(worker_key, worker_info, worker_lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register worker: " << error::to_string(err);
        return err;
    }
    
    // Start keepalive for the lease
    err = etcd_->keep_alive(worker_lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to start lease keepalive: " << error::to_string(err);
        return err;
    }
    
    LOG(INFO) << "Registered worker " << config_.worker_id 
              << " with lease " << worker_lease_id_;
    return ErrorCode::OK;
}

ErrorCode WorkerService::register_storage_pools() {
    std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
    
    for (const auto& [pool_id, backend] : storage_pools_) {
        std::string pool_key = worker_pool_key(pool_id);
        std::string pool_info = pool_to_json(pool_id, *backend).dump();
        
        auto err = etcd_->put_with_lease(pool_key, pool_info, worker_lease_id_);
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
            // Update heartbeat
            std::string hb_key = heartbeat_key();
            std::string hb_value = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
            
            auto err = etcd_->put_with_lease(hb_key, hb_value, worker_lease_id_);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to update heartbeat: " << error::to_string(err);
            }
            
            // Update storage pool usage
            std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
            for (const auto& [pool_id, backend] : storage_pools_) {
                std::string pool_key = worker_pool_key(pool_id);
                std::string pool_info = pool_to_json(pool_id, *backend).dump();
                
                err = etcd_->put_with_lease(pool_key, pool_info, worker_lease_id_);
                if (err != ErrorCode::OK) {
                    LOG(WARNING) << "Failed to update storage pool " << pool_id 
                                << ": " << error::to_string(err);
                }
            }
            
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
    
    // UCX address placeholder (empty for now)
    pool["ucx_address"] = nlohmann::json::array();
    
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

ErrorCode load_worker_config_from_file(const std::string& config_file, WorkerServiceConfig& config) {
    // For now, just provide a simple default configuration
    // TODO: Implement YAML parsing later
    LOG(INFO) << "Loading default configuration (YAML parsing TODO)";
    
    config.cluster_id = "blackbird_cluster";
    config.etcd_endpoints = "localhost:2379";
    config.rpc_endpoint = "0.0.0.0:0";
    config.ucx_endpoint = "";
    config.interconnects = {"tcp"};
    config.storage_classes = {StorageClass::RAM_CPU};
    config.max_bw_gbps = 10.0;
    config.numa_node = 0;
    config.version = "1.0.0";
    config.lease_ttl_sec = 30;
    config.heartbeat_interval_sec = 10;
    config.allocation_poll_interval_ms = 100;
    
    // Create a default RAM pool
    WorkerServiceConfig::PoolConfig pool;
    pool.pool_id = "ram_pool_0";
    pool.storage_class = StorageClass::RAM_CPU;
    pool.size_bytes = 2147483648; // 2GB
    config.storage_pools.push_back(pool);
    
    return ErrorCode::OK;
}

} // namespace blackbird 