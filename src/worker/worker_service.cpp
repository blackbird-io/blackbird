#include "blackbird/worker/worker_service.h"

#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <thread>

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
    allocation_watcher_thread_ = std::thread(&WorkerService::run_allocation_watcher, this);
    
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
    
    if (allocation_watcher_thread_.joinable()) {
        allocation_watcher_thread_.join();
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

void WorkerService::run_allocation_watcher() {
    LOG(INFO) << "Starting allocation watcher";
    
    std::string prefix = allocations_prefix();
    auto callback = [this](const std::string& key, const std::string& value, bool is_delete) {
        if (!is_delete && running_.load()) {
            handle_allocation_intent(key, value);
        }
    };
    
    auto err = etcd_->watch_prefix(prefix, callback);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to setup allocation watcher: " << error::to_string(err);
        return;
    }
    
    LOG(INFO) << "Allocation watcher setup completed";
    
    // Keep the watcher alive
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.allocation_poll_interval_ms));
    }
    
    LOG(INFO) << "Allocation watcher stopped";
}

void WorkerService::handle_allocation_intent(const std::string& key, const std::string& value) {
    try {
        auto intent = nlohmann::json::parse(value);
        
        // Check if this allocation is for us
        std::string worker_id = intent.value("worker_id", "");
        if (worker_id != config_.worker_id) {
            return; // Not for us
        }
        
        std::string state = intent.value("state", "");
        if (state != "planned") {
            return; // Already processed
        }
        
        std::string pool_id = intent.value("pool_id", "");
        uint64_t requested_size = intent.value("requested_size", 0UL);
        
        LOG(INFO) << "Processing allocation intent for pool " << pool_id 
                  << ", size " << requested_size;
        
        // Find the storage backend
        std::shared_lock<std::shared_mutex> lock(storage_pools_mutex_);
        auto pool_it = storage_pools_.find(pool_id);
        if (pool_it == storage_pools_.end()) {
            LOG(WARNING) << "Storage pool " << pool_id << " not found";
            
            // Update intent to failed
            intent["state"] = "failed";
            intent["error"] = {{"code", static_cast<uint32_t>(ErrorCode::MEMORY_POOL_NOT_FOUND)}, 
                              {"message", "Storage pool not found"}};
            etcd_->put(key, intent.dump());
            return;
        }
        
        // Try to reserve space
        auto reservation_result = pool_it->second->reserve_shard(requested_size);
        if (!is_ok(reservation_result)) {
            LOG(WARNING) << "Failed to reserve shard in pool " << pool_id 
                        << ": " << error::to_string(get_error(reservation_result));
            
            // Update intent to failed
            intent["state"] = "failed";
            intent["error"] = {{"code", static_cast<uint32_t>(get_error(reservation_result))}, 
                              {"message", "Failed to reserve shard"}};
            etcd_->put(key, intent.dump());
            return;
        }
        
        auto token = get_value(reservation_result);
        
        // Update intent to reserved with reservation details
        intent["state"] = "reserved";
        intent["reservation"] = {
            {"rkey", token.rkey},
            {"remote_addr", token.remote_addr},
            {"size", token.size},
            {"token", token.token_id},
            {"expires_at_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
                token.expires_at.time_since_epoch()).count()}
        };
        
        auto err = etcd_->put(key, intent.dump());
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to update allocation intent: " << error::to_string(err);
            // Abort the reservation since we couldn't update etcd
            pool_it->second->abort_shard(token);
        } else {
            LOG(INFO) << "Successfully reserved shard for allocation intent";
        }
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception handling allocation intent: " << e.what();
    }
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

std::string WorkerService::allocations_prefix() const {
    return make_etcd_key("allocations/");
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

} // namespace blackbird 