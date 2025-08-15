#include "blackbird/keystone/keystone_service.h"

#include <glog/logging.h>
#include <algorithm>
#include <nlohmann/json.hpp>
#include "blackbird/allocation/allocator_interface.h"

namespace blackbird {

// todo: arnavb this is becoming a big file with too much logic, need to split it up
KeystoneService::KeystoneService(const KeystoneConfig& config) 
    : config_(config) {
    LOG(INFO) << "Creating Keystone service with cluster_id: " << config_.cluster_id;
    
    // Initialize allocator with range-based strategy
    auto allocator = allocation::AllocatorFactory::create(allocation::AllocatorFactory::Strategy::RANGE_BASED);
    allocator_ = std::make_unique<allocation::KeystoneAllocatorAdapter>(std::move(allocator));
    LOG(INFO) << "Initialized range-based memory allocator";
}

KeystoneService::~KeystoneService() {
    if (running_.load()) {
        stop();
    }
    
    // Cleanup etcd resources
    if (etcd_) {
        etcd_.reset();
    }
}

ErrorCode KeystoneService::initialize() {
    LOG(INFO) << "Initializing Keystone service...";
    
    if (!config_.etcd_endpoints.empty()) {
        auto err = setup_etcd_integration();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to setup etcd integration: " << error::to_string(err);
            return err;
        }
    } else {
        LOG(WARNING) << "No etcd endpoints configured, running without etcd";
    }
    
    LOG(INFO) << "Keystone service initialized successfully";
    return ErrorCode::OK;
}

ErrorCode KeystoneService::start() {
    LOG(INFO) << "Starting Keystone service...";
    
    if (running_.load()) {
        LOG(WARNING) << "Keystone service is already running";
        return ErrorCode::OK;
    }
    
    running_.store(true);
    view_version_.store(1); 
    
    if (config_.enable_gc) {
        gc_thread_ = std::thread(&KeystoneService::run_garbage_collection, this);
    }
    
    health_check_thread_ = std::thread(&KeystoneService::run_health_checks, this);
    
    if (etcd_) {
        load_existing_state_from_etcd();
        
        setup_watcher_with_error_handling("workers", [this]() { watch_worker_registry_namespace(); });
        setup_watcher_with_error_handling("memory_pools", [this]() { watch_memory_pools_namespace(); });
        setup_watcher_with_error_handling("heartbeats", [this]() { watch_heartbeat_namespace(); });
        
        // Start TTL refresh thread
        etcd_keepalive_thread_ = std::thread(&KeystoneService::run_etcd_keepalive, this);
    }
    
    LOG(INFO) << "Keystone service started successfully";
    return ErrorCode::OK;
}

void KeystoneService::stop() {
	LOG(INFO) << "Stopping Keystone service...";
	
	running_.store(false);
	
	if (gc_thread_.joinable()) {
		gc_thread_.join();
	}
	
	if (health_check_thread_.joinable()) {
		health_check_thread_.join();
	}
	
	if (etcd_keepalive_thread_.joinable()) {
		etcd_keepalive_thread_.join();
	}
	
	// Service registration will automatically expire due to TTL
	
	LOG(INFO) << "Keystone service stopped";
}

// Manual worker removal for cluster management
ErrorCode KeystoneService::remove_worker(const std::string& worker_id) {
    LOG(INFO) << "Manually removing worker from cluster: " << worker_id;
    
    {
        std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
        auto worker_it = workers_.find(worker_id);
        if (worker_it != workers_.end()) {
            for (const auto& [memory_pool_id, memory_pool] : worker_it->second.memory_pools) {
                std::unique_lock<std::shared_mutex> seg_lock(memory_pools_mutex_);
                memory_pools_.erase(memory_pool_id);
            }
            workers_.erase(worker_it);
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> heartbeat_lock(heartbeat_mutex_);
        worker_heartbeats_.erase(worker_id);
    }
    
    if (etcd_) {
        // Todo: have a blacklist to prevent worker re-registration on TTL refresh in ETCD.
        LOG(ERROR) << "Worker removal from ETCD not implemented yet";
        throw std::runtime_error("Worker removal from ETCD not implemented yet");
    }
    
    increment_view_version();
    return ErrorCode::OK;
}

ErrorCode KeystoneService::get_workers_info(std::vector<WorkerInfo>& workers) const {
    std::shared_lock<std::shared_mutex> lock(worker_registry_mutex_);
    
    workers.clear();
    workers.reserve(workers_.size());
    
    auto now = std::chrono::steady_clock::now();
    auto ttl = std::chrono::seconds(config_.worker_heartbeat_ttl_sec);
    
    for (const auto& [worker_id, worker_info] : workers_) {
        if (!worker_info.is_stale(ttl)) {
            workers.push_back(worker_info);
        }
    }
    
    return ErrorCode::OK;
}


ErrorCode KeystoneService::get_memory_pools(std::vector<MemoryPool>& memory_pools) const {
    std::shared_lock<std::shared_mutex> lock(memory_pools_mutex_);
    
    memory_pools.clear();
    memory_pools.reserve(memory_pools_.size());
    
    for (const auto& [memory_pool_id, memory_pool] : memory_pools_) {
        memory_pools.push_back(memory_pool);
    }
    
    return ErrorCode::OK;
}

// Object Management
Result<bool> KeystoneService::object_exists(const ObjectKey& key) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    bool exists = (it != objects_.end()) && !it->second.is_expired();
    
    if (exists) {
        const_cast<ObjectInfo&>(it->second).touch();
    }
    
    return exists;
}

Result<std::vector<CopyPlacement>> KeystoneService::get_workers(const ObjectKey& key) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end() || it->second.is_expired()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    const_cast<ObjectInfo&>(it->second).touch();
    
    return it->second.copies;
}

Result<std::vector<CopyPlacement>> KeystoneService::put_start(const ObjectKey& key, 
                                                         size_t data_size, 
                                                         const WorkerConfig& config) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    std::cout<< "Printing all objects: "<<std::endl;
    for (const auto& [key, object] : objects_) {
        std::cout<< "Object: "<<key << " " << object.copies.size() <<std::endl;
    }
    if (it != objects_.end() && !it->second.is_expired()) {
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }
    
    std::vector<CopyPlacement> copies;
    auto err = allocate_data_copies(key, data_size, config, copies);
    if (err != ErrorCode::OK) {
        return err;
    }
    
    // Initialize object metadata
    ObjectInfo object_info;
    object_info.key = key;
    object_info.size = data_size;
    object_info.created = std::chrono::system_clock::now();
    object_info.last_accessed = object_info.created;
    object_info.config = config;
    
    object_info.copies = copies;
    
    objects_[key] = std::move(object_info);
    
    LOG(INFO) << "Created object: " << key << " with " << copies.size() << " copies";
    
    increment_view_version();
    
    return copies;
}

ErrorCode KeystoneService::put_complete(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    LOG(INFO) << "Completed put operation for key: " << key;
    
    increment_view_version();
    return ErrorCode::OK;
}

ErrorCode KeystoneService::put_cancel(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    // Free any provisional reservations made for this object via allocator
    if (allocator_) {
        auto free_result = allocator_->free_object(key);
        if (free_result != ErrorCode::OK) {
            LOG(WARNING) << "Failed to free memory during cancel for object " << key
                         << ", error=" << static_cast<int>(free_result);
        }
    }
    
    objects_.erase(it);
    
    LOG(INFO) << "Cancelled put operation for key: " << key;
    return ErrorCode::OK;
}

ErrorCode KeystoneService::remove_object(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    if (allocator_) {
        auto free_result = allocator_->free_object(key);
        if (free_result != ErrorCode::OK) {
            LOG(WARNING) << "Failed to free memory for object " << key;
        }
    }
    
    objects_.erase(it);
    
    LOG(INFO) << "Removed object: " << key;
    return ErrorCode::OK;
}

Result<size_t> KeystoneService::remove_all_objects() {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    size_t count = objects_.size();
    objects_.clear();
    
    LOG(INFO) << "Removed all objects (count: " << count << ")";
    return count;
}

// Batch operations
std::vector<Result<bool>> KeystoneService::batch_object_exists(const std::vector<ObjectKey>& keys) {
    std::vector<Result<bool>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(object_exists(key));
    }
    
    return results;
}

std::vector<Result<std::vector<CopyPlacement>>> KeystoneService::batch_get_workers(const std::vector<ObjectKey>& keys) {
    std::vector<Result<std::vector<CopyPlacement>>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(get_workers(key));
    }
    
    return results;
}

std::vector<Result<std::vector<CopyPlacement>>> KeystoneService::batch_put_start(
    const std::vector<std::pair<ObjectKey, std::pair<size_t, WorkerConfig>>>& requests) {
    std::vector<Result<std::vector<CopyPlacement>>> results;
    results.reserve(requests.size());
    
    for (const auto& request : requests) {
        const auto& key = request.first;
        const auto& size = request.second.first;
        const auto& config = request.second.second;
        
        results.push_back(put_start(key, size, config));
    }
    
    return results;
}

std::vector<ErrorCode> KeystoneService::batch_put_complete(const std::vector<ObjectKey>& keys) {
    std::vector<ErrorCode> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(put_complete(key));
    }
    
    return results;
}

std::vector<ErrorCode> KeystoneService::batch_put_cancel(const std::vector<ObjectKey>& keys) {
    std::vector<ErrorCode> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(put_cancel(key));
    }
    
    return results;
}

Result<ClusterStats> KeystoneService::get_cluster_stats() const {
    ClusterStats stats;
    
    {
        std::shared_lock<std::shared_mutex> lock(memory_pools_mutex_);
        stats.total_memory_pools = memory_pools_.size();
        
        for (const auto& [memory_pool_id, memory_pool] : memory_pools_) {
            stats.total_capacity += memory_pool.size;
            stats.used_capacity += memory_pool.used;
        }
    }
    
    {
        std::shared_lock<std::shared_mutex> lock(objects_mutex_);
        stats.total_objects = objects_.size();
    }
    
    if (stats.total_capacity > 0) {
        stats.avg_utilization = static_cast<double>(stats.used_capacity) / stats.total_capacity;
    }
    
    return stats;
}

ErrorCode KeystoneService::setup_etcd_integration() {
    if (config_.etcd_endpoints.empty()) {
        return ErrorCode::OK;  
    }
    
    etcd_ = std::make_unique<EtcdService>(config_.etcd_endpoints);
    auto err = etcd_->connect();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd: " << error::to_string(err);
        return err;
    }
    
    if (config_.service_id.empty()) {
        service_id_ = "keystone-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    } else {
        service_id_ = config_.service_id;
    }
    
    std::string key = "/blackbird/services/blackbird-keystone/" + service_id_;
    err = etcd_->put_with_ttl(key, config_.listen_address, config_.service_registration_ttl_sec);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register keystone service with etcd: " << error::to_string(err);
        return err;
    }
    
    LOG(INFO) << "Registered keystone service with etcd using TTL (service_id: " << service_id_ << ")";
    return ErrorCode::OK;
}

// arnavb: todo make this push based instead of threaded polling for efficiency and instant removal
void KeystoneService::run_garbage_collection() {
    LOG(INFO) << "Starting garbage collection thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.gc_interval_sec));
        
        if (!running_.load()) break;
        
        try {
            std::unique_lock<std::shared_mutex> lock(objects_mutex_);
            auto it = objects_.begin();
            while (it != objects_.end()) {
                if (it->second.is_expired()) {
                    LOG(INFO) << "Removing expired object: " << it->first;
                    
                    // Free allocated memory through allocator
                    if (allocator_) {
                        auto free_result = allocator_->free_object(it->first);
                        if (free_result != ErrorCode::OK) {
                            LOG(WARNING) << "Failed to free memory for object " << it->first;
                        }
                    }
                    
                    it = objects_.erase(it);
                } else {
                    ++it;
                }
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception in garbage collection: " << e.what();
        }
    }
    
    LOG(INFO) << "Garbage collection thread stopped";
}

void KeystoneService::run_health_checks() {
    LOG(INFO) << "Starting health check thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.health_check_interval_sec));
        
        if (!running_.load()) break;
        
        try {
            cleanup_stale_workers();
            
            evict_objects_if_needed();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception in health checks: " << e.what();
        }
    }
    
    LOG(INFO) << "Health check thread stopped";
}

void KeystoneService::run_etcd_keepalive() {
    LOG(INFO) << "Starting etcd TTL refresh thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.service_refresh_interval_sec));
        
        if (!running_.load()) break;
        
        if (etcd_ && !service_id_.empty()) {
            std::string key = "/blackbird/services/blackbird-keystone/" + service_id_;
            auto err = etcd_->put_with_ttl(key, config_.listen_address, config_.service_registration_ttl_sec);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to refresh keystone service registration: " << error::to_string(err);
            } else {
                VLOG(2) << "Refreshed keystone service registration with " << config_.service_registration_ttl_sec << "s TTL";
            }
        }
    }
    
    LOG(INFO) << "Etcd TTL refresh thread stopped";
}

ErrorCode KeystoneService::allocate_data_copies(const ObjectKey& key, size_t data_size,
                                               const WorkerConfig& config,
                                               std::vector<CopyPlacement>& copies) {
    LOG(INFO) << "Allocating data copies for key: " << key << " size: " << data_size;
    copies.clear();
    
    if (!allocator_) {
        LOG(ERROR) << "Allocator not initialized";
        return ErrorCode::INTERNAL_ERROR;
    }
    // arnavb: this is not thread safe, we need to allocate and mark the memory used in thread safe manner.
    std::shared_lock<std::shared_mutex> pools_lock(memory_pools_mutex_);
    std::unordered_map<MemoryPoolId, MemoryPool> current_pools = memory_pools_;
    pools_lock.unlock();
    
    if (current_pools.empty()) {
        LOG(WARNING) << "No memory pools available for allocation";
        return ErrorCode::MEMORY_POOL_NOT_FOUND;
    }
    
    auto result = allocator_->allocate_data_copies(key, data_size, config, current_pools);
    if (!is_ok(result)) {
        LOG(ERROR) << "Allocation failed for key " << key << ": " << static_cast<int>(get_error(result));
        return get_error(result);
    }
    
    copies = std::move(get_value(result));
    LOG(INFO) << "Successfully allocated " << copies.size() << " copies for key " << key;
    
    return ErrorCode::OK;
}

void KeystoneService::cleanup_stale_workers() {
}

void KeystoneService::evict_objects_if_needed() {
    auto stats_result = get_cluster_stats();
    if (!is_ok(stats_result)) {
        LOG(WARNING) << "Failed to get cluster stats for eviction check: " << error::to_string(get_error(stats_result));
        return;
    }
    
    auto stats = get_value(stats_result);
    if (stats.avg_utilization < config_.high_watermark) {
        VLOG(2) << "Storage utilization (" << (stats.avg_utilization * 100.0) 
                << "%) below high watermark (" << (config_.high_watermark * 100.0) 
                << "%), no eviction needed";
        return; 
    }
    
    LOG(INFO) << "Storage utilization (" << (stats.avg_utilization * 100.0) 
              << "%) exceeds high watermark (" << (config_.high_watermark * 100.0) 
              << "%), starting eviction process";
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    if (objects_.empty()) {
        return;
    }
    
    std::vector<std::pair<std::chrono::system_clock::time_point, ObjectKey>> candidates;
    for (const auto& [key, object_info] : objects_) {
        // Only evict objects that are not soft pinned
        if (!object_info.config.enable_soft_pin) {
            candidates.emplace_back(object_info.created, key);
        }
    }
    
    if (candidates.empty()) {
        LOG(WARNING) << "All objects are soft pinned, cannot evict despite high utilization";
        return;
    }
    
    // Sort by age (oldest first)
    std::sort(candidates.begin(), candidates.end());
    
    // Evict a percentage of eligible objects to bring utilization down
    size_t eviction_count = static_cast<size_t>(candidates.size() * config_.eviction_ratio);
    eviction_count = std::max(eviction_count, static_cast<size_t>(1));
    
    size_t evicted = 0;
    for (size_t i = 0; i < eviction_count && i < candidates.size(); ++i) {
        const auto& key = candidates[i].second;
        LOG(INFO) << "Evicting object: " << key;
        objects_.erase(key);
        evicted++;
    }
    
    LOG(INFO) << "Evicted " << evicted << " objects due to high storage utilization";
}

ViewVersionId KeystoneService::increment_view_version() {
    return view_version_.fetch_add(1) + 1;
}

std::string KeystoneService::make_etcd_key(const std::string& suffix) const {
    return "/blackbird/clusters/" + config_.cluster_id + "/" + suffix;
}

std::string KeystoneService::make_worker_key(const std::string& worker_id) const {
    return workers_prefix() + worker_id;
}

std::string KeystoneService::make_heartbeat_key(const std::string& worker_id) const {
    return heartbeat_prefix() + worker_id;
}

std::string KeystoneService::make_worker_memory_pool_key(const std::string& worker_id, const std::string& memory_pool_id) const {
    return workers_prefix() + worker_id + "/memory_pools/" + memory_pool_id;
}

void KeystoneService::upsert_worker_memory_pool_from_json(const std::string& key, const std::string& json_value) {
    try {
        // Parse memory pool metadata
        nlohmann::json doc = nlohmann::json::parse(json_value);
        
        // Extract worker_id and memory_pool_id from key: /workers/<worker_id>/memory_pools/<memory_pool_id>
        auto prefix = workers_prefix();
        if (!key.starts_with(prefix)) return;
        
        std::string suffix = key.substr(prefix.length());
        auto memory_pool_pos = suffix.find("/memory_pools/");
        if (memory_pool_pos == std::string::npos) return;
        
        std::string worker_id = suffix.substr(0, memory_pool_pos);
        // todo: this is hacky and can break if key-length changes in future
        std::string memory_pool_id = suffix.substr(memory_pool_pos + 14); // 14 = strlen("/memory_pools/")
        
        MemoryPool memory_pool;
        memory_pool.id = doc.value("id", memory_pool_id);
        memory_pool.node_id = doc.value("node_id", "");
        memory_pool.base_addr = doc.value("base_addr", 0UL);
        memory_pool.size = doc.value("size", 0UL);
        memory_pool.used = doc.value("used", 0UL);
        memory_pool.storage_class = static_cast<StorageClass>(doc.value("storage_class", static_cast<uint32_t>(StorageClass::STORAGE_UNSPECIFIED)));
        
        if (doc.contains("ucx_address")) {
            memory_pool.ucx_address = doc["ucx_address"].get<UcxAddress>();
        }
        if (doc.contains("ucx_endpoint")) {
            memory_pool.ucx_endpoint = doc.value("ucx_endpoint", std::string{});
        }
        if (doc.contains("ucx_remote_addr")) {
            memory_pool.ucx_remote_addr = doc.value("ucx_remote_addr", 0ULL);
        }
        if (doc.contains("ucx_rkey_hex")) {
            memory_pool.ucx_rkey_hex = doc.value("ucx_rkey_hex", std::string{});
        }
        
        std::unique_lock<std::shared_mutex> worker_lock(worker_registry_mutex_);
        auto worker_it = workers_.find(worker_id);
        if (worker_it != workers_.end()) {
            worker_it->second.memory_pools[memory_pool_id] = memory_pool;
        }
        
        std::unique_lock<std::shared_mutex> pool_lock(memory_pools_mutex_);
        memory_pools_[memory_pool.id] = memory_pool;
        
        worker_lock.unlock();
        pool_lock.unlock();
        
        LOG(INFO) << "Registered memory pool: " << memory_pool_id << " for worker " << worker_id
                  << " (" << memory_pool.node_id << ") size: " << memory_pool.size;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse worker memory pool JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::remove_worker_memory_pool_by_key(const std::string& key) {
    // Extract worker_id and memory_pool_id from key
    auto prefix = workers_prefix();
    if (!key.starts_with(prefix)) return;
    
    std::string suffix = key.substr(prefix.length());
    auto memory_pool_pos = suffix.find("/memory_pools/");
    if (memory_pool_pos == std::string::npos) return;
    
    std::string worker_id = suffix.substr(0, memory_pool_pos);
    std::string memory_pool_id = suffix.substr(memory_pool_pos + 14);
    
    // Remove from worker's memory pool list
    std::unique_lock<std::shared_mutex> worker_lock(worker_registry_mutex_);
    auto worker_it = workers_.find(worker_id);
    if (worker_it != workers_.end()) {
        worker_it->second.memory_pools.erase(memory_pool_id);
    }
    
    // Also remove from global memory pools map
    std::unique_lock<std::shared_mutex> pool_lock(memory_pools_mutex_);
    memory_pools_.erase(memory_pool_id);
    
    worker_lock.unlock();
    pool_lock.unlock();
    
    LOG(INFO) << "Removed memory pool: " << memory_pool_id << " from worker " << worker_id;
}

void KeystoneService::watch_worker_registry_namespace() {
    if (!etcd_) return;
    auto prefix = workers_prefix();
    auto cb = [this](const std::string& key, const std::string& value, bool is_delete) {
        auto workers_len = workers_prefix().length();
        if (key.length() <= workers_len) return;
        
        std::string suffix = key.substr(workers_len);
        auto memory_pool_pos = suffix.find("/memory_pools/");
        
        if (memory_pool_pos != std::string::npos) {
            // This is a memory pool registration: /workers/<worker_id>/memory_pools/<memory_pool_id>
            if (!is_delete) {
                upsert_worker_memory_pool_from_json(key, value);
            } else {
                remove_worker_memory_pool_by_key(key);
            }
        } else {
            // This is a worker registration: /workers/<worker_id>
            if (!is_delete) {
                upsert_worker_from_json(key, value);
            } else {
                remove_worker_by_key(key);
            }
        }
    };
    
    auto err = etcd_->watch_prefix(prefix, cb);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to attach worker registry watcher on prefix: " << prefix;
    } else {
        LOG(INFO) << "Watching worker registry prefix: " << prefix;
    }
}

void KeystoneService::watch_heartbeat_namespace() {
    if (!etcd_) return;
    auto prefix = heartbeat_prefix();
    auto cb = [this](const std::string& key, const std::string& value, bool is_delete) {
        if (is_delete) {
            // worker is dead, clean up everything
            auto pos = key.rfind('/');
            if (pos == std::string::npos || pos + 1 >= key.size()) return;
            std::string worker_id = key.substr(pos + 1);
            
            LOG(WARNING) << "Worker heartbeat expired, cleaning up dead worker: " << worker_id;
            cleanup_dead_worker(worker_id);
        } else {
            update_worker_heartbeat(key, value);
        }
    };
    
    auto err = etcd_->watch_prefix(prefix, cb);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to attach heartbeat watcher on prefix: " << prefix;
    } else {
        LOG(INFO) << "Watching heartbeat prefix: " << prefix;
    }
}

void KeystoneService::watch_memory_pools_namespace() {
    if (!etcd_) return;
    auto prefix = memory_pools_prefix();
    auto cb = [this](const std::string& key, const std::string& value, bool is_delete) {
        if (!is_delete) {
            upsert_memory_pool_from_json(key, value);
        } else {
            remove_memory_pool_by_key(key);
        }
    };
    
    auto err = etcd_->watch_prefix(prefix, cb);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to attach memory pool watcher on prefix: " << prefix;
    } else {
        LOG(INFO) << "Watching memory pool prefix: " << prefix;
    }
}

void KeystoneService::upsert_worker_from_json(const std::string& key, const std::string& json_value) {
    try {
        auto doc = nlohmann::json::parse(json_value);
        
        // Extract worker_id from key: /workers/<worker_id>
        auto pos = key.rfind('/');
        if (pos == std::string::npos || pos + 1 >= key.size()) return;
        std::string worker_id = key.substr(pos + 1);
        
        WorkerInfo info;
        info.worker_id = worker_id;
        info.node_id = doc.value("node_id", std::string{});
        info.endpoint = doc.value("endpoint", std::string{});
        info.last_heartbeat = std::chrono::steady_clock::now();
        
        bool is_new_worker = false;
        {
            std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
            is_new_worker = workers_.find(worker_id) == workers_.end();
            workers_[worker_id] = std::move(info);
        }
        
        increment_view_version();
        
        // Only log registration for genuinely new workers
        if (is_new_worker) {
            LOG(INFO) << "Registered worker: " << worker_id << " at " << info.endpoint;
        } else {
            VLOG(2) << "Updated worker info: " << worker_id;
        }
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse worker JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::remove_worker_by_key(const std::string& key) {
    auto pos = key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= key.size()) return;
    std::string worker_id = key.substr(pos + 1);
    
    {
        std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
        workers_.erase(worker_id);
    }
    
    increment_view_version();
    LOG(INFO) << "Removed worker: " << worker_id;
}

void KeystoneService::upsert_memory_pool_from_json(const std::string& key, const std::string& json_value) {
    try {
        nlohmann::json doc = nlohmann::json::parse(json_value);
        
        // Extract memory_pool_id from key: /memory_pools/<memory_pool_id>
        auto pos = key.find_last_of('/');
        if (pos == std::string::npos) return;
        
        std::string memory_pool_id = key.substr(pos + 1);
        
        MemoryPool seg;
        seg.id = doc.value("id", memory_pool_id);
        seg.node_id = doc.value("node_id", "");
        seg.base_addr = doc.value("base_addr", 0UL);
        seg.size = doc.value("size", 0UL);
        seg.used = doc.value("used", 0UL);
        
        if (doc.contains("ucx_address")) {
            seg.ucx_address = doc["ucx_address"].get<UcxAddress>();
        }
        if (doc.contains("ucx_endpoint")) {
            seg.ucx_endpoint = doc.value("ucx_endpoint", std::string{});
        }
        if (doc.contains("ucx_remote_addr")) {
            seg.ucx_remote_addr = doc.value("ucx_remote_addr", 0ULL);
        }
        if (doc.contains("ucx_rkey_hex")) {
            seg.ucx_rkey_hex = doc.value("ucx_rkey_hex", std::string{});
        }
        
        std::unique_lock<std::shared_mutex> lock(memory_pools_mutex_);
        memory_pools_[seg.id] = seg;
        lock.unlock();
        
        LOG(INFO) << "Registered memory pool from etcd: " << seg.id << " (" << seg.node_id << ") size: " << seg.size;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse memory pool JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::remove_memory_pool_by_key(const std::string& key) {
    // Extract memory_pool_id from key
    auto pos = key.find_last_of('/');
    if (pos == std::string::npos) return;
    
    std::string memory_pool_id = key.substr(pos + 1);
    
    // Remove from memory_pools map
    std::unique_lock<std::shared_mutex> lock(memory_pools_mutex_);
    memory_pools_.erase(memory_pool_id);
    lock.unlock();
    
    LOG(INFO) << "Removed memory pool from etcd: " << memory_pool_id;
}

void KeystoneService::update_worker_heartbeat(const std::string& key, const std::string& json_value) {
    try {
        // Extract worker_id from key: /heartbeat/<worker_id>
        auto pos = key.rfind('/');
        if (pos == std::string::npos || pos + 1 >= key.size()) return;
        std::string worker_id = key.substr(pos + 1);
        
        auto now = std::chrono::steady_clock::now();
        
        // Update heartbeat timestamp
        {
            std::unique_lock<std::shared_mutex> lock(heartbeat_mutex_);
            worker_heartbeats_[worker_id] = now;
        }
        
        // Also update worker's last_heartbeat if it exists
        {
            std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
            auto worker_it = workers_.find(worker_id);
            if (worker_it != workers_.end()) {
                worker_it->second.last_heartbeat = now;
            }
        }
        
        VLOG(2) << "Updated heartbeat for worker: " << worker_id;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse heartbeat JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::load_existing_state_from_etcd() {
    if (!etcd_) {
        LOG(WARNING) << "Etcd not initialized, cannot load existing state.";
        return;
    }

    std::vector<std::string> worker_keys, worker_values;
    auto err = etcd_->get_with_prefix(workers_prefix(), worker_keys, worker_values);
    if (err == ErrorCode::OK) {
        for (size_t i = 0; i < worker_keys.size(); ++i) {
            const std::string& key = worker_keys[i];
            const std::string& value = worker_values[i];
            
            if (key.find("/memory_pools/") != std::string::npos) {
                upsert_worker_memory_pool_from_json(key, value);
            } else {
                upsert_worker_from_json(key, value);
            }
        }
        LOG(INFO) << "Loaded " << worker_keys.size() << " worker entries from etcd.";
    } else {
        LOG(WARNING) << "Failed to load workers from etcd: " << error::to_string(err);
    }

    
    // Load memory pools
    std::vector<std::string> memory_pool_keys, memory_pool_values;
    err = etcd_->get_with_prefix(memory_pools_prefix(), memory_pool_keys, memory_pool_values);
    if (err == ErrorCode::OK) {
        for (size_t i = 0; i < memory_pool_keys.size(); ++i) {
            upsert_memory_pool_from_json(memory_pool_keys[i], memory_pool_values[i]);
        }
        LOG(INFO) << "Loaded " << memory_pool_keys.size() << " memory pools from etcd.";
    } else {
        LOG(WARNING) << "Failed to load memory pools from etcd: " << error::to_string(err);
    }
}

void KeystoneService::setup_watcher_with_error_handling(const std::string& watcher_name, const std::function<void()>& watcher_func) {
    try {
        watcher_func();
        LOG(INFO) << "Successfully set up " << watcher_name << " watcher.";
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to set up " << watcher_name << " watcher: " << e.what();
    }
}

void KeystoneService::cleanup_dead_worker(const std::string& worker_id) {
    LOG(INFO) << "Cleaning up dead worker: " << worker_id;
    
    std::vector<std::string> memory_pool_ids_to_remove;
    {
        std::unique_lock<std::shared_mutex> worker_lock(worker_registry_mutex_);
        auto worker_it = workers_.find(worker_id);
        if (worker_it != workers_.end()) {
            for (const auto& [memory_pool_id, memory_pool] : worker_it->second.memory_pools) {
                memory_pool_ids_to_remove.push_back(memory_pool_id);
            }
            workers_.erase(worker_it);
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> pool_lock(memory_pools_mutex_);
        for (const auto& memory_pool_id : memory_pool_ids_to_remove) {
            memory_pools_.erase(memory_pool_id);
        }
    }
    
    // Remove heartbeat tracking
    {
        std::unique_lock<std::shared_mutex> heartbeat_lock(heartbeat_mutex_);
        worker_heartbeats_.erase(worker_id);
    }
    
    // Clean up persistent etcd keys (worker info and storage pools)
    if (etcd_) {
        std::string worker_key = make_worker_key(worker_id);
        auto err = etcd_->del(worker_key);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to delete worker key from etcd: " << worker_key;
        }
        
        for (const auto& memory_pool_id : memory_pool_ids_to_remove) {
            std::string pool_key = make_worker_memory_pool_key(worker_id, memory_pool_id);
            err = etcd_->del(pool_key);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to delete storage pool key from etcd: " << pool_key;
            }
        }
    }
    
    increment_view_version();
    LOG(INFO) << "Successfully cleaned up dead worker: " << worker_id 
              << " (removed " << memory_pool_ids_to_remove.size() << " storage pools)";
}

}  // namespace blackbird 