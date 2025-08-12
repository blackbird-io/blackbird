#include "blackbird/keystone_service.h"

#include <glog/logging.h>
#include <algorithm>
#include <nlohmann/json.hpp>

namespace blackbird {

KeystoneService::KeystoneService(const KeystoneConfig& config) 
    : config_(config) {
    LOG(INFO) << "Creating Keystone service with cluster_id: " << config_.cluster_id;
}

KeystoneService::~KeystoneService() {
    if (running_.load()) {
        stop();
    }
    
    // Cleanup etcd resources
    if (etcd_) {
        // This will cleanup watchers and leases
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
    view_version_.store(1);  // Start with version 1
    
    // Start background threads
    if (config_.enable_gc) {
        gc_thread_ = std::thread(&KeystoneService::run_garbage_collection, this);
    }
    
    health_check_thread_ = std::thread(&KeystoneService::run_health_checks, this);
    
    if (etcd_) {
        load_existing_state_from_etcd();
        
        // Removed clients watcher (no client persistence)
        setup_watcher_with_error_handling("workers", [this]() { watch_worker_registry_namespace(); });
        setup_watcher_with_error_handling("heartbeats", [this]() { watch_heartbeat_namespace(); });
        setup_watcher_with_error_handling("chunks", [this]() { watch_chunks_namespace(); });
        
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
    
    if (etcd_ && keystone_lease_id_ != 0) {
        etcd_->revoke_lease(keystone_lease_id_);
        keystone_lease_id_ = 0;
    }
    
    LOG(INFO) << "Keystone service stopped";
}

// Emergency worker removal for cluster management
ErrorCode KeystoneService::remove_worker(const std::string& worker_id) {
    LOG(INFO) << "Manually removing worker from cluster: " << worker_id;
    
    // Remove from local tracking
    {
        std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
        auto worker_it = workers_.find(worker_id);
        if (worker_it != workers_.end()) {
            // Remove all segments owned by this worker
            for (const auto& [segment_id, segment] : worker_it->second.chunks) {
                std::unique_lock<std::shared_mutex> seg_lock(chunks_mutex_);
                chunks_.erase(segment_id);
            }
            workers_.erase(worker_it);
        }
    }
    
    // Remove heartbeat tracking
    {
        std::unique_lock<std::shared_mutex> heartbeat_lock(heartbeat_mutex_);
        worker_heartbeats_.erase(worker_id);
    }
    
    // arnavb
    // Remove from ETCD (this will trigger watchers)
    if (etcd_) {
        // TODO: Implement delete_key in EtcdService
        // std::string worker_key = make_worker_key(worker_id);
        // auto err = etcd_->delete_key(worker_key);
        // if (err != ErrorCode::OK) {
        //     LOG(WARNING) << "Failed to remove worker from ETCD: " << error::to_string(err);
        // }
        
        // Also remove heartbeat
        // std::string heartbeat_key = make_heartbeat_key(worker_id);
        // etcd_->delete_key(heartbeat_key);  // Best effort, ignore errors
        LOG(INFO) << "Worker removal from ETCD not implemented yet";
    }
    
    increment_view_version();
    LOG(INFO) << "Successfully removed worker: " << worker_id;
    return ErrorCode::OK;
}

// Get worker information
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

// Segment Management - REMOVED: Workers register chunks directly to etcd  
// Keystone only watches for changes and maintains read-only view
// arnavb remove this if needed
ErrorCode KeystoneService::get_chunks(std::vector<Segment>& segments) const {
    std::shared_lock<std::shared_mutex> lock(chunks_mutex_);
    
    segments.clear();
    segments.reserve(chunks_.size());
    
    for (const auto& [segment_id, segment] : chunks_) {
        segments.push_back(segment);
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
    
    // Update last access time
    const_cast<ObjectInfo&>(it->second).touch();
    
    return it->second.copies;
}

Result<std::vector<CopyPlacement>> KeystoneService::put_start(const ObjectKey& key, 
                                                         size_t data_size, 
                                                         const WorkerConfig& config) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    // Check if object already exists
    auto it = objects_.find(key);
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
    object_info.copies = std::move(copies);
    
    objects_[key] = std::move(object_info);
    
    LOG(INFO) << "Created object: " << key << " with " << copies.size() << " copies";
    
    increment_view_version();
    
    return object_info.copies;
}

ErrorCode KeystoneService::put_complete(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    // Mark operation as complete - using YLT struct-based approach
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

// Metrics and status
Result<ClusterStats> KeystoneService::get_cluster_stats() const {
    ClusterStats stats;
    
    // Segment statistics
    {
        std::shared_lock<std::shared_mutex> lock(chunks_mutex_);
        stats.total_segments = chunks_.size();
        
        for (const auto& [segment_id, segment] : chunks_) {
            stats.total_capacity += segment.size;
            stats.used_capacity += segment.used;
        }
    }
    
    // Object statistics
    {
        std::shared_lock<std::shared_mutex> lock(objects_mutex_);
        stats.total_objects = objects_.size();
    }
    
    // Calculate utilization
    if (stats.total_capacity > 0) {
        stats.utilization = static_cast<double>(stats.used_capacity) / stats.total_capacity;
    }
    
    return stats;
}

// Private methods
ErrorCode KeystoneService::setup_etcd_integration() {
    if (config_.etcd_endpoints.empty()) {
        return ErrorCode::OK;  // No etcd configured
    }
    
    etcd_ = std::make_unique<EtcdService>(config_.etcd_endpoints);
    auto err = etcd_->connect();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd: " << error::to_string(err);
        return err;
    }
    
    // Register keystone service with etcd
    std::string service_id = "keystone-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    err = etcd_->register_service("blackbird-keystone", service_id, config_.listen_address, 
                                 config_.client_ttl_sec, keystone_lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register keystone service with etcd: " << error::to_string(err);
        return err;
    }
    
    LOG(INFO) << "Registered keystone service with etcd (lease_id: " << keystone_lease_id_ << ")";
    return ErrorCode::OK;
}

void KeystoneService::run_garbage_collection() {
    LOG(INFO) << "Starting garbage collection thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(30));  // Run every 30 seconds
        
        if (!running_.load()) break;
        
        try {
            // Clean up expired objects
            std::unique_lock<std::shared_mutex> lock(objects_mutex_);
            auto it = objects_.begin();
            while (it != objects_.end()) {
                if (it->second.is_expired()) {
                    LOG(INFO) << "GC: Removing expired object: " << it->first;
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
        std::this_thread::sleep_for(std::chrono::seconds(10));  // Check every 10 seconds
        
        if (!running_.load()) break;
        
        try {
            // Clean up stale workers that stopped sending heartbeats
            cleanup_stale_workers();
            
            // Check storage utilization and evict objects if above high watermark
            evict_objects_if_needed();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception in health checks: " << e.what();
        }
    }
    
    LOG(INFO) << "Health check thread stopped";
}

void KeystoneService::run_etcd_keepalive() {
    LOG(INFO) << "Starting etcd keepalive thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.client_ttl_sec / 3));  // Keep alive at 1/3 of TTL
        
        if (!running_.load()) break;
        
        if (etcd_ && keystone_lease_id_ != 0) {
            auto err = etcd_->keep_alive(keystone_lease_id_);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to keep alive etcd lease: " << error::to_string(err);
            }
        }
    }
    
    LOG(INFO) << "Etcd keepalive thread stopped";
}

ErrorCode KeystoneService::allocate_data_copies(const ObjectKey& key, size_t data_size,
                                               const WorkerConfig& config,
                                               std::vector<CopyPlacement>& copies) {
    copies.clear();
    
    // Determine effective parameters
    size_t replication_factor = config.replication_factor;
    size_t max_workers_per_copy = config.max_workers_per_copy;
    
    // Allocate each copy
    for (size_t copy_id = 0; copy_id < replication_factor; ++copy_id) {
        CopyPlacement copy_placement;
        copy_placement.copy_index = copy_id;
        
        std::vector<ShardPlacement> shards_for_copy;
        auto err = allocate_shards_for_copy(key, data_size, copy_id, max_workers_per_copy, shards_for_copy);
        if (err != ErrorCode::OK) {
            return err;
        }
        
        // Add shards to the copy using struct field assignment
        copy_placement.shards = std::move(shards_for_copy);
        
        copies.push_back(std::move(copy_placement));
    }
    
    return ErrorCode::OK;
}

ErrorCode KeystoneService::allocate_shards_for_copy(const ObjectKey& key, size_t data_size,
                                                   size_t copy_id, size_t max_workers,
                                                   std::vector<ShardPlacement>& shards) {
    shards.clear();
    
    // Simple sharding logic - divide data across available workers
    std::shared_lock<std::shared_mutex> chunks_lock(chunks_mutex_);
    
    std::vector<SegmentId> available_chunks;
    for (const auto& [chunk_id, chunk] : chunks_) {
        if (chunk.available() > 0) {
            available_chunks.push_back(chunk_id);
        }
    }
    chunks_lock.unlock();
    
    if (available_chunks.empty()) {
        return ErrorCode::SEGMENT_NOT_FOUND;  // Use existing error code
    }
    
    // Limit workers per copy
    size_t workers_to_use = std::min(max_workers, available_chunks.size());
    size_t shard_size = data_size / workers_to_use;
    size_t remainder = data_size % workers_to_use;
    
    for (size_t i = 0; i < workers_to_use; ++i) {
        ShardPlacement shard;
        shard.worker_id = available_chunks[i];
        shard.pool_id = "default"; // TODO: implement proper pool selection
        
        // Use preferred storage class if available, otherwise default to RAM_CPU
        StorageClass storage_class = StorageClass::RAM_CPU;
        // TODO: Implement preferred_classes selection logic based on available workers
        // For now, use default storage class since we don't have per-operation config here
        shard.storage_class = storage_class;
        
        size_t current_shard_size = shard_size + (i < remainder ? 1 : 0);
        shard.length = current_shard_size;
        
        // TODO: Set proper endpoint and location details based on available chunks
        // For now, set default memory location
        shard.location = MemoryLocation{0, 0, current_shard_size};
        
        shards.push_back(std::move(shard));
    }
    
    return ErrorCode::OK;
}

void KeystoneService::cleanup_stale_workers() {
    std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
    
    auto now = std::chrono::steady_clock::now();
    auto ttl = std::chrono::seconds(config_.worker_heartbeat_ttl_sec);
    
    auto it = workers_.begin();
    while (it != workers_.end()) {
        if (it->second.is_stale(ttl)) {
            LOG(INFO) << "Removing stale worker: " << it->first << " (" << it->second.node_id << ")";
            it = workers_.erase(it);
            increment_view_version();
        } else {
            ++it;
        }
    }
}

void KeystoneService::evict_objects_if_needed() {
    // First check if eviction is actually needed
    auto stats_result = get_cluster_stats();
    if (!is_ok(stats_result)) {
        LOG(WARNING) << "Failed to get cluster stats for eviction check: " << error::to_string(get_error(stats_result));
        return;
    }
    
    auto stats = get_value(stats_result);
    if (stats.utilization < config_.high_watermark) {
        VLOG(2) << "Storage utilization (" << (stats.utilization * 100.0) 
                << "%) below high watermark (" << (config_.high_watermark * 100.0) 
                << "%), no eviction needed";
        return;  // No eviction needed
    }
    
    LOG(INFO) << "Storage utilization (" << (stats.utilization * 100.0) 
              << "%) exceeds high watermark (" << (config_.high_watermark * 100.0) 
              << "%), starting eviction process";
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    if (objects_.empty()) {
        return;
    }
    
    // Build a list of candidates for eviction (non-soft-pinned objects)
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
    eviction_count = std::max(eviction_count, static_cast<size_t>(1)); // Evict at least 1 object
    
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

std::string KeystoneService::make_worker_chunk_key(const std::string& worker_id, const std::string& chunk_id) const {
    return workers_prefix() + worker_id + "/chunks/" + chunk_id;
}

std::string KeystoneService::make_heartbeat_key(const std::string& worker_id) const {
    return heartbeat_prefix() + worker_id;
}

// === Worker registry watchers ===
void KeystoneService::watch_worker_registry_namespace() {
    if (!etcd_) return;
    auto prefix = workers_prefix();
    auto cb = [this](const std::string& key, const std::string& value, bool is_delete) {
        // Determine if this is a worker registration or chunk registration
        auto workers_len = workers_prefix().length();
        if (key.length() <= workers_len) return;
        
        std::string suffix = key.substr(workers_len);
        auto chunk_pos = suffix.find("/chunks/");
        
        if (chunk_pos != std::string::npos) {
            // This is a chunk registration: /workers/<worker_id>/chunks/<chunk_id>
            if (!is_delete) {
                upsert_worker_chunk_from_json(key, value);
            } else {
                remove_worker_chunk_by_key(key);
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
        if (!is_delete) {
            update_worker_heartbeat(key, value);
        }
        // Note: we don't remove heartbeats on delete; they'll age out naturally
    };
    
    auto err = etcd_->watch_prefix(prefix, cb);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to attach heartbeat watcher on prefix: " << prefix;
    } else {
        LOG(INFO) << "Watching heartbeat prefix: " << prefix;
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
        
        {
            std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
            workers_[worker_id] = std::move(info);
        }
        
        increment_view_version();
        LOG(INFO) << "Registered worker: " << worker_id << " at " << info.endpoint;
        
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

void KeystoneService::upsert_worker_chunk_from_json(const std::string& key, const std::string& json_value) {
    try {
        auto doc = nlohmann::json::parse(json_value);
        
        // Extract worker_id and chunk_id from key: /workers/<worker_id>/chunks/<chunk_id>
        auto workers_len = workers_prefix().length();
        if (key.length() <= workers_len) return;
        
        std::string suffix = key.substr(workers_len);
        auto chunk_pos = suffix.find("/chunks/");
        if (chunk_pos == std::string::npos) return;
        
        std::string worker_id = suffix.substr(0, chunk_pos);
        std::string chunk_id = suffix.substr(chunk_pos + 8); // 8 = strlen("/chunks/")
        
        // Parse segment metadata using Mooncake's segment schema
        Segment seg;
        seg.id = doc.value("id", chunk_id); // fallback to chunk_id if not specified
        seg.node_id = doc.value("node_id", std::string{});
        seg.base_addr = doc.value("base_addr", static_cast<uintptr_t>(0));
        seg.size = doc.value("size", static_cast<size_t>(0));
        seg.used = doc.value("used", static_cast<size_t>(0));
        auto addr_vec = doc.value("ucx_address", std::vector<uint8_t>{});
        seg.ucx_address = addr_vec;
        
        {
            std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
            auto worker_it = workers_.find(worker_id);
            if (worker_it != workers_.end()) {
                worker_it->second.chunks[chunk_id] = seg;
            }
        }
        
        // Also update the global segments map for compatibility
        {
            std::unique_lock<std::shared_mutex> seg_lock(chunks_mutex_);
            chunks_[seg.id] = seg;
        }
        
        increment_view_version();
        LOG(INFO) << "Registered chunk: " << chunk_id << " for worker " << worker_id 
                  << " (size: " << seg.size << " bytes)";
                  
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse worker chunk JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::remove_worker_chunk_by_key(const std::string& key) {
    // Extract worker_id and chunk_id from key
    auto workers_len = workers_prefix().length();
    if (key.length() <= workers_len) return;
    
    std::string suffix = key.substr(workers_len);
    auto chunk_pos = suffix.find("/chunks/");
    if (chunk_pos == std::string::npos) return;
    
    std::string worker_id = suffix.substr(0, chunk_pos);
    std::string chunk_id = suffix.substr(chunk_pos + 8);
    
    {
        std::unique_lock<std::shared_mutex> lock(worker_registry_mutex_);
        auto worker_it = workers_.find(worker_id);
        if (worker_it != workers_.end()) {
            worker_it->second.chunks.erase(chunk_id);
        }
    }
    
    // Also remove from global segments map
    {
        std::unique_lock<std::shared_mutex> seg_lock(chunks_mutex_);
        chunks_.erase(chunk_id);
    }
    
    increment_view_version();
    LOG(INFO) << "Removed chunk: " << chunk_id << " from worker " << worker_id;
}

// === Client registry watchers ===
void KeystoneService::watch_chunks_namespace() {
    if (!etcd_) return;
    auto prefix = chunks_prefix();
    auto cb = [this](const std::string& key, const std::string& value, bool is_delete) {
        if (!is_delete) {
            upsert_chunk_from_json(key, value);
        } else {
            remove_chunk_by_key(key);
        }
    };
    
    auto err = etcd_->watch_prefix(prefix, cb);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to attach chunk watcher on prefix: " << prefix;
    } else {
        LOG(INFO) << "Watching chunk prefix: " << prefix;
    }
}

void KeystoneService::upsert_chunk_from_json(const std::string& key, const std::string& json_value) {
    try {
        auto doc = nlohmann::json::parse(json_value);
        
        // Extract segment_id from key: /segments/<segment_id>
        auto pos = key.rfind('/');
        if (pos == std::string::npos || pos + 1 >= key.size()) return;
        std::string segment_id = key.substr(pos + 1);
        
        Segment seg;
        seg.id = doc.value("id", segment_id);
        seg.node_id = doc.value("node_id", std::string{});
        seg.base_addr = doc.value("base_addr", static_cast<uintptr_t>(0));
        seg.size = doc.value("size", static_cast<size_t>(0));
        seg.used = doc.value("used", static_cast<size_t>(0));
        auto addr_vec = doc.value("ucx_address", std::vector<uint8_t>{});
        seg.ucx_address = addr_vec;
        
        {
            std::unique_lock<std::shared_mutex> lock(chunks_mutex_);
            chunks_[seg.id] = seg;
        }
        
        increment_view_version();
        LOG(INFO) << "Registered segment from etcd: " << seg.id << " (" << seg.node_id << ") size: " << seg.size;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse segment JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::remove_chunk_by_key(const std::string& key) {
    auto pos = key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= key.size()) return;
    std::string chunk_id = key.substr(pos + 1);
    
    {
        std::unique_lock<std::shared_mutex> lock(chunks_mutex_);
        chunks_.erase(chunk_id);
    }
    
    increment_view_version();
    LOG(INFO) << "Removed chunk from etcd: " << chunk_id;
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

    // Removed clients load (no client persistence)

    // Load segments  
    std::vector<std::string> segment_keys, segment_values;
    auto err = etcd_->get_with_prefix(chunks_prefix(), segment_keys, segment_values);
    if (err == ErrorCode::OK) {
        for (size_t i = 0; i < segment_keys.size(); ++i) {
            upsert_chunk_from_json(segment_keys[i], segment_values[i]);
        }
        LOG(INFO) << "Loaded " << segment_keys.size() << " segments from etcd.";
    } else {
        LOG(WARNING) << "Failed to load segments from etcd: " << error::to_string(err);
    }

    // Load workers and worker chunks
    std::vector<std::string> worker_keys, worker_values;
    err = etcd_->get_with_prefix(workers_prefix(), worker_keys, worker_values);
    if (err == ErrorCode::OK) {
        for (size_t i = 0; i < worker_keys.size(); ++i) {
            const std::string& key = worker_keys[i];
            const std::string& value = worker_values[i];
            
            // Check if this is a chunk or worker registration
            if (key.find("/chunks/") != std::string::npos) {
                upsert_worker_chunk_from_json(key, value);
            } else {
                upsert_worker_from_json(key, value);
            }
        }
        LOG(INFO) << "Loaded " << worker_keys.size() << " worker entries from etcd.";
    } else {
        LOG(WARNING) << "Failed to load workers from etcd: " << error::to_string(err);
    }

    // Load worker heartbeats
    std::vector<std::string> heartbeat_keys, heartbeat_values;
    err = etcd_->get_with_prefix(heartbeat_prefix(), heartbeat_keys, heartbeat_values);
    if (err == ErrorCode::OK) {
        for (size_t i = 0; i < heartbeat_keys.size(); ++i) {
            update_worker_heartbeat(heartbeat_keys[i], heartbeat_values[i]);
        }
        LOG(INFO) << "Loaded " << heartbeat_keys.size() << " worker heartbeats from etcd.";
    } else {
        LOG(WARNING) << "Failed to load worker heartbeats from etcd: " << error::to_string(err);
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

}  // namespace blackbird 