#include "blackbird/keystone_service.h"

#include <glog/logging.h>
#include <algorithm>
#include <nlohmann/json.hpp>

// Keystone is the master node for blackbird.   
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
        // CRITICAL: Load existing state from etcd BEFORE starting any threads or watchers
        // to avoid missing existing registrations and ensure consistency
        load_existing_state_from_etcd();
        
        // Set up etcd watchers for cluster state (after loading existing state)
        setup_watcher_with_error_handling("clients", [this]() { watch_clients_namespace(); });
        setup_watcher_with_error_handling("workers", [this]() { watch_worker_registry_namespace(); });
        setup_watcher_with_error_handling("heartbeats", [this]() { watch_heartbeat_namespace(); });
        setup_watcher_with_error_handling("chunks", [this]() { watch_chunks_namespace(); });
        
        // Start keepalive thread AFTER watchers are set up and lease is established
        etcd_keepalive_thread_ = std::thread(&KeystoneService::run_etcd_keepalive, this);
    }
    
    LOG(INFO) << "Keystone service started successfully";
    return ErrorCode::OK;
}

void KeystoneService::stop() {
    LOG(INFO) << "Stopping Keystone service...";
    
    running_.store(false);
    
    // Join background threads
    if (gc_thread_.joinable()) {
        gc_thread_.join();
    }
    
    if (health_check_thread_.joinable()) {
        health_check_thread_.join();
    }
    
    if (etcd_keepalive_thread_.joinable()) {
        etcd_keepalive_thread_.join();
    }
    
    // Cleanup etcd lease
    if (etcd_ && keystone_lease_id_ != 0) {
        etcd_->revoke_lease(keystone_lease_id_);
        keystone_lease_id_ = 0;
    }
    
    LOG(INFO) << "Keystone service stopped";
}

// Client Management - REMOVED: Clients register directly to etcd
// Keystone only watches for changes and maintains read-only view

ErrorCode KeystoneService::get_active_clients(std::vector<ClientInfo>& clients) const {
    std::shared_lock<std::shared_mutex> lock(clients_mutex_);
    
    clients.clear();
    clients.reserve(clients_.size());
    
    auto now = std::chrono::steady_clock::now();
    auto ttl = std::chrono::seconds(config_.client_ttl_sec);
    
    for (const auto& [client_id, client_info] : clients_) {
        if (!client_info.is_stale(ttl)) {
            clients.push_back(client_info);
        }
    }
    
    return ErrorCode::OK;
}

// Client ping for future compatibility 
Result<PingResponse> KeystoneService::ping_client(const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock(clients_mutex_);
    
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        return ErrorCode::CLIENT_NOT_FOUND;
    }
    
    // Update last ping time
    it->second.last_ping = std::chrono::steady_clock::now();
    
    PingResponse response;
    response.view_version = get_view_version();
    response.client_status = ClientStatus::ACTIVE;
    
    return response;
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

ErrorCode KeystoneService::get_chunks(std::vector<Segment>& segments) const {
    std::shared_lock<std::shared_mutex> lock(chunks_mutex_);
    
    segments.clear();
    segments.reserve(chunks_.size());
    
    for (const auto& [segment_id, segment] : chunks_) {
        segments.push_back(segment);
    }
    
    return ErrorCode::OK;
}

// Object Management - Stub implementations
Result<bool> KeystoneService::object_exists(const ObjectKey& key) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    bool exists = (it != objects_.end()) && !it->second.is_expired();
    
    if (exists) {
        const_cast<ObjectInfo&>(it->second).touch();
    }
    
    return exists;
}

Result<std::vector<WorkerPlacement>> KeystoneService::get_workers(const ObjectKey& key) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end() || it->second.is_expired()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    const_cast<ObjectInfo&>(it->second).touch();
    return it->second.workers;
}

Result<std::vector<WorkerPlacement>> KeystoneService::put_start(const ObjectKey& key, 
                                                              size_t data_size, 
                                                              const WorkerConfig& config) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    // Check if object already exists
    auto it = objects_.find(key);
    if (it != objects_.end() && !it->second.is_expired()) {
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }
    
    std::vector<WorkerPlacement> workers;
    auto err = allocate_workers(key, data_size, config, workers);
    if (err != ErrorCode::OK) {
        return err;
    }
    
    // Create object info
    ObjectInfo object_info;
    object_info.key = key;
    object_info.workers = workers;
    object_info.created_at = std::chrono::steady_clock::now();
    object_info.last_accessed = object_info.created_at;
    object_info.soft_pinned = config.enable_soft_pin;
    object_info.ttl_ms = config.ttl_ms;
    
    objects_[key] = std::move(object_info);
    
    LOG(INFO) << "Started put operation for key: " << key << " (size: " << data_size << ")";
    
    return workers;
}

ErrorCode KeystoneService::put_complete(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    // Mark all workers as complete
    for (auto& w : it->second.workers) {
        w.status = WorkerStatus::COMPLETE;
    }
    
    LOG(INFO) << "Completed put operation for key: " << key;
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

// Batch operations - Simple implementations
std::vector<Result<bool>> KeystoneService::batch_object_exists(const std::vector<ObjectKey>& keys) {
    std::vector<Result<bool>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(object_exists(key));
    }
    
    return results;
}

std::vector<Result<std::vector<WorkerPlacement>>> KeystoneService::batch_get_workers(const std::vector<ObjectKey>& keys) {
    std::vector<Result<std::vector<WorkerPlacement>>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(get_workers(key));
    }
    
    return results;
}

std::vector<Result<std::vector<WorkerPlacement>>> KeystoneService::batch_put_start(
    const std::vector<ObjectKey>& keys,
    const std::vector<size_t>& data_sizes,
    const WorkerConfig& config) {
    
    if (keys.size() != data_sizes.size()) {
        // Return error for all operations
        std::vector<Result<std::vector<WorkerPlacement>>> results(keys.size(), ErrorCode::INVALID_PARAMETERS);
        return results;
    }
    
    std::vector<Result<std::vector<WorkerPlacement>>> results;
    results.reserve(keys.size());
    
    for (size_t i = 0; i < keys.size(); ++i) {
        results.push_back(put_start(keys[i], data_sizes[i], config));
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
    
    // Client statistics
    {
        std::shared_lock<std::shared_mutex> lock(clients_mutex_);
        stats.total_clients = clients_.size();
        
        auto now = std::chrono::steady_clock::now();
        auto ttl = std::chrono::seconds(config_.client_ttl_sec);
        
        for (const auto& [client_id, client_info] : clients_) {
            if (!client_info.is_stale(ttl)) {
                stats.active_clients++;
            }
        }
    }
    
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
            cleanup_stale_clients();
            cleanup_stale_workers();
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

ErrorCode KeystoneService::allocate_workers(const ObjectKey& key, size_t data_size, 
                                          const WorkerConfig& config,
                                          std::vector<WorkerPlacement>& workers) {
    workers.clear();
    
    // Validate input parameters
    if (config.worker_count == 0) {
        LOG(ERROR) << "Worker count cannot be zero";
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    if (data_size == 0) {
        LOG(ERROR) << "Data size cannot be zero";
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    workers.reserve(config.worker_count);
    
    // Thread-safe copy of segments to avoid iterator invalidation
    std::vector<Segment> available_segments;
    {
        std::shared_lock<std::shared_mutex> segments_lock(chunks_mutex_);
        if (chunks_.empty()) {
            LOG(ERROR) << "No segments available for worker allocation";
            return ErrorCode::SEGMENT_NOT_FOUND;
        }
        
        // Copy segments to avoid holding lock during allocation
        available_segments.reserve(chunks_.size());
        for (const auto& [id, segment] : chunks_) {
            available_segments.push_back(segment);
        }
    }
    
    // Calculate data distribution
    size_t chunk_size = data_size / config.worker_count;
    size_t remainder = data_size % config.worker_count;
    size_t current_offset = 0;
    
    for (size_t i = 0; i < config.worker_count; ++i) {
        WorkerPlacement placement;
        placement.status = WorkerStatus::ALLOCATED;
        
        // Round-robin segment selection
        const auto& segment = available_segments[i % available_segments.size()];
        placement.node_id = segment.node_id;
        
        // Calculate this worker's data size (distribute remainder among first workers)
        size_t this_worker_size = chunk_size + (i < remainder ? 1 : 0);
        
        MemoryPlacement mem;
        UcxRegion region;
        region.worker_address = segment.ucx_address;
        region.remote_key = 0;
        region.remote_addr = segment.base_addr + current_offset; // CRITICAL: Offset to avoid overlap
        region.size = this_worker_size;
        mem.kind = MemoryKind::CPU_DRAM;
        mem.regions.push_back(region);
        placement.storage = mem;
        
        workers.push_back(placement);
        current_offset += this_worker_size;
    }
    
    LOG(INFO) << "Allocated " << workers.size() << " workers for key: " << key 
              << " (total_size: " << data_size << ", chunk_size: " << chunk_size << ")";
    return ErrorCode::OK;
}

void KeystoneService::cleanup_stale_clients() {
    std::unique_lock<std::shared_mutex> lock(clients_mutex_);
    
    auto now = std::chrono::steady_clock::now();
    auto ttl = std::chrono::seconds(config_.client_ttl_sec);
    
    auto it = clients_.begin();
    while (it != clients_.end()) {
        if (it->second.is_stale(ttl)) {
            LOG(INFO) << "Removing stale client: " << it->first << " (" << it->second.node_id << ")";
            it = clients_.erase(it);
            increment_view_version();
        } else {
            ++it;
        }
    }
}

void KeystoneService::evict_objects_if_needed() {
    auto stats_result = get_cluster_stats();
    if (!is_ok(stats_result)) {
        return;
    }
    
    auto stats = get_value(stats_result);
    if (stats.utilization < config_.high_watermark) {
        return;  // No need to evict
    }
    
    LOG(INFO) << "High watermark reached (" << (stats.utilization * 100.0) 
              << "%), starting eviction process";
    
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    std::vector<std::pair<std::chrono::steady_clock::time_point, ObjectKey>> lru_objects;
    for (const auto& [key, object_info] : objects_) {
        if (!object_info.soft_pinned) {
            lru_objects.emplace_back(object_info.last_accessed, key);
        }
    }
    
    std::sort(lru_objects.begin(), lru_objects.end());
    
    size_t target_evictions = static_cast<size_t>(objects_.size() * config_.eviction_ratio);
    size_t evicted = 0;
    
    for (const auto& [last_access, key] : lru_objects) {
        if (evicted >= target_evictions) break;
        
        objects_.erase(key);
        evicted++;
        LOG(INFO) << "Evicted object: " << key;
    }
    
    LOG(INFO) << "Evicted " << evicted << " objects";
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
void KeystoneService::watch_clients_namespace() {
    if (!etcd_) return;
    auto prefix = clients_prefix();
    auto cb = [this](const std::string& key, const std::string& value, bool is_delete) {
        if (!is_delete) {
            upsert_client_from_json(key, value);
        } else {
            remove_client_by_key(key);
        }
    };
    
    auto err = etcd_->watch_prefix(prefix, cb);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to attach client watcher on prefix: " << prefix;
    } else {
        LOG(INFO) << "Watching client prefix: " << prefix;
    }
}

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

void KeystoneService::upsert_client_from_json(const std::string& key, const std::string& json_value) {
    try {
        auto doc = nlohmann::json::parse(json_value);
        
        // Extract client_id from key: /clients/<client_id>
        auto pos = key.rfind('/');
        if (pos == std::string::npos || pos + 1 >= key.size()) {
            LOG(WARNING) << "Invalid client key format: " << key;
            return;
        }
        std::string client_id_str = key.substr(pos + 1);
        
        // Parse UUID from string (format: "high-low")
        auto dash_pos = client_id_str.find('-');
        if (dash_pos == std::string::npos) {
            LOG(WARNING) << "Invalid client UUID format (missing dash): " << client_id_str;
            return;
        }
        
        UUID client_id;
        try {
            client_id.first = std::stoull(client_id_str.substr(0, dash_pos));
            client_id.second = std::stoull(client_id_str.substr(dash_pos + 1));
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to parse client UUID from key: " << client_id_str << " - " << e.what();
            return;
        }
        
        // Validate required fields
        if (!doc.contains("node_id") || !doc.contains("endpoint")) {
            LOG(WARNING) << "Client JSON missing required fields (node_id, endpoint): " << key;
            return;
        }
        
        ClientInfo info;
        info.client_id = client_id;
        info.node_id = doc.value("node_id", std::string{});
        info.endpoint = doc.value("endpoint", std::string{});
        info.last_ping = std::chrono::steady_clock::now();
        
        {
            std::unique_lock<std::shared_mutex> lock(clients_mutex_);
            clients_[client_id] = std::move(info);
        }
        
        increment_view_version();
        LOG(INFO) << "Registered client from etcd: " << client_id << " (" << info.node_id << ") at " << info.endpoint;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse client JSON from etcd (key=" << key << "): " << e.what();
    }
}

void KeystoneService::remove_client_by_key(const std::string& key) {
    auto pos = key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= key.size()) return;
    std::string client_id_str = key.substr(pos + 1);
    
    // Parse UUID from string
    auto dash_pos = client_id_str.find('-');
    if (dash_pos == std::string::npos) return;
    
    UUID client_id;
    try {
        client_id.first = std::stoull(client_id_str.substr(0, dash_pos));
        client_id.second = std::stoull(client_id_str.substr(dash_pos + 1));
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse client UUID for removal: " << client_id_str;
        return;
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(clients_mutex_);
        clients_.erase(client_id);
    }
    
    increment_view_version();
    LOG(INFO) << "Removed client from etcd: " << client_id;
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

void KeystoneService::load_existing_state_from_etcd() {
    if (!etcd_) {
        LOG(WARNING) << "Etcd not initialized, cannot load existing state.";
        return;
    }

    // Load clients
    std::vector<std::string> client_keys, client_values;
    auto err = etcd_->get_with_prefix(clients_prefix(), client_keys, client_values);
    if (err == ErrorCode::OK) {
        for (size_t i = 0; i < client_keys.size(); ++i) {
            upsert_client_from_json(client_keys[i], client_values[i]);
        }
        LOG(INFO) << "Loaded " << client_keys.size() << " clients from etcd.";
    } else {
        LOG(WARNING) << "Failed to load clients from etcd: " << error::to_string(err);
    }

    // Load segments  
    std::vector<std::string> segment_keys, segment_values;
    err = etcd_->get_with_prefix(chunks_prefix(), segment_keys, segment_values);
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