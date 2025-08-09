#include "blackbird/master_service.h"

#include <glog/logging.h>
#include <algorithm>

namespace blackbird {

MasterService::MasterService(const MasterConfig& config) 
    : config_(config) {
    LOG(INFO) << "Creating MasterService with cluster_id: " << config_.cluster_id;
}

MasterService::~MasterService() {
    if (running_.load()) {
        stop();
    }
}

ErrorCode MasterService::initialize() {
    LOG(INFO) << "Initializing MasterService...";
    
    if (!config_.etcd_endpoints.empty()) {
        auto err = setup_etcd_integration();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to setup etcd integration: " << error::to_string(err);
            return err;
        }
    } else {
        LOG(WARNING) << "No etcd endpoints configured, running without etcd";
    }
    
    LOG(INFO) << "MasterService initialized successfully";
    return ErrorCode::OK;
}

ErrorCode MasterService::start() {
    LOG(INFO) << "Starting MasterService...";
    
    if (running_.load()) {
        LOG(WARNING) << "MasterService is already running";
        return ErrorCode::OK;
    }
    
    running_.store(true);
    view_version_.store(1);  // Start with version 1
    
    // Start background threads
    if (config_.enable_gc) {
        gc_thread_ = std::thread(&MasterService::run_garbage_collection, this);
    }
    
    health_check_thread_ = std::thread(&MasterService::run_health_checks, this);
    
    if (etcd_) {
        etcd_keepalive_thread_ = std::thread(&MasterService::run_etcd_keepalive, this);
    }
    
    LOG(INFO) << "MasterService started successfully";
    return ErrorCode::OK;
}

void MasterService::stop() {
    LOG(INFO) << "Stopping MasterService...";
    
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
    if (etcd_ && master_lease_id_ != 0) {
        etcd_->revoke_lease(master_lease_id_);
        master_lease_id_ = 0;
    }
    
    LOG(INFO) << "MasterService stopped";
}

// Client Management
ErrorCode MasterService::register_client(const UUID& client_id, const NodeId& node_id, const std::string& endpoint) {
    std::unique_lock<std::shared_mutex> lock(clients_mutex_);
    
    auto now = std::chrono::steady_clock::now();
    ClientInfo& client = clients_[client_id];
    client.client_id = client_id;
    client.node_id = node_id;
    client.endpoint = endpoint;
    client.last_ping = now;
    
    LOG(INFO) << "Registered client: " << client_id << " (" << node_id << ") at " << endpoint;
    increment_view_version();
    
    return ErrorCode::OK;
}

Result<PingResponse> MasterService::ping_client(const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock(clients_mutex_);
    
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        LOG(WARNING) << "Ping from unknown client: " << client_id;
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    it->second.last_ping = std::chrono::steady_clock::now();
    
    PingResponse response;
    response.view_version = view_version_.load();
    response.client_status = ClientStatus::ACTIVE;
    
    return response;
}

ErrorCode MasterService::unregister_client(const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock(clients_mutex_);
    
    auto it = clients_.find(client_id);
    if (it == clients_.end()) {
        LOG(WARNING) << "Attempt to unregister unknown client: " << client_id;
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    LOG(INFO) << "Unregistering client: " << client_id << " (" << it->second.node_id << ")";
    clients_.erase(it);
    increment_view_version();
    
    return ErrorCode::OK;
}

ErrorCode MasterService::get_active_clients(std::vector<ClientInfo>& clients) const {
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

// Segment Management
ErrorCode MasterService::register_segment(const Segment& segment, const UUID& client_id) {
    std::unique_lock<std::shared_mutex> segments_lock(segments_mutex_);
    std::unique_lock<std::shared_mutex> clients_lock(clients_mutex_);
    
    // Check if client exists
    auto client_it = clients_.find(client_id);
    if (client_it == clients_.end()) {
        LOG(ERROR) << "Attempt to register segment from unknown client: " << client_id;
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    // Check if segment already exists
    if (segments_.find(segment.id) != segments_.end()) {
        LOG(WARNING) << "Segment already exists: " << segment.id;
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    
    segments_[segment.id] = segment;
    client_it->second.segments.push_back(segment.id);
    
    LOG(INFO) << "Registered segment: " << segment.id << " from client " << client_id 
              << " (size: " << segment.size << " bytes)";
    
    increment_view_version();
    return ErrorCode::OK;
}

ErrorCode MasterService::unregister_segment(const SegmentId& segment_id, const UUID& client_id) {
    std::unique_lock<std::shared_mutex> segments_lock(segments_mutex_);
    std::unique_lock<std::shared_mutex> clients_lock(clients_mutex_);
    
    auto segment_it = segments_.find(segment_id);
    if (segment_it == segments_.end()) {
        LOG(WARNING) << "Attempt to unregister unknown segment: " << segment_id;
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    
    auto client_it = clients_.find(client_id);
    if (client_it != clients_.end()) {
        auto& client_segments = client_it->second.segments;
        client_segments.erase(
            std::remove(client_segments.begin(), client_segments.end(), segment_id),
            client_segments.end()
        );
    }
    
    segments_.erase(segment_it);
    
    LOG(INFO) << "Unregistered segment: " << segment_id << " from client " << client_id;
    increment_view_version();
    
    return ErrorCode::OK;
}

ErrorCode MasterService::get_segments(std::vector<Segment>& segments) const {
    std::shared_lock<std::shared_mutex> lock(segments_mutex_);
    
    segments.clear();
    segments.reserve(segments_.size());
    
    for (const auto& [segment_id, segment] : segments_) {
        segments.push_back(segment);
    }
    
    return ErrorCode::OK;
}

// Object Management - Stub implementations
Result<bool> MasterService::object_exists(const ObjectKey& key) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    bool exists = (it != objects_.end()) && !it->second.is_expired();
    
    if (exists) {
        const_cast<ObjectInfo&>(it->second).touch();
    }
    
    return exists;
}

Result<std::vector<WorkerPlacement>> MasterService::get_workers(const ObjectKey& key) {
    std::shared_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end() || it->second.is_expired()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    const_cast<ObjectInfo&>(it->second).touch();
    return it->second.workers;
}

Result<std::vector<WorkerPlacement>> MasterService::put_start(const ObjectKey& key, 
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

ErrorCode MasterService::put_complete(const ObjectKey& key) {
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

ErrorCode MasterService::put_cancel(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    objects_.erase(it);
    
    LOG(INFO) << "Cancelled put operation for key: " << key;
    return ErrorCode::OK;
}

ErrorCode MasterService::remove_object(const ObjectKey& key) {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    auto it = objects_.find(key);
    if (it == objects_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    objects_.erase(it);
    
    LOG(INFO) << "Removed object: " << key;
    return ErrorCode::OK;
}

Result<size_t> MasterService::remove_all_objects() {
    std::unique_lock<std::shared_mutex> lock(objects_mutex_);
    
    size_t count = objects_.size();
    objects_.clear();
    
    LOG(INFO) << "Removed all objects (count: " << count << ")";
    return count;
}

// Batch operations - Simple implementations
std::vector<Result<bool>> MasterService::batch_object_exists(const std::vector<ObjectKey>& keys) {
    std::vector<Result<bool>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(object_exists(key));
    }
    
    return results;
}

std::vector<Result<std::vector<WorkerPlacement>>> MasterService::batch_get_workers(const std::vector<ObjectKey>& keys) {
    std::vector<Result<std::vector<WorkerPlacement>>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(get_workers(key));
    }
    
    return results;
}

std::vector<Result<std::vector<WorkerPlacement>>> MasterService::batch_put_start(
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

std::vector<ErrorCode> MasterService::batch_put_complete(const std::vector<ObjectKey>& keys) {
    std::vector<ErrorCode> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(put_complete(key));
    }
    
    return results;
}

std::vector<ErrorCode> MasterService::batch_put_cancel(const std::vector<ObjectKey>& keys) {
    std::vector<ErrorCode> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
        results.push_back(put_cancel(key));
    }
    
    return results;
}

// Metrics and status
Result<MasterService::ClusterStats> MasterService::get_cluster_stats() const {
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
        std::shared_lock<std::shared_mutex> lock(segments_mutex_);
        stats.total_segments = segments_.size();
        
        for (const auto& [segment_id, segment] : segments_) {
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
ErrorCode MasterService::setup_etcd_integration() {
    if (config_.etcd_endpoints.empty()) {
        return ErrorCode::OK;  // No etcd configured
    }
    
    etcd_ = std::make_unique<EtcdService>(config_.etcd_endpoints);
    auto err = etcd_->connect();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd: " << error::to_string(err);
        return err;
    }
    
    // Register master service with etcd
    std::string master_id = "master-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    err = etcd_->register_service("blackbird-master", master_id, config_.listen_address, 
                                 config_.client_ttl_sec, master_lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register master service with etcd: " << error::to_string(err);
        return err;
    }
    
    LOG(INFO) << "Registered master service with etcd (lease_id: " << master_lease_id_ << ")";
    return ErrorCode::OK;
}

void MasterService::run_garbage_collection() {
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

void MasterService::run_health_checks() {
    LOG(INFO) << "Starting health check thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(10));  // Check every 10 seconds
        
        if (!running_.load()) break;
        
        try {
            cleanup_stale_clients();
            evict_objects_if_needed();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception in health checks: " << e.what();
        }
    }
    
    LOG(INFO) << "Health check thread stopped";
}

void MasterService::run_etcd_keepalive() {
    LOG(INFO) << "Starting etcd keepalive thread";
    
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.client_ttl_sec / 3));  // Keep alive at 1/3 of TTL
        
        if (!running_.load()) break;
        
        if (etcd_ && master_lease_id_ != 0) {
            auto err = etcd_->keep_alive(master_lease_id_);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to keep alive etcd lease: " << error::to_string(err);
            }
        }
    }
    
    LOG(INFO) << "Etcd keepalive thread stopped";
}

ErrorCode MasterService::allocate_workers(const ObjectKey& key, size_t data_size, 
                                          const WorkerConfig& config,
                                          std::vector<WorkerPlacement>& workers) {
    workers.clear();
    workers.reserve(config.worker_count);
    
    std::shared_lock<std::shared_mutex> segments_lock(segments_mutex_);
    
    if (segments_.empty()) {
        LOG(ERROR) << "No segments available for worker allocation";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    
    size_t segment_idx = 0;
    for (size_t i = 0; i < config.worker_count; ++i) {
        WorkerPlacement placement;
        placement.status = WorkerStatus::ALLOCATED;
        
        auto segment_it = segments_.begin();
        std::advance(segment_it, segment_idx % segments_.size());
        placement.node_id = segment_it->second.node_id;
        
        MemoryPlacement mem;
        UcxRegion region;
        region.worker_address = segment_it->second.ucx_address;
        region.remote_key = 0;
        region.remote_addr = segment_it->second.base_addr;
        region.size = data_size;
        mem.kind = MemoryKind::CPU_DRAM;
        mem.regions.push_back(region);
        placement.storage = mem;
        
        workers.push_back(placement);
        segment_idx++;
    }
    
    LOG(INFO) << "Allocated " << workers.size() << " workers for key: " << key;
    return ErrorCode::OK;
}

void MasterService::cleanup_stale_clients() {
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

void MasterService::evict_objects_if_needed() {
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

ViewVersionId MasterService::increment_view_version() {
    return view_version_.fetch_add(1) + 1;
}

std::string MasterService::make_etcd_key(const std::string& suffix) const {
    return "/blackbird/clusters/" + config_.cluster_id + "/" + suffix;
}

}  // namespace blackbird 