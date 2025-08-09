#include "blackbird/rpc_service.h"

#include <glog/logging.h>

namespace blackbird {

RpcService::RpcService(std::shared_ptr<MasterService> master_service, const MasterConfig& config)
    : master_service_(std::move(master_service)), config_(config) {
    LOG(INFO) << "Creating RpcService";
}

RpcService::~RpcService() {
    if (running_.load()) {
        stop();
    }
}

ErrorCode RpcService::start() {
    LOG(INFO) << "Starting RpcService...";
    
    if (running_.load()) {
        LOG(WARNING) << "RpcService is already running";
        return ErrorCode::OK;
    }
    
    auto err = setup_rpc_server();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to setup RPC server: " << toString(err);
        return err;
    }
    
    err = setup_http_server();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to setup HTTP server: " << toString(err);
        return err;
    }
    
    running_.store(true);
    
    LOG(INFO) << "RpcService started successfully";
    return ErrorCode::OK;
}

void RpcService::stop() {
    LOG(INFO) << "Stopping RpcService...";
    
    running_.store(false);
    
    if (rpc_server_) {
        LOG(INFO) << "Stopping RPC server...";
        // Stop RPC server - YLT server stop logic would go here
        rpc_server_.reset();
    }
    
    if (http_server_) {
        LOG(INFO) << "Stopping HTTP server...";
        // Stop HTTP server - YLT HTTP server stop logic would go here
        http_server_.reset();
    }
    
    if (http_thread_.joinable()) {
        http_thread_.join();
    }
    
    LOG(INFO) << "RpcService stopped";
}

// RPC method implementations - these delegate to the master service
Result<PingResponse> RpcService::ping(const UUID& client_id) {
    return handle_service_call<PingResponse>([&]() {
        return master_service_->ping_client(client_id);
    });
}

ErrorCode RpcService::register_client(const UUID& client_id, const NodeId& node_id, const std::string& endpoint) {
    return handle_service_call([&]() {
        return master_service_->register_client(client_id, node_id, endpoint);
    });
}

ErrorCode RpcService::register_segment(const Segment& segment, const UUID& client_id) {
    return handle_service_call([&]() {
        return master_service_->register_segment(segment, client_id);
    });
}

ErrorCode RpcService::unregister_segment(const SegmentId& segment_id, const UUID& client_id) {
    return handle_service_call([&]() {
        return master_service_->unregister_segment(segment_id, client_id);
    });
}

Result<bool> RpcService::object_exists(const ObjectKey& key) {
    return handle_service_call<bool>([&]() {
        return master_service_->object_exists(key);
    });
}

Result<std::vector<ReplicaDescriptor>> RpcService::get_replicas(const ObjectKey& key) {
    return handle_service_call<std::vector<ReplicaDescriptor>>([&]() {
        return master_service_->get_replicas(key);
    });
}

Result<std::vector<ReplicaDescriptor>> RpcService::put_start(const ObjectKey& key, 
                                                             size_t data_size, 
                                                             const ReplicaConfig& config) {
    return handle_service_call<std::vector<ReplicaDescriptor>>([&]() {
        return master_service_->put_start(key, data_size, config);
    });
}

ErrorCode RpcService::put_complete(const ObjectKey& key) {
    return handle_service_call([&]() {
        return master_service_->put_complete(key);
    });
}

ErrorCode RpcService::put_cancel(const ObjectKey& key) {
    return handle_service_call([&]() {
        return master_service_->put_cancel(key);
    });
}

ErrorCode RpcService::remove_object(const ObjectKey& key) {
    return handle_service_call([&]() {
        return master_service_->remove_object(key);
    });
}

Result<size_t> RpcService::remove_all_objects() {
    return handle_service_call<size_t>([&]() {
        return master_service_->remove_all_objects();
    });
}

// Batch operations
std::vector<Result<bool>> RpcService::batch_object_exists(const std::vector<ObjectKey>& keys) {
    try {
        return master_service_->batch_object_exists(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_object_exists: " << e.what();
        return std::vector<Result<bool>>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<Result<std::vector<ReplicaDescriptor>>> RpcService::batch_get_replicas(const std::vector<ObjectKey>& keys) {
    try {
        return master_service_->batch_get_replicas(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_get_replicas: " << e.what();
        return std::vector<Result<std::vector<ReplicaDescriptor>>>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<Result<std::vector<ReplicaDescriptor>>> RpcService::batch_put_start(
    const std::vector<ObjectKey>& keys,
    const std::vector<size_t>& data_sizes,
    const ReplicaConfig& config) {
    try {
        return master_service_->batch_put_start(keys, data_sizes, config);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_put_start: " << e.what();
        return std::vector<Result<std::vector<ReplicaDescriptor>>>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<ErrorCode> RpcService::batch_put_complete(const std::vector<ObjectKey>& keys) {
    try {
        return master_service_->batch_put_complete(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_put_complete: " << e.what();
        return std::vector<ErrorCode>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<ErrorCode> RpcService::batch_put_cancel(const std::vector<ObjectKey>& keys) {
    try {
        return master_service_->batch_put_cancel(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_put_cancel: " << e.what();
        return std::vector<ErrorCode>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

// Admin/Monitoring methods
Result<MasterService::ClusterStats> RpcService::get_cluster_stats() {
    return handle_service_call<MasterService::ClusterStats>([&]() {
        return master_service_->get_cluster_stats();
    });
}

ViewVersionId RpcService::get_view_version() {
    return master_service_->get_view_version();
}

// Private methods
ErrorCode RpcService::setup_rpc_server() {
    LOG(INFO) << "Setting up RPC server...";
    
    try {
        // Parse listen address
        auto pos = config_.listen_address.find(':');
        if (pos == std::string::npos) {
            LOG(ERROR) << "Invalid listen address format: " << config_.listen_address;
            return ErrorCode::INVALID_PARAMS;
        }
        
        std::string host = config_.listen_address.substr(0, pos);
        std::string port_str = config_.listen_address.substr(pos + 1);
        
        // Create YLT RPC server
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, std::stoi(port_str));
        
        // Register RPC methods
        register_rpc_methods();
        
        // Start the server (this is a simplified stub - real YLT setup would be more complex)
        LOG(INFO) << "RPC server configured to listen on " << config_.listen_address;
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception setting up RPC server: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode RpcService::setup_http_server() {
    LOG(INFO) << "Setting up HTTP server for metrics...";
    
    try {
        // Create YLT HTTP server for metrics
        http_server_ = std::make_unique<coro_http::coro_http_server>(1, std::stoi(config_.http_metrics_port));
        
        // Setup metrics endpoint
        setup_metrics_endpoint();
        
        // Start HTTP server in background thread
        http_thread_ = std::thread(&RpcService::run_http_server, this);
        
        LOG(INFO) << "HTTP metrics server configured on port " << config_.http_metrics_port;
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception setting up HTTP server: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

void RpcService::register_rpc_methods() {
    LOG(INFO) << "Registering RPC methods...";
    
    // In a real implementation, this would register all the RPC methods with the YLT server
    // For now, this is just a stub that logs the registration
    
    LOG(INFO) << "Registered RPC methods: ping, register_client, register_segment, etc.";
}

void RpcService::setup_metrics_endpoint() {
    LOG(INFO) << "Setting up metrics endpoint...";
    
    // In a real implementation, this would setup HTTP handlers for metrics
    // For now, this is just a stub
    
    LOG(INFO) << "Metrics endpoint configured at /metrics";
}

void RpcService::run_http_server() {
    LOG(INFO) << "HTTP server thread started";
    
    try {
        // In a real implementation, this would run the YLT HTTP server
        // For now, just simulate running
        while (running_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in HTTP server thread: " << e.what();
    }
    
    LOG(INFO) << "HTTP server thread stopped";
}

// Helper methods for error handling
template<typename T>
Result<T> RpcService::handle_service_call(std::function<Result<T>()> service_call) {
    try {
        return service_call();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in service call: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode RpcService::handle_service_call(std::function<ErrorCode()> service_call) {
    try {
        return service_call();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in service call: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

// Helper functions
void register_rpc_methods(coro_rpc::coro_rpc_server& server, RpcService& rpc_service) {
    LOG(INFO) << "Registering RPC methods with server...";
    
    // In a real implementation, this would use YLT's method registration API
    // For example:
    // server.register_handler<&RpcService::ping>(&rpc_service);
    // server.register_handler<&RpcService::register_client>(&rpc_service);
    // etc.
    
    LOG(INFO) << "All RPC methods registered";
}

std::shared_ptr<RpcService> create_and_start_master(const MasterConfig& config) {
    LOG(INFO) << "Creating and starting Blackbird master...";
    
    try {
        // Create master service
        auto master_service = std::make_shared<MasterService>(config);
        
        auto err = master_service->initialize();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize master service: " << toString(err);
            return nullptr;
        }
        
        err = master_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start master service: " << toString(err);
            return nullptr;
        }
        
        // Create RPC service
        auto rpc_service = std::make_shared<RpcService>(master_service, config);
        
        err = rpc_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start RPC service: " << toString(err);
            return nullptr;
        }
        
        LOG(INFO) << "Blackbird master created and started successfully";
        return rpc_service;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception creating master: " << e.what();
        return nullptr;
    }
}

}  // namespace blackbird 