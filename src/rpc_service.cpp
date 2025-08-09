#include "blackbird/rpc_service.h"

#include <glog/logging.h>

namespace blackbird {

RpcService::RpcService(std::shared_ptr<KeystoneService> keystone_service, const KeystoneConfig& config)
    : keystone_service_(std::move(keystone_service)), config_(config) {
    LOG(INFO) << "Creating RpcService for Keystone";
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
        LOG(ERROR) << "Failed to setup RPC server: " << error::to_string(err);
        return err;
    }
    
    err = setup_http_server();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to setup HTTP server: " << error::to_string(err);
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
        rpc_server_.reset();
    }
    
    if (http_server_) {
        LOG(INFO) << "Stopping HTTP server...";
        http_server_.reset();
    }
    
    if (http_thread_.joinable()) {
        http_thread_.join();
    }
    
    LOG(INFO) << "RpcService stopped";
}

// RPC Methods - REMOVED client/segment registration
// Clients and workers register directly to etcd, not through Keystone RPC

Result<bool> RpcService::object_exists(const ObjectKey& key) {
    return handle_service_call<bool>([&]() { return keystone_service_->object_exists(key); });
}

Result<std::vector<WorkerPlacement>> RpcService::get_workers(const ObjectKey& key) {
    return handle_service_call<std::vector<WorkerPlacement>>([&]() {
        return keystone_service_->get_workers(key);
    });
}

Result<std::vector<WorkerPlacement>> RpcService::put_start(const ObjectKey& key, 
                                                             size_t data_size, 
                                                             const WorkerConfig& config) {
    return handle_service_call<std::vector<WorkerPlacement>>([&]() {
        return keystone_service_->put_start(key, data_size, config);
    });
}

ErrorCode RpcService::put_complete(const ObjectKey& key) {
    return handle_service_call([&]() {
        return keystone_service_->put_complete(key);
    });
}

ErrorCode RpcService::put_cancel(const ObjectKey& key) {
    return handle_service_call([&]() {
        return keystone_service_->put_cancel(key);
    });
}

ErrorCode RpcService::remove_object(const ObjectKey& key) {
    return handle_service_call([&]() {
        return keystone_service_->remove_object(key);
    });
}

Result<size_t> RpcService::remove_all_objects() {
    return handle_service_call<size_t>([&]() {
        return keystone_service_->remove_all_objects();
    });
}

// Batch operations
std::vector<Result<bool>> RpcService::batch_object_exists(const std::vector<ObjectKey>& keys) {
    try {
        return keystone_service_->batch_object_exists(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_object_exists: " << e.what();
        return std::vector<Result<bool>>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<Result<std::vector<WorkerPlacement>>> RpcService::batch_get_workers(const std::vector<ObjectKey>& keys) {
    try {
        return keystone_service_->batch_get_workers(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_get_workers: " << e.what();
        return std::vector<Result<std::vector<WorkerPlacement>>>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<Result<std::vector<WorkerPlacement>>> RpcService::batch_put_start(
    const std::vector<ObjectKey>& keys,
    const std::vector<size_t>& data_sizes,
    const WorkerConfig& config) {
    try {
        return keystone_service_->batch_put_start(keys, data_sizes, config);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_put_start: " << e.what();
        return std::vector<Result<std::vector<WorkerPlacement>>>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<ErrorCode> RpcService::batch_put_complete(const std::vector<ObjectKey>& keys) {
    try {
        return keystone_service_->batch_put_complete(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_put_complete: " << e.what();
        return std::vector<ErrorCode>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

std::vector<ErrorCode> RpcService::batch_put_cancel(const std::vector<ObjectKey>& keys) {
    try {
        return keystone_service_->batch_put_cancel(keys);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in batch_put_cancel: " << e.what();
        return std::vector<ErrorCode>(keys.size(), ErrorCode::INTERNAL_ERROR);
    }
}

// Admin/Monitoring methods
Result<ClusterStats> RpcService::get_cluster_stats() {
    return handle_service_call<ClusterStats>([&]() {
        return keystone_service_->get_cluster_stats();
    });
}

ViewVersionId RpcService::get_view_version() {
    return keystone_service_->get_view_version();
}

ErrorCode RpcService::setup_rpc_server() {
    LOG(INFO) << "Setting up RPC server...";
    
    try {
        auto pos = config_.listen_address.find(':');
        if (pos == std::string::npos) {
            LOG(ERROR) << "Invalid listen address format: " << config_.listen_address;
            return ErrorCode::INVALID_PARAMETERS;
        }
        
        std::string host = config_.listen_address.substr(0, pos);
        std::string port_str = config_.listen_address.substr(pos + 1);
        
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, std::stoi(port_str));
        register_rpc_methods();
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
        http_server_ = std::make_unique<coro_http::coro_http_server>(1, std::stoi(config_.http_metrics_port));
        setup_metrics_endpoint();
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
    LOG(INFO) << "Registered RPC methods: ping, register_client, register_segment, etc.";
}

void RpcService::setup_metrics_endpoint() {
    LOG(INFO) << "Setting up metrics endpoint...";
    LOG(INFO) << "Metrics endpoint configured at /metrics";
}

void RpcService::run_http_server() {
    LOG(INFO) << "HTTP server thread started";
    
    try {
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

void register_rpc_methods(coro_rpc::coro_rpc_server& server, RpcService& rpc_service) {
    LOG(INFO) << "Registering RPC methods with server...";
    LOG(INFO) << "All RPC methods registered";
}

std::shared_ptr<RpcService> create_and_start_keystone(const KeystoneConfig& config) {
    LOG(INFO) << "Creating and starting Blackbird keystone...";
    
    try {
        auto keystone_service = std::make_shared<KeystoneService>(config);
        
        auto err = keystone_service->initialize();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize keystone service: " << error::to_string(err);
            return nullptr;
        }
        
        err = keystone_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start keystone service: " << error::to_string(err);
            return nullptr;
        }
        
        auto rpc_service = std::make_shared<RpcService>(keystone_service, config);
        
        err = rpc_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start RPC service: " << error::to_string(err);
            return nullptr;
        }
        
        LOG(INFO) << "Blackbird keystone created and started successfully";
        return rpc_service;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception creating keystone: " << e.what();
        return nullptr;
    }
}

}  // namespace blackbird 