#include <iostream>
#include <string>
#include <signal.h>
#include <thread>
#include <chrono>
#include <iomanip>

#include <glog/logging.h>
#include <nlohmann/json.hpp>

#include "blackbird/common/types.h"
#include "blackbird/keystone/keystone_service.h"
#include "blackbird/rpc/rpc_service.h"
#include "blackbird/etcd/etcd_service.h"

using namespace blackbird;

volatile bool g_running = true;

void signal_handler(int signal) {
    LOG(INFO) << "Received signal " << signal << ", shutting down...";
    g_running = false;
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]\n";
    std::cout << "Options:\n";
    std::cout << "  --etcd-endpoints <endpoints>  Comma-separated etcd endpoints (default: localhost:2379)\n";
    std::cout << "  --listen-address <address>    RPC listen address (default: 0.0.0.0:9090)\n";
    std::cout << "  --http-port <port>           HTTP metrics port (default: 9091)\n";
    std::cout << "  --cluster-id <id>            Cluster identifier (default: blackbird_cluster)\n";
    std::cout << "  --help                       Show this help message\n";
    std::cout << "\n";
    std::cout << "Example:\n";
    std::cout << "  " << program_name << " --etcd-endpoints localhost:2379,localhost:2380\n";
}

int main(int argc, char* argv[]) {
    // Initialize logging
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::GLOG_INFO);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Default configuration
    KeystoneConfig config;
    config.etcd_endpoints = "localhost:2379";
    config.listen_address = "0.0.0.0:9090";
    config.http_metrics_port = "9091";
    config.cluster_id = "blackbird_cluster";
    config.enable_gc = true;
    config.enable_ha = true;
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--etcd-endpoints" && i + 1 < argc) {
            config.etcd_endpoints = argv[++i];
        } else if (arg == "--listen-address" && i + 1 < argc) {
            config.listen_address = argv[++i];
        } else if (arg == "--http-port" && i + 1 < argc) {
            config.http_metrics_port = argv[++i];
        } else if (arg == "--cluster-id" && i + 1 < argc) {
            config.cluster_id = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }
    
    LOG(INFO) << "Starting up Keystone Services";
    LOG(INFO) << "Configuration:";
    LOG(INFO) << "  Cluster ID: " << config.cluster_id;
    LOG(INFO) << "  Etcd Endpoints: " << config.etcd_endpoints;
    LOG(INFO) << "  Listen Address: " << config.listen_address;
    LOG(INFO) << "  HTTP Metrics Port: " << config.http_metrics_port;
    LOG(INFO) << "  Enable GC: " << (config.enable_gc ? "true" : "false");
    LOG(INFO) << "  Enable HA: " << (config.enable_ha ? "true" : "false");
    
    try {
        LOG(INFO) << "Testing etcd connectivity...";
        EtcdService etcd_test(config.etcd_endpoints);
        auto err = etcd_test.connect();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd: " << error::to_string(err);
            LOG(ERROR) << "Please ensure etcd is running and accessible at: " << config.etcd_endpoints;
            return 1;
        }
        LOG(INFO) << "Etcd connectivity test passed";
        
        // Create and initialize keystone service
        LOG(INFO) << "Creating keystone service...";
        auto keystone_service = std::make_shared<KeystoneService>(config);
        
        err = keystone_service->initialize();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize keystone: " << error::to_string(err);
            return 1;
        }
        
        err = keystone_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start keystone: " << error::to_string(err);
            return 1;
        }
        
        // Create and start RPC service
        LOG(INFO) << "Creating RPC service...";
        auto rpc_service = std::make_shared<RpcService>(keystone_service, config);
        
        err = rpc_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start RPC service: " << error::to_string(err);
            return 1;
        }
        
        LOG(INFO) << "Keystone started successfully";
        LOG(INFO) << "RPC server listening on: " << config.listen_address;
        LOG(INFO) << "HTTP metrics available on port: " << config.http_metrics_port;
        LOG(INFO) << "Press Ctrl+C to shutdown";

        // Main service loop
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            static int status_counter = 0;
            if (++status_counter >= 60) {  // Every 60 seconds
                status_counter = 0;
                
                auto stats_result = keystone_service->get_cluster_stats();
                if (is_ok(stats_result)) {
                    auto stats = get_value(stats_result);
                    LOG(INFO) << "Cluster Status: "
                              << "workers=" << stats.total_workers 
                              << ", memory_pools=" << stats.total_memory_pools
                              << ", objects=" << stats.total_objects
                              << ", utilization=" << std::fixed << std::setprecision(1) 
                              << (stats.avg_utilization * 100.0) << "%";
                } else {
                    LOG(WARNING) << "Failed to get cluster stats: " << error::to_string(get_error(stats_result));
                }
            }
        }
        
        LOG(INFO) << "Shutting down services...";
        rpc_service->stop();
        keystone_service->stop();
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in main: " << e.what();
        return 1;
    }
    
    LOG(INFO) << "Keystone Service shutdown complete";
    return 0;
} 