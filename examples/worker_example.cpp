#include <iostream>
#include <signal.h>
#include <glog/logging.h>

#include "blackbird/worker/worker_service.h"

using namespace blackbird;

// Global worker service for signal handling
std::shared_ptr<WorkerService> g_worker_service;

void signal_handler(int signal) {
    LOG(INFO) << "Received signal " << signal << ", shutting down worker...";
    if (g_worker_service) {
        g_worker_service->stop();
    }
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n";
    std::cout << "\nOptions:\n";
    std::cout << "  --config <file>           YAML configuration file (default: ../configs/worker.yaml)\n";
    std::cout << "  --worker-id <id>          Override worker ID from config\n";
    std::cout << "  --node-id <id>            Override node ID from config\n";
    std::cout << "  --help                    Show this help message\n";
    std::cout << "\nExample:\n";
    std::cout << "  " << program_name << " --config /path/to/worker.yaml\n";
    std::cout << "  " << program_name << " --worker-id worker-2 --node-id node-b\n";
}

int main(int argc, char* argv[]) {
    // Initialize logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    FLAGS_v = 1;
    
    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Parse command line arguments
    std::string config_file = "../configs/worker.yaml";
    std::string override_worker_id;
    std::string override_node_id;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--config" && i + 1 < argc) {
            config_file = argv[++i];
        } else if (arg == "--worker-id" && i + 1 < argc) {
            override_worker_id = argv[++i];
        } else if (arg == "--node-id" && i + 1 < argc) {
            override_node_id = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }
    
    try {
        // Load configuration from YAML
        LOG(INFO) << "Loading configuration from: " << config_file;
        WorkerServiceConfig config;
        auto err = load_worker_config_from_file(config_file, config);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to load configuration: " << error::to_string(err);
            return 1;
        }
        
        // Apply command line overrides
        if (!override_worker_id.empty()) {
            config.worker_id = override_worker_id;
            LOG(INFO) << "Overriding worker_id to: " << config.worker_id;
        }
        if (!override_node_id.empty()) {
            config.node_id = override_node_id;
            LOG(INFO) << "Overriding node_id to: " << config.node_id;
        }
        
        // Validate required configuration
        if (config.worker_id.empty()) {
            LOG(ERROR) << "worker_id is required in configuration";
            return 1;
        }
        if (config.node_id.empty()) {
            LOG(ERROR) << "node_id is required in configuration";
            return 1;
        }
        
        // Print configuration
        LOG(INFO) << "=== Blackbird Worker Configuration ===";
        LOG(INFO) << "  Worker ID: " << config.worker_id;
        LOG(INFO) << "  Node ID: " << config.node_id;
        LOG(INFO) << "  Cluster ID: " << config.cluster_id;
        LOG(INFO) << "  Etcd Endpoints: " << config.etcd_endpoints;
        LOG(INFO) << "  Storage Pools: " << config.storage_pools.size();
        
        // Create worker service
        LOG(INFO) << "Creating worker service...";
        g_worker_service = std::make_shared<WorkerService>(config);
        
        // Create storage pools from configuration
        LOG(INFO) << "Creating storage pools from configuration...";
        err = g_worker_service->create_storage_pools_from_config();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to create storage pools: " << error::to_string(err);
            return 1;
        }
        
        // Initialize worker service
        LOG(INFO) << "Initializing worker service...";
        err = g_worker_service->initialize();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize worker service: " << error::to_string(err);
            return 1;
        }
        
        // Start worker service
        LOG(INFO) << "Starting worker service...";
        err = g_worker_service->start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start worker service: " << error::to_string(err);
            return 1;
        }
        
        LOG(INFO) << "Worker service started successfully!";
        LOG(INFO) << "Worker is ready to receive client connections...";
        LOG(INFO) << "Press Ctrl+C to stop the worker";
        
        // Wait for service to stop
        while (g_worker_service->is_running()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        LOG(INFO) << "Worker service stopped";
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in worker: " << e.what();
        return 1;
    }
    
    return 0;
} 