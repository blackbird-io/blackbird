#include <iostream>
#include <signal.h>
#include <glog/logging.h>

#include "blackbird/worker/worker_service.h"
#include "blackbird/worker/storage/storage_backend.h"

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
    std::cout << "  --worker-id <id>          Unique worker identifier (required)\n";
    std::cout << "  --node-id <id>            Physical node identifier (required)\n";
    std::cout << "  --etcd-endpoints <eps>    Comma-separated etcd endpoints (default: localhost:2379)\n";
    std::cout << "  --cluster-id <id>         Cluster identifier (default: blackbird_cluster)\n";
    std::cout << "  --memory-size <bytes>     RAM memory pool size in bytes (default: 1GB)\n";
    std::cout << "  --storage-class <class>   Storage class: RAM_CPU, RAM_GPU (default: RAM_CPU)\n";
    std::cout << "  --help                    Show this help message\n";
    std::cout << "\nExample:\n";
    std::cout << "  " << program_name << " --worker-id worker-1 --node-id node-a --memory-size 2147483648\n";
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
    WorkerServiceConfig config;
    config.etcd_endpoints = "localhost:2379";
    uint64_t memory_size = 1ULL << 30; // 1GB default
    StorageClass storage_class = StorageClass::RAM_CPU;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--worker-id" && i + 1 < argc) {
            config.worker_id = argv[++i];
        } else if (arg == "--node-id" && i + 1 < argc) {
            config.node_id = argv[++i];
        } else if (arg == "--etcd-endpoints" && i + 1 < argc) {
            config.etcd_endpoints = argv[++i];
        } else if (arg == "--cluster-id" && i + 1 < argc) {
            config.cluster_id = argv[++i];
        } else if (arg == "--memory-size" && i + 1 < argc) {
            memory_size = std::stoull(argv[++i]);
        } else if (arg == "--storage-class" && i + 1 < argc) {
            std::string class_str = argv[++i];
            if (class_str == "RAM_CPU") {
                storage_class = StorageClass::RAM_CPU;
            } else if (class_str == "RAM_GPU") {
                storage_class = StorageClass::RAM_GPU;
            } else {
                std::cerr << "Unknown storage class: " << class_str << std::endl;
                return 1;
            }
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }
    
    // Validate required arguments
    if (config.worker_id.empty()) {
        std::cerr << "Error: --worker-id is required" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    if (config.node_id.empty()) {
        std::cerr << "Error: --node-id is required" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    // Print configuration
    LOG(INFO) << "=== Blackbird Worker Configuration ===";
    LOG(INFO) << "  Worker ID: " << config.worker_id;
    LOG(INFO) << "  Node ID: " << config.node_id;
    LOG(INFO) << "  Cluster ID: " << config.cluster_id;
    LOG(INFO) << "  Etcd Endpoints: " << config.etcd_endpoints;
    LOG(INFO) << "  Memory Size: " << memory_size << " bytes (" << (memory_size / (1024*1024)) << " MB)";
    LOG(INFO) << "  Storage Class: " << static_cast<uint32_t>(storage_class);
    
    try {
        // Create worker service
        LOG(INFO) << "Creating worker service...";
        g_worker_service = std::make_shared<WorkerService>(config);
        
        // Add memory pool
        LOG(INFO) << "Adding memory pool...";
        auto backend = create_storage_backend(storage_class, memory_size);
        if (!backend) {
            LOG(ERROR) << "Failed to create storage backend";
            return 1;
        }
        
        std::string pool_id = config.worker_id + "_pool_0";
        auto err = g_worker_service->add_storage_pool(pool_id, std::move(backend));
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to add storage pool: " << error::to_string(err);
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
        LOG(INFO) << "Worker is ready to receive allocation requests...";
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