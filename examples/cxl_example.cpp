/**
 * @file cxl_example.cpp
 * @brief Example demonstrating CXL memory tier integration in Blackbird
 * 
 * This example shows how to use Blackbird with CXL-attached memory as a
 * first-class storage tier, supporting various CXL configurations including
 * CXL.mem, CXL Type 2 devices, and fabric topologies.
 */

#include <iostream>
#include <memory>
#include <chrono>
#include <glog/logging.h>

#include "blackbird/worker/storage/cxl_memory_backend.h"
#include "blackbird/transport/cxl_transport_config.h"
#include "blackbird/common/types.h"

using namespace blackbird;

void print_cxl_stats(const StorageStats& stats) {
    std::cout << "\n=== CXL Memory Statistics ===" << std::endl;
    std::cout << "Total Capacity:     " << (stats.total_capacity / (1024 * 1024)) << " MB" << std::endl;
    std::cout << "Used Capacity:      " << (stats.used_capacity / (1024 * 1024)) << " MB" << std::endl;
    std::cout << "Available Capacity: " << (stats.available_capacity / (1024 * 1024)) << " MB" << std::endl;
    std::cout << "Utilization:        " << (stats.utilization * 100.0) << "%" << std::endl;
    std::cout << "Reservations:       " << stats.num_reservations << std::endl;
    std::cout << "Committed Shards:   " << stats.num_committed_shards << std::endl;
    std::cout << "============================\n" << std::endl;
}

int main(int argc, char* argv[]) {
    // Initialize logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    
    LOG(INFO) << "Starting CXL Memory Example";
    
    // Example 1: Basic CXL.mem configuration
    {
        LOG(INFO) << "\n=== Example 1: Basic CXL.mem Device ===";
        
        CxlDeviceConfig config;
        config.device_id = "cxl0";
        config.device_path = "/dev/cxl/mem0";
        config.dax_device = "/dev/dax0.0";  // DAX device for direct access
        config.capacity = 64ULL * 1024 * 1024 * 1024;  // 64GB
        config.interleave_granularity = 256;           // 256B interleaving
        config.enable_numa_binding = true;
        config.numa_node = 1;                          // Bind to NUMA node 1
        config.enable_persistent_mode = false;         // Volatile memory
        
        auto backend = std::make_unique<CxlMemoryBackend>(config, StorageClass::CXL_MEMORY);
        
        if (backend->initialize() != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize CXL backend";
            return 1;
        }
        
        LOG(INFO) << "CXL backend initialized successfully";
        LOG(INFO) << "Device: " << config.device_id;
        LOG(INFO) << "Capacity: " << (config.capacity / (1024 * 1024 * 1024)) << " GB";
        LOG(INFO) << "NUMA Node: " << config.numa_node;
        
        // Allocate some memory shards
        auto result = backend->reserve_shard(1024 * 1024);  // 1MB shard
        if (is_ok(result)) {
            auto token = get_value(result);
            LOG(INFO) << "Reserved 1MB shard at address: 0x" << std::hex << token.remote_addr;
            
            // Commit the shard
            if (backend->commit_shard(token) == ErrorCode::OK) {
                LOG(INFO) << "Shard committed successfully";
            }
            
            print_cxl_stats(backend->get_stats());
            
            // Free the shard
            backend->free_shard(token.remote_addr, token.size);
            LOG(INFO) << "Shard freed";
        }
        
        backend->shutdown();
    }
    
    // Example 2: CXL Type 2 Device (Compute + Memory)
    {
        LOG(INFO) << "\n=== Example 2: CXL Type 2 Device ===";
        
        CxlDeviceConfig config;
        config.device_id = "cxl_type2_accelerator";
        config.device_path = "/dev/cxl/mem1";
        config.capacity = 128ULL * 1024 * 1024 * 1024;  // 128GB
        config.interleave_granularity = 4096;           // 4KB interleaving
        config.enable_numa_binding = true;
        config.numa_node = 2;
        
        auto backend = std::make_unique<CxlMemoryBackend>(config, StorageClass::CXL_TYPE2_DEVICE);
        
        LOG(INFO) << "Created CXL Type 2 device backend";
        LOG(INFO) << "This device provides both compute and memory capabilities";
    }
    
    // Example 3: CXL Transport Configuration
    {
        LOG(INFO) << "\n=== Example 3: CXL Transport Configuration ===";
        
        CxlTransportConfig transport_config;
        transport_config.interconnect_type = CxlInterconnectType::CXL_FABRIC;
        transport_config.transport_protocol = CxlTransportProtocol::RDMA_OVER_CXL;
        transport_config.enable_fabric_manager = true;
        transport_config.fabric_manager_endpoint = "localhost:8080";
        transport_config.enable_zero_copy = true;
        transport_config.max_transfer_size = 4ULL * 1024 * 1024 * 1024;  // 4GB
        
        // Configure multi-path with fallback
        transport_config.enable_multipath = true;
        transport_config.allow_ucx_fallback = true;
        transport_config.allow_nvlink_fallback = true;
        transport_config.allow_roce_fallback = true;
        
        LOG(INFO) << "Transport Configuration:";
        LOG(INFO) << "  Interconnect: " << to_string(transport_config.interconnect_type);
        LOG(INFO) << "  Protocol: " << to_string(transport_config.transport_protocol);
        LOG(INFO) << "  Zero-copy: " << (transport_config.enable_zero_copy ? "enabled" : "disabled");
        LOG(INFO) << "  Max transfer: " << (transport_config.max_transfer_size / (1024 * 1024 * 1024)) << " GB";
        LOG(INFO) << "  Multipath: " << (transport_config.enable_multipath ? "enabled" : "disabled");
    }
    
    // Example 4: CXL Memory Pool Configuration
    {
        LOG(INFO) << "\n=== Example 4: CXL Memory Pool Configuration ===";
        
        CxlMemoryPoolConfig pool_config;
        pool_config.device_id = "cxl_pool_0";
        pool_config.capacity = 256ULL * 1024 * 1024 * 1024;  // 256GB
        pool_config.latency_ns = 150;                         // 150ns latency
        pool_config.bandwidth_gbps = 64;                      // 64 Gbps bandwidth
        pool_config.is_persistent = false;
        pool_config.supports_cache_coherency = true;
        pool_config.numa_node = 3;
        pool_config.interleave_ways = 4;                      // 4-way interleaving
        pool_config.interleave_granularity = 256;
        
        LOG(INFO) << "CXL Memory Pool:";
        LOG(INFO) << "  Capacity: " << (pool_config.capacity / (1024 * 1024 * 1024)) << " GB";
        LOG(INFO) << "  Latency: " << pool_config.latency_ns << " ns";
        LOG(INFO) << "  Bandwidth: " << pool_config.bandwidth_gbps << " Gbps";
        LOG(INFO) << "  Cache Coherency: " << (pool_config.supports_cache_coherency ? "yes" : "no");
        LOG(INFO) << "  Interleaving: " << pool_config.interleave_ways << "-way";
    }
    
    LOG(INFO) << "\n=== CXL Integration Complete ===";
    LOG(INFO) << "Blackbird now supports CXL-attached memory as a first-class tier";
    LOG(INFO) << "Supported configurations:";
    LOG(INFO) << "  - CXL.mem (volatile and persistent)";
    LOG(INFO) << "  - CXL Type 2 devices (compute + memory)";
    LOG(INFO) << "  - CXL fabric topologies";
    LOG(INFO) << "  - Multi-path with fallback (UCX, NVLink, RoCE)";
    
    google::ShutdownGoogleLogging();
    return 0;
}

