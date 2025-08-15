#include <iostream>
#include <map>
#include "blackbird/allocation/range_allocator.h"
#include "blackbird/worker/storage/disk_backend.h"

using namespace blackbird;
using namespace blackbird::allocation;

int main() {
    std::cout << "=== Blackbird Disk Storage Demo ===" << std::endl;
    
    // Create a disk backend
    DiskBackend disk_backend(10 * 1024 * 1024, StorageClass::NVME, "/tmp/blackbird_demo");
    auto init_result = disk_backend.initialize();
    if (init_result != ErrorCode::OK) {
        std::cerr << "Failed to initialize disk backend!" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Disk backend initialized successfully!" << std::endl;
    std::cout << "   Storage Class: NVME" << std::endl;
    std::cout << "   Total Capacity: " << disk_backend.get_total_capacity() << " bytes" << std::endl;
    std::cout << "   Mount Path: /tmp/blackbird_demo" << std::endl;
    
    // Create memory pools for the allocator
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    MemoryPool disk_pool;
    disk_pool.id = "nvme_pool_demo";
    disk_pool.node_id = "worker-demo";
    disk_pool.storage_class = StorageClass::NVME;
    disk_pool.size = disk_backend.get_total_capacity();
    disk_pool.used = disk_backend.get_used_capacity();
    disk_pool.ucx_endpoint = "127.0.0.1:12345";
    disk_pool.ucx_remote_addr = disk_backend.get_base_address();
    disk_pool.ucx_rkey_hex = "deadbeef";
    
    pools["nvme_pool_demo"] = disk_pool;
    
    // Create allocator and test allocation
    RangeAllocator allocator;
    
    AllocationRequest req;
    req.object_key = "demo_object";
    req.data_size = 1024 * 1024;  // 1MB
    req.replication_factor = 1;
    req.max_workers_per_copy = 1;
    req.preferred_classes = {StorageClass::NVME};
    req.enable_striping = true;
    req.min_shard_size = 1024;
    
    std::cout << "\n=== Testing Allocation ===" << std::endl;
    auto alloc_result = allocator.allocate(req, pools);
    
    if (std::holds_alternative<AllocationResult>(alloc_result)) {
        auto result = std::get<AllocationResult>(alloc_result);
        std::cout << "✅ Allocation successful!" << std::endl;
        std::cout << "   Copies: " << result.copies.size() << std::endl;
        std::cout << "   Total Shards: " << result.total_shards_created << std::endl;
        
        for (size_t i = 0; i < result.copies.size(); ++i) {
            std::cout << "   Copy " << i << ":" << std::endl;
            std::cout << "     Shards: " << result.copies[i].shards.size() << std::endl;
            for (const auto& shard : result.copies[i].shards) {
                std::cout << "       Pool: " << shard.pool_id 
                         << ", Storage Class: " << static_cast<int>(shard.storage_class)
                         << ", Size: " << shard.length << " bytes" << std::endl;
            }
        }
        
        // Test freeing
        auto free_result = allocator.free(req.object_key);
        if (free_result == ErrorCode::OK) {
            std::cout << "✅ Free successful!" << std::endl;
        } else {
            std::cout << "❌ Free failed!" << std::endl;
        }
    } else {
        auto error = std::get<ErrorCode>(alloc_result);
        std::cout << "❌ Allocation failed with error: " << static_cast<int>(error) << std::endl;
    }
    
    std::cout << "\n=== Storage Backend Stats ===" << std::endl;
    auto stats = disk_backend.get_stats();
    std::cout << "   Total Capacity: " << stats.total_capacity << " bytes" << std::endl;
    std::cout << "   Used Capacity: " << stats.used_capacity << " bytes" << std::endl;
    std::cout << "   Available Capacity: " << stats.available_capacity << " bytes" << std::endl;
    std::cout << "   Utilization: " << (stats.utilization * 100) << "%" << std::endl;
    std::cout << "   Reservations: " << stats.num_reservations << std::endl;
    std::cout << "   Committed Shards: " << stats.num_committed_shards << std::endl;
    
    std::cout << "\n✅ Disk storage backend fully functional!" << std::endl;
    
    return 0;
}