#include <gtest/gtest.h>

#include "blackbird/allocation/allocator_interface.h"
#include "blackbird/allocation/range_allocator.h"
#include "blackbird/common/types.h"

using namespace blackbird;
using namespace blackbird::allocation;

namespace {

MemoryPool make_pool(const std::string& id, size_t size, size_t used = 0) {
    MemoryPool pool{};
    pool.id = id;
    pool.node_id = "node-1";
    pool.base_addr = 0;
    pool.size = size;
    pool.used = used;
    pool.storage_class = StorageClass::RAM_CPU;
    // UCX sockaddr fields needed by PoolAllocator created by RangeAllocator
    pool.ucx_endpoint = "127.0.0.1:12345";
    pool.ucx_remote_addr = 0x20000000ULL;
    pool.ucx_rkey_hex = "FEEDC0DE";
    return pool;
}

} // namespace

TEST(RangeAllocatorBasics, EmptyStats) {
    RangeAllocator ra;
    auto stats = ra.get_stats(std::nullopt);
    EXPECT_EQ(stats.total_allocated_bytes, 0u);
    EXPECT_EQ(stats.total_free_bytes, 0u);
    EXPECT_EQ(stats.total_objects, 0u);
    EXPECT_EQ(stats.total_shards, 0u);
}

TEST(RangeAllocatorBasics, CanAllocateEstimationRespectsPreferredClasses) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1024 * 1024);

    AllocationRequest req{
        .object_key = "obj",
        .data_size = 4096,
        .replication_factor = 2,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = ""
    };
    EXPECT_TRUE(ra.can_allocate(req, pools));

    // If preferred classes exclude RAM_CPU, current implementation returns false
    req.preferred_classes = {StorageClass::NVME};
    EXPECT_FALSE(ra.can_allocate(req, pools));
}

TEST(RangeAllocatorStriping, BasicStripingAllocation) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1024 * 8);
    pools["p2"] = make_pool("p2", 1024 * 8);

    AllocationRequest req{
        .object_key = "obj-striping",
        .data_size = 1024 * 4,
        .replication_factor = 1,
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Allocation should succeed, got error: " << static_cast<int>(std::get<ErrorCode>(result));
    
    auto alloc_result = std::get<AllocationResult>(result);
    
    // Validate basic allocation structure
    EXPECT_EQ(alloc_result.copies.size(), req.replication_factor);
    EXPECT_GT(alloc_result.total_shards_created, 0u);
    EXPECT_EQ(alloc_result.pools_used, 2u);
    
    // Validate copy structure
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    const auto& copy = alloc_result.copies[0];
    EXPECT_EQ(copy.copy_index, 0u);
    EXPECT_EQ(copy.shards.size(), 2u); // Should stripe across 2 workers
    
    // Validate shard details
    size_t total_shard_size = 0;
    for (const auto& shard : copy.shards) {
        EXPECT_GT(shard.length, 0u);
        EXPECT_GE(shard.length, req.min_shard_size);
        EXPECT_FALSE(shard.pool_id.empty());
        EXPECT_FALSE(shard.worker_id.empty());
        EXPECT_EQ(shard.storage_class, StorageClass::RAM_CPU);
        total_shard_size += shard.length;
        
        // Validate UCX endpoint
        EXPECT_FALSE(shard.endpoint.ip.empty());
        EXPECT_GT(shard.endpoint.port, 0);
        
        // Validate memory location
        EXPECT_TRUE(std::holds_alternative<MemoryLocation>(shard.location));
        auto mem_loc = std::get<MemoryLocation>(shard.location);
        EXPECT_GT(mem_loc.remote_addr, 0u);
        EXPECT_GT(mem_loc.rkey, 0u);
        EXPECT_EQ(mem_loc.size, shard.length);
    }
    
    // Total shard size should equal requested data size
    EXPECT_EQ(total_shard_size, req.data_size);
    
    // Cleanup should work
    EXPECT_EQ(ra.free("obj-striping"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, MultipleReplicasSpreadAcrossPools) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    // Create 6 pools to allow spreading of 3 replicas with 2 workers each
    for (int i = 0; i < 6; ++i) {
        pools["p" + std::to_string(i)] = make_pool("p" + std::to_string(i), 1024 * 16);
    }

    AllocationRequest req{
        .object_key = "obj-multi-replica",
        .data_size = 1024 * 8,
        .replication_factor = 3,
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Multi-replica allocation should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    
    // Should have 3 copies
    ASSERT_EQ(alloc_result.copies.size(), 3u);
    EXPECT_EQ(alloc_result.total_shards_created, 6u); // 3 copies * 2 shards each
    
    // Track which pools are used to verify spreading
    std::set<std::string> used_pools;
    
    for (size_t copy_idx = 0; copy_idx < alloc_result.copies.size(); ++copy_idx) {
        const auto& copy = alloc_result.copies[copy_idx];
        EXPECT_EQ(copy.copy_index, copy_idx);
        EXPECT_EQ(copy.shards.size(), 2u);
        
        for (const auto& shard : copy.shards) {
            used_pools.insert(shard.pool_id);
            EXPECT_GE(shard.length, req.min_shard_size);
        }
    }
    
    // Should use multiple pools (ideally 6, but at least more than 2)
    EXPECT_GE(used_pools.size(), 3u) << "Should spread replicas across multiple pools";
    
    EXPECT_EQ(ra.free("obj-multi-replica"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, MinShardSizeViolationFails) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1024 * 8);
    pools["p2"] = make_pool("p2", 1024 * 8);
    pools["p3"] = make_pool("p3", 1024 * 8);
    pools["p4"] = make_pool("p4", 1024 * 8);

    AllocationRequest req{
        .object_key = "obj-small-shards",
        .data_size = 1024,           // 1KB data
        .replication_factor = 1,
        .max_workers_per_copy = 4,   // 4 workers = 256 bytes per shard
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 4096       // 4KB minimum, but shards would be 256 bytes
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result)) 
        << "Should fail when shards are too small";
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INSUFFICIENT_SPACE);
}

TEST(RangeAllocatorStriping, InsufficientCapacityFails) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1024);     // 1KB pool
    pools["p2"] = make_pool("p2", 1024);     // 1KB pool

    AllocationRequest req{
        .object_key = "obj-too-big",
        .data_size = 1024 * 8,       // 8KB data - too big for small pools
        .replication_factor = 2,     // 16KB total needed
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result)) 
        << "Should fail when pools lack capacity";
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INSUFFICIENT_SPACE);
}

TEST(RangeAllocatorStriping, StorageClassPreferenceRespected) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Mix of storage classes
    auto pool1 = make_pool("gpu1", 1024 * 8);
    pool1.storage_class = StorageClass::RAM_GPU;
    pools["gpu1"] = pool1;
    
    auto pool2 = make_pool("gpu2", 1024 * 8);
    pool2.storage_class = StorageClass::RAM_GPU;
    pools["gpu2"] = pool2;
    
    auto pool3 = make_pool("cpu1", 1024 * 8);
    pool3.storage_class = StorageClass::RAM_CPU;
    pools["cpu1"] = pool3;

    AllocationRequest req{
        .object_key = "obj-gpu-preferred",
        .data_size = 1024 * 4,
        .replication_factor = 1,
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_GPU},  // Prefer GPU
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "GPU allocation should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    
    // All shards should be on GPU pools
    for (const auto& shard : alloc_result.copies[0].shards) {
        EXPECT_EQ(shard.storage_class, StorageClass::RAM_GPU);
        EXPECT_TRUE(shard.pool_id == "gpu1" || shard.pool_id == "gpu2");
    }
    
    EXPECT_EQ(ra.free("obj-gpu-preferred"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, FallbackToNonPreferredPools) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Create small GPU pools and large CPU pools
    auto gpu_pool = make_pool("gpu1", 1024);  // Small GPU pool
    gpu_pool.storage_class = StorageClass::RAM_GPU;
    pools["gpu1"] = gpu_pool;
    
    auto cpu_pool1 = make_pool("cpu1", 1024 * 16);  // Large CPU pool
    cpu_pool1.storage_class = StorageClass::RAM_CPU;
    pools["cpu1"] = cpu_pool1;
    
    auto cpu_pool2 = make_pool("cpu2", 1024 * 16);  // Large CPU pool
    cpu_pool2.storage_class = StorageClass::RAM_CPU;
    pools["cpu2"] = cpu_pool2;

    AllocationRequest req{
        .object_key = "obj-fallback",
        .data_size = 1024 * 8,  // 8KB - too big for GPU pool
        .replication_factor = 1,
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_GPU},  // Prefer GPU but not enough space
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Should fall back to CPU pools when GPU insufficient";
    
    auto alloc_result = std::get<AllocationResult>(result);
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    
    // Should have fallen back to CPU pools
    for (const auto& shard : alloc_result.copies[0].shards) {
        EXPECT_EQ(shard.storage_class, StorageClass::RAM_CPU) 
            << "Should use CPU pools as fallback";
        EXPECT_TRUE(shard.pool_id == "cpu1" || shard.pool_id == "cpu2");
    }
    
    EXPECT_EQ(ra.free("obj-fallback"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, EdgeCaseShardSizes) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1024 * 32);
    pools["p2"] = make_pool("p2", 1024 * 32);
    pools["p3"] = make_pool("p3", 1024 * 32);  // Add third pool to support 3 workers

    // Test with data size not evenly divisible by workers
    AllocationRequest req{
        .object_key = "obj-uneven",
        .data_size = 1000,  // 1000 bytes across 3 workers = 333, 333, 334
        .replication_factor = 1,
        .max_workers_per_copy = 3,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 300
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Uneven shard allocation should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    
    // Verify shards are reasonably sized and total correctly
    size_t total_shard_size = 0;
    for (const auto& shard : alloc_result.copies[0].shards) {
        EXPECT_GE(shard.length, 300u) << "Each shard should meet min_shard_size";
        EXPECT_LE(shard.length, 400u) << "Shards should not be too uneven";
        total_shard_size += shard.length;
    }
    EXPECT_EQ(total_shard_size, 1000u) << "Total shard size should equal data size";
    
    EXPECT_EQ(ra.free("obj-uneven"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, HighReplicationFactorStress) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Create 10 pools for high replication
    for (int i = 0; i < 10; ++i) {
        pools["pool" + std::to_string(i)] = make_pool("pool" + std::to_string(i), 1024 * 64);
    }

    AllocationRequest req{
        .object_key = "obj-high-replication",
        .data_size = 1024 * 16,
        .replication_factor = 5,  // 5 replicas
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "High replication factor should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    EXPECT_EQ(alloc_result.copies.size(), 5u) << "Should have 5 replicas";
    EXPECT_EQ(alloc_result.total_shards_created, 10u) << "Should have 10 total shards (5*2)";
    
    // Verify each copy has proper structure
    std::set<std::string> all_used_pools;
    for (size_t i = 0; i < alloc_result.copies.size(); ++i) {
        const auto& copy = alloc_result.copies[i];
        EXPECT_EQ(copy.copy_index, i) << "Copy index should be sequential";
        EXPECT_EQ(copy.shards.size(), 2u) << "Each copy should have 2 shards";
        
        for (const auto& shard : copy.shards) {
            all_used_pools.insert(shard.pool_id);
            EXPECT_EQ(shard.length, 8192u) << "Each shard should be 8KB (16KB/2)";
        }
    }
    
    // Should use multiple pools for better distribution
    EXPECT_GE(all_used_pools.size(), 5u) << "Should use at least 5 different pools";
    
    EXPECT_EQ(ra.free("obj-high-replication"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, SingleWorkerFallback) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["small1"] = make_pool("small1", 1024);      // 1KB pool
    pools["small2"] = make_pool("small2", 1024);      // 1KB pool

    AllocationRequest req{
        .object_key = "obj-single-worker",
        .data_size = 1024 * 3,  // 3KB - too big for 2KB total capacity
        .replication_factor = 1,
        .max_workers_per_copy = 4,  // Want 4 workers but insufficient total space
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result)) 
        << "Should fail when pools have insufficient total capacity";
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INSUFFICIENT_SPACE);
}

TEST(RangeAllocatorStriping, UCXEndpointDataIntegrity) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Create pool with specific UCX data
    auto pool = make_pool("test-pool", 1024 * 8);
    pool.ucx_endpoint = "192.168.1.100:9999";
    pool.ucx_remote_addr = 0x1000;
    pool.ucx_rkey_hex = "deadbeef";
    pools["test-pool"] = pool;

    AllocationRequest req{
        .object_key = "obj-ucx-test",
        .data_size = 1024 * 4,
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,   // Use striping but with 1 worker for single shard
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "UCX endpoint allocation should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    ASSERT_EQ(alloc_result.copies[0].shards.size(), 1u);
    
    const auto& shard = alloc_result.copies[0].shards[0];
    
    // Verify UCX endpoint parsing
    EXPECT_EQ(shard.endpoint.ip, "192.168.1.100");
    EXPECT_EQ(shard.endpoint.port, 9999);
    
    // Verify memory location
    ASSERT_TRUE(std::holds_alternative<MemoryLocation>(shard.location));
    auto mem_loc = std::get<MemoryLocation>(shard.location);
    EXPECT_EQ(mem_loc.remote_addr, 0x1000u);  // Base address + offset 0
    EXPECT_EQ(mem_loc.rkey, 0xdeadbeef);     // Parsed hex
    EXPECT_EQ(mem_loc.size, 1024u * 4);      // Shard size
    
    EXPECT_EQ(ra.free("obj-ucx-test"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, InvalidUCXDataHandling) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Pool with malformed UCX endpoint
    auto bad_pool1 = make_pool("bad-endpoint", 1024 * 8);
    bad_pool1.ucx_endpoint = "malformed-no-port";  // Missing port
    pools["bad-endpoint"] = bad_pool1;
    
    // Pool with invalid hex rkey
    auto bad_pool2 = make_pool("bad-rkey", 1024 * 8);
    bad_pool2.ucx_endpoint = "192.168.1.100:8080";
    bad_pool2.ucx_rkey_hex = "not-hex-data";  // Invalid hex
    pools["bad-rkey"] = bad_pool2;

    AllocationRequest req{
        .object_key = "obj-bad-ucx",
        .data_size = 1024 * 4,
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result)) 
        << "Should fail with invalid UCX data";
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INVALID_PARAMETERS);
}

TEST(RangeAllocatorStriping, ConcurrentAllocationsFragmentation) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["frag-test"] = make_pool("frag-test", 1024 * 32);

    // Allocate several objects to create fragmentation
    std::vector<std::string> object_keys;
    
    for (int i = 0; i < 5; ++i) {
        AllocationRequest req{
            .object_key = "obj-frag-" + std::to_string(i),
            .data_size = 1024 * 2,  // 2KB each
            .replication_factor = 1,
            .max_workers_per_copy = 1,
            .preferred_classes = {StorageClass::RAM_CPU},
            .preferred_node = "",
            .enable_locality_awareness = true,
            .enable_striping = false,
            .prefer_contiguous = false,
            .min_shard_size = 1024
        };

        auto result = ra.allocate(req, pools);
        ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
            << "Allocation " << i << " should succeed";
        object_keys.push_back(req.object_key);
    }
    
    // Free every other object to create fragmentation
    for (size_t i = 1; i < object_keys.size(); i += 2) {
        EXPECT_EQ(ra.free(object_keys[i]), ErrorCode::OK);
    }
    
    // Try to allocate a larger object that requires merging fragments
    AllocationRequest big_req{
        .object_key = "obj-big-merge",
        .data_size = 1024 * 4,  // 4KB - requires merged fragments
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = false,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };
    
    auto big_result = ra.allocate(big_req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(big_result)) 
        << "Should successfully allocate after fragmentation and merging";
    
    // Cleanup
    EXPECT_EQ(ra.free("obj-big-merge"), ErrorCode::OK);
    for (size_t i = 0; i < object_keys.size(); i += 2) {
        EXPECT_EQ(ra.free(object_keys[i]), ErrorCode::OK);
    }
}

TEST(RangeAllocatorStriping, ZeroDataSizeEdgeCase) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["test"] = make_pool("test", 1024 * 8);

    AllocationRequest req{
        .object_key = "obj-zero",
        .data_size = 0,  // Zero size
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    // Should either fail gracefully or handle zero-size allocation
    if (std::holds_alternative<AllocationResult>(result)) {
        auto alloc_result = std::get<AllocationResult>(result);
        EXPECT_EQ(alloc_result.copies.size(), 1u);
        if (!alloc_result.copies.empty() && !alloc_result.copies[0].shards.empty()) {
            for (const auto& shard : alloc_result.copies[0].shards) {
                EXPECT_EQ(shard.length, 0u) << "Zero-size allocation should have zero-size shards";
            }
        }
        EXPECT_EQ(ra.free("obj-zero"), ErrorCode::OK);
    } else {
        // Failing on zero-size is also acceptable behavior
        EXPECT_TRUE(std::holds_alternative<ErrorCode>(result));
    }
}

TEST(RangeAllocatorStriping, NodeLocalityPreference) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Create pools on different nodes
    auto pool1 = make_pool("node1-pool", 1024 * 16);
    pool1.node_id = "node-1";
    pools["node1-pool"] = pool1;
    
    auto pool2 = make_pool("node2-pool", 1024 * 16);
    pool2.node_id = "node-2";  
    pools["node2-pool"] = pool2;
    
    auto pool3 = make_pool("node3-pool", 1024 * 16);
    pool3.node_id = "node-3";
    pools["node3-pool"] = pool3;

    AllocationRequest req{
        .object_key = "obj-locality",
        .data_size = 1024 * 8,
        .replication_factor = 1,
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "node-2",  // Prefer node-2
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Node locality allocation should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    
    // Should prefer node-2 pool if locality awareness works
    for (const auto& shard : alloc_result.copies[0].shards) {
        EXPECT_EQ(shard.worker_id, "node-2") << "Should prefer specified node";
        EXPECT_EQ(shard.pool_id, "node2-pool");
    }
    
    EXPECT_EQ(ra.free("obj-locality"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, ExtremeShardSizeConstraints) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1024 * 16);
    pools["p2"] = make_pool("p2", 1024 * 16);

    AllocationRequest req{
        .object_key = "obj-extreme-min-shard",
        .data_size = 1024 * 4,  // 4KB total
        .replication_factor = 1,
        .max_workers_per_copy = 8,  // 8 workers = 512 bytes per shard
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024 * 2  // 2KB minimum - impossible with 8 workers!
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result)) 
        << "Should fail when min_shard_size is impossible to satisfy";
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INSUFFICIENT_SPACE);
}

TEST(RangeAllocatorStriping, DuplicateObjectKeyHandling) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["test"] = make_pool("test", 1024 * 32);

    AllocationRequest req{
        .object_key = "duplicate-key",
        .data_size = 1024 * 4,
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    // First allocation should succeed
    auto result1 = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result1)) 
        << "First allocation should succeed";

    // Second allocation with same key should fail
    auto result2 = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result2)) 
        << "Second allocation with same key should fail";
    EXPECT_EQ(std::get<ErrorCode>(result2), ErrorCode::OBJECT_ALREADY_EXISTS);

    EXPECT_EQ(ra.free("duplicate-key"), ErrorCode::OK);
    
    // After freeing, should be able to allocate again
    auto result3 = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result3)) 
        << "After freeing, allocation should succeed again";
    
    EXPECT_EQ(ra.free("duplicate-key"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, EmptyPoolsHandling) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    // Deliberately empty pools map

    AllocationRequest req{
        .object_key = "obj-no-pools",
        .data_size = 1024,
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result)) 
        << "Should fail with no pools available";
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INSUFFICIENT_SPACE);
}

TEST(RangeAllocatorStriping, LargeObjectAcrossManyPools) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Create 20 small pools
    for (int i = 0; i < 20; ++i) {
        pools["pool" + std::to_string(i)] = make_pool("pool" + std::to_string(i), 1024 * 4);
    }

    AllocationRequest req{
        .object_key = "obj-large-across-many",
        .data_size = 1024 * 64,  // 64KB across 20 pools
        .replication_factor = 1,
        .max_workers_per_copy = 16,  // Use many workers
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Large object allocation across many pools should succeed";
    
    auto alloc_result = std::get<AllocationResult>(result);
    ASSERT_EQ(alloc_result.copies.size(), 1u);
    
    // Should use multiple pools for large object
    std::set<std::string> used_pools;
    size_t total_size = 0;
    for (const auto& shard : alloc_result.copies[0].shards) {
        used_pools.insert(shard.pool_id);
        total_size += shard.length;
        EXPECT_GE(shard.length, 1024u) << "Each shard should meet min_shard_size";
    }
    
    EXPECT_EQ(total_size, 1024u * 64) << "Total size should equal requested size";
    EXPECT_GE(used_pools.size(), 10u) << "Should use many different pools";
    
    EXPECT_EQ(ra.free("obj-large-across-many"), ErrorCode::OK);
}

TEST(RangeAllocatorStriping, MemoryLocationOffsetCalculation) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    auto pool = make_pool("offset-test", 1024 * 32);
    pool.ucx_endpoint = "192.168.1.1:8080";
    pool.ucx_remote_addr = 0x10000;  // Base address
    pool.ucx_rkey_hex = "12345678";
    pools["offset-test"] = pool;

    // Allocate multiple objects to verify offset calculation
    std::vector<uint64_t> remote_addresses;
    
    for (int i = 0; i < 3; ++i) {
        AllocationRequest req{
            .object_key = "obj-offset-" + std::to_string(i),
            .data_size = 1024 * 4,  // 4KB each
            .replication_factor = 1,
            .max_workers_per_copy = 1,
            .preferred_classes = {StorageClass::RAM_CPU},
            .preferred_node = "",
            .enable_locality_awareness = true,
            .enable_striping = false,
            .prefer_contiguous = false,
            .min_shard_size = 1024
        };

        auto result = ra.allocate(req, pools);
        ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
            << "Allocation " << i << " should succeed";
        
        auto alloc_result = std::get<AllocationResult>(result);
        ASSERT_EQ(alloc_result.copies.size(), 1u);
        ASSERT_EQ(alloc_result.copies[0].shards.size(), 1u);
        
        const auto& shard = alloc_result.copies[0].shards[0];
        ASSERT_TRUE(std::holds_alternative<MemoryLocation>(shard.location));
        auto mem_loc = std::get<MemoryLocation>(shard.location);
        
        remote_addresses.push_back(mem_loc.remote_addr);
        EXPECT_GE(mem_loc.remote_addr, 0x10000u) << "Remote address should be >= base address";
        EXPECT_EQ(mem_loc.rkey, 0x12345678u) << "Rkey should be consistent";
        EXPECT_EQ(mem_loc.size, 1024u * 4) << "Size should match shard length";
    }
    
    // Verify addresses are different (no overlap)
    std::sort(remote_addresses.begin(), remote_addresses.end());
    for (size_t i = 1; i < remote_addresses.size(); ++i) {
        EXPECT_GT(remote_addresses[i], remote_addresses[i-1]) 
            << "Remote addresses should be distinct and non-overlapping";
    }
    
    // Cleanup
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(ra.free("obj-offset-" + std::to_string(i)), ErrorCode::OK);
    }
}

TEST(RangeAllocatorStriping, MaxReplicationFactorStress) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    
    // Create 50 pools for extreme replication
    for (int i = 0; i < 50; ++i) {
        pools["pool" + std::to_string(i)] = make_pool("pool" + std::to_string(i), 1024 * 32);
    }

    AllocationRequest req{
        .object_key = "obj-extreme-replication",
        .data_size = 1024 * 8,
        .replication_factor = 20,  // Extreme: 20 replicas
        .max_workers_per_copy = 2,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = true,
        .prefer_contiguous = false,
        .min_shard_size = 1024
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<AllocationResult>(result)) 
        << "Extreme replication should succeed with enough pools";
    
    auto alloc_result = std::get<AllocationResult>(result);
    EXPECT_EQ(alloc_result.copies.size(), 20u) << "Should have 20 replicas";
    EXPECT_EQ(alloc_result.total_shards_created, 40u) << "Should have 40 total shards (20*2)";
    
    // Verify all copies are correctly structured
    std::set<std::string> all_used_pools;
    for (size_t i = 0; i < alloc_result.copies.size(); ++i) {
        const auto& copy = alloc_result.copies[i];
        EXPECT_EQ(copy.copy_index, i) << "Copy index should be sequential";
        EXPECT_EQ(copy.shards.size(), 2u) << "Each copy should have 2 shards";
        
        size_t copy_total_size = 0;
        for (const auto& shard : copy.shards) {
            all_used_pools.insert(shard.pool_id);
            copy_total_size += shard.length;
        }
        EXPECT_EQ(copy_total_size, 1024u * 8) << "Each copy should have full data size";
    }
    
    // Should use many different pools for good distribution
    EXPECT_GE(all_used_pools.size(), 20u) << "Should use at least 20 different pools";
    
    EXPECT_EQ(ra.free("obj-extreme-replication"), ErrorCode::OK);
}

TEST(RangeAllocatorBehavior, ContiguousStrategyNotImplemented) {
    RangeAllocator ra;
    std::unordered_map<MemoryPoolId, MemoryPool> pools;
    pools["p1"] = make_pool("p1", 1 << 20);

    AllocationRequest req{
        .object_key = "obj",
        .data_size = 4096,
        .replication_factor = 1,
        .max_workers_per_copy = 1,
        .preferred_classes = {StorageClass::RAM_CPU},
        .preferred_node = "",
        .enable_locality_awareness = true,
        .enable_striping = false,   // force allocate_contiguous path
        .prefer_contiguous = true,
        .min_shard_size = 4096
    };

    auto result = ra.allocate(req, pools);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result));
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::NOT_IMPLEMENTED);
}

TEST(RangeAllocatorBehavior, FreeUnknownObjectReturnsNotFound) {
    RangeAllocator ra;
    EXPECT_EQ(ra.free("does-not-exist"), ErrorCode::OBJECT_NOT_FOUND);
}

