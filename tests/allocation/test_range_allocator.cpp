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

