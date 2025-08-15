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
    return pool;
}

} // namespace

TEST(RangeAllocatorBasics, EmptyStatsAndValidation) {
    RangeAllocator ra;
    auto stats = ra.get_stats(std::nullopt);
    EXPECT_EQ(stats.total_allocated_bytes, 0u);
    EXPECT_EQ(stats.total_free_bytes, 0u);
    EXPECT_EQ(stats.total_objects, 0u);
    EXPECT_EQ(stats.total_shards, 0u);
    EXPECT_TRUE(ra.validate_consistency());
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
        .enable_striping = false,   // forces allocate_contiguous path
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

