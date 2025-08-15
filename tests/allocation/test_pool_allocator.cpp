#include <gtest/gtest.h>
#include <vector>
#include <thread>
#include <random>
#include <mutex>

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

TEST(PoolAllocatorBasic, InitializesWithSingleFreeRange) {
    auto pool = make_pool("pool-A", 1024);
    PoolAllocator alloc{pool};

    EXPECT_EQ(alloc.total_free(), 1024u);
    EXPECT_EQ(alloc.largest_free_block(), 1024u);
    EXPECT_DOUBLE_EQ(alloc.fragmentation_ratio(), 0.0);
    EXPECT_TRUE(alloc.can_allocate(1024));
    EXPECT_FALSE(alloc.can_allocate(1025));
}

TEST(PoolAllocatorBasic, AllocateExactAndFreeMergesBack) {
    auto pool = make_pool("pool-A", 4096);
    PoolAllocator alloc{pool};

    auto r = alloc.allocate(4096);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->offset, 0u);
    EXPECT_EQ(r->length, 4096u);
    EXPECT_EQ(alloc.total_free(), 0u);

    alloc.free(*r);
    EXPECT_EQ(alloc.total_free(), 4096u);
    EXPECT_EQ(alloc.largest_free_block(), 4096u);
    EXPECT_DOUBLE_EQ(alloc.fragmentation_ratio(), 0.0);
}

TEST(PoolAllocatorBasic, AllocateSplitLeavesRemainder) {
    auto pool = make_pool("pool-A", 1000);
    PoolAllocator alloc{pool};

    auto r = alloc.allocate(200);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->offset, 0u);
    EXPECT_EQ(r->length, 200u);
    EXPECT_EQ(alloc.total_free(), 800u);
    EXPECT_EQ(alloc.largest_free_block(), 800u);
}

TEST(PoolAllocatorFit, BestFitChoosesTightestRange) {
    auto pool = make_pool("pool-A", 1000);
    PoolAllocator alloc{pool};

    // Create allocated layout: [0..100)[100..300)[300..600)[600..1000)
    auto a = alloc.allocate(100); ASSERT_TRUE(a);
    auto b = alloc.allocate(200); ASSERT_TRUE(b);
    auto c = alloc.allocate(300); ASSERT_TRUE(c);
    auto d = alloc.allocate(400); ASSERT_TRUE(d);

    // Free B and D to form free blocks of sizes 200 and 400
    alloc.free(*b);
    alloc.free(*d);

    // Best fit for 150 should choose former B range at offset 100 (size 200)
    auto r = alloc.allocate(150, /*prefer_best_fit=*/true);
    ASSERT_TRUE(r);
    EXPECT_EQ(r->offset, 100u);
    EXPECT_EQ(r->length, 150u);
}

TEST(PoolAllocatorFit, FirstFitChoosesLowestOffset) {
    auto pool = make_pool("pool-A", 1000);
    PoolAllocator alloc{pool};

    auto a = alloc.allocate(100); ASSERT_TRUE(a);
    auto b = alloc.allocate(200); ASSERT_TRUE(b);
    auto c = alloc.allocate(300); ASSERT_TRUE(c);
    auto d = alloc.allocate(400); ASSERT_TRUE(d);

    // Free C (size 300 at offset 300) and D (size 400 at offset 600)
    alloc.free(*c);
    alloc.free(*d);

    // First fit for 250 should pick offset 300 (size 300), not 600
    auto r = alloc.allocate(250, /*prefer_best_fit=*/false);
    ASSERT_TRUE(r);
    EXPECT_EQ(r->offset, 300u);
}

TEST(PoolAllocatorStats, FragmentationComputesCorrectly) {
    auto pool = make_pool("pool-A", 1000);
    PoolAllocator alloc{pool};

    auto a = alloc.allocate(100); ASSERT_TRUE(a); // [0..100)
    auto b = alloc.allocate(200); ASSERT_TRUE(b); // [100..300)
    auto c = alloc.allocate(50);  ASSERT_TRUE(c); // [300..350)

    // Free b only: creates two free blocks [100..300) and [350..1000)
    alloc.free(*b);
    EXPECT_EQ(alloc.total_free(), 850u);  // 200 + 650
    EXPECT_EQ(alloc.largest_free_block(), 650u);
    EXPECT_NEAR(alloc.fragmentation_ratio(), 1.0 - (650.0 / 850.0), 1e-9);

    // Free c next: merges [100..300), [300..350), and [350..1000) -> [100..1000)
    alloc.free(*c);
    EXPECT_EQ(alloc.total_free(), 900u);  // 100..1000 free
    EXPECT_EQ(alloc.largest_free_block(), 900u);
    EXPECT_DOUBLE_EQ(alloc.fragmentation_ratio(), 0.0);

    // Free a finally: all free merges into single block [0..1000)
    alloc.free(*a);
    EXPECT_EQ(alloc.total_free(), 1000u);
    EXPECT_EQ(alloc.largest_free_block(), 1000u);
    EXPECT_DOUBLE_EQ(alloc.fragmentation_ratio(), 0.0);
}

TEST(PoolAllocatorConcurrency, AllocateAndFreeFromMultipleThreads) {
    constexpr size_t pool_size = 1 << 16; // 64 KiB
    constexpr size_t block_size = 64;
    auto pool = make_pool("pool-A", pool_size);
    PoolAllocator alloc{pool};

    std::mutex vec_mutex;
    std::vector<Range> allocated;

    auto allocate_worker = [&]() {
        for (;;) {
            auto r = alloc.allocate(block_size);
            if (!r) break;
            std::lock_guard<std::mutex> g(vec_mutex);
            allocated.push_back(*r);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) threads.emplace_back(allocate_worker);
    for (auto& t : threads) t.join();

    // All space should be allocated in multiples of block_size
    size_t allocated_bytes = 0;
    for (const auto& r : allocated) allocated_bytes += r.length;
    EXPECT_LE(allocated_bytes, pool_size);

    // Randomize free order and free in parallel
    std::mt19937 rng(12345);
    std::shuffle(allocated.begin(), allocated.end(), rng);

    auto free_worker = [&](size_t begin, size_t end) {
        for (size_t i = begin; i < end; ++i) alloc.free(allocated[i]);
    };

    threads.clear();
    const size_t chunk = allocated.size() / 8 + 1;
    for (size_t i = 0; i < allocated.size(); i += chunk) {
        threads.emplace_back(free_worker, i, std::min(allocated.size(), i + chunk));
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(alloc.total_free(), pool_size);
    EXPECT_EQ(alloc.largest_free_block(), pool_size);
    EXPECT_DOUBLE_EQ(alloc.fragmentation_ratio(), 0.0);
}

