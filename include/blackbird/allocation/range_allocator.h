#pragma once

#include "allocator_interface.h"
#include <map>
#include <mutex>
#include <set>

namespace blackbird::allocation {

/**
 * @brief Range representing a contiguous memory allocation
 */
struct Range {
    uint64_t offset;   
    uint64_t length;   
    
    Range(uint64_t off, uint64_t len) : offset(off), length(len) {}
    
    uint64_t end() const { return offset + length; }
    bool adjacent_to(const Range& other) const {
        return end() == other.offset || other.end() == offset;
    }
    bool can_merge_with(const Range& other) const {
        return adjacent_to(other);
    }
    Range merge_with(const Range& other) const {
        uint64_t start = std::min(offset, other.offset);
        uint64_t finish = std::max(end(), other.end());
        return Range(start, finish - start);
    }
};

/**
 * @brief Per-pool allocation state with thread-safe range management
 */
class PoolAllocator {
public:
    PoolAllocator(const MemoryPool& pool);
    
    std::optional<Range> allocate(uint64_t size, bool prefer_best_fit = true);
    void free(const Range& range);
    
    size_t total_free() const;
    size_t largest_free_block() const;
    double fragmentation_ratio() const;
    bool can_allocate(uint64_t size) const;
    
    const MemoryPoolId& pool_id() const { return pool_id_; }
    StorageClass storage_class() const { return storage_class_; }
    const std::string& node_id() const { return node_id_; }
    
    MemoryLocation to_memory_location(const Range& range) const;
    
private:
    MemoryPoolId pool_id_;
    StorageClass storage_class_;
    std::string node_id_;
    uint64_t base_addr_;
    uint32_t ucx_rkey_;
    size_t pool_size_;
    
    mutable std::mutex mutex_;
    
    std::map<uint64_t, uint64_t> free_ranges_;  // offset -> length
    
    std::map<uint64_t, uint64_t>::iterator find_best_fit(uint64_t size);
    std::map<uint64_t, uint64_t>::iterator find_first_fit(uint64_t size);
    std::map<uint64_t, uint64_t>::const_iterator find_first_fit(uint64_t size) const;
};

/**
 * @brief High-performance range-based allocator with intelligent placement strategies
 */
class RangeAllocator : public IAllocator {
public:
    RangeAllocator();
    ~RangeAllocator() override = default;
    
    // IAllocator interface
    Result<AllocationResult> 
    allocate(const AllocationRequest& request,
             const std::unordered_map<MemoryPoolId, MemoryPool>& pools) override;
    
    ErrorCode free(const ObjectKey& object_key) override;
    
    AllocatorStats get_stats(std::optional<StorageClass> storage_class) const override;
    
    size_t get_free_space(StorageClass storage_class) const override;
    
    bool can_allocate(const AllocationRequest& request,
                     const std::unordered_map<MemoryPoolId, MemoryPool>& pools) const override;

private:
    // Pool allocators by pool_id (created on-demand)
    mutable std::shared_mutex pools_mutex_;
    std::unordered_map<MemoryPoolId, std::unique_ptr<PoolAllocator>> pool_allocators_;
    
    // Object key -> allocated ranges for cleanup
    mutable std::shared_mutex allocations_mutex_;
    struct ObjectAllocation {
        std::vector<std::pair<MemoryPoolId, Range>> ranges;
        size_t total_size;
    };
    std::unordered_map<ObjectKey, ObjectAllocation> object_allocations_;
    
    // Strategy implementation
    Result<AllocationResult>
    allocate_with_striping(const AllocationRequest& request,
                          const std::vector<MemoryPoolId>& candidate_pools,
                          const std::unordered_map<MemoryPoolId, MemoryPool>& pools);
    
    Result<AllocationResult>
    allocate_contiguous(const AllocationRequest& request,
                       const std::vector<MemoryPoolId>& candidate_pools);
    
    std::vector<MemoryPoolId> 
    select_candidate_pools(const AllocationRequest& request,
                          const std::unordered_map<MemoryPoolId, MemoryPool>& pools) const;
    
    ErrorCode ensure_pool_allocator(const MemoryPool& pool);
    
    ErrorCode commit_allocation(const ObjectKey& object_key,
                               const std::vector<std::pair<MemoryPoolId, Range>>& ranges);
    
    void rollback_allocation(const std::vector<std::pair<MemoryPoolId, Range>>& ranges);
    
    // Conversion helpers
    CopyPlacement create_copy_placement(uint32_t copy_index,
                                       const std::vector<std::pair<MemoryPoolId, Range>>& ranges,
                                       const std::unordered_map<MemoryPoolId, MemoryPool>& pools) const;
    
    ShardPlacement create_shard_placement(const MemoryPoolId& pool_id,
                                         const Range& range,
                                         const MemoryPool& pool) const;
};

} // namespace blackbird::allocation