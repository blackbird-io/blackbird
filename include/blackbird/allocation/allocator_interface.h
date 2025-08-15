#pragma once

#include <vector>
#include <unordered_map>
#include <memory>
#include <shared_mutex>

#include "blackbird/common/types.h"

namespace blackbird::allocation {

/**
 * @brief Statistics about allocator state
 */
struct AllocatorStats {
    size_t total_allocated_bytes;
    size_t total_free_bytes;
    size_t total_objects;
    size_t total_shards;
    double fragmentation_ratio;    // 1.0 - (largest_free_block / total_free)
    std::unordered_map<StorageClass, size_t> bytes_per_class;
};

/**
 * @brief Allocation request containing all parameters needed for placement
 */
struct AllocationRequest {
    ObjectKey object_key;
    size_t data_size;
    size_t replication_factor;
    size_t max_workers_per_copy;
    std::vector<StorageClass> preferred_classes;
    std::string preferred_node;
    bool enable_locality_awareness = true;
    
    // Optional allocation hints
    bool enable_striping = true;        // Spread across multiple workers for bandwidth
    bool prefer_contiguous = false;     // Prefer single shard over multiple (lower overhead)
    size_t min_shard_size = 4096;      // Minimum shard size to avoid excessive fragmentation
};

/**
 * @brief Result of a successful allocation
 */
struct AllocationResult {
    std::vector<CopyPlacement> copies;
    size_t total_shards_created;
    size_t pools_used;
    
    // Allocation metadata for monitoring
    struct AllocationStats {
        size_t fragmentation_score;    // 0-100, higher = more fragmented
        bool required_spillover;       // Had to use non-preferred storage classes
        size_t avg_shard_size;
    } stats;
};

/**
 * @brief Interface for memory allocation strategies
 * 
 * Thread-safe interface for allocating object storage across distributed memory pools.
 * Implementations should handle different allocation strategies (range-based, buddy, etc.)
 */
class IAllocator {
public:
    virtual ~IAllocator() = default;
    
    /**
     * @brief Allocate storage for an object across memory pools
     * @param request Allocation parameters
     * @param pools Available memory pools (read-only reference)
     * @return Allocation result or error code
     */
    virtual Result<AllocationResult> 
    allocate(const AllocationRequest& request,
             const std::unordered_map<MemoryPoolId, MemoryPool>& pools) = 0;
    
    /**
     * @brief Free all allocated ranges for an object
     * @param object_key Key of object to free
     * @return Error code (OK on success)
     */
    virtual ErrorCode free(const ObjectKey& object_key) = 0;
    
    /**
     * @brief Get allocation statistics for monitoring
     * @param storage_class Optional filter by storage class
     * @return Statistics about current allocations
     */
    virtual AllocatorStats get_stats(std::optional<StorageClass> storage_class = std::nullopt) const = 0;
    
    /**
     * @brief Validate allocator internal consistency (for debugging)
     * @return True if internal state is consistent
     */
    virtual bool validate_consistency() const = 0;
    
    /**
     * @brief Get free space in bytes for a specific storage class
     * @param storage_class Storage class to query
     * @return Available bytes
     */
    virtual size_t get_free_space(StorageClass storage_class) const = 0;
    
    /**
     * @brief Estimate if an allocation request could succeed
     * @param request Allocation parameters to test
     * @param pools Available memory pools
     * @return True if allocation likely to succeed
     */
    virtual bool can_allocate(const AllocationRequest& request,
                             const std::unordered_map<MemoryPoolId, MemoryPool>& pools) const = 0;
};

/**
 * @brief Factory for creating allocator instances
 */
class AllocatorFactory {
public:
    enum class Strategy {
        RANGE_BASED,        // Range-based allocator with best-fit
        SLAB_ALLOCATOR,     // Fixed-size slab allocator
        HYBRID              // Combines multiple strategies
    };
    
    static std::unique_ptr<IAllocator> create(Strategy strategy);
    static std::unique_ptr<IAllocator> create_range_based();
};

} // namespace blackbird::allocation