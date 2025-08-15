#pragma once

#include "blackbird/allocation/allocator_interface.h"
#include "blackbird/keystone/keystone_service.h"
#include <memory>

namespace blackbird::allocation {

/**
 * @brief Adapter to integrate allocator with KeystoneService
 * 
 * This class bridges the gap between the generic allocator interface
 * and Keystone's specific memory pool management and RPC types.
 */
class KeystoneAllocatorAdapter {
public:
    explicit KeystoneAllocatorAdapter(std::unique_ptr<IAllocator> allocator);
    ~KeystoneAllocatorAdapter() = default;
    
    /**
     * @brief Allocate storage for an object (Keystone integration point)
     * @param key Object key
     * @param data_size Size in bytes
     * @param config Worker configuration with preferences
     * @param memory_pools Available memory pools from Keystone
     * @return Copy placements or error
     */
    Result<std::vector<CopyPlacement>>
    allocate_data_copies(const ObjectKey& key,
                        size_t data_size,
                        const WorkerConfig& config,
                        const std::unordered_map<MemoryPoolId, MemoryPool>& memory_pools);
    
    /**
     * @brief Free allocated storage for an object
     * @param key Object key to free
     * @return Error code
     */
    ErrorCode free_object(const ObjectKey& key);
    
    /**
     * @brief Get allocator statistics for monitoring
     * @param storage_class Optional filter by storage class
     * @return Current allocation statistics
     */
    IAllocator::AllocatorStats get_allocator_stats(std::optional<StorageClass> storage_class = std::nullopt) const;
    
    /**
     * @brief Check if allocation request can be satisfied
     * @param data_size Size in bytes
     * @param config Worker configuration
     * @param memory_pools Available memory pools
     * @return True if allocation likely to succeed
     */
    bool can_allocate_object(size_t data_size,
                            const WorkerConfig& config,
                            const std::unordered_map<MemoryPoolId, MemoryPool>& memory_pools) const;

private:
    std::unique_ptr<IAllocator> allocator_;
    
    // Conversion helpers
    AllocationRequest to_allocation_request(const ObjectKey& key,
                                           size_t data_size,
                                           const WorkerConfig& config) const;
    
    std::vector<CopyPlacement> to_copy_placements(const AllocationResult& result,
                                                 const std::unordered_map<MemoryPoolId, MemoryPool>& memory_pools) const;
    
    ShardPlacement create_shard_placement(const MemoryPoolId& pool_id,
                                         uint64_t offset,
                                         uint64_t length,
                                         const MemoryPool& pool) const;
    
    StorageClass get_pool_storage_class(const MemoryPool& pool) const;
};

} // namespace blackbird::allocation