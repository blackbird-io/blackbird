#include "blackbird/allocation/keystone_allocator_adapter.h"
#include <glog/logging.h>

namespace blackbird::allocation {

// Todo: arnavb too much hardcoding, defaults should not be assumed, too many assumptions, need to revisit in future
KeystoneAllocatorAdapter::KeystoneAllocatorAdapter(std::unique_ptr<IAllocator> allocator)
    : allocator_(std::move(allocator)) {
    
    if (!allocator_) {
        throw std::invalid_argument("Allocator cannot be null");
    }
    
    LOG(INFO) << "Created KeystoneAllocatorAdapter";
}

tl::expected<std::vector<CopyPlacement>, ErrorCode>
KeystoneAllocatorAdapter::allocate_data_copies(const ObjectKey& key,
                                              size_t data_size,
                                              const WorkerConfig& config,
                                              const std::unordered_map<MemoryPoolId, MemoryPool>& memory_pools) {
    
    // Convert to allocator request
    auto request = to_allocation_request(key, data_size, config);
    
    // Perform allocation
    auto result = allocator_->allocate(request, memory_pools);
    if (!result) {
        LOG(WARNING) << "Allocation failed for object " << key 
                     << " size=" << data_size << " error=" << static_cast<int>(result.error());
        return tl::unexpected(result.error());
    }
    
    // Convert result back to Keystone format
    auto copy_placements = to_copy_placements(*result, memory_pools);
    
    LOG(INFO) << "Successfully allocated " << copy_placements.size() 
              << " copies for object " << key << " size=" << data_size;
    
    return copy_placements;
}

ErrorCode KeystoneAllocatorAdapter::free_object(const ObjectKey& key) {
    auto result = allocator_->free(key);
    
    if (result == ErrorCode::OK) {
        LOG(INFO) << "Successfully freed object " << key;
    } else {
        LOG(WARNING) << "Failed to free object " << key 
                     << " error=" << static_cast<int>(result);
    }
    
    return result;
}

IAllocator::AllocatorStats 
KeystoneAllocatorAdapter::get_allocator_stats(std::optional<StorageClass> storage_class) const {
    return allocator_->get_stats(storage_class);
}

bool KeystoneAllocatorAdapter::can_allocate_object(size_t data_size,
                                                  const WorkerConfig& config,
                                                  const std::unordered_map<MemoryPoolId, MemoryPool>& memory_pools) const {
    // Create dummy request for capacity check
    AllocationRequest request{
        .object_key = "capacity_check",
        .data_size = data_size,
        .replication_factor = config.replication_factor,
        .max_workers_per_copy = config.max_workers_per_copy,
        .preferred_classes = config.preferred_classes,
        .preferred_node = config.preferred_node
    };
    
    return allocator_->can_allocate(request, memory_pools);
}

// Private Helper Methods
AllocationRequest KeystoneAllocatorAdapter::to_allocation_request(const ObjectKey& key,
                                                                 size_t data_size,
                                                                 const WorkerConfig& config) const {
    AllocationRequest request{
        .object_key = key,
        .data_size = data_size,
        .replication_factor = config.replication_factor,
        .max_workers_per_copy = config.max_workers_per_copy,
        .preferred_classes = config.preferred_classes,
        .preferred_node = config.preferred_node,
        .enable_locality_awareness = true,
        .enable_striping = (config.max_workers_per_copy > 1),
        .prefer_contiguous = false,  // Default to striping for performance
        .min_shard_size = 4096       // 4KB minimum shard size
    };
    
    return request;
}

std::vector<CopyPlacement> 
KeystoneAllocatorAdapter::to_copy_placements(const AllocationResult& result,
                                            const std::unordered_map<MemoryPoolId, MemoryPool>& memory_pools) const {
    return result.copies;
}

ShardPlacement KeystoneAllocatorAdapter::create_shard_placement(const MemoryPoolId& pool_id,
                                                               uint64_t offset,
                                                               uint64_t length,
                                                               const MemoryPool& pool) const {
    ShardPlacement shard;
    shard.pool_id = pool_id;
    shard.worker_id = pool.node_id;  // Use node_id as worker_id for now
    shard.storage_class = get_pool_storage_class(pool);
    shard.length = length;
    
    // Create UCX endpoint
    // Parse pool.ucx_endpoint (format: "host:port")
    auto colon_pos = pool.ucx_endpoint.find(':');
    if (colon_pos != std::string::npos) {
        shard.endpoint.ip = pool.ucx_endpoint.substr(0, colon_pos);
        shard.endpoint.port = std::stoi(pool.ucx_endpoint.substr(colon_pos + 1));
    } else {
        LOG(WARNING) << "Invalid UCX endpoint format: " << pool.ucx_endpoint;
        shard.endpoint.ip = "127.0.0.1";
        shard.endpoint.port = 50000;
    }
    
    // Create memory location
    MemoryLocation mem_loc{
        .remote_addr = pool.ucx_remote_addr + offset,
        .rkey = 0,  // TODO: Parse pool.ucx_rkey_hex
        .size = length
    };
    shard.location = mem_loc;
    
    return shard;
}

StorageClass KeystoneAllocatorAdapter::get_pool_storage_class(const MemoryPool& pool) const {
    // TODO: Add storage_class field to MemoryPool struct
    // For now, assume all pools are RAM_CPU
    return StorageClass::RAM_CPU;
}

} // namespace blackbird::allocation