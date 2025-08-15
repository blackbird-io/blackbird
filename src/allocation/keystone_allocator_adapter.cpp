#include "blackbird/allocation/keystone_allocator_adapter.h"
#include <glog/logging.h>

namespace blackbird::allocation {

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
    if (data_size == 0) {
        throw std::invalid_argument("Data size must be greater than zero for capacity check");
    }
    
    if (config.replication_factor == 0) {
        throw std::invalid_argument("Replication factor must be greater than zero for capacity check");
    }
    
    if (config.max_workers_per_copy == 0) {
        throw std::invalid_argument("Max workers per copy must be greater than zero for capacity check");
    }
    
    AllocationRequest request{
        .object_key = "capacity_check_dummy",
        .data_size = data_size,
        .replication_factor = config.replication_factor,
        .max_workers_per_copy = config.max_workers_per_copy,
        .preferred_classes = config.preferred_classes,
        .preferred_node = config.preferred_node,
        .enable_locality_awareness = config.enable_soft_pin,
        .enable_striping = (config.max_workers_per_copy > 1),
        .prefer_contiguous = false,  
        .min_shard_size = 4096       
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
        .enable_locality_awareness = config.enable_soft_pin, 
        .enable_striping = (config.max_workers_per_copy > 1),
        .prefer_contiguous = false,  // Could be made configurable in future
        .min_shard_size = 4096       // Could be made configurable in future
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
    shard.worker_id = pool.node_id;
    shard.storage_class = get_pool_storage_class(pool);
    shard.length = length;
    
    auto colon_pos = pool.ucx_endpoint.find(':');
    if (colon_pos == std::string::npos) {
        throw std::invalid_argument("Invalid UCX endpoint format: " + pool.ucx_endpoint + 
                                  " - expected 'host:port' format");
    }
    
    std::string host = pool.ucx_endpoint.substr(0, colon_pos);
    std::string port_str = pool.ucx_endpoint.substr(colon_pos + 1);
    
    if (host.empty() || port_str.empty()) {
        throw std::invalid_argument("Invalid UCX endpoint format: " + pool.ucx_endpoint + 
                                  " - host and port cannot be empty");
    }
    
    int port;
    try {
        port = std::stoi(port_str);
    } catch (const std::exception& e) {
        throw std::invalid_argument("Invalid port in UCX endpoint: " + port_str + 
                                  " - " + e.what());
    }
    
    if (port <= 0 || port > 65535) {
        throw std::invalid_argument("Invalid port number: " + std::to_string(port) + 
                                  " - must be between 1 and 65535");
    }
    
    shard.endpoint.ip = host;
    shard.endpoint.port = port;
    
    // Parse rkey from hex string - fail if invalid
    uint32_t rkey;
    try {
        rkey = static_cast<uint32_t>(std::stoul(pool.ucx_rkey_hex, nullptr, 16));
    } catch (const std::exception& e) {
        throw std::invalid_argument("Invalid UCX rkey_hex format: " + pool.ucx_rkey_hex + 
                                  " - " + e.what());
    }
    
    // Create memory location
    MemoryLocation mem_loc{
        .remote_addr = pool.ucx_remote_addr + offset,
        .rkey = rkey,
        .size = length
    };
    shard.location = mem_loc;
    
    return shard;
}

StorageClass KeystoneAllocatorAdapter::get_pool_storage_class(const MemoryPool& pool) const {
    // This method should not be called until MemoryPool has a storage_class field
    // Failing fast prevents silent bugs from hardcoded assumptions
    throw std::runtime_error("MemoryPool.storage_class field not implemented yet - cannot determine storage class for pool " + pool.id);
}

} // namespace blackbird::allocation