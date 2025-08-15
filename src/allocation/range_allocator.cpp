#include "blackbird/allocation/range_allocator.h"
#include <glog/logging.h>
#include <algorithm>
#include <numeric>
#include <stdexcept>

namespace blackbird::allocation {

// PoolAllocator Implementation

// Todo: arnavb fix the parsing, more strict error handling/crashes, seems overkill a bit, revisit logic in future
PoolAllocator::PoolAllocator(const MemoryPool& pool) 
    : pool_id_(pool.id)
    , storage_class_(pool.storage_class)
    , node_id_(pool.node_id)
    , base_addr_(pool.ucx_remote_addr)
    , ucx_rkey_(0)
    , pool_size_(pool.size) {
    if (pool.ucx_rkey_hex.empty()) {
        throw std::invalid_argument("UCX rkey_hex is empty for pool: " + pool.id);
    }
    try {
        ucx_rkey_ = static_cast<uint32_t>(std::stoul(pool.ucx_rkey_hex, nullptr, 16));
    } catch (const std::exception& e) {
        throw std::invalid_argument(std::string("Invalid UCX rkey_hex for pool ") + pool.id + 
                                    ": " + pool.ucx_rkey_hex + " - " + e.what());
    }
    
    // Initialize with single free range covering entire pool
    free_ranges_[0] = pool_size_;
    
    VLOG(1) << "Created PoolAllocator for " << pool_id_ 
            << " size=" << pool_size_ << " bytes"
            << ", storage_class=" << static_cast<int>(storage_class_);
}

std::optional<Range> PoolAllocator::allocate(uint64_t size, bool prefer_best_fit) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (size == 0 || size > pool_size_) {
        return std::nullopt;
    }
    
    auto it = prefer_best_fit ? find_best_fit(size) : find_first_fit(size);
    if (it == free_ranges_.end()) {
        return std::nullopt;
    }
    
    uint64_t offset = it->first;
    uint64_t available = it->second;
    
    // Remove the range we're carving from
    free_ranges_.erase(it);
    
    // If there's leftover space, add it back as a new free range
    if (available > size) {
        free_ranges_[offset + size] = available - size;
    }
    
    VLOG(2) << "Allocated range [" << offset << ", " << offset + size 
            << ") from pool " << pool_id_;
    
    return Range(offset, size);
}

void PoolAllocator::free(const Range& range) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Add the range back to free list
    free_ranges_[range.offset] = range.length;
    
    // Merge with adjacent ranges
    merge_adjacent_ranges();
    
    VLOG(2) << "Freed range [" << range.offset << ", " << range.end() 
            << ") in pool " << pool_id_;
}

size_t PoolAllocator::total_free() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::accumulate(free_ranges_.begin(), free_ranges_.end(), 0UL,
                          [](size_t sum, const auto& pair) {
                              return sum + pair.second;
                          });
}

size_t PoolAllocator::largest_free_block() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (free_ranges_.empty()) return 0;
    
    auto max_it = std::max_element(free_ranges_.begin(), free_ranges_.end(),
                                  [](const auto& a, const auto& b) {
                                      return a.second < b.second;
                                  });
    return max_it->second;
}

double PoolAllocator::fragmentation_ratio() const {
    size_t total = total_free();
    size_t largest = largest_free_block();
    
    return total > 0 ? 1.0 - (static_cast<double>(largest) / total) : 0.0;
}

bool PoolAllocator::can_allocate(uint64_t size) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return find_first_fit(size) != free_ranges_.end();
}

MemoryLocation PoolAllocator::to_memory_location(const Range& range) const {
    return MemoryLocation{
        .remote_addr = base_addr_ + range.offset,
        .rkey = ucx_rkey_,
        .size = range.length
    };
}

void PoolAllocator::merge_adjacent_ranges() {
    if (free_ranges_.size() <= 1) return;
    
    auto it = free_ranges_.begin();
    while (it != free_ranges_.end()) {
        auto next_it = std::next(it);
        if (next_it != free_ranges_.end()) {
            // Check if current range is adjacent to next range
            if (it->first + it->second == next_it->first) {
                // Merge: extend current range and remove next
                uint64_t merged_length = it->second + next_it->second;
                uint64_t offset = it->first;
                
                free_ranges_.erase(next_it);
                it->second = merged_length;
                
                VLOG(3) << "Merged adjacent ranges at offset " << offset 
                        << " new length=" << merged_length;
                continue;  // Don't increment it, check for more merges
            }
        }
        ++it;
    }
}

std::map<uint64_t, uint64_t>::iterator PoolAllocator::find_best_fit(uint64_t size) {
    // Find smallest range that can fit the request
    auto best_it = free_ranges_.end();
    uint64_t best_size = UINT64_MAX;
    
    for (auto it = free_ranges_.begin(); it != free_ranges_.end(); ++it) {
        if (it->second >= size && it->second < best_size) {
            best_it = it;
            best_size = it->second;
        }
    }
    
    return best_it;
}

std::map<uint64_t, uint64_t>::iterator PoolAllocator::find_first_fit(uint64_t size) {
    return std::find_if(free_ranges_.begin(), free_ranges_.end(),
                       [size](const auto& pair) { return pair.second >= size; });
}

std::map<uint64_t, uint64_t>::const_iterator PoolAllocator::find_first_fit(uint64_t size) const {
    return std::find_if(free_ranges_.begin(), free_ranges_.end(),
                       [size](const auto& pair) { return pair.second >= size; });
}

// RangeAllocator Implementation  
RangeAllocator::RangeAllocator() {
    LOG(INFO) << "Created RangeAllocator";
}

Result<AllocationResult> 
RangeAllocator::allocate(const AllocationRequest& request,
                        const std::unordered_map<MemoryPoolId, MemoryPool>& pools) {
    
    // Ensure all pools have allocators
    for (const auto& [pool_id, pool] : pools) {
        ensure_pool_allocator(pool);
    }
    
    // Select candidate pools based on request preferences
    auto candidate_pools = select_candidate_pools(request, pools);
    if (candidate_pools.empty()) {
        LOG(WARNING) << "No suitable pools found for allocation request " 
                     << request.object_key;
        return ErrorCode::MEMORY_POOL_NOT_FOUND;
    }
    
    // Choose allocation strategy
    if (request.enable_striping && candidate_pools.size() > 1) {
        return allocate_with_striping(request, candidate_pools);
    } else {
        return allocate_contiguous(request, candidate_pools);
    }
}

ErrorCode RangeAllocator::free(const ObjectKey& object_key) {
    std::unique_lock<std::shared_mutex> lock(allocations_mutex_);
    
    auto it = object_allocations_.find(object_key);
    if (it == object_allocations_.end()) {
        LOG(WARNING) << "Attempted to free unknown object: " << object_key;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    // Free all ranges for this object
    auto& allocation = it->second;
    for (const auto& [pool_id, range] : allocation.ranges) {
        std::shared_lock<std::shared_mutex> pools_lock(pools_mutex_);
        auto pool_it = pool_allocators_.find(pool_id);
        if (pool_it != pool_allocators_.end()) {
            pool_it->second->free(range);
        }
    }
    
    LOG(INFO) << "Freed object " << object_key << " (" << allocation.total_size 
              << " bytes across " << allocation.ranges.size() << " ranges)";
    
    object_allocations_.erase(it);
    return ErrorCode::OK;
}

AllocatorStats RangeAllocator::get_stats(std::optional<StorageClass> storage_class) const {
    std::shared_lock<std::shared_mutex> pools_lock(pools_mutex_);
    std::shared_lock<std::shared_mutex> alloc_lock(allocations_mutex_);
    
    AllocatorStats stats{};
    
    for (const auto& [pool_id, pool_alloc] : pool_allocators_) {
        if (storage_class && pool_alloc->storage_class() != *storage_class) {
            continue;
        }
        
        size_t free_bytes = pool_alloc->total_free();
        stats.total_free_bytes += free_bytes;
        stats.bytes_per_class[pool_alloc->storage_class()] += free_bytes;
    }
    
    // Calculate allocated bytes and object count
    for (const auto& [key, allocation] : object_allocations_) {
        stats.total_allocated_bytes += allocation.total_size;
        stats.total_shards += allocation.ranges.size();
        ++stats.total_objects;
    }
    
    // Calculate overall fragmentation (weighted average)
    double total_free = stats.total_free_bytes;
    double weighted_frag = 0.0;
    
    for (const auto& [pool_id, pool_alloc] : pool_allocators_) {
        if (storage_class && pool_alloc->storage_class() != *storage_class) {
            continue;
        }
        
        size_t pool_free = pool_alloc->total_free();
        if (pool_free > 0 && total_free > 0) {
            double weight = pool_free / total_free;
            weighted_frag += weight * pool_alloc->fragmentation_ratio();
        }
    }
    
    stats.fragmentation_ratio = weighted_frag;
    return stats;
}

bool RangeAllocator::validate_consistency() const {
    // TODO: Implement consistency checks
    // - Verify no overlapping allocations
    // - Check that all allocated ranges are tracked in object_allocations_
    // - Validate free lists are properly merged
    return true;
}

size_t RangeAllocator::get_free_space(StorageClass storage_class) const {
    std::shared_lock<std::shared_mutex> lock(pools_mutex_);
    
    size_t total_free = 0;
    for (const auto& [pool_id, pool_alloc] : pool_allocators_) {
        if (pool_alloc->storage_class() == storage_class) {
            total_free += pool_alloc->total_free();
        }
    }
    
    return total_free;
}

bool RangeAllocator::can_allocate(const AllocationRequest& request,
                                 const std::unordered_map<MemoryPoolId, MemoryPool>& pools) const {
    // Quick capacity check
    size_t total_needed = request.data_size * request.replication_factor;
    size_t total_available = 0;
    
    for (const auto& [pool_id, pool] : pools) {
        if (request.preferred_classes.empty() || 
            std::find(request.preferred_classes.begin(), request.preferred_classes.end(), 
                     StorageClass::RAM_CPU) != request.preferred_classes.end()) {
            total_available += pool.available();
        }
    }
    
    return total_available >= total_needed;
}

// ============================================================================
// Private Implementation Methods
// ============================================================================

Result<AllocationResult>
RangeAllocator::allocate_with_striping(const AllocationRequest& request,
                                      const std::vector<MemoryPoolId>& candidate_pools) {
    size_t per_copy_size = request.data_size;
    size_t workers_per_copy = std::min(request.max_workers_per_copy, candidate_pools.size());
    
    AllocationResult result{};
    result.copies.reserve(request.replication_factor);
    
    // Allocate each copy
    for (size_t copy_idx = 0; copy_idx < request.replication_factor; ++copy_idx) {
        std::vector<std::pair<MemoryPoolId, Range>> copy_ranges;
        
        size_t shard_size = per_copy_size / workers_per_copy;
        size_t remainder = per_copy_size % workers_per_copy;
        
        // Allocate shards across workers
        for (size_t worker_idx = 0; worker_idx < workers_per_copy; ++worker_idx) {
            MemoryPoolId pool_id = candidate_pools[worker_idx % candidate_pools.size()];
            size_t current_shard_size = shard_size + (worker_idx < remainder ? 1 : 0);
            
            if (current_shard_size < request.min_shard_size && workers_per_copy > 1) {
                // Shard too small, try to use fewer workers
                workers_per_copy = std::max(1UL, per_copy_size / request.min_shard_size);
                break; // Restart allocation with fewer workers
            }
            
            std::shared_lock<std::shared_mutex> lock(pools_mutex_);
            auto pool_it = pool_allocators_.find(pool_id);
            if (pool_it == pool_allocators_.end()) {
                rollback_allocation(copy_ranges);
                return ErrorCode::MEMORY_POOL_NOT_FOUND;
            }
            
            auto range = pool_it->second->allocate(current_shard_size);
            if (!range) {
                rollback_allocation(copy_ranges);
                return ErrorCode::INSUFFICIENT_SPACE;
            }
            
            copy_ranges.emplace_back(pool_id, *range);
        }
        
        // TODO: Create CopyPlacement from ranges
        // This requires access to pools map for endpoint information
        // For now, store the ranges and let caller convert
        
        result.total_shards_created += copy_ranges.size();
    }
    
    // Commit allocation
    auto commit_result = commit_allocation(request.object_key, {});  // TODO: Aggregate all ranges
    if (commit_result != ErrorCode::OK) {
        return commit_result;
    }
    
    result.pools_used = candidate_pools.size();
    return result;
}

Result<AllocationResult>
RangeAllocator::allocate_contiguous(const AllocationRequest& request,
                                   const std::vector<MemoryPoolId>& candidate_pools) {
    // Try to allocate each copy as a single contiguous range
    // TODO: Implement contiguous allocation strategy
    return ErrorCode::NOT_IMPLEMENTED;
}

std::vector<MemoryPoolId> 
RangeAllocator::select_candidate_pools(const AllocationRequest& request,
                                      const std::unordered_map<MemoryPoolId, MemoryPool>& pools) const {
    std::vector<MemoryPoolId> candidates;
    
    // Filter by storage class preferences
    for (const auto& [pool_id, pool] : pools) {
        // Check if pool has enough space
        if (pool.available() < request.data_size / request.max_workers_per_copy) {
            continue;
        }
        
        // Check storage class preferences
        if (!request.preferred_classes.empty()) {
            // TODO: Get storage class from pool
            // For now, assume all pools are RAM_CPU
            bool matches_preference = std::find(request.preferred_classes.begin(),
                                               request.preferred_classes.end(),
                                               StorageClass::RAM_CPU) != request.preferred_classes.end();
            if (!matches_preference) {
                continue;
            }
        }
        
        // Check node preference
        if (!request.preferred_node.empty() && pool.node_id != request.preferred_node) {
            continue;
        }
        
        candidates.push_back(pool_id);
    }
    
    // Sort by available space (largest first) for better allocation success
    std::sort(candidates.begin(), candidates.end(), 
              [&pools](const MemoryPoolId& a, const MemoryPoolId& b) {
                  return pools.at(a).available() > pools.at(b).available();
              });
    
    return candidates;
}

void RangeAllocator::ensure_pool_allocator(const MemoryPool& pool) {
    std::unique_lock<std::shared_mutex> lock(pools_mutex_);
    
    if (pool_allocators_.find(pool.id) == pool_allocators_.end()) {
        pool_allocators_[pool.id] = std::make_unique<PoolAllocator>(pool);
        VLOG(1) << "Created allocator for pool " << pool.id;
    }
}

ErrorCode RangeAllocator::commit_allocation(const ObjectKey& object_key,
                                           const std::vector<std::pair<MemoryPoolId, Range>>& ranges) {
    std::unique_lock<std::shared_mutex> lock(allocations_mutex_);
    
    if (object_allocations_.find(object_key) != object_allocations_.end()) {
        LOG(WARNING) << "Object " << object_key << " already has allocation";
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }
    
    ObjectAllocation allocation;
    allocation.ranges = ranges;
    allocation.total_size = std::accumulate(ranges.begin(), ranges.end(), 0UL,
                                           [](size_t sum, const auto& pair) {
                                               return sum + pair.second.length;
                                           });
    
    object_allocations_[object_key] = std::move(allocation);
    return ErrorCode::OK;
}

void RangeAllocator::rollback_allocation(const std::vector<std::pair<MemoryPoolId, Range>>& ranges) {
    std::shared_lock<std::shared_mutex> lock(pools_mutex_);
    
    for (const auto& [pool_id, range] : ranges) {
        auto pool_it = pool_allocators_.find(pool_id);
        if (pool_it != pool_allocators_.end()) {
            pool_it->second->free(range);
        }
    }
    
    LOG(WARNING) << "Rolled back allocation of " << ranges.size() << " ranges";
}

// Factory Implementation

std::unique_ptr<IAllocator> AllocatorFactory::create(Strategy strategy) {
    switch (strategy) {
        case Strategy::RANGE_BASED:
            return create_range_based();
        default:
            LOG(ERROR) << "Unsupported allocator strategy: " << static_cast<int>(strategy);
            return nullptr;
    }
}

std::unique_ptr<IAllocator> AllocatorFactory::create_range_based() {
    return std::make_unique<RangeAllocator>();
}

} // namespace blackbird::allocation