#include "blackbird/worker/storage/ram_backend.h"
#include "blackbird/worker/storage/disk_backend.h"
#include "blackbird/worker/storage/iouring_disk_backend.h"

#include <glog/logging.h>
#include <algorithm>
#include <sstream>
#include <iomanip>

namespace blackbird {

RamBackend::RamBackend(uint64_t capacity, StorageClass storage_class)
    : capacity_(capacity), storage_class_(storage_class), rng_(std::random_device{}()) {
    LOG(INFO) << "Creating RamBackend with capacity " << capacity 
              << " bytes, storage_class " << static_cast<uint32_t>(storage_class);
}

RamBackend::~RamBackend() {
    shutdown();
}

StorageClass RamBackend::get_storage_class() const {
    return storage_class_;
}

uint64_t RamBackend::get_total_capacity() const {
    return capacity_;
}

uint64_t RamBackend::get_used_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_capacity_;
}

uint64_t RamBackend::get_available_capacity() const {
    return get_total_capacity() - get_used_capacity();
}

uintptr_t RamBackend::get_base_address() const {
    return base_address_;
}

uint32_t RamBackend::get_rkey() const {
    return rkey_;
}

Result<ReservationToken> RamBackend::reserve_shard(uint64_t size, const std::string& hint) {
    if (size == 0) {
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if we have enough free space
    if (size > get_available_capacity()) {
        LOG(WARNING) << "Not enough free space: requested " << size 
                    << ", available " << get_available_capacity();
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    // Find a free offset
    uint64_t offset = find_free_offset(size);
    if (offset == UINT64_MAX) {
        LOG(WARNING) << "Cannot find contiguous space for " << size << " bytes";
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    // Generate token
    std::string token_id = generate_token_id();
    auto expires_at = std::chrono::system_clock::now() + std::chrono::minutes(10); // 10 minute expiry
    
    ReservationToken token;
    token.token_id = token_id;
    token.pool_id = ""; // Will be set by caller
    token.remote_addr = base_address_ + offset;
    token.rkey = rkey_;
    token.size = size;
    token.expires_at = expires_at;
    
    // Track the reservation
    Shard shard;
    shard.offset = offset;
    shard.size = size;
    shard.committed = false;
    shard.created_at = std::chrono::system_clock::now();
    
    reservations_[token_id] = shard;
    used_capacity_ += size;
    num_reservations_++;
    
    LOG(INFO) << "Reserved shard: offset " << offset << ", size " << size 
              << ", token " << token_id;
    
    return token;
}

ErrorCode RamBackend::commit_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(WARNING) << "Reservation not found: " << token.token_id;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    if (token.is_expired()) {
        LOG(WARNING) << "Reservation expired: " << token.token_id;
        // Clean up expired reservation
        used_capacity_ -= it->second.size;
        num_reservations_--;
        reservations_.erase(it);
        return ErrorCode::OPERATION_TIMEOUT;
    }
    
    // Mark as committed
    it->second.committed = true;
    committed_shards_[token.remote_addr] = token.size;
    num_committed_shards_++;
    
    LOG(INFO) << "Committed shard: token " << token.token_id 
              << ", addr " << std::hex << token.remote_addr;
    
    return ErrorCode::OK;
}

ErrorCode RamBackend::abort_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(WARNING) << "Reservation not found: " << token.token_id;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    // Free the reservation
    used_capacity_ -= it->second.size;
    num_reservations_--;
    reservations_.erase(it);
    
    LOG(INFO) << "Aborted shard: token " << token.token_id;
    
    return ErrorCode::OK;
}

ErrorCode RamBackend::free_shard(uint64_t remote_addr, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = committed_shards_.find(remote_addr);
    if (it == committed_shards_.end()) {
        LOG(WARNING) << "Committed shard not found: " << std::hex << remote_addr;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    if (it->second != size) {
        LOG(WARNING) << "Size mismatch: expected " << it->second << ", got " << size;
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    // Free the shard
    used_capacity_ -= size;
    num_committed_shards_--;
    committed_shards_.erase(it);
    
    LOG(INFO) << "Freed shard: addr " << std::hex << remote_addr << ", size " << size;
    
    return ErrorCode::OK;
}

StorageStats RamBackend::get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    StorageStats stats;
    stats.total_capacity = capacity_;
    stats.used_capacity = used_capacity_;
    stats.available_capacity = capacity_ - used_capacity_;
    stats.utilization = capacity_ > 0 ? static_cast<double>(used_capacity_) / capacity_ : 0.0;
    stats.num_reservations = num_reservations_;
    stats.num_committed_shards = num_committed_shards_;
    
    return stats;
}

ErrorCode RamBackend::initialize() {
    LOG(INFO) << "Initializing RamBackend...";
    
    if (base_memory_ != nullptr) {
        LOG(WARNING) << "RamBackend already initialized";
        return ErrorCode::OK;
    }
    
    // Allocate memory
    base_memory_ = std::malloc(capacity_);
    if (base_memory_ == nullptr) {
        LOG(ERROR) << "Failed to allocate " << capacity_ << " bytes";
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    base_address_ = reinterpret_cast<uintptr_t>(base_memory_);
    
    // Initialize rkey (placeholder for UCX integration)
    rkey_ = static_cast<uint32_t>(base_address_ & 0xFFFFFFFF);
    
    LOG(INFO) << "RamBackend initialized: base_addr=0x" << std::hex << base_address_
              << ", capacity=" << std::dec << capacity_ << " bytes";
    
    return ErrorCode::OK;
}

void RamBackend::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (base_memory_ != nullptr) {
        LOG(INFO) << "Shutting down RamBackend with " << reservations_.size() 
                  << " active reservations and " << committed_shards_.size() 
                  << " committed shards";
        
        std::free(base_memory_);
        base_memory_ = nullptr;
        base_address_ = 0;
        
        reservations_.clear();
        committed_shards_.clear();
        used_capacity_ = 0;
        num_reservations_ = 0;
        num_committed_shards_ = 0;
    }
}

std::string RamBackend::generate_token_id() {
    // Generate a random hex string
    std::stringstream ss;
    ss << std::hex << rng_() << std::hex << rng_();
    return ss.str();
}

uint64_t RamBackend::find_free_offset(uint64_t size) {
    // Simple first-fit allocation
    // This is not efficient for production but works for testing
    
    if (reservations_.empty() && committed_shards_.empty()) {
        return 0; // First allocation
    }
    
    // Collect all used ranges
    std::vector<std::pair<uint64_t, uint64_t>> used_ranges; // {offset, size}
    
    for (const auto& [token_id, shard] : reservations_) {
        used_ranges.emplace_back(shard.offset, shard.size);
    }
    
    for (const auto& [addr, shard_size] : committed_shards_) {
        uint64_t offset = addr - base_address_;
        used_ranges.emplace_back(offset, shard_size);
    }
    
    // Sort by offset
    std::sort(used_ranges.begin(), used_ranges.end());
    
    // Find first gap that fits
    uint64_t current_offset = 0;
    for (const auto& [offset, shard_size] : used_ranges) {
        if (offset >= current_offset + size) {
            return current_offset; // Found a gap
        }
        current_offset = std::max(current_offset, offset + shard_size);
    }
    
    // Check if we can fit at the end
    if (current_offset + size <= capacity_) {
        return current_offset;
    }
    
    return UINT64_MAX; // No space found
}

// Factory function implementation
std::unique_ptr<StorageBackend> create_storage_backend(StorageClass storage_class,
                                                      uint64_t capacity,
                                                      const std::string& config) {
    switch (storage_class) {
        case StorageClass::RAM_CPU:
        case StorageClass::RAM_GPU:
            return std::make_unique<RamBackend>(capacity, storage_class);
        
        case StorageClass::NVME:
        case StorageClass::SSD:
        case StorageClass::HDD: {
            // Parse mount path from config
            std::string mount_path = config.empty() ? "/tmp" : config;
            try {
                // Use high-performance io_uring backend for disk storage
                return std::make_unique<IoUringDiskBackend>(capacity, storage_class, mount_path);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to create IoUringDiskBackend: " << e.what();
                // Fallback to basic disk backend if io_uring fails
                try {
                    LOG(WARNING) << "Falling back to basic DiskBackend";
                    return std::make_unique<DiskBackend>(capacity, storage_class, mount_path);
                } catch (const std::exception& fallback_e) {
                    LOG(ERROR) << "Failed to create fallback DiskBackend: " << fallback_e.what();
                    return nullptr;
                }
            }
        }
        
        default:
            LOG(ERROR) << "Unknown storage class: " << static_cast<uint32_t>(storage_class);
            return nullptr;
    }
}

} // namespace blackbird 