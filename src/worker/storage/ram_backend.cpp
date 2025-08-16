#include "blackbird/worker/storage/ram_backend.h"
#include "blackbird/worker/storage/iouring_disk_backend.h"
#include "blackbird/worker/storage/mmap_disk_backend.h"

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
    
    if (size > get_available_capacity()) {
        LOG(WARNING) << "Not enough free space: requested " << size 
                    << ", available " << get_available_capacity();
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    uint64_t offset = find_free_offset(size);
    if (offset == UINT64_MAX) {
        LOG(WARNING) << "Cannot find contiguous space for " << size << " bytes";
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    std::string token_id = generate_token_id();
    auto expires_at = std::chrono::system_clock::now() + std::chrono::minutes(10); // 10 minute expiry
    
    ReservationToken token;
    token.token_id = token_id;
    token.pool_id = ""; 
    token.remote_addr = base_address_ + offset;
    token.rkey = rkey_;
    token.size = size;
    token.expires_at = expires_at;
    
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
        used_capacity_ -= it->second.size;
        num_reservations_--;
        reservations_.erase(it);
        return ErrorCode::OPERATION_TIMEOUT;
    }
    
    it->second.committed = true;
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
    
    used_capacity_ -= it->second.size;
    num_reservations_--;
    reservations_.erase(it);
    
    LOG(INFO) << "Aborted shard: token " << token.token_id;
    
    return ErrorCode::OK;
}

ErrorCode RamBackend::free_shard(uint64_t remote_addr, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string shard_token_id;
    bool found = false;
    for (const auto& [token_id, shard] : reservations_) {
        if (shard.committed && base_address_ + shard.offset == remote_addr && shard.size == size) {
            shard_token_id = token_id;
            found = true;
            break;
        }
    }
    
    if (!found) {
        LOG(WARNING) << "Committed shard not found: " << std::hex << remote_addr;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    used_capacity_ -= size;
    num_committed_shards_--;
    reservations_.erase(shard_token_id);
    
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
    
    base_memory_ = std::malloc(capacity_);
    if (base_memory_ == nullptr) {
        LOG(ERROR) << "Failed to allocate " << capacity_ << " bytes";
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    base_address_ = reinterpret_cast<uintptr_t>(base_memory_);
    rkey_ = static_cast<uint32_t>(base_address_ & 0xFFFFFFFF);
    
    LOG(INFO) << "RamBackend initialized: base_addr=0x" << std::hex << base_address_
              << ", capacity=" << std::dec << capacity_ << " bytes";
    
    return ErrorCode::OK;
}

void RamBackend::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (base_memory_ != nullptr) {
        LOG(INFO) << "Shutting down RamBackend with " << reservations_.size() 
                  << " active reservations";
        
        std::free(base_memory_);
        base_memory_ = nullptr;
        base_address_ = 0;
        
        reservations_.clear();
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
    std::vector<std::pair<uint64_t, uint64_t>> used_ranges;
    
    for (const auto& [token_id, shard] : reservations_) {
        if (shard.committed) {
            used_ranges.push_back({shard.offset, shard.offset + shard.size});
        }
    }
    
    std::sort(used_ranges.begin(), used_ranges.end());
    
    if (used_ranges.empty() || used_ranges[0].first >= size) {
        return 0;
    }
    
    for (size_t i = 0; i < used_ranges.size() - 1; ++i) {
        uint64_t gap_start = used_ranges[i].second;
        uint64_t gap_end = used_ranges[i + 1].first;
        if (gap_end - gap_start >= size) {
            return gap_start;
        }
    }
    
    if (!used_ranges.empty()) {
        uint64_t last_end = used_ranges.back().second;
        if (capacity_ - last_end >= size) {
            return last_end;
        }
    }
    
    return UINT64_MAX; 
}

std::unique_ptr<StorageBackend> create_storage_backend(StorageClass storage_class,
                                                      uint64_t capacity,
                                                      const std::string& config) {
    switch (storage_class) {
        case StorageClass::RAM_CPU:
        case StorageClass::RAM_GPU:
            return std::make_unique<RamBackend>(capacity, storage_class);
        
        default:
            LOG(ERROR) << "RamBackend factory only supports RAM storage classes, got: " << static_cast<uint32_t>(storage_class);
            return nullptr;
    }
}

} // namespace blackbird 