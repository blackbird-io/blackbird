#include "blackbird/worker/storage/cxl_memory_backend.h"
#include <glog/logging.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

namespace blackbird {

CxlMemoryBackend::CxlMemoryBackend(const CxlDeviceConfig& config, StorageClass storage_class)
    : config_(config), storage_class_(storage_class), rng_(std::random_device{}()) {
    LOG(INFO) << "Creating CXL memory backend for device: " << config_.device_id 
              << " with capacity: " << config_.capacity << " bytes";
}

CxlMemoryBackend::~CxlMemoryBackend() {
    shutdown();
}

StorageClass CxlMemoryBackend::get_storage_class() const {
    return storage_class_;
}

uint64_t CxlMemoryBackend::get_total_capacity() const {
    return config_.capacity;
}

uint64_t CxlMemoryBackend::get_used_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_capacity_;
}

uint64_t CxlMemoryBackend::get_available_capacity() const {
    auto used = get_used_capacity();
    return config_.capacity > used ? config_.capacity - used : 0;
}

uintptr_t CxlMemoryBackend::get_base_address() const {
    return base_address_;
}

uint32_t CxlMemoryBackend::get_rkey() const {
    return rkey_;
}

ErrorCode CxlMemoryBackend::initialize() {
    LOG(INFO) << "Initializing CXL memory backend for device: " << config_.device_id;
    
    // Map CXL device memory
    auto result = map_cxl_device();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Failed to map CXL device: " << config_.device_id;
        return result;
    }
    
    // Configure NUMA binding if enabled
    if (config_.enable_numa_binding && config_.numa_node >= 0) {
        result = configure_numa_binding();
        if (result != ErrorCode::OK) {
            LOG(WARNING) << "Failed to configure NUMA binding for CXL device, continuing anyway";
        }
    }
    
    LOG(INFO) << "CXL memory backend initialized successfully";
    LOG(INFO) << "  Device: " << config_.device_id;
    LOG(INFO) << "  Capacity: " << config_.capacity << " bytes";
    LOG(INFO) << "  Base address: 0x" << std::hex << base_address_ << std::dec;
    LOG(INFO) << "  Persistent mode: " << (config_.enable_persistent_mode ? "enabled" : "disabled");
    
    return ErrorCode::OK;
}

ErrorCode CxlMemoryBackend::map_cxl_device() {
    // In a real implementation, this would use DAX (Direct Access) to map CXL memory
    // For now, we'll use a placeholder implementation
    
    // TODO: Implement actual CXL.mem mapping via /dev/dax or libcxl
    // For demonstration, we allocate regular memory but mark it for CXL usage
    
    if (!config_.dax_device.empty()) {
        // Attempt to open DAX device for direct CXL memory access
        fd_ = open(config_.dax_device.c_str(), O_RDWR);
        if (fd_ < 0) {
            LOG(WARNING) << "Could not open DAX device: " << config_.dax_device 
                        << ", falling back to anonymous mapping";
        } else {
            // Map the CXL memory via DAX device
            void* addr = mmap(nullptr, config_.capacity, 
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_POPULATE,
                            fd_, 0);
            if (addr == MAP_FAILED) {
                LOG(ERROR) << "Failed to mmap CXL DAX device: " << strerror(errno);
                close(fd_);
                fd_ = -1;
                return ErrorCode::INTERNAL_ERROR;
            }
            base_memory_ = addr;
            base_address_ = reinterpret_cast<uintptr_t>(addr);
            LOG(INFO) << "Successfully mapped CXL memory via DAX device";
            return ErrorCode::OK;
        }
    }
    
    // Fallback: anonymous mapping (for testing/development without actual CXL hardware)
    LOG(INFO) << "Using anonymous mapping as CXL memory placeholder";
    void* addr = mmap(nullptr, config_.capacity,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                     -1, 0);
    
    if (addr == MAP_FAILED) {
        LOG(ERROR) << "Failed to allocate CXL memory placeholder: " << strerror(errno);
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    base_memory_ = addr;
    base_address_ = reinterpret_cast<uintptr_t>(addr);
    
    return ErrorCode::OK;
}

ErrorCode CxlMemoryBackend::configure_numa_binding() {
    // TODO: Implement NUMA binding for CXL memory
    // This would use libnuma or similar to bind CXL memory to specific NUMA nodes
    LOG(INFO) << "NUMA binding for CXL memory to node " << config_.numa_node 
              << " (not yet implemented)";
    return ErrorCode::OK;
}

void CxlMemoryBackend::shutdown() {
    LOG(INFO) << "Shutting down CXL memory backend";
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (base_memory_) {
        munmap(base_memory_, config_.capacity);
        base_memory_ = nullptr;
        base_address_ = 0;
    }
    
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    
    reservations_.clear();
    used_capacity_ = 0;
    num_reservations_ = 0;
    num_committed_shards_ = 0;
}

Result<ReservationToken> CxlMemoryBackend::reserve_shard(uint64_t size, const std::string& hint) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Align size to cache line boundary for CXL efficiency
    uint64_t aligned_size = align_size(size);
    
    if (used_capacity_ + aligned_size > config_.capacity) {
        LOG(WARNING) << "CXL memory backend out of capacity";
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    uint64_t offset = find_free_offset(aligned_size);
    if (offset == UINT64_MAX) {
        LOG(ERROR) << "Could not find free offset for CXL shard";
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    std::string token_id = generate_token_id();
    CxlMemoryRegionId region_id = static_cast<CxlMemoryRegionId>(offset / config_.interleave_granularity);
    
    Shard shard{
        .offset = offset,
        .size = aligned_size,
        .committed = false,
        .created_at = std::chrono::system_clock::now(),
        .region_id = region_id
    };
    
    reservations_[token_id] = shard;
    used_capacity_ += aligned_size;
    num_reservations_++;
    
    ReservationToken token{
        .token_id = token_id,
        .pool_id = config_.device_id,
        .remote_addr = base_address_ + offset,
        .rkey = rkey_,
        .size = aligned_size,
        .expires_at = std::chrono::system_clock::now() + std::chrono::seconds(300)
    };
    
    LOG(INFO) << "Reserved CXL shard: size=" << aligned_size 
              << ", offset=" << offset << ", region=" << region_id;
    
    return token;
}

ErrorCode CxlMemoryBackend::commit_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(ERROR) << "Invalid CXL reservation token: " << token.token_id;
        return ErrorCode::INVALID_ARGUMENT;
    }
    
    if (it->second.committed) {
        LOG(WARNING) << "CXL shard already committed: " << token.token_id;
        return ErrorCode::ALREADY_EXISTS;
    }
    
    it->second.committed = true;
    num_committed_shards_++;
    
    LOG(INFO) << "Committed CXL shard: " << token.token_id;
    return ErrorCode::OK;
}

ErrorCode CxlMemoryBackend::abort_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(ERROR) << "Invalid CXL reservation token: " << token.token_id;
        return ErrorCode::INVALID_ARGUMENT;
    }
    
    used_capacity_ -= it->second.size;
    if (!it->second.committed) {
        num_reservations_--;
    } else {
        num_committed_shards_--;
    }
    
    reservations_.erase(it);
    
    LOG(INFO) << "Aborted CXL shard: " << token.token_id;
    return ErrorCode::OK;
}

ErrorCode CxlMemoryBackend::free_shard(uint64_t remote_addr, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Find shard by remote address
    for (auto it = reservations_.begin(); it != reservations_.end(); ++it) {
        if (base_address_ + it->second.offset == remote_addr && it->second.size == size) {
            used_capacity_ -= it->second.size;
            if (it->second.committed) {
                num_committed_shards_--;
            } else {
                num_reservations_--;
            }
            reservations_.erase(it);
            
            LOG(INFO) << "Freed CXL shard at addr: 0x" << std::hex << remote_addr << std::dec;
            return ErrorCode::OK;
        }
    }
    
    LOG(WARNING) << "CXL shard not found for free: addr=0x" << std::hex << remote_addr << std::dec;
    return ErrorCode::NOT_FOUND;
}

StorageStats CxlMemoryBackend::get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    StorageStats stats;
    stats.total_capacity = config_.capacity;
    stats.used_capacity = used_capacity_;
    stats.available_capacity = get_available_capacity();
    stats.utilization = config_.capacity > 0 
        ? static_cast<double>(used_capacity_) / static_cast<double>(config_.capacity)
        : 0.0;
    stats.num_reservations = num_reservations_;
    stats.num_committed_shards = num_committed_shards_;
    
    return stats;
}

std::string CxlMemoryBackend::generate_token_id() {
    std::uniform_int_distribution<uint64_t> dist;
    uint64_t id = dist(rng_);
    return "cxl_token_" + std::to_string(id);
}

uint64_t CxlMemoryBackend::find_free_offset(uint64_t size) {
    // Simple first-fit allocation
    // TODO: Implement more sophisticated allocation considering CXL interleaving
    
    std::vector<std::pair<uint64_t, uint64_t>> used_ranges;
    for (const auto& [token_id, shard] : reservations_) {
        used_ranges.push_back({shard.offset, shard.offset + shard.size});
    }
    
    std::sort(used_ranges.begin(), used_ranges.end());
    
    uint64_t current_offset = 0;
    for (const auto& [start, end] : used_ranges) {
        if (start >= current_offset + size) {
            return current_offset;
        }
        current_offset = std::max(current_offset, end);
    }
    
    if (current_offset + size <= config_.capacity) {
        return current_offset;
    }
    
    return UINT64_MAX;
}

} // namespace blackbird

