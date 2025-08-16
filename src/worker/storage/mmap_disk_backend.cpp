#include "blackbird/worker/storage/mmap_disk_backend.h"

#include <glog/logging.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iomanip>
#include <sstream>
#include <random>
#include <cstring>

extern "C" {
#include <ucp/api/ucp.h>
}

namespace blackbird {

MmapDiskBackend::MmapDiskBackend(uint64_t capacity, StorageClass storage_class,
                               const std::string& mount_path)
    : capacity_(capacity), storage_class_(storage_class), mount_path_(mount_path) {
    
    if (storage_class != StorageClass::NVME && storage_class != StorageClass::SSD && 
        storage_class != StorageClass::HDD) {
        throw std::invalid_argument("MmapDiskBackend only supports NVME, SSD, or HDD storage classes");
    }
    
    storage_dir_ = mount_path_ / "blackbird_mmap_storage";
    file_path_ = (storage_dir_ / "mmap_storage.dat").string();
    
    LOG(INFO) << "Creating MmapDiskBackend with capacity " << capacity 
              << " bytes, storage_class " << static_cast<uint32_t>(storage_class)
              << ", mount_path " << mount_path;
}

MmapDiskBackend::~MmapDiskBackend() {
    shutdown();
}

StorageClass MmapDiskBackend::get_storage_class() const {
    return storage_class_;
}

uint64_t MmapDiskBackend::get_total_capacity() const {
    return capacity_;
}

uint64_t MmapDiskBackend::get_used_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_capacity_;
}

uint64_t MmapDiskBackend::get_available_capacity() const {
    return get_total_capacity() - get_used_capacity();
}

uintptr_t MmapDiskBackend::get_base_address() const {
    return reinterpret_cast<uintptr_t>(mapped_addr_);
}

uint32_t MmapDiskBackend::get_rkey() const {
    // UCX registration is now handled by WorkerService
    // This method returns 0 as rkey is managed centrally
    return 0;
}

Result<ReservationToken> MmapDiskBackend::reserve_shard(uint64_t size, const std::string& hint) {
    if (size == 0) {
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (size > get_available_capacity()) {
        LOG(WARNING) << "Not enough free space: requested " << size 
                    << ", available " << get_available_capacity();
        return ErrorCode::INSUFFICIENT_SPACE;
    }
    
    auto range_opt = pool_allocator_->allocate(size);
    if (!range_opt.has_value()) {
        LOG(WARNING) << "Cannot find contiguous space for " << size << " bytes";
        return ErrorCode::INSUFFICIENT_SPACE;
    }
    
    auto range = range_opt.value();
    
    // Generate token
    std::string token_id = generate_token_id();
    auto expires_at = std::chrono::system_clock::now() + std::chrono::minutes(10); // 10 minute expiry
    
    ReservationToken token;
    token.token_id = token_id;
    token.pool_id = ""; 
    token.remote_addr = reinterpret_cast<uintptr_t>(mapped_addr_) + range.offset;
    token.rkey = 0;  // UCX rkey managed by WorkerService now
    token.size = size;
    token.expires_at = expires_at;
    
    // Track the reservation
    Shard shard(range.offset, range.length);
    shard.committed = false;
    shard.created_at = std::chrono::system_clock::now();
    
    reservations_[token_id] = shard;
    used_capacity_ += size;
    num_reservations_++;
    
    LOG(INFO) << "Reserved mmap shard: offset " << range.offset << ", size " << range.length 
              << ", token " << token_id;
    
    return token;
}

ErrorCode MmapDiskBackend::commit_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(ERROR) << "Commit failed: unknown token " << token.token_id;
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    Shard& shard = it->second;
    if (shard.committed) {
        LOG(WARNING) << "Shard already committed: " << token.token_id;
        return ErrorCode::OK;
    }
    
    // Mark as committed
    shard.committed = true;
    num_committed_shards_++;
    
    LOG(INFO) << "Committed mmap shard: token " << token.token_id
              << ", offset " << shard.range.offset << ", size " << shard.range.length;
    
    return ErrorCode::OK;
}

ErrorCode MmapDiskBackend::abort_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    Shard& shard = it->second;
    
    // Free the space back to allocator
    pool_allocator_->free(shard.range);
    used_capacity_ -= shard.range.length;
    reservations_.erase(it);
    
    LOG(INFO) << "Aborted mmap shard: token " << token.token_id
              << ", offset " << shard.range.offset << ", size " << shard.range.length;
    
    return ErrorCode::OK;
}

ErrorCode MmapDiskBackend::free_shard(uint64_t remote_addr, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Find the shard by searching through reservations
    for (auto it = reservations_.begin(); it != reservations_.end(); ++it) {
        const auto& shard = it->second;
        if (shard.committed && (reinterpret_cast<uintptr_t>(mapped_addr_) + shard.range.offset) == remote_addr && shard.range.length == size) {
            // Free the space back to allocator
            pool_allocator_->free(shard.range);
            used_capacity_ -= size;
            num_committed_shards_--;
            reservations_.erase(it);
            
            LOG(INFO) << "Freed mmap shard: addr " << std::hex << remote_addr << std::dec 
                      << ", size " << size;
            return ErrorCode::OK;
        }
    }
    
    LOG(ERROR) << "Could not find committed shard with addr " << std::hex << remote_addr 
               << std::dec << " and size " << size;
    return ErrorCode::INVALID_ADDRESS;
}

StorageStats MmapDiskBackend::get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    StorageStats stats;
    stats.total_capacity = capacity_;
    stats.used_capacity = used_capacity_;
    stats.available_capacity = capacity_ - used_capacity_;
    stats.utilization = static_cast<double>(used_capacity_) / capacity_;
    stats.num_reservations = num_reservations_;
    stats.num_committed_shards = num_committed_shards_;
    return stats;
}

ErrorCode MmapDiskBackend::initialize() {
    LOG(INFO) << "Initializing MmapDiskBackend";
    
    std::error_code ec;
    if (!std::filesystem::exists(storage_dir_, ec)) {
        if (!std::filesystem::create_directories(storage_dir_, ec)) {
            LOG(ERROR) << "Failed to create storage directory: " << storage_dir_ << " - " << ec.message();
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    
    // Create backing file
    ErrorCode result = create_backing_file(file_path_, capacity_);
    if (result != ErrorCode::OK) {
        return result;
    }
    
    // Setup memory mapping
    result = setup_mmap(file_path_, capacity_);
    if (result != ErrorCode::OK) {
        return result;
    }
    
    // Create MemoryPool object for the allocator
    // Initialize pool allocator now that mmap is set up
    MemoryPool pool;
    pool.id = "mmap_pool_" + std::to_string(reinterpret_cast<uintptr_t>(this));
    pool.storage_class = storage_class_;
    pool.node_id = "local"; // TODO: Get actual node ID  
    pool.base_addr = reinterpret_cast<uintptr_t>(mapped_addr_);
    pool.size = capacity_;
    pool.ucx_rkey_hex = "0";  // UCX handled by WorkerService now
    
    pool_allocator_ = std::make_unique<allocation::PoolAllocator>(pool);
    
    LOG(INFO) << "MmapDiskBackend initialized: storage_dir=" << storage_dir_
              << ", capacity=" << capacity_ << " bytes, file_path=" << file_path_;
    
    return ErrorCode::OK;
}

void MmapDiskBackend::shutdown() {
    LOG(INFO) << "Shutting down MmapDiskBackend with " << reservations_.size()
              << " active reservations";
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    cleanup_mmap();
    
    reservations_.clear();
    pool_allocator_.reset();
    
    LOG(INFO) << "MmapDiskBackend shutdown complete";
}



// Private methods

uint64_t MmapDiskBackend::find_free_offset(uint64_t size) {
    // Check if there's enough space first
    if (size > get_available_capacity()) {
        return UINT64_MAX;
    }
    
    // Simple delegated allocation using PoolAllocator
    auto range_opt = pool_allocator_->allocate(size);
    if (!range_opt.has_value()) {
        return UINT64_MAX;
    }
    return range_opt.value().offset;
}

std::string MmapDiskBackend::generate_token_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;
    
    std::stringstream ss;
    ss << std::hex << dis(gen);
    return ss.str();
}

ErrorCode MmapDiskBackend::create_backing_file(const std::string& file_path, uint64_t size) {
    int fd = open(file_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd == -1) {
        LOG(ERROR) << "Failed to create backing file: " << file_path << " - " << strerror(errno);
        return ErrorCode::INTERNAL_ERROR;
    }
    
    // Pre-allocate file space
    if (ftruncate(fd, size) == -1) {
        LOG(ERROR) << "Failed to allocate space for backing file: " << file_path << " - " << strerror(errno);
        close(fd);
        return ErrorCode::INTERNAL_ERROR;
    }
    
    close(fd);
    
    LOG(INFO) << "Created backing file: " << file_path << " (" << size << " bytes)";
    
    return ErrorCode::OK;
}

ErrorCode MmapDiskBackend::setup_mmap(const std::string& file_path, uint64_t size) {
    // Open file for mmap
    fd_ = open(file_path.c_str(), O_RDWR);
    if (fd_ == -1) {
        LOG(ERROR) << "Failed to open file for mmap: " << file_path << " - " << strerror(errno);
        return ErrorCode::INTERNAL_ERROR;
    }
    
    // Memory map the file
    mapped_addr_ = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (mapped_addr_ == MAP_FAILED) {
        LOG(ERROR) << "Failed to mmap file: " << file_path << " - " << strerror(errno);
        close(fd_);
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    // Advise the kernel about our usage pattern
    if (madvise(mapped_addr_, size, MADV_RANDOM) == -1) {
        LOG(WARNING) << "madvise failed for " << file_path << " - " << strerror(errno);
    }
    
    LOG(INFO) << "Memory mapped file: " << file_path << " at address " 
              << std::hex << mapped_addr_ << std::dec << " (" << size << " bytes)";
    
    return ErrorCode::OK;
}

void MmapDiskBackend::cleanup_mmap() {
    if (mapped_addr_ && mapped_addr_ != MAP_FAILED) {
        if (munmap(mapped_addr_, capacity_) == -1) {
            LOG(ERROR) << "Failed to unmap memory: " << strerror(errno);
        }
        mapped_addr_ = nullptr;
    }
    
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
    
    LOG(INFO) << "Cleaned up mmap: " << file_path_;
}

} // namespace blackbird