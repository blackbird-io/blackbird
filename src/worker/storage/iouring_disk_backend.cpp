#include "blackbird/worker/storage/iouring_disk_backend.h"

#include <glog/logging.h>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <future>

namespace blackbird {

IoUringDiskBackend::IoUringDiskBackend(uint64_t capacity, StorageClass storage_class, 
                                       const std::string& mount_path, uint32_t queue_depth)
    : capacity_(capacity), storage_class_(storage_class), mount_path_(mount_path), 
      queue_depth_(queue_depth), rng_(std::random_device{}()) {
    
    if (storage_class != StorageClass::NVME && storage_class != StorageClass::SSD && 
        storage_class != StorageClass::HDD) {
        throw std::invalid_argument("IoUringDiskBackend only supports NVME, SSD, or HDD storage classes");
    }
    
    storage_dir_ = mount_path_ / "blackbird_storage";
    
    std::random_device rd;
    std::mt19937_64 secure_gen(rd());
    std::uniform_int_distribution<uint64_t> addr_dist;
    std::uniform_int_distribution<uint32_t> rkey_dist;
    
    base_address_hash_ = addr_dist(secure_gen);
    rkey_ = rkey_dist(secure_gen);
    
    if (base_address_hash_ == 0) base_address_hash_ = 1;
    if (rkey_ == 0) rkey_ = 1;
    
    LOG(INFO) << "Creating IoUringDiskBackend with capacity " << capacity 
              << " bytes, storage_class " << static_cast<uint32_t>(storage_class)
              << ", mount_path " << mount_path << ", queue_depth " << queue_depth;
}

IoUringDiskBackend::~IoUringDiskBackend() {
    shutdown();
}

StorageClass IoUringDiskBackend::get_storage_class() const {
    return storage_class_;
}

uint64_t IoUringDiskBackend::get_total_capacity() const {
    return capacity_;
}

uint64_t IoUringDiskBackend::get_used_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_capacity_;
}

uint64_t IoUringDiskBackend::get_available_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return capacity_ - used_capacity_;
}

uintptr_t IoUringDiskBackend::get_base_address() const {
    return base_address_hash_;
}

uint32_t IoUringDiskBackend::get_rkey() const {
    return rkey_;
}

Result<ReservationToken> IoUringDiskBackend::reserve_shard(uint64_t size, const std::string& hint) {
    if (size == 0) {
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint64_t available = capacity_ - used_capacity_;
    if (size > available) {
        LOG(WARNING) << "Not enough free space: requested " << size 
                    << ", available " << available;
        return ErrorCode::OUT_OF_MEMORY;
    }
    
    std::string file_path = generate_shard_filename();
    std::string token_id = generate_token_id();
    auto expires_at = std::chrono::system_clock::now() + std::chrono::minutes(10); // 10 minute expiry
    
    uint64_t remote_addr = encode_remote_addr(file_path, 0);
    
    ReservationToken token;
    token.token_id = token_id;
    token.pool_id = ""; // Will be set by caller
    token.remote_addr = remote_addr;
    token.rkey = rkey_;
    token.size = size;
    token.expires_at = expires_at;
    
    IoUringShard shard;
    shard.file_path = file_path;
    shard.file_offset = 0; // Start of file
    shard.size = size;
    shard.committed = false;
    shard.created_at = std::chrono::system_clock::now();
    shard.fd = -1; // Will be opened during commit
    
    reservations_[token_id] = shard;
    used_capacity_ += size;
    num_reservations_++;
    
    LOG(INFO) << "Reserved io_uring shard: file " << file_path << ", size " << size 
              << ", token " << token_id;
    
    return token;
}

ErrorCode IoUringDiskBackend::commit_shard(const ReservationToken& token) {
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
    
    // Create the actual file on disk using async I/O
    auto err = create_shard_file_async(it->second.file_path, it->second.size);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to create shard file: " << it->second.file_path;
        // Clean up failed reservation
        used_capacity_ -= it->second.size;
        num_reservations_--;
        reservations_.erase(it);
        return err;
    }
    
    // Mark as committed
    it->second.committed = true;
    committed_shards_[token.remote_addr] = it->second;
    num_committed_shards_++;
    
    LOG(INFO) << "Committed io_uring shard: token " << token.token_id 
              << ", file " << it->second.file_path;
    
    return ErrorCode::OK;
}

ErrorCode IoUringDiskBackend::abort_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(WARNING) << "Reservation not found: " << token.token_id;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    // Close file descriptor if opened
    if (it->second.fd >= 0) {
        close(it->second.fd);
    }
    
    // Free the reservation
    used_capacity_ -= it->second.size;
    num_reservations_--;
    reservations_.erase(it);
    
    LOG(INFO) << "Aborted io_uring shard: token " << token.token_id;
    
    return ErrorCode::OK;
}

ErrorCode IoUringDiskBackend::free_shard(uint64_t remote_addr, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = committed_shards_.find(remote_addr);
    if (it == committed_shards_.end()) {
        LOG(WARNING) << "Committed shard not found: " << std::hex << remote_addr;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    if (it->second.size != size) {
        LOG(WARNING) << "Size mismatch: expected " << it->second.size << ", got " << size;
        return ErrorCode::INVALID_PARAMETERS;
    }
    
    // Delete the file from disk using async I/O
    auto err = delete_shard_file_async(it->second.file_path);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to delete shard file: " << it->second.file_path;
        // Continue with cleanup even if file deletion fails
    }
    
    // Close file descriptor if still open
    if (it->second.fd >= 0) {
        close(it->second.fd);
    }
    
    // Free the shard
    used_capacity_ -= size;
    num_committed_shards_--;
    committed_shards_.erase(it);
    
    LOG(INFO) << "Freed io_uring shard: addr " << std::hex << remote_addr << ", size " << size;
    
    return ErrorCode::OK;
}

StorageStats IoUringDiskBackend::get_stats() const {
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

ErrorCode IoUringDiskBackend::initialize() {
    LOG(INFO) << "Initializing IoUringDiskBackend...";
    
    std::error_code ec;
    
    // Create storage directory if it doesn't exist
    if (!std::filesystem::exists(storage_dir_, ec)) {
        if (!std::filesystem::create_directories(storage_dir_, ec)) {
            LOG(ERROR) << "Failed to create storage directory " << storage_dir_ 
                      << ": " << ec.message();
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    
    // Verify directory is writable
    auto test_file = storage_dir_ / ".write_test";
    std::ofstream test_stream(test_file);
    if (!test_stream.is_open()) {
        LOG(ERROR) << "Storage directory " << storage_dir_ << " is not writable";
        return ErrorCode::INTERNAL_ERROR;
    }
    test_stream.close();
    std::filesystem::remove(test_file, ec);
    
    // Setup io_uring
    auto ring_result = setup_io_uring();
    if (ring_result != ErrorCode::OK) {
        LOG(ERROR) << "Failed to setup io_uring";
        return ring_result;
    }
    
    // Note: Currently using synchronous file operations, so no completion thread needed
    
    // Calculate actual disk usage from existing files
    used_capacity_ = calculate_disk_usage();
    
    LOG(INFO) << "IoUringDiskBackend initialized: storage_dir=" << storage_dir_
              << ", capacity=" << capacity_ << " bytes"
              << ", used=" << used_capacity_ << " bytes"
              << ", queue_depth=" << queue_depth_;
    
    return ErrorCode::OK;
}

void IoUringDiskBackend::shutdown() {
    if (shutdown_requested_.exchange(true)) {
        return; // Already shutting down
    }
    
    LOG(INFO) << "Shutting down IoUringDiskBackend with " << reservations_.size() 
              << " active reservations and " << committed_shards_.size() 
              << " committed shards";
    
    // Cleanup io_uring
    cleanup_io_uring();
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Close all open file descriptors
    for (auto& [token_id, shard] : reservations_) {
        if (shard.fd >= 0) {
            close(shard.fd);
        }
    }
    
    for (auto& [addr, shard] : committed_shards_) {
        if (shard.fd >= 0) {
            close(shard.fd);
        }
    }
    
    // Clean up tracking structures
    reservations_.clear();
    committed_shards_.clear();
    used_capacity_ = 0;
    num_reservations_ = 0;
    num_committed_shards_ = 0;
}

// Private methods implementation
std::string IoUringDiskBackend::generate_token_id() {
    std::stringstream ss;
    ss << std::hex << rng_() << std::hex << rng_();
    return ss.str();
}

std::string IoUringDiskBackend::generate_shard_filename() {
    std::stringstream ss;
    ss << "iouring_shard_" << std::setfill('0') << std::setw(8) << next_file_id_++ 
       << "_" << std::hex << rng_() << ".dat";
    return (storage_dir_ / ss.str()).string();
}

uint64_t IoUringDiskBackend::encode_remote_addr(const std::string& file_path, uint64_t offset) const {
    std::hash<std::string> hasher;
    uint64_t path_hash = hasher(file_path);
    return (path_hash & 0xFFFFFFFF00000000ULL) | (offset & 0xFFFFFFFFULL);
}

std::pair<std::string, uint64_t> IoUringDiskBackend::decode_remote_addr(uint64_t remote_addr) const {
    uint64_t offset = remote_addr & 0xFFFFFFFFULL;
    
    // Find matching file in committed_shards_
    for (const auto& [addr, shard] : committed_shards_) {
        if (addr == remote_addr) {
            return {shard.file_path, offset};
        }
    }
    
    return {"", 0}; // Not found
}

ErrorCode IoUringDiskBackend::setup_io_uring() {
    int ret = io_uring_queue_init(queue_depth_, &ring_, 0);
    if (ret < 0) {
        LOG(ERROR) << "Failed to initialize io_uring: " << strerror(-ret);
        return ErrorCode::INTERNAL_ERROR;
    }
    
    ring_initialized_ = true;
    LOG(INFO) << "io_uring initialized with queue depth " << queue_depth_;
    return ErrorCode::OK;
}

void IoUringDiskBackend::cleanup_io_uring() {
    if (ring_initialized_) {
        io_uring_queue_exit(&ring_);
        ring_initialized_ = false;
        LOG(INFO) << "io_uring cleaned up";
    }
}

int IoUringDiskBackend::open_file_optimized(const std::string& file_path, int flags, mode_t mode) {
    // Use O_DIRECT for high-performance storage classes
    if (storage_class_ == StorageClass::NVME || storage_class_ == StorageClass::SSD) {
        flags |= O_DIRECT;
    }
    
    int fd = open(file_path.c_str(), flags, mode);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open file " << file_path << ": " << strerror(errno);
    }
    return fd;
}

ErrorCode IoUringDiskBackend::create_shard_file_async(const std::string& file_path, uint64_t size) {
    // For now, implement synchronous file creation
    // In a full implementation, we'd use io_uring for file creation as well
    try {
        int fd = open_file_optimized(file_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) {
            return ErrorCode::INTERNAL_ERROR;
        }
        
        // For io_uring compatibility, use posix_fallocate to pre-allocate space
        if (posix_fallocate(fd, 0, size) != 0) {
            LOG(WARNING) << "Failed to pre-allocate file space: " << file_path;
            // Continue anyway - not critical for functionality
        }
        
        close(fd);
        LOG(INFO) << "Created io_uring shard file: " << file_path << " (" << size << " bytes)";
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception creating shard file " << file_path << ": " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode IoUringDiskBackend::delete_shard_file_async(const std::string& file_path) {
    try {
        std::error_code ec;
        if (std::filesystem::remove(file_path, ec)) {
            LOG(INFO) << "Deleted io_uring shard file: " << file_path;
            return ErrorCode::OK;
        } else {
            LOG(WARNING) << "Failed to delete file " << file_path << ": " << ec.message();
            return ErrorCode::INTERNAL_ERROR;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception deleting shard file " << file_path << ": " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

uint64_t IoUringDiskBackend::calculate_disk_usage() const {
    uint64_t total_size = 0;
    std::error_code ec;
    
    if (!std::filesystem::exists(storage_dir_, ec)) {
        return 0;
    }
    
    try {
        for (const auto& entry : std::filesystem::directory_iterator(storage_dir_, ec)) {
            if (entry.is_regular_file(ec)) {
                total_size += entry.file_size(ec);
            }
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception calculating disk usage: " << e.what();
    }
    
    return total_size;
}

// Note: Completion thread methods removed since we're using synchronous operations

} // namespace blackbird