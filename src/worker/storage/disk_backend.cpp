#include "blackbird/worker/storage/disk_backend.h"

#include <glog/logging.h>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <cstring>

namespace blackbird {

DiskBackend::DiskBackend(uint64_t capacity, StorageClass storage_class, const std::string& mount_path)
    : capacity_(capacity), storage_class_(storage_class), mount_path_(mount_path), 
      rng_(std::random_device{}()) {
    
    if (storage_class != StorageClass::NVME && storage_class != StorageClass::SSD && 
        storage_class != StorageClass::HDD) {
        throw std::invalid_argument("DiskBackend only supports NVME, SSD, or HDD storage classes");
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
    
    LOG(INFO) << "Creating DiskBackend with capacity " << capacity 
              << " bytes, storage_class " << static_cast<uint32_t>(storage_class)
              << ", mount_path " << mount_path;
}

DiskBackend::~DiskBackend() {
    shutdown();
}

StorageClass DiskBackend::get_storage_class() const {
    return storage_class_;
}

uint64_t DiskBackend::get_total_capacity() const {
    return capacity_;
}

uint64_t DiskBackend::get_used_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_capacity_;
}

uint64_t DiskBackend::get_available_capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return capacity_ - used_capacity_;
}

uintptr_t DiskBackend::get_base_address() const {
    return base_address_hash_;
}

uint32_t DiskBackend::get_rkey() const {
    return rkey_;
}

Result<ReservationToken> DiskBackend::reserve_shard(uint64_t size, const std::string& hint) {
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
    auto expires_at = std::chrono::system_clock::now() + std::chrono::minutes(10);
    
    uint64_t remote_addr = encode_remote_addr(file_path, 0);
    
    ReservationToken token;
    token.token_id = token_id;
    token.pool_id = ""; // Will be set by caller
    token.remote_addr = remote_addr;
    token.rkey = rkey_;
    token.size = size;
    token.expires_at = expires_at;
    
    DiskShard shard;
    shard.file_path = file_path;
    shard.file_offset = 0; // Start of file
    shard.size = size;
    shard.committed = false;
    shard.created_at = std::chrono::system_clock::now();
    
    reservations_[token_id] = shard;
    used_capacity_ += size;
    num_reservations_++;
    
    LOG(INFO) << "Reserved disk shard: file " << file_path << ", size " << size 
              << ", token " << token_id;
    
    return token;
}

ErrorCode DiskBackend::commit_shard(const ReservationToken& token) {
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
    
    auto err = create_shard_file(it->second.file_path, it->second.size);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to create shard file: " << it->second.file_path;
        // Clean up failed reservation
        used_capacity_ -= it->second.size;
        num_reservations_--;
        reservations_.erase(it);
        return err;
    }
    
    it->second.committed = true;
    committed_shards_[token.remote_addr] = it->second;
    num_committed_shards_++;
    
    LOG(INFO) << "Committed disk shard: token " << token.token_id 
              << ", file " << it->second.file_path;
    
    return ErrorCode::OK;
}

ErrorCode DiskBackend::abort_shard(const ReservationToken& token) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = reservations_.find(token.token_id);
    if (it == reservations_.end()) {
        LOG(WARNING) << "Reservation not found: " << token.token_id;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    
    used_capacity_ -= it->second.size;
    num_reservations_--;
    reservations_.erase(it);
    
    LOG(INFO) << "Aborted disk shard: token " << token.token_id;
    
    return ErrorCode::OK;
}

ErrorCode DiskBackend::free_shard(uint64_t remote_addr, uint64_t size) {
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
    
    auto err = delete_shard_file(it->second.file_path);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to delete shard file: " << it->second.file_path;
        // Continue with cleanup even if file deletion fails
    }
    
    used_capacity_ -= size;
    num_committed_shards_--;
    committed_shards_.erase(it);
    
    LOG(INFO) << "Freed disk shard: addr " << std::hex << remote_addr << ", size " << size;
    
    return ErrorCode::OK;
}

StorageStats DiskBackend::get_stats() const {
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

ErrorCode DiskBackend::initialize() {
    LOG(INFO) << "Initializing DiskBackend...";
    
    std::error_code ec;
    
    if (!std::filesystem::exists(storage_dir_, ec)) {
        if (!std::filesystem::create_directories(storage_dir_, ec)) {
            LOG(ERROR) << "Failed to create storage directory " << storage_dir_ 
                      << ": " << ec.message();
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    
    auto test_file = storage_dir_ / ".write_test";
    std::ofstream test_stream(test_file);
    if (!test_stream.is_open()) {
        LOG(ERROR) << "Storage directory " << storage_dir_ << " is not writable";
        return ErrorCode::INTERNAL_ERROR;
    }
    test_stream.close();
    std::filesystem::remove(test_file, ec);
    
    used_capacity_ = calculate_disk_usage();
    
    LOG(INFO) << "DiskBackend initialized: storage_dir=" << storage_dir_
              << ", capacity=" << capacity_ << " bytes"
              << ", used=" << used_capacity_ << " bytes";
    
    return ErrorCode::OK;
}

void DiskBackend::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    LOG(INFO) << "Shutting down DiskBackend with " << reservations_.size() 
              << " active reservations and " << committed_shards_.size() 
              << " committed shards";
    
    reservations_.clear();
    committed_shards_.clear();
    used_capacity_ = 0;
    num_reservations_ = 0;
    num_committed_shards_ = 0;
}

std::string DiskBackend::generate_token_id() {
    std::stringstream ss;
    ss << std::hex << rng_() << std::hex << rng_();
    return ss.str();
}

std::string DiskBackend::generate_shard_filename() {
    std::stringstream ss;
    ss << "shard_" << std::setfill('0') << std::setw(8) << next_file_id_++ 
       << "_" << std::hex << rng_() << ".dat";
    return (storage_dir_ / ss.str()).string();
}

uint64_t DiskBackend::encode_remote_addr(const std::string& file_path, uint64_t offset) const {
    std::hash<std::string> hasher;
    uint64_t path_hash = hasher(file_path);
    return (path_hash & 0xFFFFFFFF00000000ULL) | (offset & 0xFFFFFFFFULL);
}

std::pair<std::string, uint64_t> DiskBackend::decode_remote_addr(uint64_t remote_addr) const {
    uint64_t path_hash = remote_addr & 0xFFFFFFFF00000000ULL;
    uint64_t offset = remote_addr & 0xFFFFFFFFULL;
    
    for (const auto& [addr, shard] : committed_shards_) {
        if (addr == remote_addr) {
            return {shard.file_path, offset};
        }
    }
    
    return {"", 0}; // Not found
}

ErrorCode DiskBackend::create_shard_file(const std::string& file_path, uint64_t size) {
    try {
        std::ofstream file(file_path, std::ios::binary);
        if (!file.is_open()) {
            LOG(ERROR) << "Failed to create file: " << file_path;
            return ErrorCode::INTERNAL_ERROR;
        }
        
        file.seekp(size - 1);
        file.write("", 1);  
        if (!file.good()) {
            LOG(ERROR) << "Failed to set file size: " << file_path;
            file.close();
            std::filesystem::remove(file_path);
            return ErrorCode::INTERNAL_ERROR;
        }
        
        file.close();
        LOG(INFO) << "Created shard file: " << file_path << " (" << size << " bytes)";
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception creating shard file " << file_path << ": " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode DiskBackend::delete_shard_file(const std::string& file_path) {
    try {
        std::error_code ec;
        if (std::filesystem::remove(file_path, ec)) {
            LOG(INFO) << "Deleted shard file: " << file_path;
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

uint64_t DiskBackend::calculate_disk_usage() const {
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

} // namespace blackbird