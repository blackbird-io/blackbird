#pragma once

#include "blackbird/worker/storage/storage_backend.h"
#include <unordered_map>
#include <mutex>
#include <random>
#include <filesystem>
#include <fstream>

namespace blackbird {

/**
 * @brief Disk-based storage backend using file system operations
 * Supports NVME, SSD, and HDD storage classes
 */
class DiskBackend : public StorageBackend {
public:
    /**
     * @brief Constructor
     * @param capacity Total capacity in bytes
     * @param storage_class Storage class (NVME, SSD, or HDD)
     * @param mount_path Root directory for storage files
     */
    DiskBackend(uint64_t capacity, StorageClass storage_class, const std::string& mount_path);
    
    /**
     * @brief Destructor
     */
    ~DiskBackend() override;
    
    // StorageBackend interface
    StorageClass get_storage_class() const override;
    uint64_t get_total_capacity() const override;
    uint64_t get_used_capacity() const override;
    uint64_t get_available_capacity() const override;
    uintptr_t get_base_address() const override;
    uint32_t get_rkey() const override;
    
    Result<ReservationToken> reserve_shard(uint64_t size, const std::string& hint = "") override;
    ErrorCode commit_shard(const ReservationToken& token) override;
    ErrorCode abort_shard(const ReservationToken& token) override;
    ErrorCode free_shard(uint64_t remote_addr, uint64_t size) override;
    StorageStats get_stats() const override;
    
    ErrorCode initialize() override;
    void shutdown() override;

private:
    uint64_t capacity_;
    StorageClass storage_class_;
    std::filesystem::path mount_path_;
    std::filesystem::path storage_dir_;
    
    uintptr_t base_address_hash_{0}; 
    uint32_t rkey_{0}; 
    
    struct DiskShard {
        std::string file_path;
        uint64_t file_offset;
        uint64_t size;
        bool committed;
        std::chrono::system_clock::time_point created_at;
    };
    
    std::unordered_map<std::string, DiskShard> reservations_;
    std::unordered_map<uint64_t, DiskShard> committed_shards_; 
    mutable std::mutex mutex_;
    
    mutable uint64_t used_capacity_{0};
    mutable uint64_t num_reservations_{0};
    mutable uint64_t num_committed_shards_{0};
    
    std::mt19937 rng_;
    uint64_t next_file_id_{0};
    
    std::string generate_token_id();
    std::string generate_shard_filename();
    uint64_t encode_remote_addr(const std::string& file_path, uint64_t offset) const;
    std::pair<std::string, uint64_t> decode_remote_addr(uint64_t remote_addr) const;
    ErrorCode create_shard_file(const std::string& file_path, uint64_t size);
    ErrorCode delete_shard_file(const std::string& file_path);
    uint64_t calculate_disk_usage() const;
};

} // namespace blackbird