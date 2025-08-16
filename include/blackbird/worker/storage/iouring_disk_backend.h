#pragma once

#include "blackbird/worker/storage/storage_backend.h"
#include <liburing.h>
#include <unordered_map>
#include <mutex>
#include <random>
#include <filesystem>
#include <queue>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <future>

namespace blackbird {

/**
 * @brief High-performance storage backend using io_uring
 * Supports NVME, SSD, and HDD storage classes with async I/O operations
 */
class IoUringDiskBackend : public StorageBackend {
public:
    /**
     * @brief Constructor
     * @param capacity Total capacity in bytes
     * @param storage_class Storage class (NVME, SSD, or HDD)
     * @param mount_path Root directory for storage files
     * @param queue_depth io_uring queue depth (default: 256)
     */
    IoUringDiskBackend(uint64_t capacity, StorageClass storage_class, 
                       const std::string& mount_path, uint32_t queue_depth = 256);
    
    /**
     * @brief Destructor
     */
    ~IoUringDiskBackend() override;
    
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
    // Configuration
    uint64_t capacity_;
    StorageClass storage_class_;
    std::filesystem::path mount_path_;
    std::filesystem::path storage_dir_;
    uint32_t queue_depth_;
    
    struct io_uring ring_;
    std::atomic<bool> ring_initialized_{false};
    std::atomic<bool> shutdown_requested_{false};
    
    uintptr_t base_address_hash_{0};
    uint32_t rkey_{0};
    
    struct AsyncOperation {
        enum Type { WRITE, READ, DELETE };
        Type type;
        std::string file_path;
        std::shared_ptr<std::vector<uint8_t>> data; 
        uint64_t size;
        int fd;
        std::atomic<bool> completed{false};
        ErrorCode result{ErrorCode::OK};
        
        AsyncOperation(Type t, const std::string& path) 
            : type(t), file_path(path), fd(-1) {}
    };
    
    using AsyncOpPtr = std::shared_ptr<AsyncOperation>;
    std::unordered_map<uint64_t, AsyncOpPtr> pending_operations_;  
    std::mutex operations_mutex_;
    uint64_t next_operation_id_{1};
    
    struct IoUringShard {
        std::string file_path;
        uint64_t file_offset;
        uint64_t size;
        bool committed;
        std::chrono::system_clock::time_point created_at;
        int fd;  // File descriptor for async operations
        
        IoUringShard() : file_offset(0), size(0), committed(false), fd(-1) {}
    };
    
    std::unordered_map<std::string, IoUringShard> reservations_;
    std::unordered_map<uint64_t, IoUringShard> committed_shards_; // remote_addr -> shard
    mutable std::mutex mutex_;
    
    mutable uint64_t used_capacity_{0};
    mutable uint64_t num_reservations_{0};
    mutable uint64_t num_committed_shards_{0};
    mutable uint64_t total_async_operations_{0};
    
    std::mt19937 rng_;
    uint64_t next_file_id_{0};
    
    // Private methods
    std::string generate_token_id();
    std::string generate_shard_filename();
    uint64_t encode_remote_addr(const std::string& file_path, uint64_t offset) const;
    std::pair<std::string, uint64_t> decode_remote_addr(uint64_t remote_addr) const;
    
    ErrorCode create_shard_file_async(const std::string& file_path, uint64_t size);
    ErrorCode delete_shard_file_async(const std::string& file_path);
    uint64_t calculate_disk_usage() const;
    
    ErrorCode setup_io_uring();
    void cleanup_io_uring();

    int open_file_optimized(const std::string& file_path, int flags, mode_t mode = 0644);
};

} // namespace blackbird