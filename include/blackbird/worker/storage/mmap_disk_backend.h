#pragma once

#include "blackbird/worker/storage/storage_backend.h"
#include "blackbird/allocation/range_allocator.h"
#include <sys/mman.h>
#include <unordered_map>
#include <mutex>
#include <random>
#include <filesystem>
#include <atomic>
#include <chrono>

namespace blackbird {

/**
 * @brief Memory-mapped disk backend with RDMA support
 * 
 * This backend maps disk files into virtual memory and registers them with UCX 
 * for high-performance remote direct memory access. 
 * 
 */
class MmapDiskBackend : public StorageBackend {
public:
    /**
     * @brief Constructor
     * @param capacity Total capacity in bytes 
     * @param storage_class Storage class (NVME, SSD, or HDD)
     * @param mount_path Root directory for storage files
     */
    MmapDiskBackend(uint64_t capacity, StorageClass storage_class,
                    const std::string& mount_path);
    
    /**
     * @brief Destructor - unmaps all memory regions
     */
    ~MmapDiskBackend() override;
    
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
    
    // Single memory pool management
    void* mapped_addr_{nullptr};           // mmap() returned address
    int fd_{-1};                           // File descriptor for backing file
    std::string file_path_;                // Path to backing file

    
    // Range allocation using shared allocator
    struct Shard {
        allocation::Range range;
        bool committed;
        std::chrono::system_clock::time_point created_at;
        
        // Constructor needed for Range which doesn't have default constructor
        Shard() : range(0, 0), committed(false), created_at(std::chrono::system_clock::now()) {}
        Shard(uint64_t offset, uint64_t size) : range(offset, size), committed(false), created_at(std::chrono::system_clock::now()) {}
    };
    
    std::unordered_map<std::string, Shard> reservations_;
    std::unique_ptr<allocation::PoolAllocator> pool_allocator_;
    
    // Thread safety
    mutable std::mutex mutex_;
    
    // Statistics
    mutable uint64_t used_capacity_{0};
    mutable uint64_t num_reservations_{0};
    mutable uint64_t num_committed_shards_{0};
    

    
    // Private methods
    uint64_t find_free_offset(uint64_t size);
    std::string generate_token_id();
    ErrorCode create_backing_file(const std::string& file_path, uint64_t size);
    ErrorCode setup_mmap(const std::string& file_path, uint64_t size);
    void cleanup_mmap();
};

} // namespace blackbird