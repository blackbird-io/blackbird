#pragma once

#include "blackbird/worker/storage/storage_backend.h"
#include <unordered_map>
#include <mutex>
#include <random>

namespace blackbird {

/**
 * @brief RAM-based storage backend using malloc/free
 */
class RamBackend : public StorageBackend {
public:
    /**
     * @brief Constructor
     * @param capacity Total capacity in bytes
     * @param storage_class Storage class (RAM_CPU or RAM_GPU)
     */
    RamBackend(uint64_t capacity, StorageClass storage_class = StorageClass::RAM_CPU);
    
    /**
     * @brief Destructor
     */
    ~RamBackend() override;
    
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
    
    // Memory management
    void* base_memory_{nullptr};
    uintptr_t base_address_{0};
    uint32_t rkey_{0}; // Placeholder for future UCX integration
    
    // Allocation tracking
    struct Shard {
        uint64_t offset;
        uint64_t size;
        bool committed;
        std::chrono::system_clock::time_point created_at;
    };
    
    std::unordered_map<std::string, Shard> reservations_;
    std::unordered_map<uint64_t, uint64_t> committed_shards_; // remote_addr -> size
    mutable std::mutex mutex_;
    
    // Stats
    mutable uint64_t used_capacity_{0};
    mutable uint64_t num_reservations_{0};
    mutable uint64_t num_committed_shards_{0};
    
    // Utilities
    std::mt19937 rng_;
    std::string generate_token_id();
    uint64_t find_free_offset(uint64_t size);
};

} // namespace blackbird 