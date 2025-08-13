#pragma once

#include <string>
#include <cstdint>
#include <memory>

#include "blackbird/common/types.h"

namespace blackbird {

/**
 * @brief Reservation token for allocated storage shards
 */
struct ReservationToken {
    std::string token_id;
    std::string pool_id;
    uint64_t remote_addr;
    uint32_t rkey;
    uint64_t size;
    std::chrono::system_clock::time_point expires_at;
    
    bool is_expired() const {
        return std::chrono::system_clock::now() > expires_at;
    }
};

/**
 * @brief Storage backend statistics
 */
struct StorageStats {
    uint64_t total_capacity{0};
    uint64_t used_capacity{0};
    uint64_t available_capacity{0};
    double utilization{0.0};
    uint64_t num_reservations{0};
    uint64_t num_committed_shards{0};
    
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(StorageStats, total_capacity, used_capacity, 
                                   available_capacity, utilization, num_reservations, 
                                   num_committed_shards)
};

/**
 * @brief Abstract interface for storage backends (RAM, GPU, SSD, etc.)
 */
class StorageBackend {
public:
    virtual ~StorageBackend() = default;
    
    /**
     * @brief Get the storage class this backend implements
     */
    virtual StorageClass get_storage_class() const = 0;
    
    /**
     * @brief Get total capacity in bytes
     */
    virtual uint64_t get_total_capacity() const = 0;
    
    /**
     * @brief Get currently used capacity in bytes
     */
    virtual uint64_t get_used_capacity() const = 0;
    
    /**
     * @brief Get available capacity in bytes
     */
    virtual uint64_t get_available_capacity() const = 0;
    
    /**
     * @brief Get base address for RDMA operations (if applicable)
     */
    virtual uintptr_t get_base_address() const = 0;
    
    /**
     * @brief Get UCX memory registration key (if applicable)
     */
    virtual uint32_t get_rkey() const = 0;
    
    /**
     * @brief Reserve a shard of storage
     * @param size Size in bytes to reserve
     * @param hint Optional allocation hint
     * @return Reservation token or error
     */
    virtual Result<ReservationToken> reserve_shard(uint64_t size, 
                                                   const std::string& hint = "") = 0;
    
    /**
     * @brief Commit a reserved shard (mark as in-use)
     * @param token Reservation token from reserve_shard
     * @return ErrorCode::OK on success
     */
    virtual ErrorCode commit_shard(const ReservationToken& token) = 0;
    
    /**
     * @brief Abort/free a reserved shard
     * @param token Reservation token from reserve_shard  
     * @return ErrorCode::OK on success
     */
    virtual ErrorCode abort_shard(const ReservationToken& token) = 0;
    
    /**
     * @brief Free a committed shard
     * @param remote_addr Remote address of the shard
     * @param size Size of the shard
     * @return ErrorCode::OK on success
     */
    virtual ErrorCode free_shard(uint64_t remote_addr, uint64_t size) = 0;
    
    /**
     * @brief Get backend statistics
     */
    virtual StorageStats get_stats() const = 0;
    
    /**
     * @brief Initialize the backend
     * @return ErrorCode::OK on success
     */
    virtual ErrorCode initialize() = 0;
    
    /**
     * @brief Cleanup and shutdown the backend
     */
    virtual void shutdown() = 0;
};

/**
 * @brief Factory function to create storage backends
 */
std::unique_ptr<StorageBackend> create_storage_backend(StorageClass storage_class,
                                                      uint64_t capacity,
                                                      const std::string& config = "");

} // namespace blackbird 