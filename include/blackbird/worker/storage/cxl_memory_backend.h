#pragma once

#include "blackbird/worker/storage/storage_backend.h"
#include <unordered_map>
#include <mutex>
#include <random>

namespace blackbird {

/**
 * @brief Configuration for CXL memory devices
 */
struct CxlDeviceConfig {
    CxlDeviceId device_id;
    std::string device_path;      // e.g., /dev/cxl/mem0
    std::string dax_device;        // e.g., /dev/dax0.0 for direct access
    uint64_t capacity;
    uint64_t interleave_granularity{256};  // Bytes, typically 256B or 4KB
    bool enable_numa_binding{true};
    int numa_node{-1};             // NUMA node affinity if available
    
    // CXL.mem specific parameters
    bool enable_persistent_mode{false};  // CXL persistent memory mode
    uint64_t cache_line_size{64};        // Cache line size for alignment
};

/**
 * @brief CXL memory backend supporting CXL.mem protocol
 * 
 * This backend manages CXL-attached memory as a first-class storage tier,
 * supporting both volatile and persistent CXL memory configurations.
 * Can be used with CXL Type 2 devices (compute + memory) or Type 3 (memory only).
 */
class CxlMemoryBackend : public StorageBackend {
public:
    /**
     * @brief Constructor
     * @param config CXL device configuration
     * @param storage_class Storage class (CXL_MEMORY or CXL_TYPE2_DEVICE)
     */
    CxlMemoryBackend(const CxlDeviceConfig& config, 
                     StorageClass storage_class = StorageClass::CXL_MEMORY);
    
    /**
     * @brief Destructor
     */
    ~CxlMemoryBackend() override;
    
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
    
    /**
     * @brief Get CXL device configuration
     */
    const CxlDeviceConfig& get_device_config() const { return config_; }
    
    /**
     * @brief Check if device supports persistent memory mode
     */
    bool supports_persistent_mode() const { return config_.enable_persistent_mode; }

private:
    CxlDeviceConfig config_;
    StorageClass storage_class_;
    
    void* base_memory_{nullptr};
    uintptr_t base_address_{0};
    uint32_t rkey_{0};
    int fd_{-1};  // File descriptor for DAX device
    
    struct Shard {
        uint64_t offset;
        uint64_t size;
        bool committed;
        std::chrono::system_clock::time_point created_at;
        CxlMemoryRegionId region_id;  // CXL region identifier
    };
    
    std::unordered_map<std::string, Shard> reservations_;
    mutable std::mutex mutex_;
    
    mutable uint64_t used_capacity_{0};
    mutable uint64_t num_reservations_{0};
    mutable uint64_t num_committed_shards_{0};
    
    std::mt19937 rng_;
    
    // Helper methods
    std::string generate_token_id();
    uint64_t find_free_offset(uint64_t size);
    ErrorCode map_cxl_device();
    ErrorCode configure_numa_binding();
    
    // CXL-specific alignment (typically cache line aligned)
    uint64_t align_size(uint64_t size) const {
        return (size + config_.cache_line_size - 1) & ~(config_.cache_line_size - 1);
    }
};

} // namespace blackbird

