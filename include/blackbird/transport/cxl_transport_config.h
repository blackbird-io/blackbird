#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include "blackbird/common/types.h"

namespace blackbird {

/**
 * @brief CXL interconnect type
 */
enum class CxlInterconnectType : uint32_t {
    CXL_MEM = 0,           // CXL.mem protocol (memory semantic)
    CXL_CACHE = 1,         // CXL.cache protocol (cache coherent)
    CXL_IO = 2,            // CXL.io protocol (PCIe-based)
    CXL_FABRIC = 3,        // CXL fabric/switch topology
    HYBRID = 4             // Hybrid configuration
};

/**
 * @brief Transport protocol for CXL data movement
 */
enum class CxlTransportProtocol : uint32_t {
    DIRECT_CXL = 0,        // Direct CXL.mem access
    RDMA_OVER_CXL = 1,     // RDMA over CXL (RoCE/UCX)
    NVLINK = 2,            // NVLink for GPU-attached CXL memory
    CUSTOM = 999           // Custom transport
};

/**
 * @brief Configuration for CXL transport layer
 * 
 * This configuration enables Blackbird to proxy data over multiple
 * interconnect types (CXL, NVLink, RoCE, etc.) based on deployment.
 */
struct CxlTransportConfig {
    CxlInterconnectType interconnect_type{CxlInterconnectType::CXL_MEM};
    CxlTransportProtocol transport_protocol{CxlTransportProtocol::DIRECT_CXL};
    
    // CXL fabric configuration
    bool enable_fabric_manager{false};
    std::string fabric_manager_endpoint;
    std::vector<CxlDeviceId> fabric_devices;
    
    // Performance tuning
    uint64_t max_transfer_size{1ULL << 30};  // 1GB max transfer
    uint32_t queue_depth{128};                // Command queue depth
    bool enable_zero_copy{true};              // Zero-copy transfers
    
    // Multi-path configuration
    bool enable_multipath{false};             // Enable multiple paths for redundancy
    std::vector<std::string> fallback_transports;  // Fallback to UCX, TCP, etc.
    
    // QoS and scheduling
    uint32_t priority{0};                     // Traffic priority (0 = normal)
    uint32_t bandwidth_limit_gbps{0};         // 0 = unlimited
    
    // Hardware-specific
    bool enable_cxl_hdm{true};                // CXL Host-Managed Device Memory
    bool enable_cxl_switch{false};            // CXL switch/fabric support
    std::string cxl_port_id;                  // CXL port identifier
    
    // Compatibility with other transports
    bool allow_ucx_fallback{true};            // Fall back to UCX if CXL unavailable
    bool allow_nvlink_fallback{true};         // Fall back to NVLink for GPU memory
    bool allow_roce_fallback{true};           // Fall back to RoCE/IB
};

/**
 * @brief CXL memory pool configuration extending standard memory pools
 */
struct CxlMemoryPoolConfig {
    CxlDeviceId device_id;
    uint64_t capacity;
    uint64_t base_address{0};
    
    // CXL-specific attributes
    uint32_t latency_ns{0};                   // Expected latency in nanoseconds
    uint32_t bandwidth_gbps{0};               // Available bandwidth in Gbps
    bool is_persistent{false};                // Persistent CXL memory
    bool supports_cache_coherency{true};      // Cache coherent access
    
    // NUMA configuration
    int numa_node{-1};                        // Associated NUMA node
    std::vector<int> cpu_affinity;            // CPU cores with best access
    
    // Interleaving
    uint64_t interleave_ways{1};              // Number of interleave ways
    uint64_t interleave_granularity{256};     // Interleave granularity in bytes
};

/**
 * @brief Get string representation of interconnect type
 */
inline const char* to_string(CxlInterconnectType type) {
    switch (type) {
        case CxlInterconnectType::CXL_MEM: return "CXL.mem";
        case CxlInterconnectType::CXL_CACHE: return "CXL.cache";
        case CxlInterconnectType::CXL_IO: return "CXL.io";
        case CxlInterconnectType::CXL_FABRIC: return "CXL.fabric";
        case CxlInterconnectType::HYBRID: return "Hybrid";
        default: return "Unknown";
    }
}

/**
 * @brief Get string representation of transport protocol
 */
inline const char* to_string(CxlTransportProtocol protocol) {
    switch (protocol) {
        case CxlTransportProtocol::DIRECT_CXL: return "Direct CXL";
        case CxlTransportProtocol::RDMA_OVER_CXL: return "RDMA over CXL";
        case CxlTransportProtocol::NVLINK: return "NVLink";
        case CxlTransportProtocol::CUSTOM: return "Custom";
        default: return "Unknown";
    }
}

} // namespace blackbird

