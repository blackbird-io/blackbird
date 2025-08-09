#pragma once

#include <cstdint>

namespace blackbird {
namespace error {

/**
 * @brief Defines the major error domains in the system
 * 
 * Each domain represents a distinct subsystem or category of errors.
 * Error codes within each domain start at the domain's base value.
 */
enum class Domain : uint32_t {
    // Success has its own domain starting at 0
    SUCCESS = 0,
    
    // Core system errors start at 1000
    SYSTEM = 1000,
    
    // Storage and memory management (2000-2999)
    STORAGE = 2000,
    
    // Network and communication (3000-3999)
    NETWORK = 3000,
    
    // Coordination and service discovery (4000-4999)
    COORDINATION = 4000,
    
    // Data management and operations (5000-5999)
    DATA = 5000,
    
    // Client and session management (6000-6999)
    CLIENT = 6000,
    
    // Configuration and validation (7000-7999)
    CONFIG = 7000
};

/**
 * @brief Gets the base error code value for a given domain
 * 
 * @param domain The error domain
 * @return uint32_t The starting error code for this domain
 */
constexpr uint32_t get_domain_base(Domain domain) noexcept {
    return static_cast<uint32_t>(domain);
}

/**
 * @brief Checks if an error code belongs to a specific domain
 * 
 * @param error_code The error code to check
 * @param domain The domain to check against
 * @return true if the error code belongs to the domain
 */
constexpr bool is_in_domain(uint32_t error_code, Domain domain) noexcept {
    const uint32_t domain_base = get_domain_base(domain);
    const uint32_t next_domain_base = domain_base + 1000;
    return error_code >= domain_base && error_code < next_domain_base;
}

} // namespace error
} // namespace blackbird 