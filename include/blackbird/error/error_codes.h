#pragma once

#include <cstdint>
#include <string_view>
#include "blackbird/error/error_domain.h"

namespace blackbird {
namespace error {

/**
 * @brief Comprehensive error code enumeration for blackbird
 * 
 * Error codes are organized by domain, with each domain having a 1000-number range.
 */
enum class ErrorCode : uint32_t {
    // Success domain (0-999)
    OK = 0,
    
    // System domain (1000-1999)
    INTERNAL_ERROR = get_domain_base(Domain::SYSTEM),
    INITIALIZATION_FAILED,
    INVALID_STATE,
    OPERATION_TIMEOUT,
    RESOURCE_EXHAUSTED,
    
    // Storage domain (2000-2999)
    BUFFER_OVERFLOW = get_domain_base(Domain::STORAGE),
    OUT_OF_MEMORY,
    SEGMENT_NOT_FOUND,
    SEGMENT_ALREADY_EXISTS,
    INVALID_SEGMENT,
    ALLOCATION_FAILED,
    MEMORY_ACCESS_ERROR,
    
    // Network domain (3000-3999)
    NETWORK_ERROR = get_domain_base(Domain::NETWORK),
    CONNECTION_FAILED,
    TRANSFER_FAILED,
    UCX_ERROR,
    INVALID_ADDRESS,
    REMOTE_ENDPOINT_ERROR,
    RPC_FAILED,
    
    // Coordination domain (4000-4999)
    ETCD_ERROR = get_domain_base(Domain::COORDINATION),
    ETCD_KEY_NOT_FOUND,
    ETCD_TRANSACTION_FAILED,
    ETCD_LEASE_ERROR,
    ETCD_WATCH_ERROR,
    LEADER_ELECTION_FAILED,
    SERVICE_REGISTRATION_FAILED,
    
    // Data domain (5000-5999)
    OBJECT_NOT_FOUND = get_domain_base(Domain::DATA),
    OBJECT_ALREADY_EXISTS,
    INVALID_KEY,
    INVALID_WORKER,
    WORKER_NOT_READY,
    NO_COMPLETE_WORKER,
    DATA_CORRUPTION,
    CHECKSUM_MISMATCH,
    
    // Client domain (6000-6999)
    CLIENT_ERROR = get_domain_base(Domain::CLIENT),
    CLIENT_NOT_FOUND,
    CLIENT_ALREADY_EXISTS,
    CLIENT_DISCONNECTED,
    SESSION_EXPIRED,
    INVALID_CLIENT_STATE,
    
    // Config domain (7000-7999)
    CONFIG_ERROR = get_domain_base(Domain::CONFIG),
    INVALID_CONFIGURATION,
    INVALID_PARAMETERS,
    MISSING_REQUIRED_FIELD,
    VALUE_OUT_OF_RANGE
};

/**
 * @brief Get the domain of an error code
 * 
 * @param code The error code to check
 * @return Domain The domain this error belongs to
 */
constexpr Domain get_error_domain(ErrorCode code) noexcept {
    const uint32_t value = static_cast<uint32_t>(code);
    if (value < 1000) return Domain::SUCCESS;
    return static_cast<Domain>((value / 1000) * 1000);
}

/**
 * @brief Check if an error code indicates success
 * 
 * @param code The error code to check
 * @return true if the operation was successful
 */
constexpr bool is_ok(ErrorCode code) noexcept {
    return code == ErrorCode::OK;
}

/**
 * @brief Get a string representation of an error code
 * 
 * @param code The error code to convert
 * @return std::string_view A human-readable description of the error
 */
std::string_view to_string(ErrorCode code) noexcept;

/**
 * @brief Get a detailed description of an error code
 * 
 * @param code The error code to describe
 * @return std::string_view A detailed description of what the error means
 */
std::string_view get_error_description(ErrorCode code) noexcept;

} // namespace error

// For backward compatibility and ease of use
using ErrorCode = error::ErrorCode;
inline bool is_ok(ErrorCode code) noexcept { return error::is_ok(code); }
// inline std::string_view to_string(ErrorCode code) noexcept { return error::to_string(code); }

} // namespace blackbird 