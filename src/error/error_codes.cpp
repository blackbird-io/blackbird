#include "blackbird/error/error_codes.h"
#include <unordered_map>

namespace blackbird {
namespace error {

namespace {
// String representations of error codes
const std::unordered_map<ErrorCode, std::string_view> error_strings{
    {ErrorCode::OK, "OK"},
    
    // System domain
    {ErrorCode::INTERNAL_ERROR, "INTERNAL_ERROR"},
    {ErrorCode::INITIALIZATION_FAILED, "INITIALIZATION_FAILED"},
    {ErrorCode::INVALID_STATE, "INVALID_STATE"},
    {ErrorCode::OPERATION_TIMEOUT, "OPERATION_TIMEOUT"},
    {ErrorCode::RESOURCE_EXHAUSTED, "RESOURCE_EXHAUSTED"},
    
    // Storage domain
    {ErrorCode::BUFFER_OVERFLOW, "BUFFER_OVERFLOW"},
    {ErrorCode::OUT_OF_MEMORY, "OUT_OF_MEMORY"},
    {ErrorCode::SEGMENT_NOT_FOUND, "SEGMENT_NOT_FOUND"},
    {ErrorCode::SEGMENT_ALREADY_EXISTS, "SEGMENT_ALREADY_EXISTS"},
    {ErrorCode::INVALID_SEGMENT, "INVALID_SEGMENT"},
    {ErrorCode::ALLOCATION_FAILED, "ALLOCATION_FAILED"},
    {ErrorCode::MEMORY_ACCESS_ERROR, "MEMORY_ACCESS_ERROR"},
    
    // Network domain
    {ErrorCode::NETWORK_ERROR, "NETWORK_ERROR"},
    {ErrorCode::CONNECTION_FAILED, "CONNECTION_FAILED"},
    {ErrorCode::TRANSFER_FAILED, "TRANSFER_FAILED"},
    {ErrorCode::UCX_ERROR, "UCX_ERROR"},
    {ErrorCode::INVALID_ADDRESS, "INVALID_ADDRESS"},
    {ErrorCode::REMOTE_ENDPOINT_ERROR, "REMOTE_ENDPOINT_ERROR"},
    {ErrorCode::RPC_FAILED, "RPC_FAILED"},
    
    // Coordination domain
    {ErrorCode::ETCD_ERROR, "ETCD_ERROR"},
    {ErrorCode::ETCD_KEY_NOT_FOUND, "ETCD_KEY_NOT_FOUND"},
    {ErrorCode::ETCD_TRANSACTION_FAILED, "ETCD_TRANSACTION_FAILED"},
    {ErrorCode::ETCD_LEASE_ERROR, "ETCD_LEASE_ERROR"},
    {ErrorCode::ETCD_WATCH_ERROR, "ETCD_WATCH_ERROR"},
    {ErrorCode::LEADER_ELECTION_FAILED, "LEADER_ELECTION_FAILED"},
    {ErrorCode::SERVICE_REGISTRATION_FAILED, "SERVICE_REGISTRATION_FAILED"},
    
    // Data domain
    {ErrorCode::OBJECT_NOT_FOUND, "OBJECT_NOT_FOUND"},
    {ErrorCode::OBJECT_ALREADY_EXISTS, "OBJECT_ALREADY_EXISTS"},
    {ErrorCode::INVALID_KEY, "INVALID_KEY"},
    {ErrorCode::INVALID_WORKER, "INVALID_WORKER"},
    {ErrorCode::WORKER_NOT_READY, "WORKER_NOT_READY"},
    {ErrorCode::NO_COMPLETE_WORKER, "NO_COMPLETE_WORKER"},
    {ErrorCode::DATA_CORRUPTION, "DATA_CORRUPTION"},
    {ErrorCode::CHECKSUM_MISMATCH, "CHECKSUM_MISMATCH"},
    
    // Client domain
    {ErrorCode::CLIENT_ERROR, "CLIENT_ERROR"},
    {ErrorCode::CLIENT_NOT_FOUND, "CLIENT_NOT_FOUND"},
    {ErrorCode::CLIENT_ALREADY_EXISTS, "CLIENT_ALREADY_EXISTS"},
    {ErrorCode::CLIENT_DISCONNECTED, "CLIENT_DISCONNECTED"},
    {ErrorCode::SESSION_EXPIRED, "SESSION_EXPIRED"},
    {ErrorCode::INVALID_CLIENT_STATE, "INVALID_CLIENT_STATE"},
    
    // Config domain
    {ErrorCode::CONFIG_ERROR, "CONFIG_ERROR"},
    {ErrorCode::INVALID_CONFIGURATION, "INVALID_CONFIGURATION"},
    {ErrorCode::INVALID_PARAMETERS, "INVALID_PARAMETERS"},
    {ErrorCode::MISSING_REQUIRED_FIELD, "MISSING_REQUIRED_FIELD"},
    {ErrorCode::VALUE_OUT_OF_RANGE, "VALUE_OUT_OF_RANGE"}
};

// Detailed descriptions of error codes
const std::unordered_map<ErrorCode, std::string_view> error_descriptions{
    {ErrorCode::OK, "Operation completed successfully"},
    
    // System domain
    {ErrorCode::INTERNAL_ERROR, "An unexpected internal error occurred"},
    {ErrorCode::INITIALIZATION_FAILED, "System initialization failed"},
    {ErrorCode::INVALID_STATE, "Operation cannot be performed in current state"},
    {ErrorCode::OPERATION_TIMEOUT, "Operation timed out"},
    {ErrorCode::RESOURCE_EXHAUSTED, "System resources have been exhausted"},
    
    // Storage domain
    {ErrorCode::BUFFER_OVERFLOW, "Operation would exceed buffer capacity"},
    {ErrorCode::OUT_OF_MEMORY, "Insufficient memory to complete operation"},
    {ErrorCode::SEGMENT_NOT_FOUND, "The specified memory segment does not exist"},
    {ErrorCode::SEGMENT_ALREADY_EXISTS, "A segment with this ID already exists"},
    {ErrorCode::INVALID_SEGMENT, "The segment configuration is invalid"},
    {ErrorCode::ALLOCATION_FAILED, "Failed to allocate requested memory"},
    {ErrorCode::MEMORY_ACCESS_ERROR, "Error accessing memory region"},
    
    // Network domain
    {ErrorCode::NETWORK_ERROR, "Generic network communication error"},
    {ErrorCode::CONNECTION_FAILED, "Failed to establish network connection"},
    {ErrorCode::TRANSFER_FAILED, "Data transfer operation failed"},
    {ErrorCode::UCX_ERROR, "UCX communication library error"},
    {ErrorCode::INVALID_ADDRESS, "Invalid network address or endpoint"},
    {ErrorCode::REMOTE_ENDPOINT_ERROR, "Error communicating with remote endpoint"},
    {ErrorCode::RPC_FAILED, "Remote procedure call failed"},
    
    // Coordination domain
    {ErrorCode::ETCD_ERROR, "Generic etcd operation error"},
    {ErrorCode::ETCD_KEY_NOT_FOUND, "The requested etcd key does not exist"},
    {ErrorCode::ETCD_TRANSACTION_FAILED, "Etcd transaction failed to commit"},
    {ErrorCode::ETCD_LEASE_ERROR, "Error managing etcd lease"},
    {ErrorCode::ETCD_WATCH_ERROR, "Error watching etcd key changes"},
    {ErrorCode::LEADER_ELECTION_FAILED, "Failed to complete leader election"},
    {ErrorCode::SERVICE_REGISTRATION_FAILED, "Failed to register service in etcd"},
    
    // Data domain
    {ErrorCode::OBJECT_NOT_FOUND, "The requested object does not exist"},
    {ErrorCode::OBJECT_ALREADY_EXISTS, "An object with this key already exists"},
    {ErrorCode::INVALID_KEY, "The object key is invalid"},
    {ErrorCode::INVALID_WORKER, "The worker placement configuration is invalid"},
    {ErrorCode::WORKER_NOT_READY, "The worker is not ready for operations"},
    {ErrorCode::NO_COMPLETE_WORKER, "No complete worker placement found for object"},
    {ErrorCode::DATA_CORRUPTION, "Data corruption detected"},
    {ErrorCode::CHECKSUM_MISMATCH, "Data checksum verification failed"},
    
    // Client domain
    {ErrorCode::CLIENT_ERROR, "Generic client operation error"},
    {ErrorCode::CLIENT_NOT_FOUND, "The specified client does not exist"},
    {ErrorCode::CLIENT_ALREADY_EXISTS, "A client with this ID already exists"},
    {ErrorCode::CLIENT_DISCONNECTED, "Client connection lost"},
    {ErrorCode::SESSION_EXPIRED, "Client session has expired"},
    {ErrorCode::INVALID_CLIENT_STATE, "Invalid client state for operation"},
    
    // Config domain
    {ErrorCode::CONFIG_ERROR, "Generic configuration error"},
    {ErrorCode::INVALID_CONFIGURATION, "Configuration validation failed"},
    {ErrorCode::INVALID_PARAMETERS, "Invalid parameters provided"},
    {ErrorCode::MISSING_REQUIRED_FIELD, "Required configuration field missing"},
    {ErrorCode::VALUE_OUT_OF_RANGE, "Configuration value out of valid range"}
};

} // anonymous namespace

std::string_view to_string(ErrorCode code) noexcept {
    auto it = error_strings.find(code);
    return it != error_strings.end() ? it->second : "UNKNOWN_ERROR";
}

std::string_view get_error_description(ErrorCode code) noexcept {
    auto it = error_descriptions.find(code);
    return it != error_descriptions.end() ? it->second : "Unknown error occurred";
}

} // namespace error
} // namespace blackbird 