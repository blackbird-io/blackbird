#pragma once

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <mutex>

#include "blackbird/common/types.h"

namespace blackbird {

/**
 * @brief Callback function type for etcd watch operations
 * @param key The key that changed
 * @param value The new value (empty if deleted)
 * @param is_delete True if the key was deleted
 */
using EtcdWatchCallback = std::function<void(const std::string& key, const std::string& value, bool is_delete)>;

/**
 * @brief Simplified etcd client for service discovery and coordination
 * 
 * This class provides a minimal interface to etcd for:
 * - Service registration and discovery
 * - Leader election
 * - Configuration management
 * - Watch operations for cluster state changes
 */
class EtcdService {
public:
    /**
     * @brief Constructor
     * @param endpoints Comma-separated list of etcd endpoints (e.g., "127.0.0.1:2379,127.0.0.1:2380")
     */
    explicit EtcdService(const std::string& endpoints);
    
    /**
     * @brief Destructor - cleans up connections and watches
     */
    ~EtcdService();
    
    // Non-copyable, movable
    EtcdService(const EtcdService&) = delete;
    EtcdService& operator=(const EtcdService&) = delete;
    EtcdService(EtcdService&&) = default;
    EtcdService& operator=(EtcdService&&) = default;
    
    /**
     * @brief Initialize connection to etcd cluster
     * @return ErrorCode::OK on success
     */
    ErrorCode connect();
    
    /**
     * @brief Check if connected to etcd
     */
    bool is_connected() const noexcept { return connected_; }
    
    // === Basic Key-Value Operations ===
    
    /**
     * @brief Get a value by key
     * @param key The key to retrieve
     * @param value Output parameter for the value
     * @return ErrorCode::OK on success, ETCD_KEY_NOT_FOUND if key doesn't exist
     */
    ErrorCode get(const std::string& key, std::string& value);
    
    /**
     * @brief Put a key-value pair
     * @param key The key to store
     * @param value The value to store
     * @return ErrorCode::OK on success
     */
    ErrorCode put(const std::string& key, const std::string& value);
    
    /**
     * @brief Delete a key
     * @param key The key to delete
     * @return ErrorCode::OK on success
     */
    ErrorCode del(const std::string& key);
    
    /**
     * @brief Get all keys with a given prefix
     * @param prefix The key prefix to search for
     * @param keys Output parameter for the keys found
     * @param values Output parameter for the corresponding values
     * @return ErrorCode::OK on success
     */
    ErrorCode get_with_prefix(const std::string& prefix, 
                             std::vector<std::string>& keys,
                             std::vector<std::string>& values);
    
    // === Lease Operations ===
    
    /**
     * @brief Grant a new lease with specified TTL
     * @param ttl_seconds TTL in seconds
     * @param lease_id Output parameter for the lease ID
     * @return ErrorCode::OK on success
     */
    ErrorCode grant_lease(int64_t ttl_seconds, EtcdLeaseId& lease_id);
    
    /**
     * @brief Put a key-value pair with a lease
     * @param key The key to store
     * @param value The value to store
     * @param lease_id The lease ID to associate with the key
     * @return ErrorCode::OK on success
     */
    ErrorCode put_with_lease(const std::string& key, const std::string& value, EtcdLeaseId lease_id);
    
    /**
     * @brief Renew a lease (keep-alive)
     * @param lease_id The lease ID to renew
     * @return ErrorCode::OK on success
     */
    ErrorCode keep_alive(EtcdLeaseId lease_id);
    
    /**
     * @brief Revoke a lease
     * @param lease_id The lease ID to revoke
     * @return ErrorCode::OK on success
     */
    ErrorCode revoke_lease(EtcdLeaseId lease_id);
    
    // === Watch Operations ===
    
    /**
     * @brief Start watching a key for changes
     * @param key The key to watch
     * @param callback Function to call when the key changes
     * @return ErrorCode::OK on success
     */
    ErrorCode watch_key(const std::string& key, EtcdWatchCallback callback);
    
    /**
     * @brief Start watching keys with a prefix
     * @param prefix The key prefix to watch
     * @param callback Function to call when any matching key changes
     * @return ErrorCode::OK on success
     */
    ErrorCode watch_prefix(const std::string& prefix, EtcdWatchCallback callback);
    
    /**
     * @brief Stop watching a key
     * @param key The key to stop watching
     * @return ErrorCode::OK on success
     */
    ErrorCode unwatch_key(const std::string& key);
    
    /**
     * @brief Register a service with etcd
     * @param service_name The name of the service
     * @param service_id Unique ID for this service instance
     * @param endpoint The service endpoint (host:port)
     * @param ttl_seconds TTL for the registration
     * @param lease_id Output parameter for the lease ID (for keep-alive)
     * @return ErrorCode::OK on success
     */
    ErrorCode register_service(const std::string& service_name,
                              const std::string& service_id,
                              const std::string& endpoint,
                              int64_t ttl_seconds,
                              EtcdLeaseId& lease_id);
    
    /**
     * @brief Discover all instances of a service
     * @param service_name The name of the service to discover
     * @param service_ids Output parameter for service instance IDs
     * @param endpoints Output parameter for service endpoints
     * @return ErrorCode::OK on success
     */
    ErrorCode discover_service(const std::string& service_name,
                              std::vector<std::string>& service_ids,
                              std::vector<std::string>& endpoints);
    
    /**
     * @brief Unregister a service
     * @param service_name The name of the service
     * @param service_id The service instance ID
     * @return ErrorCode::OK on success
     */
    ErrorCode unregister_service(const std::string& service_name, const std::string& service_id);
    
    /**
     * @brief Attempt to become leader for a given election
     * @param election_name The name of the election
     * @param candidate_id Unique ID for this candidate
     * @param ttl_seconds TTL for the leadership
     * @param lease_id Output parameter for the lease ID
     * @return ErrorCode::OK if became leader, other error codes if not
     */
    ErrorCode campaign_leader(const std::string& election_name,
                             const std::string& candidate_id,
                             int64_t ttl_seconds,
                             EtcdLeaseId& lease_id);
    
    /**
     * @brief Check who is the current leader
     * @param election_name The name of the election
     * @param leader_id Output parameter for the current leader ID
     * @return ErrorCode::OK on success, ETCD_KEY_NOT_FOUND if no leader
     */
    ErrorCode get_leader(const std::string& election_name, std::string& leader_id);
    
    /**
     * @brief Resign from leadership
     * @param election_name The name of the election
     * @param candidate_id The candidate ID that is resigning
     * @return ErrorCode::OK on success
     */
    ErrorCode resign_leader(const std::string& election_name, const std::string& candidate_id);

private:
    std::string endpoints_;
    bool connected_{false};
    mutable std::mutex mutex_;
    
    struct Impl;
    std::unique_ptr<Impl> impl_;
    
    // Helper functions
    std::string make_service_key(const std::string& service_name, const std::string& service_id) const;
    std::string make_leader_key(const std::string& election_name) const;
};

}  // namespace blackbird 