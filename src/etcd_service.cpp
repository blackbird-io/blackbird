#include "blackbird/etcd_service.h"

#include <glog/logging.h>

#include <etcd/SyncClient.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Watcher.hpp>

namespace blackbird {

// Implementation details
struct EtcdService::Impl {
    std::unique_ptr<etcd::SyncClient> client;
    std::unordered_map<std::string, std::unique_ptr<etcd::Watcher>> watchers;
    std::unordered_map<EtcdLeaseId, std::unique_ptr<etcd::KeepAlive>> keep_alives;
    std::vector<std::string> endpoint_list;
};

EtcdService::EtcdService(const std::string& endpoints) 
    : endpoints_(endpoints), impl_(std::make_unique<Impl>()) {
    // Parse comma-separated endpoints
    std::string delimiter = ",";
    size_t pos = 0;
    std::string token;
    std::string endpoints_copy = endpoints;
    
    while ((pos = endpoints_copy.find(delimiter)) != std::string::npos) {
        token = endpoints_copy.substr(0, pos);
        // Trim whitespace
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (!token.empty()) {
            impl_->endpoint_list.push_back(token);
        }
        endpoints_copy.erase(0, pos + delimiter.length());
    }
    
    // Don't forget the last endpoint
    endpoints_copy.erase(0, endpoints_copy.find_first_not_of(" \t"));
    endpoints_copy.erase(endpoints_copy.find_last_not_of(" \t") + 1);
    if (!endpoints_copy.empty()) {
        impl_->endpoint_list.push_back(endpoints_copy);
    }
    
    if (impl_->endpoint_list.empty()) {
        LOG(WARNING) << "No valid etcd endpoints provided: " << endpoints;
    }
}

EtcdService::~EtcdService() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Cleanup watches and keep-alives first
    impl_->watchers.clear();
    impl_->keep_alives.clear();
    
    // Reset connection state
    connected_ = false;
    
    // Client will be automatically cleaned up when impl_ is destroyed
}

ErrorCode EtcdService::connect() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (impl_->endpoint_list.empty()) {
        LOG(ERROR) << "No etcd endpoints configured";
        return ErrorCode::ETCD_ERROR;
    }
    
    try {
        // Create etcd sync client with the first endpoint
        impl_->client = std::make_unique<etcd::SyncClient>(impl_->endpoint_list.front());
        
        // Test connection with a simple operation
        auto response = impl_->client->head();
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to connect to etcd: " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        connected_ = true;
        LOG(INFO) << "Successfully connected to etcd cluster";
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while connecting to etcd: " << e.what();
        return ErrorCode::ETCD_ERROR;
    }

}

ErrorCode EtcdService::get(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        auto response = impl_->client->get(key);
        if (!response.is_ok()) {
            LOG(WARNING) << "Failed to get key '" << key << "': " << response.error_message();
            return ErrorCode::ETCD_KEY_NOT_FOUND;
        }
        
        std::string v = response.value().as_string();
        if (v.empty()) {
            return ErrorCode::ETCD_KEY_NOT_FOUND;
        }
        
        value = std::move(v);
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while getting key '" << key << "': " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        auto response = impl_->client->set(key, value);
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to put key '" << key << "': " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while putting key '" << key << "': " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        auto response = impl_->client->rm(key);
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to delete key '" << key << "': " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while deleting key '" << key << "': " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::get_with_prefix(const std::string& prefix, 
                                     std::vector<std::string>& keys,
                                     std::vector<std::string>& values) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    
    keys.clear();
    values.clear();
    
    try {
        auto response = impl_->client->ls(prefix);
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to list keys with prefix '" << prefix << "': " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        for (size_t i = 0; i < response.keys().size(); ++i) {
            keys.push_back(response.key(i));
            values.push_back(response.value(i).as_string());
        }
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while listing keys with prefix '" << prefix << "': " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::grant_lease(int64_t ttl_seconds, EtcdLeaseId& lease_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        auto response = impl_->client->leasegrant(ttl_seconds);
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to grant lease with TTL " << ttl_seconds << ": " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        lease_id = response.value().lease();
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while granting lease: " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::put_with_lease(const std::string& key, const std::string& value, EtcdLeaseId lease_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        auto response = impl_->client->set(key, value, lease_id);
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to put key '" << key << "' with lease " << lease_id << ": " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while putting key '" << key << "' with lease: " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::keep_alive(EtcdLeaseId lease_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        // Create a keep-alive if it doesn't exist
        if (impl_->keep_alives.find(lease_id) == impl_->keep_alives.end()) {
            impl_->keep_alives[lease_id] = std::make_unique<etcd::KeepAlive>(*impl_->client, lease_id);
        }
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while setting up keep-alive for lease " << lease_id << ": " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::revoke_lease(EtcdLeaseId lease_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    try {
        // Stop keep-alive if it exists
        auto it = impl_->keep_alives.find(lease_id);
        if (it != impl_->keep_alives.end()) {
            impl_->keep_alives.erase(it);
        }
        
        auto response = impl_->client->leaserevoke(lease_id);
        if (!response.is_ok()) {
            LOG(ERROR) << "Failed to revoke lease " << lease_id << ": " << response.error_message();
            return ErrorCode::ETCD_ERROR;
        }
        
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while revoking lease " << lease_id << ": " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::watch_key(const std::string& key, EtcdWatchCallback callback) {
    LOG(WARNING) << "watch_key not yet implemented";
    return ErrorCode::ETCD_ERROR;
}

ErrorCode EtcdService::watch_prefix(const std::string& prefix, EtcdWatchCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!connected_) return ErrorCode::ETCD_ERROR;
    
    try {
        // Create a watcher for the prefix with callback and recursive=true
        auto watcher_callback = [callback, prefix](etcd::Response response) {
            if (response.is_ok()) {
                for (size_t i = 0; i < response.events().size(); ++i) {
                    const auto& event = response.events()[i];
                    std::string key = event.kv().key();
                    std::string value = event.kv().as_string(); // Use as_string() method
                    bool is_delete = (event.event_type() == etcd::Event::EventType::DELETE_); // Use DELETE_ not DELETE
                    
                    callback(key, value, is_delete);
                }
            } else {
                LOG(ERROR) << "Watch error for prefix '" << prefix << "': " << response.error_message();
            }
        };
        
        auto watcher = std::make_unique<etcd::Watcher>(*impl_->client, prefix, watcher_callback, true); // true for recursive/prefix watch
        
        // Store the watcher to keep it alive
        impl_->watchers[prefix] = std::move(watcher);
        
        LOG(INFO) << "Started watching prefix: " << prefix;
        return ErrorCode::OK;
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception while setting up watch for prefix '" << prefix << "': " << e.what();
        return ErrorCode::ETCD_ERROR;
    }
}

ErrorCode EtcdService::unwatch_key(const std::string& key) {
    LOG(WARNING) << "unwatch_key not yet implemented";
    return ErrorCode::OK;
}

// Service discovery helpers
ErrorCode EtcdService::register_service(const std::string& service_name,
                                      const std::string& service_id,
                                      const std::string& endpoint,
                                      int64_t ttl_seconds,
                                      EtcdLeaseId& lease_id) {
    auto err = grant_lease(ttl_seconds, lease_id);
    if (err != ErrorCode::OK) {
        return err;
    }
    
    std::string key = make_service_key(service_name, service_id);
    return put_with_lease(key, endpoint, lease_id);
}

ErrorCode EtcdService::discover_service(const std::string& service_name,
                                      std::vector<std::string>& service_ids,
                                      std::vector<std::string>& endpoints) {
    std::string prefix = "/blackbird/services/" + service_name + "/";
    
    std::vector<std::string> keys;
    auto err = get_with_prefix(prefix, keys, endpoints);
    if (err != ErrorCode::OK) {
        return err;
    }
    
    service_ids.clear();
    for (const auto& key : keys) {
        // Extract service ID from key
        if (key.size() > prefix.size()) {
            service_ids.push_back(key.substr(prefix.size()));
        }
    }
    
    return ErrorCode::OK;
}

ErrorCode EtcdService::unregister_service(const std::string& service_name, const std::string& service_id) {
    std::string key = make_service_key(service_name, service_id);
    return del(key);
}

// Leader election helpers
ErrorCode EtcdService::campaign_leader(const std::string& election_name,
                                     const std::string& candidate_id,
                                     int64_t ttl_seconds,
                                     EtcdLeaseId& lease_id) {
    LOG(WARNING) << "campaign_leader not yet implemented";
    return ErrorCode::ETCD_ERROR;
}

ErrorCode EtcdService::get_leader(const std::string& election_name, std::string& leader_id) {
    std::string key = make_leader_key(election_name);
    return get(key, leader_id);
}

ErrorCode EtcdService::resign_leader(const std::string& election_name, const std::string& candidate_id) {
    std::string key = make_leader_key(election_name);
    return del(key);
}

// Helper methods
std::string EtcdService::make_service_key(const std::string& service_name, const std::string& service_id) const {
    return "/blackbird/services/" + service_name + "/" + service_id;
}

std::string EtcdService::make_leader_key(const std::string& election_name) const {
    return "/blackbird/elections/" + election_name + "/leader";
}

} // namespace blackbird 