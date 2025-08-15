#include "blackbird/common/types.h"

#include <random>
#include <thread>
#include <yaml-cpp/yaml.h>
#include <glog/logging.h>

namespace blackbird {

namespace {
// Thread-local random number generator for UUID generation
thread_local std::random_device rd;
thread_local std::mt19937_64 gen(rd());
} // anonymous namespace

UUID generate_uuid() {
    return {gen(), gen()};
}

KeystoneConfig KeystoneConfig::from_yaml(const std::string& file_path) {
    KeystoneConfig config;
    
    try {
        YAML::Node root = YAML::LoadFile(file_path);
        
        if (!root || !root.IsMap()) {
            LOG(ERROR) << "Invalid YAML root in " << file_path;
            throw std::runtime_error("Invalid YAML configuration file: " + file_path);
        }
        
        // Keystone section
        if (root["keystone"]) {
            auto k = root["keystone"];
            
            if (k["cluster_id"]) config.cluster_id = k["cluster_id"].as<std::string>();
            if (k["service_id"]) config.service_id = k["service_id"].as<std::string>();
            if (k["listen_address"]) config.listen_address = k["listen_address"].as<std::string>();
            if (k["http_metrics_port"]) config.http_metrics_port = k["http_metrics_port"].as<std::string>();
            
            // Handle etcd_endpoints (array or string)
            if (k["etcd_endpoints"]) {
                const auto& eps = k["etcd_endpoints"];
                std::string combined;
                if (eps.IsSequence()) {
                    for (size_t i = 0; i < eps.size(); ++i) {
                        if (i > 0) combined += ",";
                        combined += eps[i].as<std::string>();
                    }
                } else if (eps.IsScalar()) {
                    combined = eps.as<std::string>();
                }
                if (!combined.empty()) config.etcd_endpoints = combined;
            }
            
            // Boolean settings
            if (k["enable_gc"]) config.enable_gc = k["enable_gc"].as<bool>();
            if (k["enable_ha"]) config.enable_ha = k["enable_ha"].as<bool>();
            
            // Performance settings
            if (k["eviction_ratio"]) config.eviction_ratio = k["eviction_ratio"].as<double>();
            if (k["high_watermark"]) config.high_watermark = k["high_watermark"].as<double>();
            
            // TTL and timing settings
            if (k["client_ttl_sec"]) config.client_ttl_sec = k["client_ttl_sec"].as<int64_t>();
            if (k["worker_heartbeat_ttl_sec"]) config.worker_heartbeat_ttl_sec = k["worker_heartbeat_ttl_sec"].as<int64_t>();
            if (k["service_registration_ttl_sec"]) config.service_registration_ttl_sec = k["service_registration_ttl_sec"].as<int64_t>();
            if (k["service_refresh_interval_sec"]) config.service_refresh_interval_sec = k["service_refresh_interval_sec"].as<int64_t>();
            if (k["gc_interval_sec"]) config.gc_interval_sec = k["gc_interval_sec"].as<int64_t>();
            if (k["health_check_interval_sec"]) config.health_check_interval_sec = k["health_check_interval_sec"].as<int64_t>();
            
            // Object management
            if (k["max_replicas"]) config.max_replicas = k["max_replicas"].as<int32_t>();
            if (k["default_replicas"]) config.default_replicas = k["default_replicas"].as<int32_t>();
        }
        
        // Validate required configuration
        if (config.cluster_id.empty()) {
            throw std::runtime_error("cluster_id is required in keystone configuration");
        }
        if (config.etcd_endpoints.empty()) {
            throw std::runtime_error("etcd_endpoints is required in keystone configuration");
        }
        if (config.listen_address.empty()) {
            throw std::runtime_error("listen_address is required in keystone configuration");
        }
        
        LOG(INFO) << "Loaded Keystone configuration from " << file_path;
        LOG(INFO) << "  cluster_id: " << config.cluster_id;
        LOG(INFO) << "  listen_address: " << config.listen_address;
        LOG(INFO) << "  etcd_endpoints: " << config.etcd_endpoints;
        
    } catch (const YAML::Exception& e) {
        LOG(ERROR) << "Error parsing YAML config: " << e.what();
        throw std::runtime_error("Failed to parse YAML configuration file: " + file_path + " - " + e.what());
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error loading config file " << file_path << ": " << e.what();
        throw std::runtime_error("Failed to load configuration file: " + file_path + " - " + e.what());
    }
    
    return config;
}

} // namespace blackbird 