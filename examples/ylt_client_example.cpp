/**
 * @file ylt_client_example.cpp
 * @brief Example demonstrating YLT struct_pack RPC client usage
 * 
 * This example shows how to use the high-performance YLT RPC client
 * with structured request/response types for maximum performance.
 */

#include <iostream>
#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "blackbird/types.h"

using namespace blackbird;
using namespace coro_rpc;

async_simple::coro::Lazy<void> run_examples() {
    try {
        // Create YLT RPC client
        coro_rpc_client client;
        
        // Connect to Keystone server
        auto ec = co_await client.connect("127.0.0.1", "9090");
        if (ec) {
            std::cerr << "Failed to connect to Keystone server" << std::endl;
            co_return;
        }
        
        std::cout << "Connected to Keystone server!" << std::endl;
        
        // Example 1: Check if object exists
        {
            ObjectExistsRequest request;
            request.key = "example-object";
            
            auto response = co_await client.call<ObjectExistsResponse>(
                "rpc_object_exists", request);
            if (response.has_value()) {
                const auto& resp = response.value();
                if (resp.error_code == ErrorCode::OK) {
                    std::cout << "Object '" << request.key << "' exists: " 
                              << (resp.exists ? "yes" : "no") << std::endl;
                } else {
                    std::cout << "Error checking object: " << static_cast<int>(resp.error_code) << std::endl;
                }
            } else {
                std::cout << "RPC call failed" << std::endl;
            }
        }
        
        // Example 2: Start a put operation
        {
            PutStartRequest request;
            request.key = "test-object";
            request.data_size = 1024;
            request.config.replication_factor = 2;
            request.config.max_workers_per_copy = 1;
            
            auto response = co_await client.call<PutStartResponse>(
                "rpc_put_start", request);
            if (response.has_value()) {
                const auto& resp = response.value();
                if (resp.error_code == ErrorCode::OK) {
                    std::cout << "Put operation started for '" << request.key 
                              << "' with " << resp.copies.size() << " copies" << std::endl;
                    
                    // Complete the put operation
                    PutCompleteRequest complete_req;
                    complete_req.key = request.key;
                    
                    auto complete_resp = co_await client.call<PutCompleteResponse>(
                        "rpc_put_complete", complete_req);
                    if (complete_resp.has_value() && complete_resp.value().error_code == ErrorCode::OK) {
                        std::cout << "Put operation completed successfully" << std::endl;
                    }
                } else {
                    std::cout << "Error starting put: " << static_cast<int>(resp.error_code) << std::endl;
                }
            }
        }
        
        // Example 3: Get worker placements
        {
            GetWorkersRequest request;
            request.key = "test-object";
            
            auto response = co_await client.call<GetWorkersResponse>(
                "rpc_get_workers", request);
            if (response.has_value()) {
                const auto& resp = response.value();
                if (resp.error_code == ErrorCode::OK) {
                    std::cout << "Found " << resp.copies.size() << " copies for object '" 
                              << request.key << "'" << std::endl;
                    for (size_t i = 0; i < resp.copies.size(); ++i) {
                        std::cout << "  Copy " << i << " has " << resp.copies[i].shards.size() 
                                  << " shards" << std::endl;
                    }
                } else {
                    std::cout << "Error getting workers: " << static_cast<int>(resp.error_code) << std::endl;
                }
            }
        }
        
        // Example 4: Get cluster statistics
        {
            GetClusterStatsRequest request;
            
            auto response = co_await client.call<GetClusterStatsResponse>(
                "rpc_get_cluster_stats", request);
            if (response.has_value()) {
                const auto& resp = response.value();
                if (resp.error_code == ErrorCode::OK) {
                    const auto& stats = resp.stats;
                    std::cout << "\nCluster Statistics:" << std::endl;
                    std::cout << "  Total workers: " << stats.total_workers << std::endl;
                    std::cout << "  Active workers: " << stats.active_workers << std::endl;
                    std::cout << "  Total objects: " << stats.total_objects << std::endl;
                    std::cout << "  Utilization: " << (stats.utilization * 100.0) << "%" << std::endl;
                } else {
                    std::cout << "Error getting cluster stats: " << static_cast<int>(resp.error_code) << std::endl;
                }
            }
        }
        
        // Example 5: Get view version
        {
            GetViewVersionRequest request;
            
            auto response = co_await client.call<GetViewVersionResponse>(
                "rpc_get_view_version", request);
            if (response.has_value()) {
                const auto& resp = response.value();
                std::cout << "Current view version: " << resp.view_version << std::endl;
            }
        }
        
        // Example 6: Remove object
        {
            RemoveObjectRequest request;
            request.key = "test-object";
            
            auto response = co_await client.call<RemoveObjectResponse>(
                "rpc_remove_object", request);
            if (response.has_value()) {
                const auto& resp = response.value();
                if (resp.error_code == ErrorCode::OK) {
                    std::cout << "Object '" << request.key << "' removed successfully" << std::endl;
                } else {
                    std::cout << "Error removing object: " << static_cast<int>(resp.error_code) << std::endl;
                }
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // Initialize logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_alsologtostderr = 1;
    FLAGS_colorlogtostderr = 1;
    FLAGS_v = 1;
    
    std::cout << "YLT struct_pack RPC Client Example" << std::endl;
    std::cout << "===================================" << std::endl;
    
    // Run the async examples
    async_simple::coro::syncAwait(run_examples());
    
    std::cout << "\nFeatures demonstrated:" << std::endl;
    std::cout << "- YLT coro_rpc client with async/await" << std::endl;
    std::cout << "- struct_pack binary serialization (~10x faster than protobuf)" << std::endl;
    std::cout << "- Type-safe request/response structures" << std::endl;
    std::cout << "- Complete object lifecycle management" << std::endl;
    std::cout << "- Cluster monitoring and statistics" << std::endl;
    
    return 0;
} 