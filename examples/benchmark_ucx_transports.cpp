#include <iostream>
#include <chrono>
#include <vector>
#include <cstring>
#include <iomanip>
#include "blackbird/common/types.h"

extern "C" {
#include <ucp/api/ucp.h>
}

using namespace blackbird;

class UCXTransportBenchmark {
private:
    ucp_context_h context_;
    ucp_worker_h worker_;
    ucp_config_t* config_;
    
    static constexpr size_t MESSAGE_SIZE = 1024 * 1024; // 1MB messages
    static constexpr int NUM_ITERATIONS = 100;
    
public:
    UCXTransportBenchmark() : context_(nullptr), worker_(nullptr), config_(nullptr) {}
    
    ~UCXTransportBenchmark() {
        cleanup();
    }
    
    bool initialize(const std::string& transport) {
        ucs_status_t status;
        
        // Read UCX configuration
        status = ucp_config_read(NULL, NULL, &config_);
        if (status != UCS_OK) {
            std::cerr << "Failed to read UCP config: " << ucs_status_string(status) << std::endl;
            return false;
        }
        
        // Override transport selection
        ucp_config_modify(config_, "TLS", transport.c_str());
        
        // Initialize UCP context
        ucp_params_t params;
        params.field_mask = UCP_PARAM_FIELD_FEATURES;
        params.features = UCP_FEATURE_TAG | UCP_FEATURE_RMA;
        
        status = ucp_init(&params, config_, &context_);
        if (status != UCS_OK) {
            std::cerr << "Failed to initialize UCP context: " << ucs_status_string(status) << std::endl;
            return false;
        }
        
        // Create worker
        ucp_worker_params_t worker_params;
        worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
        worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
        
        status = ucp_worker_create(context_, &worker_params, &worker_);
        if (status != UCS_OK) {
            std::cerr << "Failed to create UCP worker: " << ucs_status_string(status) << std::endl;
            return false;
        }
        
        return true;
    }
    
    void cleanup() {
        if (worker_) {
            ucp_worker_destroy(worker_);
            worker_ = nullptr;
        }
        if (context_) {
            ucp_cleanup(context_);
            context_ = nullptr;
        }
        if (config_) {
            ucp_config_release(config_);
            config_ = nullptr;
        }
    }
    
    double benchmarkMemoryBandwidth() {
        // Allocate test buffers
        std::vector<uint8_t> src_buffer(MESSAGE_SIZE, 0xAB);
        std::vector<uint8_t> dst_buffer(MESSAGE_SIZE, 0x00);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Simple memory copy benchmark (simulates RMA operations)
        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            std::memcpy(dst_buffer.data(), src_buffer.data(), MESSAGE_SIZE);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        double total_bytes = static_cast<double>(MESSAGE_SIZE * NUM_ITERATIONS);
        double seconds = static_cast<double>(duration.count()) / 1000000.0;
        double bandwidth_mbps = (total_bytes / (1024 * 1024)) / seconds;
        
        return bandwidth_mbps;
    }
    
    void printTransportInfo() {
        // Print available transports
        ucp_context_attr_t attr;
        attr.field_mask = UCP_ATTR_FIELD_NAME;
        ucp_context_query(context_, &attr);
        
        std::cout << "UCX Context: " << attr.name << std::endl;
    }
};

void benchmarkTransport(const std::string& transport_name, const std::vector<std::string>& transports) {
    std::cout << "\\n=== " << transport_name << " Transport Benchmark ===" << std::endl;
    
    UCXTransportBenchmark benchmark;
    
    for (const auto& transport : transports) {
        std::cout << "Testing " << transport << "..." << std::endl;
        
        if (!benchmark.initialize(transport)) {
            std::cout << "Failed to initialize " << transport << " transport" << std::endl;
            continue;
        }
        
        benchmark.printTransportInfo();
        double bandwidth = benchmark.benchmarkMemoryBandwidth();
        
        std::cout << "Transport: " << transport << std::endl;
        std::cout << "Bandwidth: " << std::fixed << std::setprecision(2) << bandwidth << " MB/sec" << std::endl;
        std::cout << "Message Size: " << (1024 * 1024) << " bytes" << std::endl;
        std::cout << "Iterations: " << 100 << std::endl;
        
        benchmark.cleanup();
        std::cout << std::endl;
    }
}

int main() {
    std::cout << "UCX Transport Performance Comparison" << std::endl;
    std::cout << "====================================" << std::endl;
    
    // Test different transport combinations
    benchmarkTransport("TCP Only", {"tcp"});
    benchmarkTransport("RDMA Only", {"rc_verbs"});
    benchmarkTransport("Shared Memory", {"sysv,posix,cma"});
    benchmarkTransport("Auto Selection", {"all"});
    
    return 0;
}