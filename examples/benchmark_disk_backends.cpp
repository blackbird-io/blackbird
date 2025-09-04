// Performance benchmarking tool for disk storage backends using io_uring for async I/O
#include "blackbird/worker/storage/iouring_disk_backend.h"
#include <chrono>
#include <iostream>
#include <iomanip>
#include <vector>
#include <filesystem>
#include <memory>

using namespace blackbird;
using namespace std::chrono;

struct BenchmarkResult {
    std::string name;
    double avg_reserve_time_ms;
    double avg_commit_time_ms; 
    double avg_free_time_ms;
    double total_time_ms;
    size_t num_operations;
    bool success;
};

BenchmarkResult benchmark_backend(std::unique_ptr<StorageBackend> backend, 
                                  const std::string& name, 
                                  size_t num_operations = 100,
                                  size_t shard_size = 4096) {
    BenchmarkResult result;
    result.name = name;
    result.num_operations = num_operations;
    result.success = false;
    
    // Initialize backend
    if (backend->initialize() != ErrorCode::OK) {
        std::cerr << "Failed to initialize " << name << std::endl;
        return result;
    }
    
    std::vector<ReservationToken> tokens;
    tokens.reserve(num_operations);
    
    auto start_total = high_resolution_clock::now();
    
    // Benchmark reserve operations
    auto start_reserve = high_resolution_clock::now();
    for (size_t i = 0; i < num_operations; ++i) {
        auto reserve_result = backend->reserve_shard(shard_size);
        if (!std::holds_alternative<ReservationToken>(reserve_result)) {
            std::cerr << "Reserve failed at operation " << i << std::endl;
            return result;
        }
        tokens.push_back(std::get<ReservationToken>(reserve_result));
    }
    auto end_reserve = high_resolution_clock::now();
    
    // Benchmark commit operations  
    auto start_commit = high_resolution_clock::now();
    for (const auto& token : tokens) {
        auto commit_result = backend->commit_shard(token);
        if (commit_result != ErrorCode::OK) {
            std::cerr << "Commit failed for token " << token.token_id << std::endl;
            return result;
        }
    }
    auto end_commit = high_resolution_clock::now();
    
    // Benchmark free operations
    auto start_free = high_resolution_clock::now();
    for (const auto& token : tokens) {
        auto free_result = backend->free_shard(token.remote_addr, token.size);
        if (free_result != ErrorCode::OK) {
            std::cerr << "Free failed for token " << token.token_id << std::endl;
            return result;
        }
    }
    auto end_free = high_resolution_clock::now();
    
    auto end_total = high_resolution_clock::now();
    
    // Calculate timing results
    result.avg_reserve_time_ms = duration_cast<microseconds>(end_reserve - start_reserve).count() / 1000.0 / num_operations;
    result.avg_commit_time_ms = duration_cast<microseconds>(end_commit - start_commit).count() / 1000.0 / num_operations;
    result.avg_free_time_ms = duration_cast<microseconds>(end_free - start_free).count() / 1000.0 / num_operations;
    result.total_time_ms = duration_cast<microseconds>(end_total - start_total).count() / 1000.0;
    result.success = true;
    
    backend->shutdown();
    return result;
}

void print_results(const BenchmarkResult& result) {
    std::cout << "\n=== " << result.name << " ===\n";
    if (!result.success) {
        std::cout << "FAILED\n";
        return;
    }
    
    std::cout << "Operations: " << result.num_operations << "\n";
    std::cout << "Avg Reserve Time: " << std::fixed << std::setprecision(3) << result.avg_reserve_time_ms << " ms\n";
    std::cout << "Avg Commit Time:  " << std::fixed << std::setprecision(3) << result.avg_commit_time_ms << " ms\n";
    std::cout << "Avg Free Time:    " << std::fixed << std::setprecision(3) << result.avg_free_time_ms << " ms\n";
    std::cout << "Total Time:       " << std::fixed << std::setprecision(3) << result.total_time_ms << " ms\n";
    std::cout << "Throughput:       " << std::fixed << std::setprecision(1) << (result.num_operations * 1000.0 / result.total_time_ms) << " ops/sec\n";
}

int main() {
    const std::string test_dir = "/tmp/disk_backend_bench";
    const size_t num_operations = 50;  // Reduced for faster testing
    const size_t shard_size = 4096;    // 4KB shards
    const uint64_t capacity = 100 * 1024 * 1024; // 100MB
    
    // Clean up test directory
    std::error_code ec;
    std::filesystem::remove_all(test_dir, ec);
    std::filesystem::create_directories(test_dir);
    
    std::cout << "Disk Storage Backend Benchmark\n";
    std::cout << "==============================\n";
    std::cout << "Operations per test: " << num_operations << "\n";
    std::cout << "Shard size: " << shard_size << " bytes\n";
    std::cout << "Test directory: " << test_dir << "\n";
    
    // Benchmark IoUring DiskBackend (only backend now)
    try {
        auto iouring_backend = std::make_unique<IoUringDiskBackend>(capacity, StorageClass::SSD, test_dir + "/iouring");
        auto iouring_result = benchmark_backend(std::move(iouring_backend), "High-Performance IoUring DiskBackend", num_operations, shard_size);
        print_results(iouring_result);
    } catch (const std::exception& e) {
        std::cerr << "IoUring DiskBackend benchmark failed: " << e.what() << std::endl;
    }
    
    // Clean up test directory
    std::filesystem::remove_all(test_dir, ec);
    
    return 0;
}