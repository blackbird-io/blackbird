#include <gtest/gtest.h>
#include "blackbird/worker/storage/iouring_disk_backend.h"
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

namespace blackbird {

class IoUringDiskBackendTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for testing
        test_dir_ = std::filesystem::temp_directory_path() / "iouring_test";
        std::filesystem::create_directories(test_dir_);
        
        // Create backend with 100MB capacity
        backend_ = std::make_unique<IoUringDiskBackend>(
            100 * 1024 * 1024,  // 100MB capacity
            StorageClass::NVME,
            test_dir_.string(),
            64  // Small queue depth for testing
        );
        
        // Initialize the backend
        auto result = backend_->initialize();
        ASSERT_EQ(result, ErrorCode::OK) << "Failed to initialize IoUringDiskBackend";
    }
    
    void TearDown() override {
        if (backend_) {
            backend_->shutdown();
            backend_.reset();
        }
        
        // Clean up test directory
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }
    
    std::filesystem::path test_dir_;
    std::unique_ptr<IoUringDiskBackend> backend_;
};

TEST_F(IoUringDiskBackendTest, Initialization) {
    EXPECT_EQ(backend_->get_storage_class(), StorageClass::NVME);
    EXPECT_EQ(backend_->get_total_capacity(), 100 * 1024 * 1024);
    EXPECT_EQ(backend_->get_used_capacity(), 0);
    EXPECT_EQ(backend_->get_available_capacity(), 100 * 1024 * 1024);
    EXPECT_NE(backend_->get_base_address(), 0);
    EXPECT_NE(backend_->get_rkey(), 0);
}

TEST_F(IoUringDiskBackendTest, StorageClassSupport) {
    // Test NVME
    auto nvme_backend = std::make_unique<IoUringDiskBackend>(
        1024 * 1024, StorageClass::NVME, test_dir_.string());
    EXPECT_EQ(nvme_backend->get_storage_class(), StorageClass::NVME);
    
    // Test SSD
    auto ssd_backend = std::make_unique<IoUringDiskBackend>(
        1024 * 1024, StorageClass::SSD, test_dir_.string());
    EXPECT_EQ(ssd_backend->get_storage_class(), StorageClass::SSD);
    
    // Test HDD
    auto hdd_backend = std::make_unique<IoUringDiskBackend>(
        1024 * 1024, StorageClass::HDD, test_dir_.string());
    EXPECT_EQ(hdd_backend->get_storage_class(), StorageClass::HDD);
    
    // Test invalid storage class (should throw)
    EXPECT_THROW(
        IoUringDiskBackend(1024 * 1024, StorageClass::RAM_CPU, test_dir_.string()),
        std::invalid_argument
    );
}

TEST_F(IoUringDiskBackendTest, ReserveAndCommitShard) {
    const uint64_t shard_size = 4096;  // 4KB
    
    // Reserve a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    
    auto token = std::get<ReservationToken>(result);
    EXPECT_EQ(token.size, shard_size);
    EXPECT_NE(token.remote_addr, 0);
    EXPECT_EQ(token.rkey, backend_->get_rkey());
    EXPECT_FALSE(token.token_id.empty());
    
    // Check capacity updates
    EXPECT_EQ(backend_->get_used_capacity(), shard_size);
    EXPECT_EQ(backend_->get_available_capacity(), 100 * 1024 * 1024 - shard_size);
    
    // Commit the shard
    auto commit_result = backend_->commit_shard(token);
    EXPECT_EQ(commit_result, ErrorCode::OK);
    
    // Verify stats
    auto stats = backend_->get_stats();
    EXPECT_EQ(stats.num_committed_shards, 1);
    EXPECT_EQ(stats.used_capacity, shard_size);
}

TEST_F(IoUringDiskBackendTest, OutOfSpace) {
    const uint64_t large_size = 200 * 1024 * 1024;  // 200MB (larger than capacity)
    
    auto result = backend_->reserve_shard(large_size);
    EXPECT_TRUE(std::holds_alternative<ErrorCode>(result));
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::OUT_OF_MEMORY);
    
    // Capacity should remain unchanged
    EXPECT_EQ(backend_->get_used_capacity(), 0);
    EXPECT_EQ(backend_->get_available_capacity(), 100 * 1024 * 1024);
}

TEST_F(IoUringDiskBackendTest, InvalidParameters) {
    // Test zero size reservation
    auto result = backend_->reserve_shard(0);
    EXPECT_TRUE(std::holds_alternative<ErrorCode>(result));
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INVALID_PARAMETERS);
}

TEST_F(IoUringDiskBackendTest, NonExistentTokenOperations) {
    ReservationToken fake_token;
    fake_token.token_id = "non_existent_token";
    fake_token.remote_addr = 0x12345678;
    fake_token.size = 4096;
    fake_token.rkey = backend_->get_rkey();
    fake_token.expires_at = std::chrono::system_clock::now() + std::chrono::minutes(10);
    
    // Test commit with non-existent token
    auto commit_result = backend_->commit_shard(fake_token);
    EXPECT_EQ(commit_result, ErrorCode::OBJECT_NOT_FOUND);
    
    // Test abort with non-existent token
    auto abort_result = backend_->abort_shard(fake_token);
    EXPECT_EQ(abort_result, ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(IoUringDiskBackendTest, AbortShard) {
    const uint64_t shard_size = 4096;
    
    // Reserve a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    // Abort the shard
    auto abort_result = backend_->abort_shard(token);
    EXPECT_EQ(abort_result, ErrorCode::OK);
    
    // Check that capacity is restored
    EXPECT_EQ(backend_->get_used_capacity(), 0);
    EXPECT_EQ(backend_->get_available_capacity(), 100 * 1024 * 1024);
    
    // Verify that committing the aborted token fails
    auto commit_result = backend_->commit_shard(token);
    EXPECT_EQ(commit_result, ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(IoUringDiskBackendTest, ExpiredToken) {
    const uint64_t shard_size = 4096;
    
    // Reserve a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    // Manually expire the token
    token.expires_at = std::chrono::system_clock::now() - std::chrono::minutes(1);
    
    // Try to commit expired token
    auto commit_result = backend_->commit_shard(token);
    EXPECT_EQ(commit_result, ErrorCode::OPERATION_TIMEOUT);
    
    // Check that capacity is cleaned up
    EXPECT_EQ(backend_->get_used_capacity(), 0);
}

TEST_F(IoUringDiskBackendTest, FreeShard) {
    const uint64_t shard_size = 4096;
    
    // Reserve and commit a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    auto commit_result = backend_->commit_shard(token);
    ASSERT_EQ(commit_result, ErrorCode::OK);
    
    // Free the shard
    auto free_result = backend_->free_shard(token.remote_addr, shard_size);
    EXPECT_EQ(free_result, ErrorCode::OK);
    
    // Check that capacity is restored
    EXPECT_EQ(backend_->get_used_capacity(), 0);
    EXPECT_EQ(backend_->get_available_capacity(), 100 * 1024 * 1024);
    
    // Verify stats
    auto stats = backend_->get_stats();
    EXPECT_EQ(stats.num_committed_shards, 0);
}

TEST_F(IoUringDiskBackendTest, FreeSizeMismatch) {
    const uint64_t shard_size = 4096;
    
    // Reserve and commit a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    auto commit_result = backend_->commit_shard(token);
    ASSERT_EQ(commit_result, ErrorCode::OK);
    
    // Try to free with wrong size
    auto free_result = backend_->free_shard(token.remote_addr, shard_size * 2);
    EXPECT_EQ(free_result, ErrorCode::INVALID_PARAMETERS);
    
    // Original shard should still be there
    EXPECT_EQ(backend_->get_used_capacity(), shard_size);
}

TEST_F(IoUringDiskBackendTest, FreeNonExistentShard) {
    auto free_result = backend_->free_shard(0x12345678, 4096);
    EXPECT_EQ(free_result, ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(IoUringDiskBackendTest, FileSystemOperations) {
    const uint64_t shard_size = 1024;
    
    // Reserve and commit a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    auto commit_result = backend_->commit_shard(token);
    ASSERT_EQ(commit_result, ErrorCode::OK);
    
    // Give the async operation time to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Check that a file was created in the storage directory
    auto storage_dir = test_dir_ / "blackbird_storage";
    EXPECT_TRUE(std::filesystem::exists(storage_dir));
    
    // Count files in storage directory
    int file_count = 0;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(storage_dir, ec)) {
        if (entry.is_regular_file(ec)) {
            file_count++;
            // Check that file has expected size (may be sparse)
            auto file_size = entry.file_size(ec);
            EXPECT_GE(file_size, 0);  // File should exist with some size
        }
    }
    EXPECT_GT(file_count, 0);  // At least one file should exist
}

TEST_F(IoUringDiskBackendTest, MultipleShards) {
    const uint64_t shard_size = 4096;
    const int num_shards = 5;
    std::vector<ReservationToken> tokens;
    
    // Reserve multiple shards
    for (int i = 0; i < num_shards; i++) {
        auto result = backend_->reserve_shard(shard_size);
        ASSERT_TRUE(std::holds_alternative<ReservationToken>(result)) << "Failed to reserve shard " << i;
        tokens.push_back(std::get<ReservationToken>(result));
    }
    
    // Check capacity
    EXPECT_EQ(backend_->get_used_capacity(), num_shards * shard_size);
    
    // Commit all shards
    for (const auto& token : tokens) {
        auto commit_result = backend_->commit_shard(token);
        EXPECT_EQ(commit_result, ErrorCode::OK);
    }
    
    // Verify stats
    auto stats = backend_->get_stats();
    EXPECT_EQ(stats.num_committed_shards, num_shards);
    EXPECT_EQ(stats.used_capacity, num_shards * shard_size);
    
    // Free all shards
    for (const auto& token : tokens) {
        auto free_result = backend_->free_shard(token.remote_addr, token.size);
        EXPECT_EQ(free_result, ErrorCode::OK);
    }
    
    // Check final state
    EXPECT_EQ(backend_->get_used_capacity(), 0);
    auto final_stats = backend_->get_stats();
    EXPECT_EQ(final_stats.num_committed_shards, 0);
}

TEST_F(IoUringDiskBackendTest, InvalidDirectory) {
    // Test with non-existent directory path (without permission to create)
    auto invalid_backend = std::make_unique<IoUringDiskBackend>(
        1024 * 1024, StorageClass::SSD, "/root/no_permission");
    
    // Initialize should fail
    auto result = invalid_backend->initialize();
    EXPECT_NE(result, ErrorCode::OK);
}

TEST_F(IoUringDiskBackendTest, StatsCalculation) {
    const uint64_t shard_size = 4096;
    
    // Initial stats
    auto stats = backend_->get_stats();
    EXPECT_EQ(stats.total_capacity, 100 * 1024 * 1024);
    EXPECT_EQ(stats.used_capacity, 0);
    EXPECT_EQ(stats.available_capacity, 100 * 1024 * 1024);
    EXPECT_FLOAT_EQ(stats.utilization, 0.0);
    EXPECT_EQ(stats.num_reservations, 0);
    EXPECT_EQ(stats.num_committed_shards, 0);
    
    // Reserve a shard
    auto result = backend_->reserve_shard(shard_size);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    stats = backend_->get_stats();
    EXPECT_EQ(stats.used_capacity, shard_size);
    EXPECT_EQ(stats.available_capacity, 100 * 1024 * 1024 - shard_size);
    EXPECT_FLOAT_EQ(stats.utilization, static_cast<double>(shard_size) / (100 * 1024 * 1024));
    EXPECT_EQ(stats.num_reservations, 1);
    EXPECT_EQ(stats.num_committed_shards, 0);
    
    // Commit the shard
    auto commit_result = backend_->commit_shard(token);
    ASSERT_EQ(commit_result, ErrorCode::OK);
    
    stats = backend_->get_stats();
    EXPECT_EQ(stats.num_reservations, 1);  // Still reserved but also committed
    EXPECT_EQ(stats.num_committed_shards, 1);
    
    // Free the shard
    auto free_result = backend_->free_shard(token.remote_addr, shard_size);
    ASSERT_EQ(free_result, ErrorCode::OK);
    
    stats = backend_->get_stats();
    EXPECT_EQ(stats.used_capacity, 0);
    EXPECT_EQ(stats.available_capacity, 100 * 1024 * 1024);
    EXPECT_FLOAT_EQ(stats.utilization, 0.0);
    EXPECT_EQ(stats.num_committed_shards, 0);
}

TEST_F(IoUringDiskBackendTest, ConcurrentOperations) {
    const uint64_t shard_size = 4096;
    const int num_threads = 4;
    const int shards_per_thread = 3;
    
    std::vector<std::thread> threads;
    std::vector<std::vector<ReservationToken>> all_tokens(num_threads);
    std::atomic<int> successful_operations{0};
    
    // Launch concurrent reservation threads
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([this, t, shard_size, shards_per_thread, &all_tokens, &successful_operations]() {
            for (int i = 0; i < shards_per_thread; i++) {
                auto result = backend_->reserve_shard(shard_size);
                if (std::holds_alternative<ReservationToken>(result)) {
                    all_tokens[t].push_back(std::get<ReservationToken>(result));
                    successful_operations++;
                }
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify that operations succeeded
    EXPECT_EQ(successful_operations.load(), num_threads * shards_per_thread);
    
    // Commit all tokens
    for (const auto& tokens : all_tokens) {
        for (const auto& token : tokens) {
            auto commit_result = backend_->commit_shard(token);
            EXPECT_EQ(commit_result, ErrorCode::OK);
        }
    }
    
    // Verify final stats
    auto stats = backend_->get_stats();
    EXPECT_EQ(stats.num_committed_shards, num_threads * shards_per_thread);
    EXPECT_EQ(stats.used_capacity, num_threads * shards_per_thread * shard_size);
}

} // namespace blackbird