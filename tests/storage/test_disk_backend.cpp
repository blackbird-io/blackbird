#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include "blackbird/worker/storage/disk_backend.h"
#include "blackbird/common/types.h"

using namespace blackbird;

class DiskBackendTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir = std::filesystem::temp_directory_path() / "blackbird_disk_test";
        std::filesystem::remove_all(test_dir);  // Clean up from previous tests
        std::filesystem::create_directories(test_dir);
    }
    
    void TearDown() override {
        std::filesystem::remove_all(test_dir);
    }
    
    std::filesystem::path test_dir;
};

TEST_F(DiskBackendTest, BasicInitialization) {
    DiskBackend backend(1024 * 1024, StorageClass::NVME, test_dir.string());
    
    EXPECT_EQ(backend.initialize(), ErrorCode::OK);
    EXPECT_EQ(backend.get_storage_class(), StorageClass::NVME);
    EXPECT_EQ(backend.get_total_capacity(), 1024 * 1024);
    EXPECT_EQ(backend.get_used_capacity(), 0);
    EXPECT_EQ(backend.get_available_capacity(), 1024 * 1024);
    EXPECT_NE(backend.get_base_address(), 0);
    EXPECT_NE(backend.get_rkey(), 0);
}

TEST_F(DiskBackendTest, StorageClassSupport) {
    // Test NVME
    {
        DiskBackend nvme_backend(1024, StorageClass::NVME, test_dir.string());
        EXPECT_EQ(nvme_backend.get_storage_class(), StorageClass::NVME);
    }
    
    // Test SSD
    {
        DiskBackend ssd_backend(1024, StorageClass::SSD, test_dir.string());
        EXPECT_EQ(ssd_backend.get_storage_class(), StorageClass::SSD);
    }
    
    // Test HDD
    {
        DiskBackend hdd_backend(1024, StorageClass::HDD, test_dir.string());
        EXPECT_EQ(hdd_backend.get_storage_class(), StorageClass::HDD);
    }
}

TEST_F(DiskBackendTest, InvalidStorageClass) {
    // Should throw exception for unsupported storage class
    EXPECT_THROW({
        DiskBackend backend(1024, StorageClass::RAM_CPU, test_dir.string());
    }, std::invalid_argument);
}

TEST_F(DiskBackendTest, ReserveAndCommitShard) {
    DiskBackend backend(1024 * 1024, StorageClass::NVME, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Reserve a shard
    auto result = backend.reserve_shard(4096);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    
    auto token = std::get<ReservationToken>(result);
    EXPECT_FALSE(token.token_id.empty());
    EXPECT_EQ(token.size, 4096);
    EXPECT_NE(token.remote_addr, 0);
    EXPECT_EQ(token.rkey, backend.get_rkey());
    EXPECT_FALSE(token.is_expired());
    
    // Check capacity after reservation
    EXPECT_EQ(backend.get_used_capacity(), 4096);
    EXPECT_EQ(backend.get_available_capacity(), 1024 * 1024 - 4096);
    
    // Commit the shard
    EXPECT_EQ(backend.commit_shard(token), ErrorCode::OK);
    
    // Stats should reflect committed shard
    auto stats = backend.get_stats();
    EXPECT_EQ(stats.num_reservations, 1);
    EXPECT_EQ(stats.num_committed_shards, 1);
    EXPECT_EQ(stats.used_capacity, 4096);
}

TEST_F(DiskBackendTest, AbortShard) {
    DiskBackend backend(1024 * 1024, StorageClass::SSD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Reserve a shard
    auto result = backend.reserve_shard(2048);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    EXPECT_EQ(backend.get_used_capacity(), 2048);
    
    // Abort the shard
    EXPECT_EQ(backend.abort_shard(token), ErrorCode::OK);
    
    // Capacity should be restored
    EXPECT_EQ(backend.get_used_capacity(), 0);
    EXPECT_EQ(backend.get_available_capacity(), 1024 * 1024);
    
    auto stats = backend.get_stats();
    EXPECT_EQ(stats.num_reservations, 0);
    EXPECT_EQ(stats.num_committed_shards, 0);
}

TEST_F(DiskBackendTest, FreeShard) {
    DiskBackend backend(1024 * 1024, StorageClass::HDD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Reserve and commit a shard
    auto result = backend.reserve_shard(8192);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    ASSERT_EQ(backend.commit_shard(token), ErrorCode::OK);
    EXPECT_EQ(backend.get_used_capacity(), 8192);
    
    // Free the committed shard
    EXPECT_EQ(backend.free_shard(token.remote_addr, token.size), ErrorCode::OK);
    
    // Capacity should be restored
    EXPECT_EQ(backend.get_used_capacity(), 0);
    EXPECT_EQ(backend.get_available_capacity(), 1024 * 1024);
    
    auto stats = backend.get_stats();
    EXPECT_EQ(stats.num_committed_shards, 0);
}

TEST_F(DiskBackendTest, OutOfSpace) {
    DiskBackend backend(1024, StorageClass::NVME, test_dir.string());  // Small 1KB capacity
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Try to reserve more than capacity
    auto result = backend.reserve_shard(2048);  // 2KB > 1KB capacity
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result));
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::OUT_OF_MEMORY);
}

TEST_F(DiskBackendTest, InvalidParameters) {
    DiskBackend backend(1024 * 1024, StorageClass::SSD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Try to reserve zero size
    auto result = backend.reserve_shard(0);
    ASSERT_TRUE(std::holds_alternative<ErrorCode>(result));
    EXPECT_EQ(std::get<ErrorCode>(result), ErrorCode::INVALID_PARAMETERS);
}

TEST_F(DiskBackendTest, NonExistentReservation) {
    DiskBackend backend(1024 * 1024, StorageClass::NVME, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Create a fake token
    ReservationToken fake_token;
    fake_token.token_id = "fake_token";
    fake_token.remote_addr = 12345;
    fake_token.size = 1024;
    
    // Try to commit non-existent reservation
    EXPECT_EQ(backend.commit_shard(fake_token), ErrorCode::OBJECT_NOT_FOUND);
    
    // Try to abort non-existent reservation
    EXPECT_EQ(backend.abort_shard(fake_token), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DiskBackendTest, NonExistentCommittedShard) {
    DiskBackend backend(1024 * 1024, StorageClass::SSD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Try to free non-existent shard
    EXPECT_EQ(backend.free_shard(99999, 1024), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DiskBackendTest, SizeMismatchOnFree) {
    DiskBackend backend(1024 * 1024, StorageClass::HDD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Reserve and commit a shard
    auto result = backend.reserve_shard(4096);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    ASSERT_EQ(backend.commit_shard(token), ErrorCode::OK);
    
    // Try to free with wrong size
    EXPECT_EQ(backend.free_shard(token.remote_addr, 2048), ErrorCode::INVALID_PARAMETERS);
}

TEST_F(DiskBackendTest, FileSystemOperations) {
    DiskBackend backend(1024 * 1024, StorageClass::NVME, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    // Verify storage directory was created
    auto storage_dir = test_dir / "blackbird_storage";
    EXPECT_TRUE(std::filesystem::exists(storage_dir));
    EXPECT_TRUE(std::filesystem::is_directory(storage_dir));
    
    // Reserve and commit a shard
    auto result = backend.reserve_shard(1024);
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
    auto token = std::get<ReservationToken>(result);
    
    // Before commit, no file should exist
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(storage_dir),
                           std::filesystem::directory_iterator{}), 0);
    
    // After commit, file should exist
    ASSERT_EQ(backend.commit_shard(token), ErrorCode::OK);
    EXPECT_GT(std::distance(std::filesystem::directory_iterator(storage_dir),
                           std::filesystem::directory_iterator{}), 0);
    
    // Find the created file and verify its size
    bool found_file = false;
    for (const auto& entry : std::filesystem::directory_iterator(storage_dir)) {
        if (entry.is_regular_file()) {
            EXPECT_EQ(entry.file_size(), 1024);
            found_file = true;
            break;
        }
    }
    EXPECT_TRUE(found_file);
    
    // After free, file should be deleted
    ASSERT_EQ(backend.free_shard(token.remote_addr, token.size), ErrorCode::OK);
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(storage_dir),
                           std::filesystem::directory_iterator{}), 0);
}

TEST_F(DiskBackendTest, MultipleShards) {
    DiskBackend backend(1024 * 1024, StorageClass::SSD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    std::vector<ReservationToken> tokens;
    
    // Reserve multiple shards
    for (int i = 0; i < 5; ++i) {
        auto result = backend.reserve_shard(1024);
        ASSERT_TRUE(std::holds_alternative<ReservationToken>(result));
        tokens.push_back(std::get<ReservationToken>(result));
    }
    
    EXPECT_EQ(backend.get_used_capacity(), 5 * 1024);
    
    // Commit all shards
    for (const auto& token : tokens) {
        ASSERT_EQ(backend.commit_shard(token), ErrorCode::OK);
    }
    
    auto stats = backend.get_stats();
    EXPECT_EQ(stats.num_committed_shards, 5);
    EXPECT_EQ(stats.used_capacity, 5 * 1024);
    
    // Free half of the shards
    for (size_t i = 0; i < tokens.size() / 2; ++i) {
        ASSERT_EQ(backend.free_shard(tokens[i].remote_addr, tokens[i].size), ErrorCode::OK);
    }
    
    stats = backend.get_stats();
    EXPECT_EQ(stats.num_committed_shards, 3);  // 5 - 2 = 3
    EXPECT_EQ(stats.used_capacity, 3 * 1024);
}

TEST_F(DiskBackendTest, InvalidDirectory) {
    std::string invalid_dir = "/root/nonexistent/invalid/path";
    DiskBackend backend(1024 * 1024, StorageClass::NVME, invalid_dir);
    
    // Should fail to initialize with invalid directory
    EXPECT_EQ(backend.initialize(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(DiskBackendTest, StatsCalculation) {
    DiskBackend backend(1024 * 1024, StorageClass::HDD, test_dir.string());
    ASSERT_EQ(backend.initialize(), ErrorCode::OK);
    
    auto stats = backend.get_stats();
    EXPECT_EQ(stats.total_capacity, 1024 * 1024);
    EXPECT_EQ(stats.used_capacity, 0);
    EXPECT_EQ(stats.available_capacity, 1024 * 1024);
    EXPECT_EQ(stats.utilization, 0.0);
    EXPECT_EQ(stats.num_reservations, 0);
    EXPECT_EQ(stats.num_committed_shards, 0);
    
    // Reserve and commit some shards
    auto result1 = backend.reserve_shard(100 * 1024);  // 100KB
    auto result2 = backend.reserve_shard(200 * 1024);  // 200KB
    
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result1));
    ASSERT_TRUE(std::holds_alternative<ReservationToken>(result2));
    
    auto token1 = std::get<ReservationToken>(result1);
    auto token2 = std::get<ReservationToken>(result2);
    
    ASSERT_EQ(backend.commit_shard(token1), ErrorCode::OK);
    ASSERT_EQ(backend.commit_shard(token2), ErrorCode::OK);
    
    stats = backend.get_stats();
    EXPECT_EQ(stats.total_capacity, 1024 * 1024);
    EXPECT_EQ(stats.used_capacity, 300 * 1024);
    EXPECT_EQ(stats.available_capacity, 1024 * 1024 - 300 * 1024);
    EXPECT_DOUBLE_EQ(stats.utilization, 300.0 / 1024.0);  // 300KB / 1MB
    EXPECT_EQ(stats.num_reservations, 2);
    EXPECT_EQ(stats.num_committed_shards, 2);
}