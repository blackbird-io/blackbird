#pragma once

#include <string>
#include <vector>
#include <future>
#include <memory>
#include <unordered_map>
#include <mutex>

#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "blackbird/common/types.h"

namespace blackbird {

/**
 * @brief Options controlling how the BlackbirdClient interacts with the cluster.
 *
 * Feel free to start with the defaults – they are chosen to work "out of the box" on
 * a typical local deployment.  Tweak them later once you have performance numbers.
 */
struct BlackbirdClientOptions {
    /** Address of the Keystone RPC endpoint (hostname or IP). */
    std::string keystone_host{"127.0.0.1"};

    /** TCP port of the Keystone RPC endpoint. */
    uint16_t keystone_port{9090};

    /** Timeout for individual Keystone RPC calls. */
    std::chrono::seconds rpc_timeout{10};

    /**
     * Maximum number of worker-to-worker transfers that may run simultaneously.
     * Increase this value if a beefy network stack (e.g. RoCE) is available.
     */
    size_t io_parallelism{8};
};

/**
 * @brief High-level client that hides the complexity of interacting with Keystone and Worker
 *        nodes.  A single instance is thread-safe and may be shared across the application.
 *
 *  – Metadata operations are executed via the high-performance coro_rpc layer.
 *  – Bulk data movement is performed in parallel over RDMA/UCX according to the shard placement
 *    returned by metadata server.
 */
class BlackbirdClient {
public:
    /**
     * @brief Construct a new \ref BlackbirdClient.
     *
     * The constructor is lightweight – it does touch the network.
     * Connection establishment is deferred until the first operation,
     * making it safe to create an instance in global scope.
     */
    explicit BlackbirdClient(const BlackbirdClientOptions &opts = {});

    /**
     * @brief Explicitly (re-)connect to Keystone.
     *
     * Generally this call will not be needed, every public method will ensure a
     * valid connection automatically.  It is provided for cases where the client
     * needs to verify connectivity ahead of time.
     */
    ErrorCode connect();

    /** Check if an object exists in the cluster. */
    [[nodiscard]] Result<bool> object_exists(const ObjectKey &key);

    /**
     * Request placement information (workers & shards) for an existing object.
     * The result can be fed directly into \ref get or your own custom I/O code.
     */
    [[nodiscard]] Result<std::vector<CopyPlacement>> get_workers(const ObjectKey &key);

    /**
     * @name Data-plane helpers
     * The main convenience wrappers around \ref put and \ref get.
     * @{ */

    /** Retrieve an object. */
    [[nodiscard]] Result<std::vector<uint8_t>> get(const ObjectKey &key);

    /**
     * Store object data in the cache.
     *
     * Internally performs the following steps:
     *  1. keystone::put_start – obtain worker allocations
     *  2. Transfer data to all shards in parallel (RDMA/UCX)
     *  3. keystone::put_complete
     *
     * If any step fails everything is rolled back via keystone::put_cancel.
     */
    ErrorCode put(const ObjectKey &key,
                  const uint8_t   *data,
                  size_t           size,
                  const WorkerConfig &cfg = {});

    ErrorCode put(const ObjectKey &key,
                  const std::vector<uint8_t> &buffer,
                  const WorkerConfig &cfg = {}) {
        return put(key, buffer.data(), buffer.size(), cfg);
    }

    ErrorCode remove(const ObjectKey &key);

private:
    using RpcClientPtr = std::unique_ptr<coro_rpc::coro_rpc_client>;

    RpcClientPtr & acquire_rpc();

    ErrorCode ensure_rpc_connected(RpcClientPtr &cli);

    ErrorCode transfer_shards_put(const std::vector<CopyPlacement> &placements,
                                  const uint8_t *data,
                                  size_t size,
                                  size_t parallelism);
    Result<std::vector<uint8_t>> transfer_shards_get(const std::vector<CopyPlacement> &placements);

    BlackbirdClientOptions opts_;

    struct RpcCacheEntry {
        RpcClientPtr client;
        bool         connected{false};
    };

    static thread_local RpcCacheEntry tls_rpc_;

#ifdef BLACKBIRD_TEST
public:
    // Test-only helper to inject a pre-constructed RPC client into the
    // thread-local cache and mark it as connected.
    static void inject_rpc_for_test(RpcClientPtr ptr) {
        tls_rpc_.client = std::move(ptr);
        tls_rpc_.connected = true;
    }
#endif
};

} // namespace blackbird