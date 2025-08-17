// SPDX-License-Identifier: Apache-2.0
// Blackbird Client implementation

#include "blackbird/client/blackbird_client.h"

#include <glog/logging.h>
#include <async_simple/coro/SyncAwait.h>
#include <ucp/api/ucp.h>

#include <future>
#include <optional>
#include <shared_mutex>

#include "blackbird/rpc/rpc_service.h" // for RPC method ids

namespace blackbird {

// Thread-local RPC client cache
thread_local BlackbirdClient::RpcCacheEntry BlackbirdClient::tls_rpc_{};

BlackbirdClient::BlackbirdClient(const BlackbirdClientOptions &opts) : opts_(opts) {}

ErrorCode BlackbirdClient::connect() {
    auto &cli = acquire_rpc();
    return ensure_rpc_connected(cli);
}

BlackbirdClient::RpcClientPtr & BlackbirdClient::acquire_rpc() {
    if (!tls_rpc_.client) {
        tls_rpc_.client = std::make_unique<coro_rpc::coro_rpc_client>();
    }
    return tls_rpc_.client;
}

ErrorCode BlackbirdClient::ensure_rpc_connected(RpcClientPtr &cli) {
    if (tls_rpc_.connected) {
        return ErrorCode::OK;
    }

    auto ec = async_simple::coro::syncAwait(
        cli->connect(opts_.keystone_host, std::to_string(opts_.keystone_port)));

    if (ec != coro_rpc::errc::ok) {
        LOG(ERROR) << "[BlackbirdClient] Unable to reach Keystone at "
                   << opts_.keystone_host << ':' << opts_.keystone_port
                   << " (err=" << static_cast<int>(ec) << ')';
        return ErrorCode::NETWORK_ERROR;
    }

    tls_rpc_.connected = true;
    return ErrorCode::OK;
}

// Metadata helpers
// Fast path helpers talking only to Keystone (no UCX involved)
Result<bool> BlackbirdClient::object_exists(const ObjectKey &key) {
    auto &cli = acquire_rpc();
    auto st = ensure_rpc_connected(cli);
    if (st != ErrorCode::OK) return st;

    auto result = async_simple::coro::syncAwait(
        cli->call<&RpcService::rpc_object_exists>(ObjectExistsRequest{key}));

    if (!result.has_value()) {
        return ErrorCode::NETWORK_ERROR; // Transport level failure
    }
    if (result->error_code != ErrorCode::OK) {
        return result->error_code;
    }
    return result->exists; // unwrap struct
}

Result<std::vector<CopyPlacement>> BlackbirdClient::get_workers(const ObjectKey &key) {
    auto &cli = acquire_rpc();
    auto st = ensure_rpc_connected(cli);
    if (st != ErrorCode::OK) return st;

    auto result = async_simple::coro::syncAwait(
        cli->call<&RpcService::rpc_get_workers>(GetWorkersRequest{key}));

    if (!result.has_value()) return ErrorCode::NETWORK_ERROR;
    if (result->error_code != ErrorCode::OK) return result->error_code;
    return result->copies;
}

// PUT
ErrorCode BlackbirdClient::put(const ObjectKey &key,
                               const uint8_t *data,
                               size_t size,
                               const WorkerConfig &cfg) {
    auto &cli = acquire_rpc();
    // adding rpc overhead here, check if this can be avoided in a safe manner
    auto st = ensure_rpc_connected(cli);
    if (st != ErrorCode::OK) return st;

    auto start_res = async_simple::coro::syncAwait(
        cli->call<&RpcService::rpc_put_start>(PutStartRequest{key, size, cfg}));

    if (!start_res.has_value()) return ErrorCode::NETWORK_ERROR;
    if (start_res->error_code != ErrorCode::OK) return start_res->error_code;

    const auto &placements = start_res->copies;

    auto transfer_ec = transfer_shards_put(placements, data, size, opts_.io_parallelism);
    if (transfer_ec != ErrorCode::OK) {
        async_simple::coro::syncAwait(cli->call<&RpcService::rpc_put_cancel>(PutCancelRequest{key}));
        return transfer_ec;
    }

    auto complete_resp = async_simple::coro::syncAwait(
        cli->call<&RpcService::rpc_put_complete>(PutCompleteRequest{key}));
    if (!complete_resp.has_value() || complete_resp->error_code != ErrorCode::OK) {
        return ErrorCode::NETWORK_ERROR;
    }
    return ErrorCode::OK;
}

// GET 
Result<std::vector<uint8_t>> BlackbirdClient::get(const ObjectKey &key) {
    auto placement_res = get_workers(key);
    if (!is_ok(placement_res)) return get_error(placement_res);

    auto data_res = transfer_shards_get(get_value(placement_res));
    return data_res;
}

// ------------------ Data transfer helpers (UCX) ----------------------------
namespace {

struct UcxContext {
    ucp_context_h ctx{nullptr};
    ucp_worker_h  worker{nullptr};
    UcxContext() {
        ucp_params_t params{};
        params.field_mask = UCP_PARAM_FIELD_FEATURES;
        params.features  = UCP_FEATURE_RMA;
        ucp_config_t *cfg{};
        if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) {
            throw std::runtime_error("ucp_config_read failed");
        }
        if (ucp_init(&params, cfg, &ctx) != UCS_OK) {
            ucp_config_release(cfg);
            throw std::runtime_error("ucp_init failed");
        }
        ucp_config_release(cfg);

        ucp_worker_params_t wparams{};
        wparams.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
        wparams.thread_mode = UCS_THREAD_MODE_SINGLE;
        if (ucp_worker_create(ctx, &wparams, &worker) != UCS_OK) {
            ucp_cleanup(ctx);
            throw std::runtime_error("ucp_worker_create failed");
        }
    }
    ~UcxContext() {
        if (worker) ucp_worker_destroy(worker);
        if (ctx)    ucp_cleanup(ctx);
    }
};

// Connect to "ip:port" and create ep
ucp_ep_h create_ep(ucp_worker_h worker, const std::string &endpoint) {
    auto pos = endpoint.find(':');
    if (pos == std::string::npos) {
        LOG(ERROR) << "Invalid UCX endpoint " << endpoint;
        return nullptr;
    }
    std::string host = endpoint.substr(0, pos);
    uint16_t port    = static_cast<uint16_t>(std::stoi(endpoint.substr(pos + 1)));

    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(port);
    inet_pton(AF_INET, host.c_str(), &sa.sin_addr);

    ucp_ep_params_t ep_params{};
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_SOCK_ADDR | UCP_EP_PARAM_FIELD_FLAGS;
    ep_params.sockaddr.addr   = reinterpret_cast<const struct sockaddr*>(&sa);
    ep_params.sockaddr.addrlen = sizeof(sa);
    ep_params.flags           = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;

    ucp_ep_h ep{};
    if (ucp_ep_create(worker, &ep_params, &ep) != UCS_OK) {
        LOG(ERROR) << "ucp_ep_create failed for " << endpoint;
        return nullptr;
    }
    return ep;
}

// Wait for a UCX request to complete
ucs_status_t wait(ucp_worker_h worker, void *request) {
    if (request == nullptr) return UCS_OK;
    ucs_status_t status = UCS_INPROGRESS;
    while (status == UCS_INPROGRESS) {
        ucp_worker_progress(worker);
        status = ucp_request_check_status(request);
    }
    ucp_request_free(request);
    return status;
}

} // anonymous namespace

ErrorCode BlackbirdClient::transfer_shards_put(
    const std::vector<CopyPlacement> &placements, const uint8_t *data, size_t, size_t) {

    auto write_single_shard = [&](ucp_worker_h worker,
                                  const ShardPlacement &shard) -> ErrorCode {
        if (!std::holds_alternative<MemoryLocation>(shard.location)) {
            return ErrorCode::NOT_IMPLEMENTED; // only RDMA shards for now
        }

        const auto &mem_loc = std::get<MemoryLocation>(shard.location);

        // Establish UCX endpoint
        ucp_ep_h ep = create_ep(worker,
            shard.endpoint.ip + ':' + std::to_string(shard.endpoint.port));

        if (!ep) {
            return ErrorCode::NETWORK_ERROR;
        }

        // Remote-key unpack
        ucp_rkey_h rkey{};
        if (ucp_ep_rkey_unpack(ep, shard.endpoint.worker_key.data(), &rkey) != UCS_OK) {
            ucp_ep_destroy(ep);
            return ErrorCode::NETWORK_ERROR;
        }

        // RDMA PUT transfer
        ucp_request_param_t prm{};
        void *req = ucp_put_nbx(ep,
            /*buffer*/ data + mem_loc.remote_addr,
            /*length*/ mem_loc.size,
            /*remote*/ mem_loc.remote_addr,
            rkey,
            &prm);

        if (UCS_PTR_IS_ERR(req) || wait(worker, req) != UCS_OK) {
            ucp_rkey_destroy(rkey);
            ucp_ep_destroy(ep);
            return ErrorCode::NETWORK_ERROR;
        }

        ucp_rkey_destroy(rkey);
        ucp_ep_destroy(ep);
        return ErrorCode::OK;
    };

    try {
        UcxContext ucx;
        std::vector<std::future<ErrorCode>> tasks;

        for (const auto &copy : placements) {
            for (const auto &shard : copy.shards) {
                tasks.emplace_back(std::async(std::launch::async,
                    write_single_shard,
                    ucx.worker,
                    std::cref(shard)));
            }
        }

        for (auto &f : tasks) {
            if (f.get() != ErrorCode::OK) {
                return ErrorCode::NETWORK_ERROR;
            }
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << "transfer_shards_put(): " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

Result<std::vector<uint8_t>> BlackbirdClient::transfer_shards_get(
    const std::vector<CopyPlacement> &placements) {

    if (placements.empty()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    // TODO: Attempt for all replicas in parallel in the future.
    const auto &copy = placements.front();
    if (copy.shards.empty()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    size_t total_size = 0;
    for (const auto &shard : copy.shards) {
        if (!std::holds_alternative<MemoryLocation>(shard.location)) {
            return ErrorCode::NOT_IMPLEMENTED;
        }
        total_size += std::get<MemoryLocation>(shard.location).size;
    }

    std::vector<uint8_t> buffer(total_size);

    auto read_single_shard = [&](ucp_worker_h worker,
            const ShardPlacement &shard,
            uint8_t *dst) -> ErrorCode {

        const auto &mem_loc = std::get<MemoryLocation>(shard.location);

        ucp_ep_h ep = create_ep(worker,
                                shard.endpoint.ip + ':' + std::to_string(shard.endpoint.port));
        if (!ep) return ErrorCode::NETWORK_ERROR;

        ucp_rkey_h rkey{};
        if (ucp_ep_rkey_unpack(ep, shard.endpoint.worker_key.data(), &rkey) != UCS_OK) {
            ucp_ep_destroy(ep);
            return ErrorCode::NETWORK_ERROR;
        }

        ucp_request_param_t prm{};
        void *req = ucp_get_nbx(ep,
            dst,
            mem_loc.size,
            mem_loc.remote_addr,
            rkey,
            &prm);

        if (UCS_PTR_IS_ERR(req) || wait(worker, req) != UCS_OK) {
            ucp_rkey_destroy(rkey);
            ucp_ep_destroy(ep);
            return ErrorCode::NETWORK_ERROR;
        }

        ucp_rkey_destroy(rkey);
        ucp_ep_destroy(ep);
        return ErrorCode::OK;
    };

    try {
        UcxContext ucx;
        size_t offset = 0;

        for (const auto &shard : copy.shards) {
            auto ec = read_single_shard(ucx.worker, shard, buffer.data() + offset);
            if (ec != ErrorCode::OK) {
                return ec;
            }
            offset += std::get<MemoryLocation>(shard.location).size;
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << "transfer_shards_get(): " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    return buffer;
}

// remove 
ErrorCode BlackbirdClient::remove(const ObjectKey &key) {
    auto &cli = acquire_rpc();
    auto st = ensure_rpc_connected(cli);
    if (st != ErrorCode::OK) return st;

    auto resp_opt = async_simple::coro::syncAwait(cli->call<&RpcService::rpc_remove_object>(RemoveObjectRequest{key}));
    if (!resp_opt.has_value()) return ErrorCode::NETWORK_ERROR;
    return resp_opt->error_code;
}

} // namespace blackbird