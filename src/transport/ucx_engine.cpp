#include "blackbird/transport/ucx_engine.h"

#include <glog/logging.h>
#include <cstring>
#include <sstream>
#include <chrono>

namespace blackbird {

static std::string rkey_to_hex(const void* rkey_buf, size_t rkey_size) {
    const uint8_t* b = static_cast<const uint8_t*>(rkey_buf);
    std::ostringstream oss;
    oss.setf(std::ios::hex, std::ios::basefield);
    for (size_t i = 0; i < rkey_size; ++i) {
        oss << static_cast<int>(b[i]);
        if (i + 1 < rkey_size) oss << ":";
    }
    return oss.str();
}

UcxEngine::UcxEngine() {}

UcxEngine::~UcxEngine() {
    running_.store(false);
    if (progress_thread_.joinable()) progress_thread_.join();

    if (listener_) {
        ucp_listener_destroy(listener_);
        listener_ = nullptr;
    }
    if (worker_) {
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
    }
    if (context_) {
        ucp_cleanup(context_);
        context_ = nullptr;
    }
}

bool UcxEngine::init() {
    ucp_params_t params;
    std::memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES;
    params.features = UCP_FEATURE_RMA | UCP_FEATURE_AM;

    ucp_config_t* cfg = nullptr;
    ucs_status_t st = ucp_config_read(nullptr, nullptr, &cfg);
    if (st != UCS_OK) {
        LOG(ERROR) << "ucp_config_read failed: " << ucs_status_string(st);
        return false;
    }

    st = ucp_init(&params, cfg, &context_);
    ucp_config_release(cfg);
    if (st != UCS_OK) {
        LOG(ERROR) << "ucp_init failed: " << ucs_status_string(st);
        return false;
    }

    ucp_worker_params_t wparams;
    std::memset(&wparams, 0, sizeof(wparams));
    wparams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    wparams.thread_mode = UCS_THREAD_MODE_SINGLE;

    st = ucp_worker_create(context_, &wparams, &worker_);
    if (st != UCS_OK) {
        LOG(ERROR) << "ucp_worker_create failed: " << ucs_status_string(st);
        return false;
    }

    running_.store(true);
    progress_thread_ = std::thread(&UcxEngine::progress_loop, this);
    return true;
}

void UcxEngine::progress_loop() {
    while (running_.load()) {
        if (worker_) ucp_worker_progress(worker_);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void UcxEngine::conn_request_cb(ucp_conn_request_h request, void* arg) {
    // Minimal accept: create endpoint so clients can complete handshake
    auto* self = static_cast<UcxEngine*>(arg);

    ucp_ep_params_t ep_params;
    std::memset(&ep_params, 0, sizeof(ep_params));
    ep_params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.conn_request = request;

    ucp_ep_h ep{};
    ucs_status_t st = ucp_ep_create(self->worker_, &ep_params, &ep);
    if (st != UCS_OK) {
        LOG(WARNING) << "ucp_ep_create (accept) failed: " << ucs_status_string(st);
    }
}

std::string UcxEngine::start_listener(const std::string& host, uint16_t port) {
    ucp_listener_params_t lparams;
    std::memset(&lparams, 0, sizeof(lparams));
    lparams.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                         UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;

    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (host.empty()) {
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        inet_pton(AF_INET, host.c_str(), &sa.sin_addr);
    }

    lparams.sockaddr.addr = reinterpret_cast<const struct sockaddr*>(&sa);
    lparams.sockaddr.addrlen = sizeof(sa);
    lparams.conn_handler.cb = &UcxEngine::conn_request_cb;
    lparams.conn_handler.arg = this;

    ucs_status_t st = ucp_listener_create(worker_, &lparams, &listener_);
    if (st != UCS_OK) {
        LOG(ERROR) << "ucp_listener_create failed: " << ucs_status_string(st);
        return {};
    }

    char ip_str[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &sa.sin_addr, ip_str, sizeof(ip_str));
    std::ostringstream oss;
    oss << (host.empty() ? ip_str : host) << ":" << ntohs(sa.sin_port);
    return oss.str();
}

bool UcxEngine::register_memory(void* base_ptr, size_t size, UcxRegInfo& out_info) {
    ucp_mem_map_params_t mparams{};
    mparams.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mparams.address = base_ptr;
    mparams.length = size;

    ucs_status_t st = ucp_mem_map(context_, &mparams, &out_info.memh);
    if (st != UCS_OK) {
        LOG(ERROR) << "ucp_mem_map failed: " << ucs_status_string(st);
        return false;
    }

    void* rkey_buf = nullptr;
    size_t rkey_size = 0;
    st = ucp_rkey_pack(context_, out_info.memh, &rkey_buf, &rkey_size);
    if (st != UCS_OK) {
        LOG(ERROR) << "ucp_rkey_pack failed: " << ucs_status_string(st);
        ucp_mem_unmap(context_, out_info.memh);
        out_info.memh = nullptr;
        return false;
    }

    out_info.base_ptr = base_ptr;
    out_info.size = size;
    out_info.remote_addr = reinterpret_cast<uint64_t>(base_ptr);
    out_info.rkey_raw.assign(reinterpret_cast<uint8_t*>(rkey_buf), reinterpret_cast<uint8_t*>(rkey_buf) + rkey_size);
    out_info.rkey_hex = rkey_to_hex(rkey_buf, rkey_size);

    ucp_rkey_buffer_release(rkey_buf);
    return true;
}

} // namespace blackbird 