#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <thread>

#include <ucp/api/ucp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

namespace blackbird {

struct UcxRegInfo {
    uint64_t remote_addr{0};
    std::string rkey_hex;
    std::vector<uint8_t> rkey_raw;
    void* base_ptr{nullptr};
    size_t size{0};
    ucp_mem_h memh{nullptr};
};

class UcxEngine {
public:
    UcxEngine();
    ~UcxEngine();

    UcxEngine(const UcxEngine&) = delete;
    UcxEngine& operator=(const UcxEngine&) = delete;

    bool init();

    // Returns "host:port" actually in use
    std::string start_listener(const std::string& host, uint16_t port);

    bool register_memory(void* base_ptr, size_t size, UcxRegInfo& out_info);

private:
    static void conn_request_cb(ucp_conn_request_h request, void* arg);

    void progress_loop();

    std::atomic<bool> running_{false};
    std::thread progress_thread_;

    ucp_context_h context_{nullptr};
    ucp_worker_h worker_{nullptr};
    ucp_listener_h listener_{nullptr};
};

} // namespace blackbird 