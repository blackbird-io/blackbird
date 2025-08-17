#define BLACKBIRD_TEST 1

#include <gtest/gtest.h>
#include "blackbird/client/blackbird_client.h"
#include "blackbird/rpc/rpc_service.h" // RPC tag symbols

using namespace blackbird;

class StubRpcClient : public coro_rpc::coro_rpc_client {
public:
    template<auto Tag, typename Req>
    auto call(const Req &req) {
        using TagT = decltype(Tag);
        if constexpr (std::is_same_v<TagT, decltype(&RpcService::rpc_object_exists)>) {
            (void)req;
            ObjectExistsResponse resp;
            resp.exists = true;
            resp.error_code = ErrorCode::OK;
            return std::optional<ObjectExistsResponse>{resp};
        }
        if constexpr (std::is_same_v<TagT, decltype(&RpcService::rpc_get_workers)>) {
            (void)req;
            GetWorkersResponse resp; resp.error_code = ErrorCode::OBJECT_NOT_FOUND;
            return std::optional<GetWorkersResponse>{resp};
        }
        return std::optional<std::decay_t<decltype(*Tag(nullptr))>>{};
    }

    template<typename HostT, typename PortT>
    auto connect(HostT, PortT) { return coro_rpc::errc::ok; }
    virtual ~StubRpcClient() = default;
};

TEST(BlackbirdClientLogic, DISABLED_ObjectExistsTrue) {
    BlackbirdClient client;

    auto stub = std::make_unique<coro_rpc::coro_rpc_client>();
    BlackbirdClient::inject_rpc_for_test(std::move(stub));

    auto st = client.connect();
    ASSERT_EQ(st, ErrorCode::OK) << "Failed to connect: " << static_cast<int>(st);

    auto res = client.object_exists("foo");
    EXPECT_TRUE(is_ok(res)) << "Expected OK but got error: " << static_cast<int>(get_error(res));
    if (is_ok(res)) {
        EXPECT_TRUE(get_value(res));
    }
}