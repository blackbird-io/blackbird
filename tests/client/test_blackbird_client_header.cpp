#include <gtest/gtest.h>
#include "blackbird/client/blackbird_client.h"

namespace {

TEST(BlackbirdClientHeader, DefaultAndCustomConstruction) {
    using namespace blackbird;

    EXPECT_NO_THROW({ BlackbirdClient client; });

    BlackbirdClientOptions opts;    
    opts.keystone_host = "localhost";
    opts.keystone_port = 12345;
    opts.io_parallelism = 4;

    EXPECT_NO_THROW({ BlackbirdClient client{opts}; });
}

} // anonymous namespace