#include "blackbird/types.h"

#include <random>
#include <thread>

namespace blackbird {

namespace {
// Thread-local random number generator for UUID generation
thread_local std::random_device rd;
thread_local std::mt19937_64 gen(rd());
} // anonymous namespace

UUID generate_uuid() {
    return {gen(), gen()};
}

} // namespace blackbird 