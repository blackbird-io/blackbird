# Blackbird Examples

This directory contains example programs demonstrating the usage of Blackbird components.

## Building Examples

Examples are built as part of the main project when `BUILD_EXAMPLES=ON` (default):

```bash
mkdir build && cd build
cmake .. -DBUILD_EXAMPLES=ON
make -j4
```

The compiled examples will be located in `build/examples/`.

## Available Examples

### benchmark_disk_backends

Benchmarks the performance of the high-performance disk storage backend:
- `IoUringDiskBackend`: High-performance io_uring-based async I/O implementation

Usage:
```bash
./build/examples/benchmark_disk_backends
```

### test_disk_allocation

Simple test program demonstrating disk-based storage allocation.

Usage:
```bash
./build/examples/test_disk_allocation
```

## Notes

- All examples require the Blackbird library to be built first
- Examples use temporary directories for storage and clean up after themselves
- Performance results may vary depending on your storage hardware