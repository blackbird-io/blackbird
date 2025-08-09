#include "blackbird/types.h"
#include "worker.pb.h"

namespace blackbird {
namespace proto_adapters {

inline blackbird::proto::WorkerStatus to_proto(WorkerStatus s) {
    switch (s) {
        case WorkerStatus::ALLOCATED: return blackbird::proto::WORKER_STATUS_ALLOCATED;
        case WorkerStatus::WRITING: return blackbird::proto::WORKER_STATUS_WRITING;
        case WorkerStatus::COMPLETE: return blackbird::proto::WORKER_STATUS_COMPLETE;
        case WorkerStatus::FAILED: return blackbird::proto::WORKER_STATUS_FAILED;
        case WorkerStatus::REMOVED: return blackbird::proto::WORKER_STATUS_REMOVED;
        default: return blackbird::proto::WORKER_STATUS_UNDEFINED;
    }
}

inline WorkerStatus from_proto(blackbird::proto::WorkerStatus s) {
    switch (s) {
        case blackbird::proto::WORKER_STATUS_ALLOCATED: return WorkerStatus::ALLOCATED;
        case blackbird::proto::WORKER_STATUS_WRITING: return WorkerStatus::WRITING;
        case blackbird::proto::WORKER_STATUS_COMPLETE: return WorkerStatus::COMPLETE;
        case blackbird::proto::WORKER_STATUS_FAILED: return WorkerStatus::FAILED;
        case blackbird::proto::WORKER_STATUS_REMOVED: return WorkerStatus::REMOVED;
        default: return WorkerStatus::UNDEFINED;
    }
}

inline blackbird::proto::MemoryKind to_proto(MemoryKind k) {
    switch (k) {
        case MemoryKind::CPU_DRAM: return blackbird::proto::MEMORY_KIND_CPU_DRAM;
        case MemoryKind::GPU_HBM:  return blackbird::proto::MEMORY_KIND_GPU_HBM;
        default: return blackbird::proto::MEMORY_KIND_UNSPECIFIED;
    }
}

inline MemoryKind from_proto(blackbird::proto::MemoryKind k) {
    switch (k) {
        case blackbird::proto::MEMORY_KIND_CPU_DRAM: return MemoryKind::CPU_DRAM;
        case blackbird::proto::MEMORY_KIND_GPU_HBM:  return MemoryKind::GPU_HBM;
        default: return MemoryKind::UNSPECIFIED;
    }
}

inline blackbird::proto::DiskKind to_proto(DiskKind k) {
    switch (k) {
        case DiskKind::NVME: return blackbird::proto::DISK_KIND_NVME;
        case DiskKind::SSD:  return blackbird::proto::DISK_KIND_SSD;
        case DiskKind::HDD:  return blackbird::proto::DISK_KIND_HDD;
        default: return blackbird::proto::DISK_KIND_UNSPECIFIED;
    }
}

inline DiskKind from_proto(blackbird::proto::DiskKind k) {
    switch (k) {
        case blackbird::proto::DISK_KIND_NVME: return DiskKind::NVME;
        case blackbird::proto::DISK_KIND_SSD:  return DiskKind::SSD;
        case blackbird::proto::DISK_KIND_HDD:  return DiskKind::HDD;
        default: return DiskKind::UNSPECIFIED;
    }
}

inline void to_proto(const UcxRegion& in, blackbird::proto::UcxRegion* out) {
    out->set_worker_address({reinterpret_cast<const char*>(in.worker_address.data()), in.worker_address.size()});
    out->set_remote_key(in.remote_key);
    out->set_remote_addr(in.remote_addr);
    out->set_size(in.size);
}

inline void from_proto(const blackbird::proto::UcxRegion& in, UcxRegion* out) {
    const auto& bytes = in.worker_address();
    out->worker_address.assign(bytes.begin(), bytes.end());
    out->remote_key = in.remote_key();
    out->remote_addr = in.remote_addr();
    out->size = static_cast<size_t>(in.size());
}

inline void to_proto(const MemoryPlacement& in, blackbird::proto::MemoryPlacement* out) {
    out->set_kind(to_proto(in.kind));
    out->set_device_id(in.device_id);
    out->set_numa_node(in.numa_node);
    for (const auto& r : in.regions) {
        auto* out_r = out->add_regions();
        to_proto(r, out_r);
    }
}

inline void from_proto(const blackbird::proto::MemoryPlacement& in, MemoryPlacement* out) {
    out->kind = from_proto(in.kind());
    out->device_id = in.device_id();
    out->numa_node = in.numa_node();
    out->regions.clear();
    out->regions.resize(in.regions_size());
    for (int i = 0; i < in.regions_size(); ++i) {
        from_proto(in.regions(i), &out->regions[i]);
    }
}

inline void to_proto(const DiskPlacement& in, blackbird::proto::DiskPlacement* out) {
    out->set_kind(to_proto(in.kind));
    out->set_mount_path(in.mount_path);
    out->set_capacity(in.capacity);
    out->set_node_id(in.node_id);
}

inline void from_proto(const blackbird::proto::DiskPlacement& in, DiskPlacement* out) {
    out->kind = from_proto(in.kind());
    out->mount_path = in.mount_path();
    out->capacity = static_cast<size_t>(in.capacity());
    out->node_id = in.node_id();
}

inline blackbird::proto::WorkerPlacement to_proto(const WorkerPlacement& in) {
    blackbird::proto::WorkerPlacement out;
    out.set_status(to_proto(in.status));
    out.set_node_id(in.node_id);
    if (in.is_memory_placement()) {
        auto* mem = out.mutable_memory();
        to_proto(in.get_memory_placement(), mem);
    } else if (in.is_disk_placement()) {
        auto* d = out.mutable_disk();
        to_proto(in.get_disk_placement(), d);
    }
    return out;
}

inline WorkerPlacement from_proto(const blackbird::proto::WorkerPlacement& in) {
    WorkerPlacement out;
    out.status = from_proto(in.status());
    out.node_id = in.node_id();
    if (in.has_memory()) {
        MemoryPlacement m;
        from_proto(in.memory(), &m);
        out.storage = std::move(m);
    } else if (in.has_disk()) {
        DiskPlacement d;
        from_proto(in.disk(), &d);
        out.storage = std::move(d);
    }
    return out;
}

} // namespace proto_adapters
} // namespace blackbird 