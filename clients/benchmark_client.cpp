#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <async_simple/coro/SyncAwait.h>
#include <nlohmann/json.hpp>
#include <ucp/api/ucp.h>

#include <cstring>
#include <iostream>
#include <thread>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <random>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "blackbird/common/types.h"
#include "blackbird/rpc/rpc_service.h"
#include "blackbird/etcd/etcd_service.h"

using namespace blackbird;

static std::vector<uint8_t> hex_to_bytes(const std::string& hex) {
	std::vector<uint8_t> bytes;
	if (hex.empty()) return bytes;
	std::string s; s.reserve(hex.size());
	for (char c : hex) if (c != ':') s.push_back(c);
	if (s.size() % 2 != 0) return bytes;
	bytes.reserve(s.size() / 2);
	for (size_t i = 0; i < s.size(); i += 2) {
		uint8_t b = static_cast<uint8_t>(std::stoul(s.substr(i, 2), nullptr, 16));
		bytes.push_back(b);
	}
	return bytes;
}

struct PoolInfo {
	std::string ucx_endpoint;
	uint64_t remote_addr{0};
	std::vector<uint8_t> rkey_raw;
	uint64_t size{0};
};

static bool fetch_any_pool_from_etcd(const std::string& endpoints, const std::string& cluster_id, PoolInfo& out) {
	EtcdService etcd(endpoints);
	if (etcd.connect() != ErrorCode::OK) return false;
	std::vector<std::string> keys, values;
	if (etcd.get_with_prefix("/blackbird/clusters/" + cluster_id + "/workers/", keys, values) != ErrorCode::OK) return false;
	for (size_t i = 0; i < keys.size(); ++i) {
		if (keys[i].find("/memory_pools/") == std::string::npos) continue;
		try {
			auto j = nlohmann::json::parse(values[i]);
			std::string ep = j.value("ucx_endpoint", std::string{});
			if (ep.rfind("0.0.0.0:", 0) == 0) {
				ep.replace(0, std::string("0.0.0.0").size(), "127.0.0.1");
			}
			uint64_t addr = j.value("ucx_remote_addr", 0ULL);
			std::string rkey_hex = j.value("ucx_rkey_hex", std::string{});
			out.ucx_endpoint = ep;
			out.remote_addr = addr;
			out.size = j.value("size", 0ULL);
			out.rkey_raw = hex_to_bytes(rkey_hex);
			if (!out.ucx_endpoint.empty() && out.remote_addr != 0 && !out.rkey_raw.empty()) {
				return true;
			}
		} catch (...) {
		}
	}
	return false;
}

struct BenchmarkResults {
	size_t iterations = 0;
	size_t total_bytes_written = 0;
	size_t total_bytes_read = 0;
	double total_write_time_ms = 0.0;
	double total_read_time_ms = 0.0;
	double setup_time_ms = 0.0;
	std::vector<double> write_latencies;
	std::vector<double> read_latencies;
};

int main(int argc, char** argv) {
	google::InitGoogleLogging(argv[0]);
	FLAGS_alsologtostderr = 1;
	FLAGS_colorlogtostderr = 1;

	std::string keystone_host = "127.0.0.1";
	int keystone_port = 9090;
	std::string etcd_endpoints = "localhost:2379";
	std::string cluster_id = DEFAULT_CLUSTER_ID;
	std::string key_prefix = "benchmark";
	size_t data_size = 1024 * 1024 * 1024; 
	size_t iterations = 10;
	size_t replicas = 1;
	size_t max_workers = 1;

	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "--keystone" && i + 1 < argc) {
			std::string addr = argv[++i];
			auto pos = addr.find(':');
			if (pos != std::string::npos) { keystone_host = addr.substr(0, pos); keystone_port = std::stoi(addr.substr(pos + 1)); }
		} else if (arg == "--etcd" && i + 1 < argc) {
			etcd_endpoints = argv[++i];
		} else if (arg == "--cluster" && i + 1 < argc) {
			cluster_id = argv[++i];
		} else if (arg == "--key-prefix" && i + 1 < argc) {
			key_prefix = argv[++i];
		} else if (arg == "--size" && i + 1 < argc) {
			data_size = static_cast<size_t>(std::stoull(argv[++i]));
		} else if (arg == "--iterations" && i + 1 < argc) {
			iterations = static_cast<size_t>(std::stoul(argv[++i]));
		} else if (arg == "--replicas" && i + 1 < argc) {
			replicas = static_cast<size_t>(std::stoul(argv[++i]));
		} else if (arg == "--max-workers" && i + 1 < argc) {
			max_workers = static_cast<size_t>(std::stoul(argv[++i]));
		}
	}

	if (replicas == 0 || max_workers == 0) {
		LOG(ERROR) << "Invalid arguments: --replicas and --max-workers must be > 0";
		return 1;
	}

	LOG(INFO) << "=== Blackbird UCX Benchmark ===";
	LOG(INFO) << "Data size: " << (data_size / (1024.0 * 1024.0)) << " MB (" << data_size << " bytes)";
	LOG(INFO) << "Iterations: " << iterations;
	LOG(INFO) << "Replicas: " << replicas << ", Max workers: " << max_workers;

	BenchmarkResults results;
	auto benchmark_start = std::chrono::high_resolution_clock::now();

	// Setup UCX once
	auto setup_start = std::chrono::high_resolution_clock::now();
	
	PoolInfo pool;
	if (!fetch_any_pool_from_etcd(etcd_endpoints, cluster_id, pool)) {
		LOG(ERROR) << "Failed to fetch pool from etcd";
		return 2;
	}

	ucp_context_h context{}; ucp_worker_h worker{}; ucp_ep_h ep{}; ucp_rkey_h rkey{};
	ucp_params_t params{}; params.field_mask = UCP_PARAM_FIELD_FEATURES; params.features = UCP_FEATURE_RMA;
	ucp_config_t* cfg{}; 
	if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { LOG(ERROR) << "ucp_config_read failed"; return 3; }
	if (ucp_init(&params, cfg, &context) != UCS_OK) { LOG(ERROR) << "ucp_init failed"; ucp_config_release(cfg); return 4; }
	ucp_config_release(cfg);
	
	ucp_worker_params_t wparams{}; wparams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wparams.thread_mode = UCS_THREAD_MODE_SINGLE;
	if (ucp_worker_create(context, &wparams, &worker) != UCS_OK) { LOG(ERROR) << "ucp_worker_create failed"; ucp_cleanup(context); return 5; }

	auto pos = pool.ucx_endpoint.find(':');
	std::string host = pool.ucx_endpoint.substr(0, pos);
	uint16_t port = static_cast<uint16_t>(std::stoi(pool.ucx_endpoint.substr(pos + 1)));
	sockaddr_in sa{}; sa.sin_family = AF_INET; sa.sin_port = htons(port); inet_pton(AF_INET, host.c_str(), &sa.sin_addr);
	
	ucp_ep_params_t ep_params{};
	ep_params.field_mask = UCP_EP_PARAM_FIELD_SOCK_ADDR | UCP_EP_PARAM_FIELD_FLAGS;
	ep_params.sockaddr.addr = reinterpret_cast<const struct sockaddr*>(&sa);
	ep_params.sockaddr.addrlen = sizeof(sa);
	ep_params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
	
	ucs_status_t st = ucp_ep_create(worker, &ep_params, &ep);
	if (st != UCS_OK) { LOG(ERROR) << "ucp_ep_create failed"; ucp_worker_destroy(worker); ucp_cleanup(context); return 6; }

	void* rkey_buf = pool.rkey_raw.data();
	st = ucp_ep_rkey_unpack(ep, rkey_buf, &rkey);
	if (st != UCS_OK) { LOG(ERROR) << "ucp_ep_rkey_unpack failed"; ucp_ep_destroy(ep); ucp_worker_destroy(worker); ucp_cleanup(context); return 7; }

	auto setup_end = std::chrono::high_resolution_clock::now();
	results.setup_time_ms = std::chrono::duration<double, std::milli>(setup_end - setup_start).count();

	// Prepare data buffer once
	std::vector<uint8_t> data_buffer(data_size);
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(0, 255);
	for (auto& byte : data_buffer) {
		byte = static_cast<uint8_t>(dis(gen));
	}

	std::vector<uint8_t> read_buffer(data_size);

	LOG(INFO) << "Starting benchmark...";

	// Benchmark loop
	for (size_t iter = 0; iter < iterations; ++iter) {
		// Contact Keystone for each iteration (new object key)
		coro_rpc::coro_rpc_client client;
		std::string key = key_prefix + "_" + std::to_string(iter) + "_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
		
		auto ec = async_simple::coro::syncAwait(client.connect(keystone_host, std::to_string(keystone_port)));
		if (ec != coro_rpc::errc::ok) continue;

		WorkerConfig wcfg;
		wcfg.replication_factor = replicas;
		wcfg.max_workers_per_copy = max_workers;
		auto result = async_simple::coro::syncAwait(client.call<&blackbird::RpcService::rpc_put_start>(PutStartRequest{key, data_size, wcfg}));
		if (!result.has_value() || result.value().error_code != ErrorCode::OK) continue;

		// WRITE benchmark
		auto write_start = std::chrono::high_resolution_clock::now();
		uint64_t remote_addr = pool.remote_addr + (iter * data_size) % (pool.size - data_size); // Rotate address
		
		ucp_request_param_t prm{};
		void* req = ucp_put_nbx(ep, data_buffer.data(), data_buffer.size(), remote_addr, rkey, &prm);
		if (UCS_PTR_IS_ERR(req)) continue;
		
		if (req != nullptr) {
			ucs_status_t status = UCS_INPROGRESS;
			while (status == UCS_INPROGRESS) {
				ucp_worker_progress(worker);
				status = ucp_request_check_status(req);
			}
			ucp_request_free(req);
			if (status != UCS_OK) continue;
		}
		ucp_worker_flush(worker);
		auto write_end = std::chrono::high_resolution_clock::now();
		
		double write_time = std::chrono::duration<double, std::milli>(write_end - write_start).count();
		results.write_latencies.push_back(write_time);
		results.total_write_time_ms += write_time;
		results.total_bytes_written += data_size;

		// READ benchmark
		auto read_start = std::chrono::high_resolution_clock::now();
		void* get_req = ucp_get_nbx(ep, read_buffer.data(), read_buffer.size(), remote_addr, rkey, &prm);
		if (!UCS_PTR_IS_ERR(get_req)) {
			if (get_req != nullptr) {
				ucs_status_t get_status = UCS_INPROGRESS;
				while (get_status == UCS_INPROGRESS) {
					ucp_worker_progress(worker);
					get_status = ucp_request_check_status(get_req);
				}
				ucp_request_free(get_req);
			}
			auto read_end = std::chrono::high_resolution_clock::now();
			
			double read_time = std::chrono::duration<double, std::milli>(read_end - read_start).count();
			results.read_latencies.push_back(read_time);
			results.total_read_time_ms += read_time;
			results.total_bytes_read += data_size;
		}

		results.iterations++;
		
		if ((iter + 1) % 5 == 0) {
			LOG(INFO) << "Completed " << (iter + 1) << "/" << iterations << " iterations";
		}
	}

	auto benchmark_end = std::chrono::high_resolution_clock::now();
	double total_time = std::chrono::duration<double, std::milli>(benchmark_end - benchmark_start).count();

	// Cleanup
	ucp_rkey_destroy(rkey);
	ucp_ep_destroy(ep);
	ucp_worker_destroy(worker);
	ucp_cleanup(context);

	// Results
	double avg_write_latency = results.total_write_time_ms / results.iterations;
	double avg_read_latency = results.total_read_time_ms / results.iterations;
	double write_throughput = (results.total_bytes_written * 1000.0) / (results.total_write_time_ms * 1024 * 1024); // MB/s
	double read_throughput = (results.total_bytes_read * 1000.0) / (results.total_read_time_ms * 1024 * 1024); // MB/s

	LOG(INFO) << "=== Benchmark Results ===";
	LOG(INFO) << "Completed iterations: " << results.iterations << "/" << iterations;
	LOG(INFO) << "Total time: " << std::fixed << std::setprecision(2) << total_time << " ms";
	LOG(INFO) << "Setup time: " << std::fixed << std::setprecision(2) << results.setup_time_ms << " ms";
	LOG(INFO) << "";
	LOG(INFO) << "WRITE Performance:";
	LOG(INFO) << "  Total written: " << (results.total_bytes_written / (1024.0 * 1024.0)) << " MB";
	LOG(INFO) << "  Avg latency: " << std::fixed << std::setprecision(2) << avg_write_latency << " ms";
	LOG(INFO) << "  Throughput: " << std::fixed << std::setprecision(2) << write_throughput << " MB/s";
	LOG(INFO) << "";
	LOG(INFO) << "READ Performance:";
	LOG(INFO) << "  Total read: " << (results.total_bytes_read / (1024.0 * 1024.0)) << " MB";
	LOG(INFO) << "  Avg latency: " << std::fixed << std::setprecision(2) << avg_read_latency << " ms";
	LOG(INFO) << "  Throughput: " << std::fixed << std::setprecision(2) << read_throughput << " MB/s";

	return 0;
} 