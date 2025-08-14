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
				// Replace wildcard with localhost for local test
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
		} catch (const std::exception& e) {
			LOG(WARNING) << "Failed to parse pool data: " << e.what();
		}
	}
	return false;
}

int main(int argc, char** argv) {
	google::InitGoogleLogging(argv[0]);
	FLAGS_alsologtostderr = 1;
	FLAGS_colorlogtostderr = 1;

	std::string keystone_host = "127.0.0.1";
	int keystone_port = 9090;
	std::string etcd_endpoints = "localhost:2379";
	std::string cluster_id = DEFAULT_CLUSTER_ID;
	std::string key = "test-object";
	size_t size = 1024 * 1024; // 1MB
	// Explicit replication settings (client must send valid values)
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
		} else if (arg == "--key" && i + 1 < argc) {
			key = argv[++i];
		} else if (arg == "--size" && i + 1 < argc) {
			size = static_cast<size_t>(std::stoull(argv[++i]));
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

	// Statistics tracking
	auto test_start = std::chrono::high_resolution_clock::now();
	auto rpc_start = std::chrono::high_resolution_clock::now();
	size_t bytes_sent = 0, bytes_received = 0;

	// 1) Contact Keystone for placement (put_start)
	coro_rpc::coro_rpc_client client;
	auto ec = async_simple::coro::syncAwait(client.connect(keystone_host, std::to_string(keystone_port)));
	if (ec != coro_rpc::errc::ok) {
		LOG(ERROR) << "Failed to connect to Keystone, ec=" << (int)ec;
		return 1;
	}

	WorkerConfig wcfg;
	wcfg.replication_factor = replicas;
	wcfg.max_workers_per_copy = max_workers;
	auto result = async_simple::coro::syncAwait(client.call<&blackbird::RpcService::rpc_put_start>(PutStartRequest{key, size, wcfg}));
	if (!result.has_value()) {
		LOG(ERROR) << "put_start RPC failed";
		return 2;
	}
	auto resp = result.value();
	if (resp.error_code != ErrorCode::OK) {
		LOG(ERROR) << "put_start returned error: " << (int)resp.error_code;
		return 3;
	}
	if (resp.copies.empty() || resp.copies[0].shards.empty()) {
		LOG(ERROR) << "put_start returned no placements";
		return 4;
	}
	LOG(INFO) << "Keystone provided placements: copies=" << resp.copies.size();

	auto rpc_end = std::chrono::high_resolution_clock::now();
	auto ucx_setup_start = std::chrono::high_resolution_clock::now();

	PoolInfo pool;
	if (!fetch_any_pool_from_etcd(etcd_endpoints, cluster_id, pool)) {
		LOG(ERROR) << "Failed to fetch any pool from etcd with UCX info";
		return 5;
	}
	LOG(INFO) << "Using pool endpoint=" << pool.ucx_endpoint << " remote_addr=0x" << std::hex << pool.remote_addr << std::dec;

	// 3) UCX setup
	ucp_context_h context{}; ucp_worker_h worker{}; ucp_ep_h ep{}; ucp_rkey_h rkey{};
	ucp_params_t params{}; params.field_mask = UCP_PARAM_FIELD_FEATURES; params.features = UCP_FEATURE_RMA;
	ucp_config_t* cfg{}; if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { LOG(ERROR) << "ucp_config_read failed"; return 6; }
	if (ucp_init(&params, cfg, &context) != UCS_OK) { LOG(ERROR) << "ucp_init failed"; ucp_config_release(cfg); return 7; }
	ucp_config_release(cfg);
	ucp_worker_params_t wparams{}; wparams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wparams.thread_mode = UCS_THREAD_MODE_SINGLE;
	if (ucp_worker_create(context, &wparams, &worker) != UCS_OK) { LOG(ERROR) << "ucp_worker_create failed"; ucp_cleanup(context); return 8; }

	// 4) Connect to worker sockaddr
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
	if (st != UCS_OK) { LOG(ERROR) << "ucp_ep_create failed: " << ucs_status_string(st); ucp_worker_destroy(worker); ucp_cleanup(context); return 9; }

	// 5) Unpack rkey
	void* rkey_buf = pool.rkey_raw.data(); size_t rkey_size = pool.rkey_raw.size();
	st = ucp_ep_rkey_unpack(ep, rkey_buf, &rkey);
	if (st != UCS_OK) { LOG(ERROR) << "ucp_ep_rkey_unpack failed: " << ucs_status_string(st); ucp_ep_destroy(ep); ucp_worker_destroy(worker); ucp_cleanup(context); return 10; }

	auto ucx_setup_end = std::chrono::high_resolution_clock::now();
	auto put_start = std::chrono::high_resolution_clock::now();

	// 6) Prepare local buffer with recognizable pattern and RMA PUT
	std::vector<uint8_t> data(64); // Smaller size for clear display
	// Create recognizable pattern: "BLACKBIRD_TEST_DATA_" + incrementing bytes
	std::string pattern = "BLACKBIRD_TEST_DATA_";
	for (size_t i = 0; i < data.size(); ++i) {
		if (i < pattern.size()) {
			data[i] = static_cast<uint8_t>(pattern[i]);
		} else {
			data[i] = static_cast<uint8_t>(i % 256);
		}
	}
	
	// Show what we're sending
	LOG(INFO) << "Sending " << data.size() << " bytes:";
	std::ostringstream sent_hex;
	for (size_t i = 0; i < std::min(data.size(), size_t(32)); ++i) {
		sent_hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(data[i]) << " ";
	}
	if (data.size() > 32) sent_hex << "...";
	LOG(INFO) << "Sent data (hex): " << sent_hex.str();
	
	std::string sent_ascii;
	for (size_t i = 0; i < std::min(data.size(), size_t(32)); ++i) {
		char c = static_cast<char>(data[i]);
		sent_ascii += (c >= 32 && c <= 126) ? c : '.';
	}
	if (data.size() > 32) sent_ascii += "...";
	LOG(INFO) << "Sent data (ascii): " << sent_ascii;

	uint64_t remote_addr = pool.remote_addr; // base
	ucp_request_param_t prm{}; // no callback (we'll flush)
	void* req = ucp_put_nbx(ep, data.data(), data.size(), remote_addr, rkey, &prm);
	if (UCS_PTR_IS_ERR(req)) { LOG(ERROR) << "ucp_put_nbx failed: " << ucs_status_string(UCS_PTR_STATUS(req)); ucp_rkey_destroy(rkey); ucp_ep_destroy(ep); ucp_worker_destroy(worker); ucp_cleanup(context); return 11; }
	if (req != nullptr) {
		// Wait for completion
		ucs_status_t status = UCS_INPROGRESS;
		while (status == UCS_INPROGRESS) {
			ucp_worker_progress(worker);
			status = ucp_request_check_status(req);
		}
		ucp_request_free(req);
		if (status != UCS_OK) { LOG(ERROR) << "RMA completion error: " << ucs_status_string(status); ucp_rkey_destroy(rkey); ucp_ep_destroy(ep); ucp_worker_destroy(worker); ucp_cleanup(context); return 12; }
	}
	// Ensure remote visibility
	st = ucp_worker_flush(worker);
	if (st != UCS_OK) { LOG(ERROR) << "ucp_worker_flush failed: " << ucs_status_string(st); ucp_rkey_destroy(rkey); ucp_ep_destroy(ep); ucp_worker_destroy(worker); ucp_cleanup(context); return 13; }

	LOG(INFO) << "UCX PUT succeeded (" << data.size() << " bytes)";
	auto put_end = std::chrono::high_resolution_clock::now();
	bytes_sent = data.size();
	
	auto get_start = std::chrono::high_resolution_clock::now();
	
	// 7) Read back the data with UCX GET to verify
	std::vector<uint8_t> read_buffer(data.size(), 0x00); // Clear buffer
	void* get_req = ucp_get_nbx(ep, read_buffer.data(), read_buffer.size(), remote_addr, rkey, &prm);
	if (UCS_PTR_IS_ERR(get_req)) {
		LOG(ERROR) << "ucp_get_nbx failed: " << ucs_status_string(UCS_PTR_STATUS(get_req));
	} else {
		if (get_req != nullptr) {
			// Wait for GET completion
			ucs_status_t get_status = UCS_INPROGRESS;
			while (get_status == UCS_INPROGRESS) {
				ucp_worker_progress(worker);
				get_status = ucp_request_check_status(get_req);
			}
			ucp_request_free(get_req);
			if (get_status != UCS_OK) {
				LOG(ERROR) << "GET completion error: " << ucs_status_string(get_status);
			}
		}
		
		// Show what we received
		LOG(INFO) << "UCX GET succeeded, received " << read_buffer.size() << " bytes:";
		std::ostringstream recv_hex;
		for (size_t i = 0; i < std::min(read_buffer.size(), size_t(32)); ++i) {
			recv_hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(read_buffer[i]) << " ";
		}
		if (read_buffer.size() > 32) recv_hex << "...";
		LOG(INFO) << "Received data (hex): " << recv_hex.str();
		
		std::string recv_ascii;
		for (size_t i = 0; i < std::min(read_buffer.size(), size_t(32)); ++i) {
			char c = static_cast<char>(read_buffer[i]);
			recv_ascii += (c >= 32 && c <= 126) ? c : '.';
		}
		if (read_buffer.size() > 32) recv_ascii += "...";
		LOG(INFO) << "Received data (ascii): " << recv_ascii;
		
		// Verify data integrity
		bool match = (data == read_buffer);
		LOG(INFO) << "Data verification: " << (match ? "PASS - data matches!" : "FAIL - data mismatch!");
		bytes_received = read_buffer.size();
	}

	auto get_end = std::chrono::high_resolution_clock::now();

	// Now as a different peer, fetch placements by key and print them
	{
		coro_rpc::coro_rpc_client peer_client;
		auto ec2 = async_simple::coro::syncAwait(peer_client.connect(keystone_host, std::to_string(keystone_port)));
		if (ec2 != coro_rpc::errc::ok) {
			LOG(ERROR) << "Peer client failed to connect to Keystone, ec=" << (int)ec2;
		} else {
			auto gw = async_simple::coro::syncAwait(peer_client.call<&blackbird::RpcService::rpc_get_workers>(GetWorkersRequest{key}));
			if (!gw.has_value()) {
				LOG(ERROR) << "rpc_get_workers failed";
			} else {
				auto gw_resp = gw.value();
				if (gw_resp.error_code != ErrorCode::OK) {
					LOG(ERROR) << "get_workers returned error: " << (int)gw_resp.error_code;
				} else {
					LOG(INFO) << "Peer view: object '" << key << "' has " << gw_resp.copies.size() << " copies";
					for (const auto& cp : gw_resp.copies) {
						LOG(INFO) << "  copy_index=" << cp.copy_index << ", shards=" << cp.shards.size();
						for (const auto& sh : cp.shards) {
							LOG(INFO) << "    shard pool_id=" << sh.pool_id << ", worker_id=" << sh.worker_id
									  << ", length=" << sh.length << ", storage_class=" << static_cast<int>(sh.storage_class);
						}
					}
				}
			}
		}
	}

	ucp_rkey_destroy(rkey);
	ucp_ep_destroy(ep);
	ucp_worker_destroy(worker);
	ucp_cleanup(context);

	auto test_end = std::chrono::high_resolution_clock::now();

	// Performance Statistics Summary
	auto rpc_time = std::chrono::duration<double, std::milli>(rpc_end - rpc_start).count();
	auto setup_time = std::chrono::duration<double, std::milli>(ucx_setup_end - ucx_setup_start).count();
	auto put_time = std::chrono::duration<double, std::milli>(put_end - put_start).count();
	auto get_time = std::chrono::duration<double, std::milli>(get_end - get_start).count();
	auto total_time = std::chrono::duration<double, std::milli>(test_end - test_start).count();
	
	double put_throughput = bytes_sent > 0 ? (bytes_sent * 1000.0) / (put_time * 1024 * 1024) : 0.0; // MB/s
	double get_throughput = bytes_received > 0 ? (bytes_received * 1000.0) / (get_time * 1024 * 1024) : 0.0; // MB/s

	LOG(INFO) << "=== Performance Statistics ===";
	LOG(INFO) << "Total time: " << std::fixed << std::setprecision(2) << total_time << " ms";
	LOG(INFO) << "RPC latency: " << std::fixed << std::setprecision(2) << rpc_time << " ms";
	LOG(INFO) << "UCX setup: " << std::fixed << std::setprecision(2) << setup_time << " ms";
	LOG(INFO) << "Data sent: " << bytes_sent << " bytes (" << std::fixed << std::setprecision(2) << put_time << " ms, " << put_throughput << " MB/s)";
	LOG(INFO) << "Data recv: " << bytes_received << " bytes (" << std::fixed << std::setprecision(2) << get_time << " ms, " << get_throughput << " MB/s)";

	return 0;
} 