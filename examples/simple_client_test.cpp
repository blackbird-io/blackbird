/**
 * @file simple_client_test.cpp
 * @brief Real client to test Blackbird Keystone server connectivity
 */

#include <iostream>
#include <glog/logging.h>
#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>

#include "blackbird/common/types.h"

using namespace blackbird;

// Real socket connectivity test
bool test_server_connectivity(const std::string& host, int port) {
    std::cout << "Testing real connectivity to " << host << ":" << port << "..." << std::endl;
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cout << "✗ Failed to create socket: " << strerror(errno) << std::endl;
        return false;
    }
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        std::cout << "✗ Invalid address: " << host << std::endl;
        close(sock);
        return false;
    }
    
    // Set socket timeout
    struct timeval timeout;
    timeout.tv_sec = 2;  // 2 second timeout
    timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cout << "✗ Connection failed: " << strerror(errno) << std::endl;
        close(sock);
        return false;
    }
    
    std::cout << "✓ Successfully connected!" << std::endl;
    close(sock);
    return true;
}

// Test HTTP endpoint with basic HTTP request
bool test_http_endpoint(const std::string& host, int port, const std::string& path = "/metrics") {
    std::cout << "Testing HTTP endpoint " << host << ":" << port << path << "..." << std::endl;
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cout << "✗ Failed to create socket: " << strerror(errno) << std::endl;
        return false;
    }
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        std::cout << "✗ Invalid address: " << host << std::endl;
        close(sock);
        return false;
    }
    
    // Set socket timeout
    struct timeval timeout;
    timeout.tv_sec = 3;
    timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cout << "✗ HTTP connection failed: " << strerror(errno) << std::endl;
        close(sock);
        return false;
    }
    
    // Send basic HTTP GET request
    std::string request = "GET " + path + " HTTP/1.1\r\n";
    request += "Host: " + host + ":" + std::to_string(port) + "\r\n";
    request += "Connection: close\r\n\r\n";
    
    if (send(sock, request.c_str(), request.length(), 0) < 0) {
        std::cout << "✗ Failed to send HTTP request: " << strerror(errno) << std::endl;
        close(sock);
        return false;
    }
    
    // Try to read response
    char buffer[1024];
    int bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    
    if (bytes_received > 0) {
        buffer[bytes_received] = '\0';
        std::cout << "✓ HTTP response received (" << bytes_received << " bytes)" << std::endl;
        
        // Show first line of response
        std::string response(buffer);
        size_t first_line_end = response.find('\n');
        if (first_line_end != std::string::npos) {
            std::cout << "  Response: " << response.substr(0, first_line_end) << std::endl;
        }
        
        close(sock);
        return true;
    } else if (bytes_received == 0) {
        std::cout << "✓ Connection established, but server closed connection (no HTTP implementation?)" << std::endl;
        close(sock);
        return true;  // Connection worked, just no HTTP
    } else {
        std::cout << "✗ Failed to receive HTTP response: " << strerror(errno) << std::endl;
        close(sock);
        return false;
    }
}

// Print server information
void print_server_info() {
    std::cout << "\n=== Blackbird Keystone Server REAL Test ===" << std::endl;
    std::cout << "Testing ACTUAL socket connections to Keystone server..." << std::endl;
    std::cout << "Expected server endpoints:" << std::endl;
    std::cout << "  RPC Port: 9090 (YLT struct_pack)" << std::endl;
    std::cout << "  HTTP Port: 9091 (metrics)" << std::endl;
    std::cout << "===========================================" << std::endl;
}

int main(int argc, char* argv[]) {
    // Initialize logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_alsologtostderr = 1;
    FLAGS_colorlogtostderr = 1;
    
    print_server_info();
    
    std::string host = "127.0.0.1";
    int rpc_port = 9090;
    int http_port = 9091;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--rpc-port" && i + 1 < argc) {
            rpc_port = std::stoi(argv[++i]);
        } else if (arg == "--http-port" && i + 1 < argc) {
            http_port = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "  --host <host>        Server host (default: 127.0.0.1)" << std::endl;
            std::cout << "  --rpc-port <port>    RPC port (default: 9090)" << std::endl;
            std::cout << "  --http-port <port>   HTTP port (default: 9091)" << std::endl;
            return 0;
        }
    }
    
    bool rpc_success = false;
    bool http_success = false;
    
    // Test RPC port connectivity
    std::cout << "\n1. Testing RPC port connectivity..." << std::endl;
    rpc_success = test_server_connectivity(host, rpc_port);
    
    // Test HTTP port connectivity  
    std::cout << "\n2. Testing HTTP port connectivity..." << std::endl;
    http_success = test_http_endpoint(host, http_port);
    
    // Print summary
    std::cout << "\n=== REAL Test Summary ===" << std::endl;
    std::cout << "RPC Port (9090): " << (rpc_success ? "✓ WORKING" : "✗ FAILED") << std::endl;
    std::cout << "HTTP Port (9091): " << (http_success ? "✓ WORKING" : "✗ FAILED") << std::endl;
    
    if (rpc_success && http_success) {
        std::cout << "\n🎉 ALL TESTS PASSED! Server is running and accessible." << std::endl;
    } else if (rpc_success) {
        std::cout << "\n⚠️  RPC server is running, but HTTP metrics might not be implemented." << std::endl;
    } else {
        std::cout << "\n❌ Server appears to be DOWN or not listening on expected ports." << std::endl;
        std::cout << "\nTo start the server:" << std::endl;
        std::cout << "1. etcd --listen-client-urls=http://localhost:2379 --advertise-client-urls=http://localhost:2379" << std::endl;
        std::cout << "2. ./keystone_example --etcd-endpoints localhost:2379" << std::endl;
    }
    
    return (rpc_success || http_success) ? 0 : 1;
} 