#include "rdma_connection_pool.h"
#include <iostream>
#include <chrono>
#include <algorithm>
#include <iomanip>

namespace diskann {

RDMAConnectionPool::RDMAConnectionPool(const char* server_addr, int port, size_t pool_size, size_t buffer_size)
    : server_addr_(server_addr), server_port_(port), pool_size_(pool_size), 
      buffer_size_(buffer_size), initialized_(false) {
    
    std::cout << "Creating RDMA connection pool:" << std::endl;
    std::cout << "  Server: " << server_addr_ << ":" << server_port_ << std::endl;
    std::cout << "  Pool size: " << pool_size_ << std::endl;
    std::cout << "  Buffer size: " << (buffer_size_ / 1024) << " KB per connection" << std::endl;
}

RDMAConnectionPool::~RDMAConnectionPool() {
    // 停止健康检查
    health_check_running_ = false;
    if (health_check_thread_.joinable()) {
        health_check_thread_.join();
    }
    
    // 清理所有连接
    std::lock_guard<std::mutex> lock(pool_mutex_);
    connections_.clear();
    while (!available_connections_.empty()) {
        available_connections_.pop();
    }
    
    std::cout << "RDMA connection pool destroyed" << std::endl;
}

bool RDMAConnectionPool::initialize() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    
    if (initialized_) {
        return true;
    }
    
    std::cout << "Initializing RDMA connection pool..." << std::endl;
    
    // 创建所有连接
    // Buffer must hold at least MAX_IN_FLIGHT_BATCHES * BATCH_SLOT_SIZE (256KB for right-sized config)
    size_t min_buf = RDMAConnection::MAX_IN_FLIGHT_BATCHES * RDMAConnection::BATCH_SLOT_SIZE;
    size_t conn_buf_size = std::max(buffer_size_, min_buf);
    std::cout << "  Buffer size per connection: " << (conn_buf_size / 1024) << " KB"
              << " (" << RDMAConnection::MAX_IN_FLIGHT_BATCHES << " slots x "
              << (RDMAConnection::BATCH_SLOT_SIZE / 1024) << " KB)" << std::endl;
    for (size_t i = 0; i < pool_size_; ++i) {
        std::cout << "Creating connection " << (i + 1) << "/" << pool_size_ << "..." << std::endl;
        auto conn = std::make_shared<RDMAConnection>(server_addr_.c_str(), server_port_, conn_buf_size);
        if (conn->connect_to_server() != 0) {
            std::cerr << "Failed to create connection " << (i + 1) << std::endl;
            return false;
        }
        
        
        connections_.push_back(conn);
        available_connections_.push(conn);
        
        std::cout << "✓ Connection " << (i + 1) << " created successfully" << std::endl;
    }
    
    initialized_ = true;
    
    // 启动健康检查线程
    health_check_running_ = true;
    health_check_thread_ = std::thread(&RDMAConnectionPool::health_check_worker, this);
    
    size_t total_registered = pool_size_ * conn_buf_size;
    std::cout << "✓ RDMA connection pool initialized: " << pool_size_ << " connections, "
              << (total_registered / 1024) << " KB total registered memory"
              << " (" << (conn_buf_size / 1024) << " KB each)" << std::endl;
    return true;
}

// bool RDMAConnectionPool::initialize() {
//     std::lock_guard<std::mutex> lock(pool_mutex_);
    
//     if (initialized_) {
//         return true;
//     }
    
//     std::cout << "Initializing RDMA connection pool..." << std::endl;
    
//     // Create the first connection to get protection domain
//     // auto first_conn = create_connection();
//     std::cout << "Step 1: Creating temporary connection to extract Protection Domain..." << std::endl;
//     auto first_conn = create_connection_for_pd();
//     if (!first_conn) {
//         std::cerr << "Failed to create initial connection for PD" << std::endl;
//         return false;
//     }
    
//     // Create shared buffer manager using the protection domain
//     std::cout << "Step 2: Creating shared buffer manager with extracted PD..." << std::endl;
//     buffer_manager_ = std::make_shared<RDMABufferManager>(first_conn->get_pd());
//     if (!buffer_manager_->initialize()) {
//         std::cerr << "Failed to initialize buffer manager" << std::endl;
//         return false;
//     }
//         // Recreate the first connection with buffer manager
//     std::cout << "Step 3: Creating pool connections with shared buffer manager..." << std::endl;
//     first_conn.reset(); // Destroy the temporary connection
//     // first_conn = create_connection();
//     // if (!first_conn) {
//     //     std::cerr << "Failed to recreate first connection with buffer manager" << std::endl;
//     //     return false;
//     // }
//     // // Now create all connections with shared buffer manager
//     // connections_.push_back(first_conn);
//     // available_connections_.push(first_conn);
    
//     for (size_t i = 1; i < pool_size_; ++i) {
//         std::cout << "Creating connection " << (i + 1) << "/" << pool_size_ << "..." << std::endl;
        
//         auto conn = std::make_shared<RDMAConnection>(server_addr_.c_str(), server_port_, buffer_manager_);
//         if (conn->connect_to_server() != 0) {
//             std::cerr << "Failed to create connection " << (i + 1) << std::endl;
//             return false;
//         }

//         // auto conn = create_connection();
//         // if (!conn) {
//         //     std::cerr << "Failed to create connection " << (i + 1) << std::endl;
//         //     return false;
//         // }
//         connections_.push_back(conn);
//         available_connections_.push(conn);
        
//         std::cout << "✓ Connection " << (i + 1) << " created successfully" << std::endl;
//     }
    
//     initialized_ = true;
//     std::cout << "✓ RDMA connection pool initialized with shared buffer manager" << std::endl;
//     // buffer_manager_->print_stats();
    
//     return true;
// }

std::shared_ptr<RDMAConnection> RDMAConnectionPool::create_connection() {
    try {
        auto conn = std::make_shared<RDMAConnection>(server_addr_.c_str(), server_port_, buffer_size_);
        // auto conn = std::make_shared<RDMAConnection>(server_addr_.c_str(), server_port_, buffer_manager_);
        
        if (conn->connect_to_server() != 0) {
            std::cerr << "Failed to connect to server" << std::endl;
            return nullptr;
        }
        
        return conn;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception creating connection: " << e.what() << std::endl;
        return nullptr;
    }
}

// BUG FIX: Add missing create_connection_for_pd function
// std::shared_ptr<RDMAConnection> RDMAConnectionPool::create_connection_for_pd() {
//     // Create a temporary connection just to get the protection domain
//     auto temp_conn = std::make_shared<RDMAConnection>(server_addr_.c_str(), server_port_, nullptr);
//     if (temp_conn->connect_to_server() != 0) {
//         std::cerr << "Failed to create temporary connection for PD" << std::endl;
//         return nullptr;
//     }
//     return temp_conn;
// }

ConnectionWrapper RDMAConnectionPool::get_connection(std::chrono::milliseconds timeout) {
    auto start_time = std::chrono::steady_clock::now();
    
    std::unique_lock<std::mutex> lock(pool_mutex_);
    
    // 等待可用连接
    bool got_connection = pool_cv_.wait_for(lock, timeout, [this] {
        return !available_connections_.empty();
    });
    
    auto wait_time = std::chrono::steady_clock::now() - start_time;
    auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(wait_time).count();
    
    if (!got_connection) {
        stats_.connection_timeouts++;
        std::cerr << "Timeout waiting for connection after " << wait_ms << "ms" << std::endl;
        return ConnectionWrapper(nullptr, [](std::shared_ptr<RDMAConnection>) {});
    }
    
    if (wait_ms > 0) {
        stats_.connection_waits++;
        // 更新平均等待时间（简单的移动平均）
        double current_avg = stats_.avg_wait_time_ms.load();
        stats_.avg_wait_time_ms = (current_avg * 0.9 + wait_ms * 0.1);
    }
    
    auto conn = available_connections_.front();
    available_connections_.pop();
    
    // 创建自动归还的包装器
    auto return_func = [this](std::shared_ptr<RDMAConnection> c) {
        this->return_connection(c);
    };
    
    return ConnectionWrapper(conn, return_func);
}

ConnectionWrapper RDMAConnectionPool::get_connection_blocking() {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    
    // 阻塞等待可用连接
    pool_cv_.wait(lock, [this] {
        return !available_connections_.empty();
    });
    
    auto conn = available_connections_.front();
    available_connections_.pop();
    
    auto return_func = [this](std::shared_ptr<RDMAConnection> c) {
        this->return_connection(c);
    };
    
    return ConnectionWrapper(conn, return_func);
}

// void* RDMAConnectionPool::allocate_registered_buffer(size_t size) {
//     if (!buffer_manager_) {
//         return nullptr;
//     }
//     return buffer_manager_->get_buffer_segment(size);
// }

// void RDMAConnectionPool::free_registered_buffer(void* buffer) {
//     if (buffer_manager_) {
//         buffer_manager_->return_buffer_segment(buffer);
//     }
// }

// bool RDMAConnectionPool::is_registered_buffer(void* buf) const {
//     if (!buffer_manager_) {
//         return false;
//     }
//     return buffer_manager_->is_managed_buffer(buf);
// }

void RDMAConnectionPool::return_connection(std::shared_ptr<RDMAConnection> conn) {
    if (!conn) return;
    
    // 简单的健康检查
    if (!conn->is_connected()) {
        std::cerr << "Returned connection is not healthy, discarding" << std::endl;
        return;
    }
    
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        available_connections_.push(conn);
    }
    
    pool_cv_.notify_one();
}

size_t RDMAConnectionPool::available_count() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(pool_mutex_));
    return available_connections_.size();
}

bool RDMAConnectionPool::is_healthy() const {
    return initialized_ && available_count() > 0;
}

char* RDMAConnectionPool::read_vector_direct(uint64_t internal_id, std::chrono::milliseconds timeout) {
    auto conn_wrapper = get_connection(timeout);
    if (!conn_wrapper.get()) {
        return nullptr;
    }
    
    stats_.total_reads++;
    
    char* result = conn_wrapper->rdma_read_vector_data_direct(internal_id);
    
    // 连接会在conn_wrapper析构时自动归还
    return result;
}

bool RDMAConnectionPool::batch_read_vectors(const std::vector<uint64_t>& internal_ids, 
                                          std::vector<char*>& results,
                                          std::chrono::milliseconds timeout) {
    results.clear();
    results.reserve(internal_ids.size());
    
    // 计算每个连接处理的任务数
    size_t batch_size = std::max((size_t)1, internal_ids.size() / pool_size_);
    std::vector<std::thread> workers;
    std::vector<std::vector<char*>> worker_results(pool_size_);
    std::atomic<bool> success{true};
    
    for (size_t i = 0; i < pool_size_ && success; ++i) {
        size_t start_idx = i * batch_size;
        size_t end_idx = (i == pool_size_ - 1) ? internal_ids.size() : (i + 1) * batch_size;
        
        if (start_idx >= internal_ids.size()) break;
        
        workers.emplace_back([this, &internal_ids, &worker_results, i, start_idx, end_idx, timeout, &success]() {
            auto conn_wrapper = get_connection(timeout);
            if (!conn_wrapper.get()) {
                success = false;
                return;
            }
            
            for (size_t j = start_idx; j < end_idx; ++j) {
                char* result = conn_wrapper->rdma_read_vector_data_direct(internal_ids[j]);
                if (!result) {
                    success = false;
                    return;
                }
                worker_results[i].push_back(result);
                stats_.total_reads++;
            }
        });
    }
    
    // 等待所有工作线程完成
    for (auto& worker : workers) {
        worker.join();
    }
    
    if (!success) {
        return false;
    }
    
    // 合并结果
    for (const auto& worker_result : worker_results) {
        results.insert(results.end(), worker_result.begin(), worker_result.end());
    }
    
    return true;
}

// 实现直接读取方法
bool RDMAConnectionPool::rdma_read_direct(uint64_t remote_offset, size_t size, void* dest_buf, 
                                        std::chrono::milliseconds timeout) {
    auto conn_wrapper = get_connection(timeout);
    if (!conn_wrapper.get()) {
        return false;
    }
    
    stats_.total_reads++;
    return (conn_wrapper->rdma_read(dest_buf, size, remote_offset) == 0);
}

bool RDMAConnectionPool::batch_rdma_read(const std::vector<std::pair<uint64_t, size_t>>& read_requests,
                                       std::vector<void*>& dest_buffers,
                                       std::chrono::milliseconds timeout) {
    if (read_requests.size() != dest_buffers.size()) {
        return false;
    }
    
    // 并行处理大批量读取
    size_t num_workers = std::min(read_requests.size(), pool_size_);
    size_t reqs_per_worker = (read_requests.size() + num_workers - 1) / num_workers;
    
    std::vector<std::thread> workers;
    std::atomic<bool> success{true};
    
    for (size_t i = 0; i < num_workers && success; ++i) {
        size_t start_idx = i * reqs_per_worker;
        size_t end_idx = std::min((i + 1) * reqs_per_worker, read_requests.size());
        
        if (start_idx >= read_requests.size()) break;
        
        workers.emplace_back([this, &read_requests, &dest_buffers, start_idx, end_idx, timeout, &success]() {
            auto conn_wrapper = get_connection(timeout);
            if (!conn_wrapper.get()) {
                success = false;
                return;
            }
            
            for (size_t j = start_idx; j < end_idx && success; ++j) {
                const auto& req = read_requests[j];
                if (conn_wrapper->rdma_read(dest_buffers[j], req.second, req.first) != 0) {
                    success = false;
                    return;
                }
                stats_.total_reads++;
            }
        });
    }
    
    for (auto& worker : workers) {
        worker.join();
    }
    
    return success.load();
}

void RDMAConnectionPool::health_check_worker() {
    while (health_check_running_) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        
        if (!health_check_running_) break;
        
        // 简单的健康检查：确保有可用连接
        size_t avail = available_count();
        if (avail == 0) {
            // This is normal during search — all threads hold their connections
            // via tls_conn_cache. Only a problem if get_connection() blocks.
            std::cerr << "[RDMA Pool] All " << pool_size_
                      << " connections in use (normal during search)" << std::endl;
        }
        
        // 更详细的健康检查可以在这里添加
    }
}

bool RDMAConnectionPool::is_connection_healthy(std::shared_ptr<RDMAConnection> conn) {
    if (!conn || !conn->is_connected()) {
        return false;
    }
    
    // 可以添加更多健康检查，如ping测试等
    return true;
}

void RDMAConnectionPool::print_stats() const {
    std::cout << "\n=== RDMA Connection Pool Statistics ===" << std::endl;
    std::cout << "Total reads: " << stats_.total_reads.load() << std::endl;
    std::cout << "Total writes: " << stats_.total_writes.load() << std::endl;
    std::cout << "Connection waits: " << stats_.connection_waits.load() << std::endl;
    std::cout << "Connection timeouts: " << stats_.connection_timeouts.load() << std::endl;
    std::cout << "Average wait time: " << std::fixed << std::setprecision(2) 
              << stats_.avg_wait_time_ms.load() << " ms" << std::endl;
    std::cout << "Available connections: " << available_count() << "/" << total_count() << std::endl;
    std::cout << "=========================================" << std::endl;
}

void RDMAConnectionPool::reset_stats() {
    stats_.total_reads = 0;
    stats_.total_writes = 0;
    stats_.connection_waits = 0;
    stats_.connection_timeouts = 0;
    stats_.avg_wait_time_ms = 0.0;
}

} // namespace hnswlib