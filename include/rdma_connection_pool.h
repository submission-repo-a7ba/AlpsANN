

#pragma once
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <functional>
#include <atomic>
#include <thread>
#include "rdma_connection.h"

namespace diskann {

// 连接池统计信息
struct PoolStats {
    std::atomic<uint64_t> total_reads{0};
    std::atomic<uint64_t> total_writes{0};
    std::atomic<uint64_t> connection_waits{0};
    std::atomic<uint64_t> connection_timeouts{0};
    std::atomic<double> avg_wait_time_ms{0.0};
};

// 连接包装器，用于自动归还连接
class ConnectionWrapper {
private:
    std::shared_ptr<RDMAConnection> connection_;
    std::function<void(std::shared_ptr<RDMAConnection>)> return_func_;
    bool returned_;

public:
    ConnectionWrapper(std::shared_ptr<RDMAConnection> conn, 
                     std::function<void(std::shared_ptr<RDMAConnection>)> return_func)
        : connection_(conn), return_func_(return_func), returned_(false) {}
    
    ~ConnectionWrapper() {
        if (!returned_ && connection_) {
            return_func_(connection_);
        }
    }
    
    // 禁止拷贝，只允许移动
    ConnectionWrapper(const ConnectionWrapper&) = delete;
    ConnectionWrapper& operator=(const ConnectionWrapper&) = delete;
    
    ConnectionWrapper(ConnectionWrapper&& other) noexcept 
        : connection_(std::move(other.connection_)), 
          return_func_(std::move(other.return_func_)),
          returned_(other.returned_) {
        other.returned_ = true;
    }
    
    ConnectionWrapper& operator=(ConnectionWrapper&& other) noexcept {
        if (this != &other) {
            if (!returned_ && connection_) {
                return_func_(connection_);
            }
            connection_ = std::move(other.connection_);
            return_func_ = std::move(other.return_func_);
            returned_ = other.returned_;
            other.returned_ = true;
        }
        return *this;
    }
    
    RDMAConnection* operator->() { return connection_.get(); }
    RDMAConnection& operator*() { return *connection_; }
    RDMAConnection* get() { return connection_.get(); }

        // Add just this one method
    // bool is_buffer_registered(void* buf) const {
    //     // Simple check - implement this based on your pool
    //     return false; // For now, will enhance later
    // }
        // Direct read method using shared buffer
    int rdma_read_direct(void* dest_buf, size_t size, uint64_t remote_offset) {
        if (!connection_) return -1;
        return connection_->rdma_read_direct(dest_buf, size, remote_offset);
    }


    int rdma_read_direct_async(void* dest_buf, size_t size, uint64_t remote_offset) {
        if (!connection_) return -1;
        return connection_->rdma_read_direct_async(dest_buf, size, remote_offset);
    }

    // Compatibility read method
    int rdma_read(void* dest_buf, size_t size, uint64_t remote_offset) {
        if (!connection_) return -1;
        return connection_->rdma_read(dest_buf, size, remote_offset);
    }
    // 手动归还连接
    void return_connection() {
        if (!returned_ && connection_) {
            return_func_(connection_);
            returned_ = true;
        }
    }
};

class RDMAConnectionPool {
private:
    std::vector<std::shared_ptr<RDMAConnection>> connections_;
    std::queue<std::shared_ptr<RDMAConnection>> available_connections_;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    
    std::string server_addr_;
    int server_port_;
    size_t pool_size_;
    size_t buffer_size_;
    bool initialized_;
    
    PoolStats stats_;
    
    // 连接健康检查
    std::atomic<bool> health_check_running_{false};
    std::thread health_check_thread_;

        // Simple buffer management - just add these few lines
    // std::vector<std::pair<void*, size_t>> registered_buffers_;
    // std::mutex buffer_mutex_;
    // std::shared_ptr<RDMABufferManager> buffer_manager_;
    
public:
    RDMAConnectionPool(const char* server_addr, int port, size_t pool_size = 1, size_t buffer_size = 256 * 1024);
    ~RDMAConnectionPool();
    
    // 初始化连接池
    bool initialize();
    
    // 获取连接（带超时）
    ConnectionWrapper get_connection(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    
    // 获取连接（阻塞）
    ConnectionWrapper get_connection_blocking();
    
    // 归还连接到池中
    void return_connection(std::shared_ptr<RDMAConnection> conn);
    
    // 池状态查询
    size_t available_count() const;
    size_t total_count() const { return pool_size_; }
    bool is_healthy() const;
    
    // 统计信息
    const PoolStats& get_stats() const { return stats_; }
    void print_stats() const;
    void reset_stats();
    
    // 便捷的读取方法
    char* read_vector_direct(uint64_t internal_id, std::chrono::milliseconds timeout = std::chrono::milliseconds(1000));
    
    // 批量读取
    bool batch_read_vectors(const std::vector<uint64_t>& internal_ids, 
                           std::vector<char*>& results,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

        // 直接RDMA读取（用于文件读取）
    bool rdma_read_direct(uint64_t remote_offset, size_t size, void* dest_buf, 
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(1000));
    
    // 批量读取请求
    bool batch_rdma_read(const std::vector<std::pair<uint64_t, size_t>>& read_requests,
                        std::vector<void*>& dest_buffers,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

        // Add just these 3 methods
    // void* allocate_registered_buffer(size_t size);
    // void free_registered_buffer(void* buffer);
    // bool is_registered_buffer(void* buf) const;
    // std::shared_ptr<RDMABufferManager> get_buffer_manager() const { return buffer_manager_; }

private:
    // 创建单个连接
    std::shared_ptr<RDMAConnection> create_connection();
    
    // std::shared_ptr<RDMAConnection> create_connection_for_pd();
    // 健康检查线程
    void health_check_worker();
    
    // 验证连接是否健康
    bool is_connection_healthy(std::shared_ptr<RDMAConnection> conn);
};

} // namespace hnswlib

