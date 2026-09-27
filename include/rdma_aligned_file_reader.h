// #pragma once

// #include "aligned_file_reader.h"
// #include "rdma_client.h"
// #include <memory>
// #include <string>

// namespace diskann {

// class RDMAAlignedFileReader : public AlignedFileReader {
// private:
//     std::unique_ptr<RDMAClient> rdma_client_;
//     std::string server_ip_;
//     uint16_t server_port_;
//     size_t file_size_;
//     std::string index_prefix_;
    
// public:
//     RDMAAlignedFileReader(const std::string& server_ip, uint16_t port);
//     ~RDMAAlignedFileReader();
    
//     // AlignedFileReader interface
//     IOContext& get_ctx() override;
//     void open(const std::string& filename) override;
//     void close() override;
//     void register_thread() override;
//     void deregister_thread() override;
//     void deregister_all_threads() override;
    
//     void read(std::vector<AlignedRead>& read_reqs, IOContext& ctx, bool async = false) override;
//     // void wait(IOContext& ctx) override;  // Add back int& n_read parameter
//     // bool async_read_supported() override { return false; }  // Remove const
    
// private:
//     thread_local static IOContext* thread_ctx_;
// };

// }



//old multi threaded RDMA reader
// #pragma once

// #include "aligned_file_reader.h"
// #include <rdma/rdma_cma.h>
// #include <memory>
// #include <string>
// #include <mutex>
// #include <unordered_map>
// #include <thread>


// namespace diskann {


// // Create RDMA-specific IOContext (ADD THIS)
// struct RDMAIOContext {
//     std::thread::id thread_id;
//     bool initialized;
    
//     // RDMA-specific fields
//     rdma_cm_id* connection_id;
//     ibv_mr* memory_region;
//     void* buffer;
    
//     RDMAIOContext() : initialized(false), connection_id(nullptr), 
//                      memory_region(nullptr), buffer(nullptr) {}
// };

// // Use typedef to match the expected interface (ADD THIS)
// // using IOContext = RDMAIOContext;

// class RDMAAlignedFileReader : public AlignedFileReader {
// public:
//     RDMAAlignedFileReader(const std::string& server_ip, uint16_t port);
//     ~RDMAAlignedFileReader();
    
//     // Implement AlignedFileReader interface
//     IOContext& get_ctx() override;
//     void open(const std::string& fname) override;
//     uint64_t get_file_size(const std::string& fname);
//     void read(std::vector<AlignedRead>& read_reqs, IOContext& ctx, bool async = false) override;
//     void close() override;

//     void register_thread() override;
//     void deregister_thread() override;
//     void deregister_all_threads() override;
    
// private:
//     std::string server_ip_;
//     uint16_t port_;
//     //old single thread
//     // rdma_cm_id* id_;
//     // ibv_mr* mr_;
//     // void* buffer_;
//     // uint64_t remote_addr_;
//     // uint32_t remote_rkey_;
//     // uint64_t file_size_;
//     // bool connected_;

//     // New multi-threaded support
//     // Shared connection info (read-only after initialization)
//     rdma_cm_id* main_id_;
//     uint64_t base_remote_addr_;  // Base address, never changes
//     uint32_t remote_rkey_;
//     uint64_t file_size_;
//     bool connected_;
//     std::mutex connection_mutex_;
    
//     // Per-thread data structure
//     struct ThreadData {
//         rdma_cm_id* id;          // Share the main connection
//         ibv_mr* mr;              // Per-thread memory registration
//         void* buffer;            // Per-thread buffer
//         std::thread::id thread_id;
//         RDMAIOContext rdma_ctx;    // Use your RDMA context
//         IOContext io_ctx;          // Use base class IOContext (likely a pointer)          // Per-thread IOContext
//     };
    
//     // Thread management - similar to LinuxAlignedFileReader
//     static thread_local ThreadData* thread_data_;
//     static std::unordered_map<std::thread::id, std::unique_ptr<ThreadData>> thread_map_;
//     static std::mutex thread_map_mutex_;

    
//     void connect_to_server();
//     void disconnect();

//     void setup_thread_connection();
//     void cleanup_thread_connection();

//     void rdma_read_sectors(uint64_t offset, size_t size, void* dest_buf);
// };

// }


// #pragma once
// #include "aligned_file_reader.h"
// #include "rdma_connection_pool.h"
// #include <string>
// #include <memory>
// #include <mutex>

// namespace diskann {

// class RDMAAlignedFileReader : public AlignedFileReader {
// private:
//     std::string server_ip_;
//     uint16_t port_;
    
//     // 使用连接池替代单独的线程管理
//     std::unique_ptr<RDMAConnectionPool> rdma_pool_;
    
//     // 文件元数据
//     uint64_t file_size_;
//     bool metadata_parsed_;
//     std::once_flag metadata_flag_;
    
//     // 连接池配置
//     size_t pool_size_;
//     size_t buffer_size_;
    
//     // 移除原有的线程管理相关成员
//     // ThreadData, thread_map_ 等都不再需要

// public:
//     RDMAAlignedFileReader(const std::string& server_ip, uint16_t port, 
//                          size_t pool_size = 8, size_t buffer_size = 2 * 1024 * 1024);
//     ~RDMAAlignedFileReader();
    
//     // AlignedFileReader接口实现
//     IOContext& get_ctx() override;
//     void register_thread() override;
//     void deregister_thread() override;
//     void deregister_all_threads() override;
    
//     void open(const std::string& fname) override;
//     void close() override;
//     uint64_t get_file_size(const std::string& fname);
    
//     void read(std::vector<AlignedRead>& read_reqs, IOContext& ctx, bool async = false) override;
    
// private:
//     void parse_metadata_once();
//     void rdma_read_sectors(uint64_t offset, size_t size, void* dest_buf);
    
//     // 简化的IOContext管理
//     thread_local static IOContext* thread_ctx_;
// };

// } // namespace diskann


#pragma once

#include "aligned_file_reader.h"
#include "rdma_connection_pool.h"
#include <string>
#include <memory>
#include <mutex>
#include <future>
#include <unordered_map>
#include <chrono>
#include <atomic>

namespace diskann {

struct IORequest {
    uint64_t offset;
    size_t len;
    void* buf;
    std::atomic<bool> finished{false};
    
    IORequest() = default;
    IORequest(uint64_t off, size_t length, void* buffer) 
        : offset(off), len(length), buf(buffer), finished(false) {}
};

class RDMAAlignedFileReader : public AlignedFileReader {
private:
    std::string server_ip_;
    uint16_t port_;
    
    // Use your existing connection pool
    std::unique_ptr<RDMAConnectionPool> rdma_pool_;
    
    // File metadata
    uint64_t file_size_;
    bool metadata_parsed_;
    std::once_flag metadata_flag_;
    
    // Connection pool configuration
    size_t pool_size_;
    size_t buffer_size_;

    // Multi-shard support (0 = legacy single-server mode)
    size_t num_shards_ = 0;
    uint64_t shard_size_ = 0;  // bytes per shard
    std::vector<std::unique_ptr<RDMAConnectionPool>> shard_pools_;

        // For async I/O support
    std::atomic<unsigned> next_request_id_{0};
    struct PendingRequest {
        IORequest* original_req;
        std::chrono::steady_clock::time_point submit_time;
        std::future<void> future;
    };
    
    std::mutex pending_requests_mutex_;
    std::unordered_map<unsigned, PendingRequest> pending_requests_;
    
    // Remove old thread management members
    // ThreadData, thread_map_, etc. are no longer needed
        // NEW: Global throughput tracking
    

public:
    RDMAAlignedFileReader(const std::string& server_ip, uint16_t port,
                         size_t pool_size = 1, size_t buffer_size = 2 * 1024 * 1024);

    // Multi-server sharded constructor
    RDMAAlignedFileReader(const std::vector<std::pair<std::string, uint16_t>>& servers,
                         uint64_t total_index_size,
                         size_t pool_size = 1, size_t buffer_size = 2 * 1024 * 1024);

    ~RDMAAlignedFileReader();
    
    // AlignedFileReader interface implementation
    IOContext& get_ctx() override;
    void register_thread() override;
    void deregister_thread() override;
    void deregister_all_threads() override;
    
    void open(const std::string& fname) override;
    void close() override;
    // uint64_t get_file_size(const std::string& fname) override;
    uint64_t get_file_size() const { return file_size_; }
    bool is_metadata_parsed() const { return metadata_parsed_; }
    
    void read(std::vector<AlignedRead>& read_reqs, IOContext& ctx, bool async = false) override;
    
    void read_async(std::vector<AlignedRead>& read_reqs, IOContext& ctx, bool async, uint64_t* finished_flag) override;
    void print_thread_stats();
    void print_perf_diag();  // [PERF-DIAG] Print efficiency diagnostics

        // Simple RDMA buffer management - no wrappers needed
    // char* get_rdma_registered_buffer(size_t size);
    // void return_rdma_registered_buffer(char* buffer, size_t size);
    // bool is_rdma_registered_buffer(void* buf);
    // Override virtual functions with empty implementations
    int submit_reqs(std::vector<AlignedRead> &read_reqs, IOContext &ctx) override {
        // Empty implementation for RDMA - we handle reads differently
        return 0;
    }
    
    void get_events(IOContext &ctx, int n_ops) override {
        // Empty implementation for RDMA - we handle events differently
        // No-op since RDMA operations are synchronous in our implementation
    }

    // Add this simple async method - reuses your existing read logic
    // void send_read_no_alloc(IORequest& req, IOContext& ctx);
    // void poll_all(IOContext& ctx);

    // Override the virtual methods
    void poll_all(IOContext& ctx) override;
    void send_read_no_alloc(IORequest& req, IOContext& ctx);

    // void send_read_no_alloc(IORequest& req, IOContext& ctx);
    // void poll_all(IOContext& ctx);
    
private:
    void parse_metadata_once();
    void rdma_read_sectors(uint64_t offset, size_t size, void* dest_buf);
    void rdma_read_sectors_async(uint64_t offset, size_t size, void* dest_buf);
    
    // Simplified IOContext management
    thread_local static IOContext* thread_ctx_;
};

} // namespace diskann