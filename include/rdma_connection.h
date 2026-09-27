

#pragma once
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <string>
#include <stdexcept>
#include <vector>
#include <queue>
#include <unordered_map>
#include <atomic>
#include <array>
// #include "rdma_buffer_manager.h"

namespace diskann {

  // 远程内存信息结构（与服务器匹配）
  struct RemoteMR {
    uint64_t addr;
    uint32_t rkey;
    uint64_t size;
  } __attribute__((packed));

  // 向量数据布局信息
  struct VectorDataLayout {
    uint64_t data_start_offset;  // 向量数据在远程内存中的起始偏移
    uint64_t num_vectors;        // 向量总数
    uint64_t aligned_dim;        // 对齐后的维度
    uint64_t element_size;  // 每个向量的字节大小 (aligned_dim * sizeof(data_t))
    uint64_t total_size;    // 总数据大小
    uint64_t remote_addr;   // 远程内存基址
    uint32_t remote_rkey;   // 远程内存key
    uint64_t available_size;
    bool     layout_valid;  // 布局是否有效
  };

  struct RDMAReadRequest {
    void*    local_buf;
    size_t   size;
    uint64_t remote_offset;
  };

//   struct BatchInfo {
//     uint64_t start_wr_id;
//     uint64_t end_wr_id;
//     std::vector<RDMAReadRequest*> requests;
//     char* base_buffer_offset;
//     bool* completed;
// };
struct BatchInfo {
    uint64_t start_wr_id;
    uint64_t end_wr_id;
    char* base_buffer_offset;
    uint64_t* completed;
    uint64_t remote_offset;
    size_t batch_buffer_offset;  // Offset within the registered buffer
    size_t total_size;           // Total size of this batch

    struct RequestInfo {
        void* local_buf;
        size_t size;
    };

    // Fixed-size array instead of vector (no heap allocation)
    static constexpr size_t MAX_REQUESTS_PER_BATCH = 32;
    RequestInfo requests[MAX_REQUESTS_PER_BATCH];
    size_t num_requests;

    BatchInfo() : start_wr_id(0), end_wr_id(0),
                  base_buffer_offset(nullptr), completed(nullptr), num_requests(0) {}
};

  // 添加向量数据元数据结构（与服务器端保持一致）
  struct VectorDataMetadata {
    uint64_t num_vectors;         // 向量总数
    uint64_t aligned_dim;         // 对齐后的维度
    uint64_t element_size;        // 每个向量的字节大小
    uint64_t data_type_size;      // 数据类型大小
    uint64_t total_data_size;     // 总数据大小
    uint64_t data_start_offset;   // 向量数据在缓冲区中的起始偏移
    bool     metadata_valid;      // 元数据是否有效
    char     data_type_name[16];  // 数据类型名称
  } __attribute__((packed));

  class RDMAConnection {
   public:
    // Right-sized for 1-connection-per-thread: BW<=8 means <=8 in-flight batches,
    // each batch is typically 1-4 sectors (4-16KB). 16 slots with 16KB each = 256KB buffer.
    static constexpr size_t MAX_IN_FLIGHT_BATCHES = 32;   // Max concurrent batches per connection
    static constexpr size_t BATCH_SLOT_SIZE = 32 * 1024;  // 32KB per slot (R=128: 644B/node × 32 = 20KB)

   private:
    struct rdma_cm_id*         cm_id;
    struct rdma_event_channel* ec;
    struct ibv_context*        context;
    struct ibv_pd*             pd;
    struct ibv_mr*             mr;
    struct ibv_qp*             qp;
    struct ibv_cq*             cq;
    void*                      local_buffer;
    void*                      remote_addr;
    uint32_t                   remote_rkey;
    size_t                     buffer_size;
    bool                       connected;
    VectorDataLayout           vector_layout_;  // 远程布局信息

    // Fixed-size array for pending batches (lock-free, indexed by slot)
    std::array<BatchInfo, MAX_IN_FLIGHT_BATCHES> pending_batches_;
    std::array<std::atomic<bool>, MAX_IN_FLIGHT_BATCHES> batch_slot_active_;  // Track active slots

    std::atomic<uint64_t> next_wr_id_{1};     // Atomic for thread-safety
    std::atomic<size_t> next_batch_slot_{0};  // Atomic for thread-safety

    // [PERF-DIAG] Efficiency diagnostic counters
    std::atomic<uint64_t> diag_total_memcpy_bytes_{0};   // total bytes copied in poll_and_copy
    std::atomic<uint64_t> diag_total_memcpy_ns_{0};      // total ns spent in memcpy
    std::atomic<uint64_t> diag_total_poll_calls_{0};      // total poll_and_copy calls
    std::atomic<uint64_t> diag_total_completions_{0};     // total WCs processed
    std::atomic<uint64_t> diag_total_slot_waste_bytes_{0}; // wasted bytes in batch slots
    std::atomic<uint64_t> diag_total_batches_posted_{0};  // total batches posted
    struct SlotPostInfo {
        std::atomic<uint64_t> posting_thread{0};  // thread that posted this slot
    };
    std::array<SlotPostInfo, MAX_IN_FLIGHT_BATCHES> diag_slot_posters_;
    std::atomic<uint64_t> diag_cross_thread_polls_{0};    // completions polled by non-posting thread

    // Add reference to shared buffer manager
    // std::shared_ptr<RDMABufferManager> buffer_manager_;

   public:
    // RDMAConnection(const char* server_addr, int port, size_t buf_size =
    // 1024*1024);
    RDMAConnection(const char* server_addr, int port,
                   size_t buffer_size = MAX_IN_FLIGHT_BATCHES * BATCH_SLOT_SIZE);
    ~RDMAConnection();

    // 建立连接
    int connect_to_server();
    // Modified read method - reads directly to provided registered buffer
    int rdma_read_direct(void* dest_buf, size_t size, uint64_t remote_offset);

    int rdma_read_direct_async(void* dest_buf, size_t size,
                               uint64_t remote_offset);

    // 远程读取数据
    int rdma_read(void* local_buf, size_t size, uint64_t remote_offset);

    // 远程写入数据
    int rdma_write(void* local_buf, size_t size, uint64_t remote_offset);

    // 检查连接状态
    bool is_connected() const {
      return connected;
    }

    // 获取远程布局信息
    // 新增：向量数据上传接口
    template<typename data_t>
    int upload_vector_data(const data_t* local_data, uint64_t num_vectors,
                           uint64_t aligned_dim);

    // 新增：获取远程向量数据的虚拟指针
    template<typename data_t>
    data_t* get_remote_data_pointer();

    // 新增：读取单个向量
    template<typename data_t>
    int read_vector(uint64_t vector_id, data_t* dest_buffer);

    // 新增：批量读取向量
    template<typename data_t>
    int batch_read_vectors(const uint64_t* vector_ids, uint32_t count,
                           data_t* dest_buffer);

    const VectorDataLayout& get_vector_layout() const {
      return vector_layout_;
    }
    // 等待操作完成
    int wait_for_completion();

    // 简单读取测试
    int test_simple_read();

    // 直接读取向量数据
    char* rdma_read_vector_data_direct(uint64_t internal_id);

    // 加载并写入索引文件到服务器
    int load_and_write_index(const std::string& index_file_path);

    // 写入完成后验证读取
    int test_read_after_write();

    int parse_vector_metadata();

    // Add these methods to access the connection's registered buffer
    void* get_local_buffer() const {
      return local_buffer;
    }
    size_t get_buffer_size() const {
      return buffer_size;
    }
    ibv_pd* get_pd() const {
      return pd;
    }

    int rdma_read_batch(std::vector<RDMAReadRequest>& requests);
    int rdma_read_batch_async(std::vector<RDMAReadRequest>& requests, uint64_t* finished_flag);
    int rdma_read_doorbell_batch(std::vector<RDMAReadRequest>& requests,
                                 size_t start_idx, size_t batch_size);
    int rdma_read_doorbell_batch_async(std::vector<RDMAReadRequest>& requests,
                                 size_t start_idx, size_t batch_size, uint64_t* finished_flag);
    int wait_for_batch_completion(size_t expected_completions);

    int poll_and_copy_completions_best();

    // [PERF-DIAG] Print efficiency diagnostics summary
    void print_perf_diag();

    // 解析已写入的索引头部
  };

}  // namespace diskann
