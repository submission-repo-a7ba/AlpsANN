#pragma once
#include <rdma/rdma_cma.h>
#include <memory>
#include <vector>

namespace diskann {

class RDMAWriter {
private:
    rdma_cm_id* id_;
    ibv_mr* mr_;
    void* buffer_;
    size_t buffer_size_;
    uint64_t remote_addr_;
    uint32_t remote_rkey_;
    uint64_t current_offset_;
    
public:
    RDMAWriter(const std::string& server_ip, uint16_t port, size_t buffer_size);
    ~RDMAWriter();
    
    void write(const void* data, size_t size);
    void flush();
    void close();
    uint64_t get_current_offset() const { return current_offset_; }
    uint64_t get_remote_addr() const { return remote_addr_; }
    void save_rdma_info(const std::string& info_file);
};

}