#include "rdma_writer.h"
#include "utils.h"
#include <arpa/inet.h>
#include <cstring>
#include "ann_exception.h"

namespace diskann {

RDMAWriter::RDMAWriter(const std::string& server_ip, uint16_t port, size_t buffer_size)
    : buffer_size_(buffer_size), current_offset_(0) {
    
    // Initialize RDMA connection (similar to your client code)
    rdma_event_channel* ec = rdma_create_event_channel();
    if (!ec) throw ANNException("Failed to create event channel",-1);
    
    if (rdma_create_id(ec, &id_, nullptr, RDMA_PS_TCP)) 
        throw ANNException("Failed to create RDMA ID",-1);
    
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (!inet_pton(AF_INET, server_ip.c_str(), &server_addr.sin_addr))
        throw ANNException("Invalid server IP",-1);
    
    // Connect to server
    if (rdma_resolve_addr(id_, nullptr, (sockaddr*)&server_addr, 2000))
        throw ANNException("Failed to resolve address",-1);
    
    rdma_cm_event* ev;
    rdma_get_cm_event(ec, &ev);
    rdma_ack_cm_event(ev);
    
    if (rdma_resolve_route(id_, 2000))
        throw ANNException("Failed to resolve route",-1);
    
    rdma_get_cm_event(ec, &ev);
    rdma_ack_cm_event(ev);
    
    // Create protection domain and queue pair
    ibv_pd* pd = ibv_alloc_pd(id_->verbs);
    if (!pd) throw ANNException("Failed to allocate PD",-1);
    
    ibv_qp_init_attr qp_attr{};
    qp_attr.cap = {256, 256, 1, 1};
    qp_attr.qp_type = IBV_QPT_RC;
    if (rdma_create_qp(id_, pd, &qp_attr))
        throw ANNException("Failed to create QP",-1);
    
    // Connect
    rdma_conn_param conn_param{};
    conn_param.initiator_depth = 4;
    conn_param.responder_resources = 4;
    conn_param.retry_count = 7;
    if (rdma_connect(id_, &conn_param))
        throw ANNException("Failed to connect",-1);
    
    // Get remote memory info from server
    struct RemoteMR { uint64_t addr; uint32_t rkey; uint32_t size; } remote_mr{};
    bool got_mr = false;
    
    while (true) {
        rdma_get_cm_event(ec, &ev);
        if (ev->event == RDMA_CM_EVENT_ESTABLISHED) {
            if (ev->param.conn.private_data_len >= sizeof(remote_mr)) {
                memcpy(&remote_mr, ev->param.conn.private_data, sizeof(remote_mr));
                got_mr = true;
            }
            rdma_ack_cm_event(ev);
            break;
        }
        rdma_ack_cm_event(ev);
    }
    
    if (!got_mr) throw ANNException("Failed to get remote MR info",-1);
    
    remote_addr_ = remote_mr.addr;
    remote_rkey_ = remote_mr.rkey;
    
    // Allocate local buffer
    buffer_ = aligned_alloc(4096, buffer_size_);
    if (!buffer_) throw ANNException("Failed to allocate buffer",-1);
    
    mr_ = ibv_reg_mr(pd, buffer_, buffer_size_, IBV_ACCESS_LOCAL_WRITE);
    if (!mr_) throw ANNException("Failed to register MR",-1);
}

void RDMAWriter::write(const void* data, size_t size) {
    memcpy(buffer_, data, size);
    
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(buffer_);
    sge.length = size;
    sge.lkey = mr_->lkey;
    
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr_ + current_offset_;
    wr.wr.rdma.rkey = remote_rkey_;
    
    ibv_send_wr* bad_wr;
    if (ibv_post_send(id_->qp, &wr, &bad_wr))
        throw ANNException("Failed to post RDMA write",-1);
    
    // Wait for completion
    ibv_wc wc{};
    ibv_cq* cq = id_->qp->send_cq;
    while (ibv_poll_cq(cq, 1, &wc) == 0) {}
    if (wc.status != IBV_WC_SUCCESS)
        throw ANNException("RDMA write failed",-1);
    
    current_offset_ += size;
}

void RDMAWriter::save_rdma_info(const std::string& info_file) {
    struct RDMAInfo {
        uint64_t remote_addr;
        uint32_t remote_rkey;
        uint64_t total_size;
    } __attribute__((packed));
    
    RDMAInfo info{};
    info.remote_addr = remote_addr_;
    info.remote_rkey = remote_rkey_;
    info.total_size = current_offset_;
    
    std::ofstream file(info_file, std::ios::binary);
    if (!file) {
        throw ANNException("Failed to create RDMA info file: " + info_file, -1);
    }
    
    file.write(reinterpret_cast<const char*>(&info), sizeof(info));
    file.close();
    
    std::cout << "RDMA info saved to: " << info_file << std::endl;
    std::cout << "  Remote addr: 0x" << std::hex << info.remote_addr << std::dec << std::endl;
    std::cout << "  Remote rkey: " << info.remote_rkey << std::endl;
    std::cout << "  Total size: " << info.total_size << " bytes" << std::endl;
}

RDMAWriter::~RDMAWriter() {
    close();
}

void RDMAWriter::close() {
    if (mr_) {
        ibv_dereg_mr(mr_);
        mr_ = nullptr;
    }
    if (buffer_) {
        free(buffer_);
        buffer_ = nullptr;
    }
    if (id_) {
        rdma_disconnect(id_);
        rdma_destroy_qp(id_);
        rdma_destroy_id(id_);
        id_ = nullptr;
    }
}

}