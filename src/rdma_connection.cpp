#include "rdma_connection.h"
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <immintrin.h>
#include <thread>
#include <algorithm>

namespace diskann {

RDMAConnection::RDMAConnection(const char* server_addr, int port, size_t buf_size)
    : cm_id(nullptr), ec(nullptr), context(nullptr), pd(nullptr), mr(nullptr),
      qp(nullptr), cq(nullptr), local_buffer(nullptr),
      remote_addr(nullptr), remote_rkey(0), buffer_size(buf_size), connected(false) {

    // Initialize atomic flags to false (no active batches)
    for (size_t i = 0; i < MAX_IN_FLIGHT_BATCHES; ++i) {
        batch_slot_active_[i].store(false, std::memory_order_relaxed);
    }

    std::cout << "Creating RDMA connection to " << server_addr << ":" << port << std::endl;
    // 分配本地缓冲区
    local_buffer = aligned_alloc(4096, buffer_size);
    if (!local_buffer) {
        throw std::runtime_error("Failed to allocate local buffer");
    }
    memset(local_buffer, 0, buffer_size);
    
    // 初始化远程布局信息
    memset(&vector_layout_, 0, sizeof(vector_layout_));
    
    // 创建事件通道
    ec = rdma_create_event_channel();
    if (!ec) {
        throw std::runtime_error("Failed to create event channel");
    }
    
    // 创建CM ID
    int ret = rdma_create_id(ec, &cm_id, nullptr, RDMA_PS_TCP);
    if (ret) {
        rdma_destroy_event_channel(ec);
        throw std::runtime_error("Failed to create CM ID");
    }
    
    // 解析服务器地址
    struct sockaddr_in server_sockaddr;
    memset(&server_sockaddr, 0, sizeof(server_sockaddr));
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_port = htons(port);
    inet_pton(AF_INET, server_addr, &server_sockaddr.sin_addr);
    
    // 解析地址
    ret = rdma_resolve_addr(cm_id, nullptr, (struct sockaddr*)&server_sockaddr, 1000);
    if (ret) {
        throw std::runtime_error("Failed to resolve address");
    }
}
// RDMAConnection::RDMAConnection(const char* server_addr, int port, std::shared_ptr<RDMABufferManager> buffer_mgr)
//     : cm_id(nullptr), ec(nullptr), context(nullptr), pd(nullptr), 
//       qp(nullptr), cq(nullptr), remote_rkey(0), connected(false), buffer_manager_(buffer_mgr) {
    
//     std::cout << "Creating RDMA connection to " << server_addr << ":" << port << std::endl;
//     // std::cout << "Using shared buffer manager" << std::endl;
    
//     // 初始化远程布局信息
//     memset(&vector_layout_, 0, sizeof(vector_layout_));

//         // BUG FIX: Don't initialize RDMA here if buffer_manager is null (for PD creation)
//     if (buffer_manager_) {
//         std::cout << "Using shared buffer manager" << std::endl;
//     } else {
//         std::cout << "Creating connection for PD extraction" << std::endl;
//     }
    
//     // 创建事件通道
//     ec = rdma_create_event_channel();
//     if (!ec) {
//         throw std::runtime_error("Failed to create event channel");
//     }
    
//     // 创建CM ID
//     int ret = rdma_create_id(ec, &cm_id, nullptr, RDMA_PS_TCP);
//     if (ret) {
//         rdma_destroy_event_channel(ec);
//         throw std::runtime_error("Failed to create CM ID");
//     }
    
//     // 解析服务器地址
//     struct sockaddr_in server_sockaddr;
//     memset(&server_sockaddr, 0, sizeof(server_sockaddr));
//     server_sockaddr.sin_family = AF_INET;
//     server_sockaddr.sin_port = htons(port);
//     inet_pton(AF_INET, server_addr, &server_sockaddr.sin_addr);
    
//     // 解析地址
//     ret = rdma_resolve_addr(cm_id, nullptr, (struct sockaddr*)&server_sockaddr, 1000);
//     if (ret) {
//         throw std::runtime_error("Failed to resolve address");
//     }
// }

RDMAConnection::~RDMAConnection() {
    if (mr) ibv_dereg_mr(mr);
    if (cq) ibv_destroy_cq(cq);
    if (pd) ibv_dealloc_pd(pd);
    if (cm_id) rdma_destroy_id(cm_id);
    if (ec) rdma_destroy_event_channel(ec);
    if (local_buffer) free(local_buffer);
}

int RDMAConnection::connect_to_server() {
    try {
        std::cout << "Connecting to RDMA server..." << std::endl;
        
        // 等待地址解析完成
        struct rdma_cm_event *ev;
        if (rdma_get_cm_event(ec, &ev) || ev->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
            throw std::runtime_error("Address resolution failed");
        }
        rdma_ack_cm_event(ev);
        
        // 解析路由
        if (rdma_resolve_route(cm_id, 1000)) {
            throw std::runtime_error("Failed to resolve route");
        }
        
        if (rdma_get_cm_event(ec, &ev) || ev->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
            throw std::runtime_error("Route resolution failed");
        }
        rdma_ack_cm_event(ev);
        
        // 创建保护域
        pd = ibv_alloc_pd(cm_id->verbs);
        if (!pd) {
            throw std::runtime_error("Failed to allocate PD");
        }
        // if (!buffer_manager_) {
        //     throw std::runtime_error("Buffer manager not available");
        // }
        
        // BUG FIX: Handle both cases - with and without buffer manager
        // if (buffer_manager_) {
        //     // Use shared protection domain
        //     pd = buffer_manager_->get_protection_domain();
        //     if (!pd) {
        //         throw std::runtime_error("Failed to get PD from buffer manager");
        //     }
        //     std::cout << "Using shared protection domain from buffer manager" << std::endl;
        // } else {
        //     // Create own protection domain (for initial PD extraction)
        //     pd = ibv_alloc_pd(cm_id->verbs);
        //     if (!pd) {
        //         throw std::runtime_error("Failed to allocate protection domain");
        //     }
        //     std::cout << "Created own protection domain" << std::endl;
        // }
        
        // 创建完成队列
        // CQ sized to match MAX_IN_FLIGHT_BATCHES (each batch generates 1 signaled WC)
        cq = ibv_create_cq(cm_id->verbs, MAX_IN_FLIGHT_BATCHES * 2, nullptr, nullptr, 0);
        if (!cq) {
            throw std::runtime_error("Failed to create CQ");
        }
        
        // 注册本地内存
        mr = ibv_reg_mr(pd, local_buffer, buffer_size,
                       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE );
        if (!mr) {
            throw std::runtime_error("Failed to register memory");
        }
        
        // 创建队列对
        struct ibv_qp_init_attr qp_attr;
        memset(&qp_attr, 0, sizeof(qp_attr));
        qp_attr.send_cq = cq;
        qp_attr.recv_cq = cq;
        // QP sized for max concurrent WRs: MAX_IN_FLIGHT_BATCHES batches * up to 32 WRs each
        // But typical batches are 1-4 WRs, so 64 is plenty of headroom
        qp_attr.cap.max_send_wr = std::max((size_t)64, MAX_IN_FLIGHT_BATCHES * 4);
        qp_attr.cap.max_recv_wr = 16;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.sq_sig_all = 0;
        
        if (rdma_create_qp(cm_id, pd, &qp_attr)) {
            throw std::runtime_error("Failed to create QP");
        }
        qp = cm_id->qp;
        
        // 连接到服务器
        struct rdma_conn_param conn_param;
        memset(&conn_param, 0, sizeof(conn_param));
        conn_param.initiator_depth = 4;
        conn_param.responder_resources = 4;
        conn_param.retry_count = 7;
        
        if (rdma_connect(cm_id, &conn_param)) {
            throw std::runtime_error("Failed to connect");
        }
        
        // 等待连接建立
        if (rdma_get_cm_event(ec, &ev) || ev->event != RDMA_CM_EVENT_ESTABLISHED) {
            throw std::runtime_error("Connection establishment failed");
        }
        
        // 提取远程内存信息
        if (ev->param.conn.private_data_len >= sizeof(RemoteMR)) {
            RemoteMR remote_mr;
            memcpy(&remote_mr, ev->param.conn.private_data, sizeof(RemoteMR));
            
            vector_layout_.remote_addr = remote_mr.addr;
            vector_layout_.remote_rkey = remote_mr.rkey;
            vector_layout_.available_size = remote_mr.size;
            remote_rkey = remote_mr.rkey;
            
            std::cout << "✓ Connected successfully!" << std::endl;
            std::cout << "Remote memory: addr=0x" << std::hex << remote_mr.addr 
                      << ", rkey=" << std::dec << remote_mr.rkey 
                      << ", size=" << (remote_mr.size / 1024 / 1024) << " MB" << std::endl;
            
            connected = true;
        } else {
            throw std::runtime_error("No remote memory info received");
        }
        
        rdma_ack_cm_event(ev);
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Connection failed: " << e.what() << std::endl;
        return -1;
    }
}

int RDMAConnection::rdma_write(void *local_buf, size_t size, uint64_t remote_offset) {
    if (!connected) {
        std::cerr << "RDMA not connected" << std::endl;
        return -1;
    }
    
    // 检查本地缓冲区范围
    uintptr_t local_base = (uintptr_t)local_buffer;
    uintptr_t local_end = local_base + buffer_size;
    uintptr_t buf_addr = (uintptr_t)local_buf;
    
    if (buf_addr < local_base || buf_addr + size > local_end) {
        std::cerr << "ERROR: local_buf not in registered memory region!" << std::endl;
        return -1;
    }
        // Check if buffer is in shared buffer manager's memory
    // if (!buffer_manager_->is_managed_buffer(local_buf)) {
    //     std::cerr << "ERROR: local_buf not in shared RDMA buffer!" << std::endl;
    //     return -1;
    // }
    
    // // Use buffer manager's memory region
    // ibv_mr* mr = buffer_manager_->get_memory_region();
    
    // 构造RDMA写入请求
    struct ibv_sge sge;
    struct ibv_send_wr wr, *bad_wr;
    
    // sge.addr = buf_addr;
    sge.addr = reinterpret_cast<uintptr_t>(local_buf);
    sge.length = size;
    sge.lkey = mr->lkey;
    
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 1;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = vector_layout_.remote_addr + remote_offset;
    wr.wr.rdma.rkey = remote_rkey;
    
    // 发送写入请求
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    if (ret) {
        std::cerr << "RDMA write post failed: " << ret << std::endl;
        return ret;
    }
    
    // 等待写入完成
    return wait_for_completion();
}

// NEW: Batch RDMA read function
// NEW: Fixed doorbell batching with pre-allocated arrays
int RDMAConnection::rdma_read_batch(std::vector<RDMAReadRequest>& requests) {
    if (!connected || requests.empty()) {
        return -1;
    }
    
    // Process in fixed batches to avoid vector reallocation issues
    const size_t DOORBELL_BATCH_SIZE = 32;  // Hardware optimal batch size
    size_t total_requests = requests.size();
    size_t processed = 0;
    
    // std::cout << "Batch RDMA read: " << total_requests << " requests in batches of " 
    //           << DOORBELL_BATCH_SIZE << std::endl;
    
    while (processed < total_requests) {
        size_t batch_size = std::min(DOORBELL_BATCH_SIZE, total_requests - processed);
        
        // Process this batch with ONE doorbell
        if (rdma_read_doorbell_batch(requests, processed, batch_size) != 0) {
            return -1;
        }
        
        processed += batch_size;
    }
    
    return 0;
}

int RDMAConnection::rdma_read_batch_async(std::vector<RDMAReadRequest>& requests, uint64_t* finished_flag) {
    if (!connected || requests.empty()) {
        return -1;
    }
    
    // Process in fixed batches to avoid vector reallocation issues
    const size_t DOORBELL_BATCH_SIZE = 32;  // Hardware optimal batch size
    size_t total_requests = requests.size();
    size_t processed = 0;

    // std::cerr << "RDMAConnection::rdma_read_batch_async called with "
    //           << total_requests << " requests." << std::endl;
    
    // std::cout << "Batch RDMA read: " << total_requests << " requests in batches of " 
    //           << DOORBELL_BATCH_SIZE << std::endl;
    
    while (processed < total_requests) {
        size_t batch_size = std::min(DOORBELL_BATCH_SIZE, total_requests - processed);
        
        // Process this batch with ONE doorbell
        if (rdma_read_doorbell_batch_async(requests, processed, batch_size, finished_flag) != 0) {
            return -1;
        }
        
        processed += batch_size;
    }
    
    return 0;
}

// NEW: Process one batch with fixed arrays (no vector reallocation issues)
int RDMAConnection::rdma_read_doorbell_batch(std::vector<RDMAReadRequest>& requests, 
                                           size_t start_idx, size_t batch_size) {
    
    // Use stack arrays to avoid vector reallocation issues
    ibv_sge sge_array[32];
    ibv_send_wr wr_array[32];
    
    char* base_buffer = static_cast<char*>(local_buffer);

    // Calculate total size for this batch
    size_t batch_total_size = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        batch_total_size += requests[start_idx + i].size;
    }

    if (batch_total_size > buffer_size) {
        std::cerr << "[RDMA] Sync batch too large for buffer: " << batch_total_size
                  << " > " << buffer_size << std::endl;
        return -1;
    }

    // Setup WRs with proper chaining using fixed arrays
    size_t buffer_offset = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        auto& req = requests[start_idx + i];

        // SGE points to separate area in registered buffer
        sge_array[i].addr = (uint64_t)(base_buffer + buffer_offset);
        sge_array[i].length = req.size;
        sge_array[i].lkey = mr->lkey;
        
        // Setup WR
        memset(&wr_array[i], 0, sizeof(wr_array[i]));
        wr_array[i].wr_id = start_idx + i + 1;
        wr_array[i].sg_list = &sge_array[i];
        wr_array[i].num_sge = 1;
        wr_array[i].opcode = IBV_WR_RDMA_READ;
        // wr_array[i].send_flags = IBV_SEND_SIGNALED;

                // NEW: Selective signaling (major optimization!)
        if (i == batch_size - 1 || (i + 1) % 32 == 0) {
            wr_array[i].send_flags = IBV_SEND_SIGNALED;
        } else {
            wr_array[i].send_flags = 0;  // Unsignaled
        }
        wr_array[i].wr.rdma.remote_addr = vector_layout_.remote_addr + req.remote_offset;
        wr_array[i].wr.rdma.rkey = remote_rkey;
        
        // Chain WRs correctly using fixed array addresses (NO vector reallocation!)
        if (i < batch_size - 1) {
            wr_array[i].next = &wr_array[i + 1];  // Fixed array address - safe!
        } else {
            wr_array[i].next = nullptr;
        }
        
        buffer_offset += req.size;
    }
    
    // SINGLE DOORBELL for entire batch (key optimization!)
    struct ibv_send_wr *bad_wr;
    int ret = ibv_post_send(qp, &wr_array[0], &bad_wr);
    if (ret) {
        std::cerr << "Doorbell batch post failed: " << ret << std::endl;
        return ret;
    }
    
    // Wait for all completions
    // ret = wait_for_batch_completion(batch_size);
    size_t signaled_wrs = (batch_size + 31) / 32;
    ret = wait_for_batch_completion(signaled_wrs);
    if (ret != 0) {
        return ret;
    }
    
    // Copy from registered buffer to final destinations
    buffer_offset = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        auto& req = requests[start_idx + i];
        memcpy(req.local_buf, base_buffer + buffer_offset, req.size);
        buffer_offset += req.size;
    }
    
    return 0;
}

int RDMAConnection::rdma_read_doorbell_batch_async(std::vector<RDMAReadRequest>& requests, 
                                           size_t start_idx, size_t batch_size, uint64_t* finished_flag) {
    

    // Use stack arrays to avoid vector reallocation issues
    ibv_sge sge_array[32];
    ibv_send_wr wr_array[32];
    
    char* base_buffer = static_cast<char*>(local_buffer);

    // Calculate total size for this batch
    size_t batch_total_size = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        batch_total_size += requests[start_idx + i].size;
    }

    // Check batch fits in slot
    if (batch_total_size > BATCH_SLOT_SIZE) {
        std::cerr << "[RDMA] Batch too large for slot: " << batch_total_size
                  << " > " << BATCH_SLOT_SIZE
                  << " (batch_size=" << batch_size << ")" << std::endl;
        return -1;
    }

    if (batch_total_size > buffer_size) {
        std::cerr << "[RDMA] Batch too large for buffer: " << batch_total_size
                  << " > " << buffer_size << std::endl;
        return -1;
    }

    // Atomic fetch_add for thread-safe slot allocation
    // Cap to actual buffer capacity to prevent out-of-bounds RDMA writes
    size_t max_usable_slots = buffer_size / BATCH_SLOT_SIZE;
    if (max_usable_slots == 0) max_usable_slots = 1;
    if (max_usable_slots > MAX_IN_FLIGHT_BATCHES) max_usable_slots = MAX_IN_FLIGHT_BATCHES;
    size_t batch_slot = next_batch_slot_.fetch_add(1, std::memory_order_relaxed) % max_usable_slots;

    // DEBUG: Check if slot is still active (collision detection)
    if (batch_slot_active_[batch_slot].load(std::memory_order_acquire)) {
      // Count active slots for diagnostics
      unsigned n_active = 0;
      for (size_t s = 0; s < max_usable_slots; s++) {
        if (batch_slot_active_[s].load(std::memory_order_relaxed)) n_active++;
      }
      std::cerr << "[RDMA-DIAG] SLOT COLLISION: batch_slot=" << batch_slot
                << " still active! active_slots=" << n_active
                << "/" << max_usable_slots
                << " total_alloc=" << next_batch_slot_.load(std::memory_order_relaxed)
                << " batch_size=" << batch_size
                << " thread=" << std::this_thread::get_id() << std::endl;
      // Spin-wait for slot to become available
      unsigned spin = 0;
      while (batch_slot_active_[batch_slot].load(std::memory_order_acquire)) {
        _mm_pause();
        if (++spin > 10000000) {
          std::cerr << "[RDMA-DIAG] DEADLOCK: slot " << batch_slot
                    << " stuck active after 10M spins" << std::endl;
          return -1;
        }
      }
      std::cerr << "[RDMA-DIAG] Slot " << batch_slot
                << " freed after " << spin << " spins" << std::endl;
    }

    size_t this_batch_offset = batch_slot * BATCH_SLOT_SIZE;
    
    // std::cerr << "Batch slot " << batch_slot 
    //           << " at offset " << this_batch_offset 
    //           << ", size=" << batch_total_size << std::endl;


    // Atomic allocation of WR ID range
    uint64_t start_wr = next_wr_id_.fetch_add(batch_size, std::memory_order_relaxed);

    BatchInfo batch_info;
    batch_info.start_wr_id = start_wr;
    batch_info.end_wr_id = start_wr + batch_size - 1;
    batch_info.base_buffer_offset = base_buffer + this_batch_offset;
    batch_info.completed = finished_flag;
    batch_info.remote_offset = requests[start_idx].remote_offset;
    batch_info.batch_buffer_offset = this_batch_offset;
    batch_info.total_size = batch_total_size;

    // std::cerr << "Preparing async batch WR IDs " 
    //           << batch_info.start_wr_id << " to " << batch_info.end_wr_id << std::endl;
    // std::cerr << "offset of first request in batch: " 
    //           << requests[start_idx].remote_offset << std::endl;

        // std::cerr << "Batch WR " << batch_info.start_wr_id << " to " << batch_info.end_wr_id 
        //       << " using slot " << batch_slot 
        //       << " (offset=" << this_batch_offset << ")" << std::endl;

    // std::cerr <<"batch completion? " << (*finished_flag ? "yes" : "no") << std::endl;
    // std::cerr <<"assign batch completion? " << (*batch_info.completed ? "yes" : "no") << std::endl; 

    // printout the request
    // std::cerr << "start_idx=" << start_idx << ", batch_size=" << batch_size << std::endl;
    for (size_t i = 0; i < batch_size; ++i) {
        auto& req = requests[start_idx + i];
        // std::cerr << "Request " << i << ": local_buf=" << req.local_buf 
        //           << ", size=" << req.size 
        //           << ", remote_offset=" << req.remote_offset << std::endl;
    }

    // std::cerr<< "base_buffer address: " << (void*)base_buffer << std::endl;
    // std::cerr << "batch info base_buffer_offset: " 
    //           << (void*)batch_info.base_buffer_offset << std::endl;
    
    batch_info.num_requests = batch_size;

    // Setup WRs with proper chaining using fixed arrays
    size_t buffer_offset = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        auto& req = requests[start_idx + i];

        // Store in fixed-size array (no heap allocation)
        batch_info.requests[i].local_buf = req.local_buf;
        batch_info.requests[i].size = req.size;

        // SGE points to separate area in registered buffer
        sge_array[i].addr = (uint64_t)(base_buffer + this_batch_offset + buffer_offset);
        sge_array[i].length = req.size;
        sge_array[i].lkey = mr->lkey;
        
        // Setup WR - encode batch_slot in upper 16 bits for O(1) lookup
        memset(&wr_array[i], 0, sizeof(wr_array[i]));
        // Encode: upper 16 bits = batch_slot, lower 48 bits = sequence
        wr_array[i].wr_id = ((uint64_t)batch_slot << 48) | (start_wr + i);
        wr_array[i].sg_list = &sge_array[i];
        wr_array[i].num_sge = 1;
        wr_array[i].opcode = IBV_WR_RDMA_READ;
        // wr_array[i].send_flags = IBV_SEND_SIGNALED;

                // NEW: Selective signaling (major optimization!)
        if (i == batch_size - 1 || (i + 1) % 32 == 0) {
            wr_array[i].send_flags = IBV_SEND_SIGNALED;
        } else {
            wr_array[i].send_flags = 0;  // Unsignaled
        }
        wr_array[i].wr.rdma.remote_addr = vector_layout_.remote_addr + req.remote_offset;
        wr_array[i].wr.rdma.rkey = remote_rkey;
        
        // Chain WRs correctly using fixed array addresses (NO vector reallocation!)
        if (i < batch_size - 1) {
            wr_array[i].next = &wr_array[i + 1];  // Fixed array address - safe!
        } else {
            wr_array[i].next = nullptr;
        }
        
        buffer_offset += req.size;
    }
    
    // SINGLE DOORBELL for entire batch (key optimization!)
    struct ibv_send_wr *bad_wr;
    int ret = ibv_post_send(qp, &wr_array[0], &bad_wr);
    if (ret) {
        // Count active slots for context
        unsigned n_active = 0;
        for (size_t s = 0; s < max_usable_slots; s++) {
          if (batch_slot_active_[s].load(std::memory_order_relaxed)) n_active++;
        }
        std::cerr << "[RDMA-DIAG] ibv_post_send FAILED: ret=" << ret
                  << " errno=" << errno << " (" << strerror(errno) << ")"
                  << " batch_size=" << batch_size
                  << " batch_slot=" << batch_slot
                  << " active_slots=" << n_active
                  << " total_alloc=" << next_batch_slot_.load(std::memory_order_relaxed)
                  << " next_wr_id=" << next_wr_id_.load(std::memory_order_relaxed)
                  << " thread=" << std::this_thread::get_id() << std::endl;
        return ret;
    }
    // Store in fixed array (lock-free) and mark slot as active
    pending_batches_[batch_slot] = std::move(batch_info);
    batch_slot_active_[batch_slot].store(true, std::memory_order_release);


    // Wait for all completions
    // ret = wait_for_batch_completion(batch_size);
    // size_t signaled_wrs = (batch_size + 31) / 32;
    // ret = wait_for_batch_completion(signaled_wrs);
    // if (ret != 0) {
    //     return ret;
    // }
    
    // // Copy from registered buffer to final destinations
    // buffer_offset = 0;
    // for (size_t i = 0; i < batch_size; ++i) {
    //     auto& req = requests[start_idx + i];
    //     memcpy(req.local_buf, base_buffer + buffer_offset, req.size);
    //     buffer_offset += req.size;
    // }
    
    return 0;
}

int RDMAConnection::poll_and_copy_completions_best() {
    struct ibv_wc wc_array[64];
    int completed_requests = 0;

    // Non-blocking poll
    int ret = ibv_poll_cq(cq, 64, wc_array);
    if (ret <= 0) return ret;

    char* base_buffer = static_cast<char*>(local_buffer);
    
    // Process each completion (only signaled WRs appear here)
    for (int i = 0; i < ret; ++i) {
        if (wc_array[i].status != IBV_WC_SUCCESS) {
            size_t err_slot = (wc_array[i].wr_id >> 48) & 0xFFFF;
            std::cerr << "[RDMA-DIAG] WC error: status=" << wc_array[i].status
                      << " wr_id=" << wc_array[i].wr_id
                      << " batch_slot=" << err_slot
                      << " vendor_err=" << wc_array[i].vendor_err
                      << " thread=" << std::this_thread::get_id() << std::endl;
            // Still mark slot inactive to prevent deadlock
            if (err_slot < MAX_IN_FLIGHT_BATCHES &&
                batch_slot_active_[err_slot].load(std::memory_order_acquire)) {
              auto &ebatch = pending_batches_[err_slot];
              if (ebatch.completed) *ebatch.completed = 1;  // unblock waiter
              batch_slot_active_[err_slot].store(false, std::memory_order_release);
            }
            continue;
        }
        
        uint64_t completed_wr_id = wc_array[i].wr_id;

        // O(1) lookup: extract batch_slot from upper 16 bits
        size_t batch_slot = (completed_wr_id >> 48) & 0xFFFF;

        if (batch_slot >= MAX_IN_FLIGHT_BATCHES) {
            continue;  // Invalid slot
        }

        if (!batch_slot_active_[batch_slot].load(std::memory_order_acquire)) {
            continue;  // Slot already processed
        }

        BatchInfo& batch = pending_batches_[batch_slot];

        size_t buffer_offset = 0;
        // Copy data from registered buffer to user buffer (THE DOUBLE COPY)
        for (size_t idx = 0; idx < batch.num_requests; ++idx) {
            const auto& req_info = batch.requests[idx];

            if (!req_info.local_buf) {
                std::cerr << "ERROR: NULL local_buf at index " << idx << std::endl;
                continue;
            }

            char* source_addr = batch.base_buffer_offset + buffer_offset;
            memcpy(req_info.local_buf, source_addr, req_info.size);
            buffer_offset += req_info.size;
            completed_requests++;
        }

        // Mark as completed (any nonzero value; offsets can be 0 on a shard)
        if (batch.completed) *batch.completed = 1;

        // Mark slot as inactive (lock-free)
        batch_slot_active_[batch_slot].store(false, std::memory_order_release);
    }

    return completed_requests;
}

void RDMAConnection::print_perf_diag() {
    // Diagnostic counters removed from hot path for performance.
}

// Wait for multiple completions efficiently
int RDMAConnection::wait_for_batch_completion(size_t expected_completions) {
    struct ibv_wc wc_array[64];
    size_t completed = 0;
    int polls = 0;
    const int max_polls = 1000000;
    
    while (completed < expected_completions && polls < max_polls) {
        // Poll multiple completions at once
        int ret = ibv_poll_cq(cq, std::min(64UL, expected_completions - completed), wc_array);
        
        if (ret < 0) {
            std::cerr << "CQ polling error: " << ret << std::endl;
            return -1;
        }
        
        if (ret > 0) {
            // Check all completions
            for (int i = 0; i < ret; ++i) {
                if (wc_array[i].status != IBV_WC_SUCCESS) {
                    std::cerr << "Work completion error: " << wc_array[i].status << std::endl;
                    return -1;
                }
            }
            completed += ret;
        }
        polls++;
    }
    
    return (completed == expected_completions) ? 0 : -1;
}

int RDMAConnection::rdma_read_direct(void* dest_buf, size_t size, uint64_t remote_offset) {
    if (!connected) {
        std::cerr << "RDMA not connected" << std::endl;
        return -1;
    }
    
    // // Check if destination buffer is managed by our buffer manager
    // if (!buffer_manager_->is_managed_buffer(dest_buf)) {
    //     std::cerr << "ERROR: dest_buf not in managed RDMA buffer!" << std::endl;
    //     return -1;
    // }
    
    // // Use the buffer manager's memory region
    // ibv_mr* mr = buffer_manager_->get_memory_region();
    
    // Direct RDMA read to destination buffer
    struct ibv_sge sge;
    struct ibv_send_wr wr, *bad_wr;
    
    sge.addr = (uint64_t)local_buffer;
    sge.length = size;
    sge.lkey = mr->lkey;  // Use shared memory region's lkey
    
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = vector_layout_.remote_addr + remote_offset;
    wr.wr.rdma.rkey = remote_rkey;
    
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    if (ret) {
        std::cerr << "RDMA read post failed: " << ret << std::endl;
        return ret;
    }
    // Step 2: Fast copy from RDMA buffer to destination (CPU cache friendly)
    ret = wait_for_completion();
    if (ret != 0) {
        std::cerr << "RDMA read completion failed" << std::endl;
        return ret;
    }
    
    // Step 3: ONLY THEN copy to destination (data is now valid)
    memcpy(dest_buf, local_buffer, size);
    
    return 0;
}

int RDMAConnection::rdma_read_direct_async(void* dest_buf, size_t size, uint64_t remote_offset) {
    if (!connected) {
        std::cerr << "RDMA not connected" << std::endl;
        return -1;
    }
    
    // // Check if destination buffer is managed by our buffer manager
    // if (!buffer_manager_->is_managed_buffer(dest_buf)) {
    //     std::cerr << "ERROR: dest_buf not in managed RDMA buffer!" << std::endl;
    //     return -1;
    // }
    
    // // Use the buffer manager's memory region
    // ibv_mr* mr = buffer_manager_->get_memory_region();
    
    // Direct RDMA read to destination buffer
    struct ibv_sge sge;
    struct ibv_send_wr wr, *bad_wr;
    
    sge.addr = (uint64_t)local_buffer;
    sge.length = size;
    sge.lkey = mr->lkey;  // Use shared memory region's lkey
    
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = vector_layout_.remote_addr + remote_offset;
    wr.wr.rdma.rkey = remote_rkey;
    
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    if (ret) {
        std::cerr << "RDMA read post failed: " << ret << std::endl;
        return ret;
    }
    // Step 2: Fast copy from RDMA buffer to destination (CPU cache friendly)
    // ret = wait_for_completion();
    // if (ret != 0) {
    //     std::cerr << "RDMA read completion failed" << std::endl;
    //     return ret;
    // }
    
    // // Step 3: ONLY THEN copy to destination (data is now valid)
    // memcpy(dest_buf, local_buffer, size);
    
    return 0;
}

// int RDMAConnection::rdma_read(void* dest_buf, size_t size, uint64_t remote_offset) {
//     // For compatibility - check if buffer is managed, if so do direct read
//     if (buffer_manager_->is_managed_buffer(dest_buf)) {
//         return rdma_read_direct(dest_buf, size, remote_offset);
//     }
    
//     // For non-managed buffers, get a temporary segment and copy
//     void* temp_segment = buffer_manager_->get_buffer_segment(size);
//     if (!temp_segment) {
//         std::cerr << "Failed to get temporary buffer segment" << std::endl;
//         return -1;
//     }
    
//     int ret = rdma_read_direct(temp_segment, size, remote_offset);
//     if (ret == 0) {
//         memcpy(dest_buf, temp_segment, size);
//     }
    
//     buffer_manager_->return_buffer_segment(temp_segment);
//     return ret;
// }

int RDMAConnection::rdma_read(void *local_buf, size_t size, uint64_t remote_offset) {
    if (!connected) {
        std::cerr << "RDMA not connected" << std::endl;
        return -1;
    }
        // Ensure we're using registered memory
    if (local_buf != local_buffer) {
        std::cerr << "ERROR: local_buf not equals registered memory region!" << std::endl;
        return -1;
    }
    
    if (size > buffer_size) {
        std::cerr << "ERROR: read size exceeds buffer size!" << std::endl;
        return -1;
    }
    
    // 检查本地缓冲区范围
    uintptr_t local_base = (uintptr_t)local_buffer;
    uintptr_t local_end = local_base + buffer_size;
    uintptr_t buf_addr = (uintptr_t)local_buf;
    
    if (buf_addr < local_base || buf_addr + size > local_end) {
        std::cerr << "ERROR: local_buf not in registered memory region!" << std::endl;
        return -1;
    }
    
    // 构造RDMA读取请求
    struct ibv_sge sge;
    struct ibv_send_wr wr, *bad_wr;
    
    sge.addr = buf_addr;
    sge.length = size;
    sge.lkey = mr->lkey;
    
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = vector_layout_.remote_addr + remote_offset;
    wr.wr.rdma.rkey = remote_rkey;
    
    // 发送读取请求
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    if (ret) {
        std::cerr << "RDMA read post failed: " << ret << std::endl;
        return ret;
    }
    
    // 等待读取完成
    return wait_for_completion();
}

int RDMAConnection::wait_for_completion() {
    struct ibv_wc wc;
    int polls = 0;
    const int max_polls = 1000000;
    
    while (polls < max_polls) {
        int ret = ibv_poll_cq(cq, 1, &wc);
        if (ret < 0) {
            std::cerr << "CQ polling error: " << ret << std::endl;
            return -1;
        }
        if (ret > 0) {
            if (wc.status == IBV_WC_SUCCESS) {
                return 0;
            } else {
                std::cerr << "Work completion error: " << wc.status << std::endl;
                return -1;
            }
        }
        polls++;
    }
    
    std::cerr << "Completion timeout after " << polls << " polls" << std::endl;
    return -1;
}

int RDMAConnection::load_and_write_index(const std::string& index_file_path) {
    std::cout << "\n=== Loading and Writing Index File ===" << std::endl;
    
    if (!connected) {
        std::cerr << "ERROR: Not connected to server" << std::endl;
        return -1;
    }
    
    // 1. 打开索引文件
    std::ifstream file(index_file_path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open index file: " << index_file_path << std::endl;
        return -1;
    }
    
    // 获取文件大小
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::cout << "File size: " << (file_size / 1024 / 1024) << " MB" << std::endl;
    
    if (file_size > vector_layout_.available_size) {
        std::cerr << "ERROR: File too large for server buffer!" << std::endl;
        std::cerr << "  File size: " << file_size << std::endl;
        std::cerr << "  Available: " << vector_layout_.available_size << std::endl;
        file.close();
        return -1;
    }

        // Get temporary buffer segment for writing
    // void* write_buffer = buffer_manager_->get_buffer_segment(2 * 1024 * 1024); // 2MB chunks
    //no use of buffer manager
    // if (!write_buffer) {
    //     std::cerr << "Failed to get buffer segment for writing" << std::endl;
    //     file.close();
    //     return -1;
    // }
    
    // 2. 分块写入文件到服务器
    std::cout << "Writing file to server via RDMA..." << std::endl;
    
    const size_t chunk_size = buffer_size;  // 使用整个本地缓冲区
    char* write_buffer = (char*)local_buffer;
    // const size_t chunk_size = 2 * 1024 * 1024;  // 2MB chunks
    // char* write_buffer_ptr = static_cast<char*>(write_buffer);
    size_t bytes_written = 0;
    uint64_t remote_offset = 0;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    while (bytes_written < file_size) {
        size_t to_write = std::min(chunk_size, file_size - bytes_written);
        
        // 读取数据到本地缓冲区
        file.read(write_buffer, to_write);
        if (file.gcount() != to_write) {
            std::cerr << "Failed to read file chunk" << std::endl;
            file.close();
            return -1;
        }
        
        // 写入到服务器
        if (rdma_write(write_buffer, to_write, remote_offset) != 0) {
            std::cerr << "RDMA write failed at offset " << remote_offset << std::endl;
            file.close();
            return -1;
        }
        
        bytes_written += to_write;
        remote_offset += to_write;
        
        // 进度显示
        if (bytes_written % (10 * 1024 * 1024) == 0 || bytes_written == file_size) {
            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
            double rate = (bytes_written / 1024.0 / 1024.0) / (elapsed.count() / 1000.0);
            
            std::cout << "Progress: " << (bytes_written / 1024 / 1024) 
                      << "/" << (file_size / 1024 / 1024) << " MB (" 
                      << std::fixed << std::setprecision(1) << rate << " MB/s)" << std::endl;
        }
    }
    // buffer_manager_->return_buffer_segment(write_buffer);
    file.close();
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    double avg_rate = (file_size / 1024.0 / 1024.0) / (total_time.count() / 1000.0);
    
    std::cout << "✓ Write complete!" << std::endl;
    std::cout << "Total time: " << total_time.count() << " ms" << std::endl;
    std::cout << "Average rate: " << std::fixed << std::setprecision(1) << avg_rate << " MB/s" << std::endl;
    
    
    return 0;
}


int RDMAConnection::test_simple_read() {
    std::cout << "\n=== Testing Simple RDMA Read ===" << std::endl;
    
    if (!connected) {
        std::cerr << "ERROR: Not connected to server" << std::endl;
        return -1;
    }
        // Get temporary buffer segment for testing
    // void* test_buffer = buffer_manager_->get_buffer_segment(16);
    // if (!test_buffer) {
    //     std::cerr << "Failed to get buffer segment for testing" << std::endl;
    //     return -1;
    // }
    char* test_buffer = (char*)local_buffer;
    
    // 测试1: 读取文件开头的16字节
    std::cout << "Test 1: Reading first 16 bytes from server..." << std::endl;
    memset(test_buffer, 0xFF, 16);  // 填充识别模式
    
    if (rdma_read(test_buffer, 16, 0) != 0) {
        std::cerr << "✗ RDMA read failed" << std::endl;
        // buffer_manager_->return_buffer_segment(test_buffer);
        return -1;
    }
    
    std::cout << "✓ Read successful!" << std::endl;
    std::cout << "First 16 bytes: ";
    char* bytes = static_cast<char*>(test_buffer);
    for (int i = 0; i < 16; i++) {
        printf("%02x ", (unsigned char)bytes[i]);
    }
    std::cout << std::endl;
    // buffer_manager_->return_buffer_segment(test_buffer);
    return 0;
}

// 新增：上传向量数据到RDMA服务器
template<typename data_t>
int RDMAConnection::upload_vector_data(const data_t* local_data, uint64_t num_vectors, uint64_t aligned_dim) {
    if (!connected) {
        std::cerr << "RDMA not connected" << std::endl;
        return -1;
    }
    
    std::cout << "\n=== Uploading Vector Data to RDMA Server ===" << std::endl;
    
    // 首先写入向量元数据到服务器
    VectorDataMetadata metadata{};
    metadata.num_vectors = num_vectors;
    metadata.aligned_dim = aligned_dim;
    metadata.element_size = aligned_dim * sizeof(data_t);
    metadata.data_type_size = sizeof(data_t);
    metadata.total_data_size = num_vectors * metadata.element_size;
    metadata.data_start_offset = sizeof(VectorDataMetadata);  // 数据紧跟在元数据后面
    metadata.metadata_valid = true;
    
    // 设置数据类型名称
    if (std::is_same_v<data_t, float>) {
        strcpy(metadata.data_type_name, "float");
    } else if (std::is_same_v<data_t, uint8_t>) {
        strcpy(metadata.data_type_name, "uint8_t");
    } else if (std::is_same_v<data_t, int8_t>) {
        strcpy(metadata.data_type_name, "int8_t");
    } else {
        strcpy(metadata.data_type_name, "unknown");
    }
    
    // 计算总大小（包含元数据）
    uint64_t total_size_with_metadata = metadata.data_start_offset + metadata.total_data_size;
    
    std::cout << "Vector data parameters:" << std::endl;
    std::cout << "  Number of vectors: " << num_vectors << std::endl;
    std::cout << "  Aligned dimension: " << aligned_dim << std::endl;
    std::cout << "  Element size: " << metadata.element_size << " bytes" << std::endl;
    std::cout << "  Data type: " << metadata.data_type_name << std::endl;
    std::cout << "  Metadata size: " << sizeof(VectorDataMetadata) << " bytes" << std::endl;
    std::cout << "  Vector data size: " << (metadata.total_data_size / 1024 / 1024) << " MB" << std::endl;
    std::cout << "  Total size (with metadata): " << (total_size_with_metadata / 1024 / 1024) << " MB" << std::endl;
    
    // 检查服务器容量
    if (total_size_with_metadata > vector_layout_.available_size) {
        std::cerr << "ERROR: Data too large for server memory!" << std::endl;
        std::cerr << "  Required: " << (total_size_with_metadata / 1024 / 1024) << " MB" << std::endl;
        std::cerr << "  Available: " << (vector_layout_.available_size / 1024 / 1024) << " MB" << std::endl;
        return -1;
    }
    
        // Get temporary buffer segment for uploading
    size_t upload_chunk_size = 2 * 1024 * 1024; // 2MB chunks
    // void* upload_buffer = buffer_manager_->get_buffer_segment(upload_chunk_size);
    if (!local_buffer) {
        std::cerr << "Failed to get buffer segment for uploading" << std::endl;
        return -1;
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    
    // 第一步：上传元数据
    std::cout << "Step 1: Uploading vector metadata..." << std::endl;
    memcpy(local_buffer, &metadata, sizeof(VectorDataMetadata));
    // memcpy(upload_buffer, &metadata, sizeof(VectorDataMetadata));
    
    if (rdma_write(local_buffer, sizeof(VectorDataMetadata), 0) != 0) {
        std::cerr << "Failed to upload metadata to RDMA server" << std::endl;
        // buffer_manager_->return_buffer_segment(local_buffer);
        return -1;
    }
    
    std::cout << "✓ Metadata uploaded successfully" << std::endl;
    
    // 第二步：分块上传向量数据
    std::cout << "Step 2: Uploading vector data..." << std::endl;
    
    const size_t chunk_size = buffer_size;  // 修正：使用正确的成员变量名
    const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(local_data);
    uint64_t bytes_uploaded = 0;
    uint64_t remote_offset = metadata.data_start_offset;  // 从元数据后开始写入向量数据
    
    while (bytes_uploaded < metadata.total_data_size) {
        size_t to_upload = std::min(chunk_size, static_cast<size_t>(metadata.total_data_size - bytes_uploaded));
        
        // 复制数据到本地RDMA缓冲区
        memcpy(local_buffer, data_ptr + bytes_uploaded, to_upload);
        
        // 执行RDMA写入
        if (rdma_write(local_buffer, to_upload, remote_offset) != 0) {
            std::cerr << "RDMA write failed at offset " << remote_offset << std::endl;
            return -1;
        }
        
        bytes_uploaded += to_upload;
        remote_offset += to_upload;
        
        // 进度显示
        if (bytes_uploaded % (10 * 1024 * 1024) == 0 || bytes_uploaded == metadata.total_data_size) {
            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
            double rate = (bytes_uploaded / 1024.0 / 1024.0) / (elapsed.count() / 1000.0);
            
            std::cout << "Progress: " << (bytes_uploaded / 1024 / 1024) 
                      << "/" << (metadata.total_data_size / 1024 / 1024) << " MB (" 
                      << std::fixed << std::setprecision(1) << rate << " MB/s)" << std::endl;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    double avg_rate = (total_size_with_metadata / 1024.0 / 1024.0) / (total_time.count() / 1000.0);
    
    std::cout << "✓ Upload complete!" << std::endl;
    std::cout << "Total time: " << total_time.count() << " ms" << std::endl;
    std::cout << "Average rate: " << std::fixed << std::setprecision(1) << avg_rate << " MB/s" << std::endl;
    // buffer_manager_->return_buffer_segment(upload_buffer);
    // 更新本地布局信息
    vector_layout_.data_start_offset = metadata.data_start_offset;
    vector_layout_.num_vectors = num_vectors;
    vector_layout_.aligned_dim = aligned_dim;
    vector_layout_.element_size = metadata.element_size;
    vector_layout_.total_size = metadata.total_data_size;
    vector_layout_.layout_valid = true;
    
    std::cout << "✓ Vector data layout updated:" << std::endl;
    std::cout << "  Data starts at remote offset: " << vector_layout_.data_start_offset << std::endl;
    std::cout << "  Vector address calculation: remote_base + " << vector_layout_.data_start_offset 
              << " + vector_id * " << vector_layout_.element_size << std::endl;
    
    return 0;
}

// 新增：获取远程数据的虚拟指针
template<typename data_t>
data_t* RDMAConnection::get_remote_data_pointer() {
    if (!vector_layout_.layout_valid) {
        std::cerr << "Vector layout not valid" << std::endl;
        return nullptr;
    }
    
    // 重要：返回一个特殊的指针值，表示这是RDMA远程内存
    // 这个指针用于InMemDataStore中的地址计算
    // 实际的内存访问会通过RDMA操作完成
    
    // 使用远程内存的实际地址 + 数据偏移作为虚拟指针
    uint64_t virtual_data_addr = vector_layout_.remote_addr + vector_layout_.data_start_offset;
    
    std::cout << "Virtual data pointer created:" << std::endl;
    std::cout << "  Remote base: 0x" << std::hex << vector_layout_.remote_addr << std::dec << std::endl;
    std::cout << "  Data offset: " << vector_layout_.data_start_offset << std::endl;
    std::cout << "  Virtual ptr: 0x" << std::hex << virtual_data_addr << std::dec << std::endl;
    
    return reinterpret_cast<data_t*>(virtual_data_addr);
}

// 添加解析向量元数据的方法
int RDMAConnection::parse_vector_metadata() {
    if (!connected) {
        std::cerr << "Cannot parse metadata: not connected" << std::endl;
        return -1;
    }
    
    std::cout << "Parsing vector metadata from remote server..." << std::endl;
        // Get temporary buffer segment for reading metadata
    // void* header_buffer = buffer_manager_->get_buffer_segment(sizeof(VectorDataMetadata));
    // if (!header_buffer) {
    //     std::cerr << "Failed to get buffer segment for metadata" << std::endl;
    //     return -1;
    // }
    // 修复：先读取到已注册的本地缓冲区（参考旧模板）
    char* header_buffer = (char*)local_buffer;
    
    if (rdma_read(header_buffer, sizeof(VectorDataMetadata), 0) != 0) {
        std::cerr << "Failed to read metadata from server" << std::endl;
        // buffer_manager_->return_buffer_segment(header_buffer);
        return -1;
    }
    
    // 然后从本地缓冲区解析元数据（参考旧模板的解析方式）
    char* bytes = static_cast<char*>(header_buffer);
    size_t pos = 0;
    
    try {
        VectorDataMetadata metadata;
        
        // 逐字段读取（避免结构体对齐问题）
        metadata.num_vectors = *(uint64_t*)(bytes + pos); pos += sizeof(uint64_t);
        metadata.aligned_dim = *(uint64_t*)(bytes + pos); pos += sizeof(uint64_t);
        metadata.element_size = *(uint64_t*)(bytes + pos); pos += sizeof(uint64_t);
        metadata.data_type_size = *(uint64_t*)(bytes + pos); pos += sizeof(uint64_t);
        metadata.total_data_size = *(uint64_t*)(bytes + pos); pos += sizeof(uint64_t);
        metadata.data_start_offset = *(uint64_t*)(bytes + pos); pos += sizeof(uint64_t);
        metadata.metadata_valid = *(bool*)(bytes + pos); pos += sizeof(bool);
        
        // 读取数据类型名称
        memcpy(metadata.data_type_name, bytes + pos, 16);
        metadata.data_type_name[15] = '\0';  // 确保字符串结束
        
        // 验证元数据的合理性
        if (!metadata.metadata_valid ||
            metadata.num_vectors == 0 || metadata.num_vectors > 100000000 ||  // 不超过1亿个向量
            metadata.aligned_dim == 0 || metadata.aligned_dim > 10000 ||      // 维度合理
            metadata.element_size == 0 || metadata.element_size > 50000 ||    // 单向量大小合理
            metadata.data_start_offset < sizeof(VectorDataMetadata)) {        // 偏移合理
            
            std::cerr << "Invalid metadata received from server:" << std::endl;
            std::cerr << "  metadata_valid: " << metadata.metadata_valid << std::endl;
            std::cerr << "  num_vectors: " << metadata.num_vectors << std::endl;
            std::cerr << "  aligned_dim: " << metadata.aligned_dim << std::endl;
            std::cerr << "  element_size: " << metadata.element_size << std::endl;
            std::cerr << "  data_start_offset: " << metadata.data_start_offset << std::endl;
            return -1;
        }
        
        // 更新本地布局信息
        vector_layout_.data_start_offset = metadata.data_start_offset;
        vector_layout_.num_vectors = metadata.num_vectors;
        vector_layout_.aligned_dim = metadata.aligned_dim;
        vector_layout_.element_size = metadata.element_size;
        vector_layout_.total_size = metadata.total_data_size;
        vector_layout_.layout_valid = true;
        
        std::cout << "✓ Vector metadata parsed successfully:" << std::endl;
        std::cout << "  Number of vectors: " << metadata.num_vectors << std::endl;
        std::cout << "  Aligned dimension: " << metadata.aligned_dim << std::endl;
        std::cout << "  Element size: " << metadata.element_size << " bytes" << std::endl;
        std::cout << "  Data type: " << metadata.data_type_name << std::endl;
        std::cout << "  Data starts at offset: " << metadata.data_start_offset << std::endl;
        
        return 0;
        
    } catch (...) {
        std::cerr << "Metadata parsing failed" << std::endl;
        return -1;
    }
}

char* RDMAConnection::rdma_read_vector_data_direct(uint64_t vector_id) {
    if (!vector_layout_.layout_valid) {
        std::cerr << "ERROR: Vector layout not valid" << std::endl;
        std::cerr << "Trying to parse vector metadata again..." << std::endl;
        
        // 尝试重新解析向量元数据
        if (parse_vector_metadata() != 0) {
            std::cerr << "Failed to parse vector metadata" << std::endl;
            return nullptr;
        }
        
        if (!vector_layout_.layout_valid) {
            std::cerr << "Vector metadata parsing still failed" << std::endl;
            return nullptr;
        }
        
        std::cout << "✓ Vector metadata parsed successfully on retry" << std::endl;
    }
    
    if (vector_id >= vector_layout_.num_vectors) {
        std::cerr << "Vector ID out of range: " << vector_id << std::endl;
        return nullptr;
    }
    
    // 计算远程偏移
    uint64_t remote_offset = vector_layout_.data_start_offset + vector_id * vector_layout_.element_size;
    // void* temp_segment = buffer_manager_->get_buffer_segment(vector_layout_.element_size);
    // 读取到本地缓冲区
    if (rdma_read(local_buffer, vector_layout_.element_size, remote_offset) != 0) {
        std::cerr << "Failed to read vector " << vector_id << std::endl;
        return nullptr;
    }
    if (rdma_read_direct(local_buffer, vector_layout_.element_size, remote_offset) != 0) {
        // buffer_manager_->return_buffer_segment(temp_segment);
        return nullptr;
    }
    
    return static_cast<char*>(local_buffer);
    // return static_cast<char*>(temp_segment);
}

// 新增：读取单个向量
template<typename data_t>
int RDMAConnection::read_vector(uint64_t vector_id, data_t* dest_buffer) {
    if (!vector_layout_.layout_valid) {
        std::cerr << "Vector layout not valid" << std::endl;
        return -1;
    }
    
    if (vector_id >= vector_layout_.num_vectors) {
        std::cerr << "Vector ID out of range: " << vector_id << std::endl;
        return -1;
    }
    
    // 计算远程偏移
    uint64_t remote_offset = vector_layout_.data_start_offset + vector_id * vector_layout_.element_size;
    
    // 读取向量数据
    return rdma_read(dest_buffer, vector_layout_.element_size, remote_offset);
}

// 新增：批量读取向量
template<typename data_t>
int RDMAConnection::batch_read_vectors(const uint64_t* vector_ids, uint32_t count, data_t* dest_buffer) {
    if (!vector_layout_.layout_valid) {
        std::cerr << "Vector layout not valid" << std::endl;
        return -1;
    }
    
    for (uint32_t i = 0; i < count; ++i) {
        uint64_t vector_id = vector_ids[i];
        data_t* dest = dest_buffer + i * vector_layout_.aligned_dim;
        
        if (read_vector(vector_id, dest) != 0) {
            std::cerr << "Failed to read vector " << vector_id << std::endl;
            return -1;
        }
    }
    
    return 0;
}

// 显式实例化模板
template int RDMAConnection::upload_vector_data<float>(const float*, uint64_t, uint64_t);
template int RDMAConnection::upload_vector_data<uint8_t>(const uint8_t*, uint64_t, uint64_t);
template int RDMAConnection::upload_vector_data<int8_t>(const int8_t*, uint64_t, uint64_t);

template float* RDMAConnection::get_remote_data_pointer<float>();
template uint8_t* RDMAConnection::get_remote_data_pointer<uint8_t>();
template int8_t* RDMAConnection::get_remote_data_pointer<int8_t>();

template int RDMAConnection::read_vector<float>(uint64_t, float*);
template int RDMAConnection::read_vector<uint8_t>(uint64_t, uint8_t*);
template int RDMAConnection::read_vector<int8_t>(uint64_t, int8_t*);

template int RDMAConnection::batch_read_vectors<float>(const uint64_t*, uint32_t, float*);
template int RDMAConnection::batch_read_vectors<uint8_t>(const uint64_t*, uint32_t, uint8_t*);
template int RDMAConnection::batch_read_vectors<int8_t>(const uint64_t*, uint32_t, int8_t*);


} // namespace hnswlib