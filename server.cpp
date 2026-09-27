
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <mutex>
#include <memory>
#include <vector>
#include <atomic>
#include <csignal>
#include <unordered_map>
#include <chrono>
#include <queue>
#include <condition_variable>

constexpr uint16_t DEFAULT_PORT = 7471;
constexpr size_t SECTOR_LEN = 4096;
constexpr size_t DEFAULT_MAX_SECTORS = 170000000;  // ~664 GB for BigANN 1B (index is 636 GB)
constexpr size_t DEFAULT_BUFFER_SIZE = SECTOR_LEN * DEFAULT_MAX_SECTORS;

struct RemoteMR {
    uint64_t addr;
    uint32_t rkey;
    uint64_t size;  // must match client (include/rdma_connection.h)
} __attribute__((packed));

struct IndexMetadata {
    uint64_t npts;
    uint64_t ndims;
    uint64_t medoid;
    uint64_t max_node_len;
    uint64_t nnodes_per_sector;
    uint64_t vamana_frozen_num;
    uint64_t frozen_loc;
    uint64_t has_reorder_data;
    uint64_t total_size;
};

enum class ConnectionState {
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
    ERROR
};

// Fixed ClientConnection with proper thread management
struct ClientConnection {
    rdma_cm_id* client_id;
    ibv_pd* pd;
    ibv_mr* mr;
    ibv_cq* cq;
    std::thread worker_thread;
    std::atomic<ConnectionState> state;
    std::atomic<bool> should_stop{false};  // Add stop flag
    uint32_t connection_id;
    std::chrono::steady_clock::time_point connect_time;
    
    ClientConnection() : client_id(nullptr), pd(nullptr), mr(nullptr), 
                        cq(nullptr), state(ConnectionState::CONNECTING), connection_id(0) {}
    
    ~ClientConnection() {
        cleanup();
    }
    
    void cleanup() {
        // Signal thread to stop first
        should_stop = true;
        state = ConnectionState::DISCONNECTED;
        
        // Only join if we're not the worker thread itself
        if (worker_thread.joinable()) {
            std::thread::id current_id = std::this_thread::get_id();
            std::thread::id worker_id = worker_thread.get_id();
            
            if (current_id != worker_id) {
                // Safe to join from different thread
                worker_thread.join();
            } else {
                // We're in the worker thread - detach instead
                worker_thread.detach();
            }
        }
        
        // Clean up RDMA resources
        if (mr) {
            ibv_dereg_mr(mr);
            mr = nullptr;
        }
        if (cq) {
            ibv_destroy_cq(cq);
            cq = nullptr;
        }
        if (pd) {
            ibv_dealloc_pd(pd);
            pd = nullptr;
        }
        if (client_id) {
            rdma_destroy_id(client_id);
            client_id = nullptr;
        }
    }
};

class RDMAIndexServer {
private:
    std::unique_ptr<char[]> index_buffer_;
    size_t buffer_size_;
    IndexMetadata metadata_;
    bool index_uploaded_;
    mutable std::mutex buffer_mutex_;
    
    rdma_event_channel* event_channel_;
    rdma_cm_id* listen_id_;

    // Shared RDMA resources — one PD + MR for all clients (avoids 664GB per-client reg)
    ibv_pd* shared_pd_{nullptr};
    ibv_mr* shared_mr_{nullptr};
    std::mutex shared_rdma_mutex_;

    std::unordered_map<rdma_cm_id*, std::shared_ptr<ClientConnection>> clients_;
    std::mutex clients_mutex_;
    std::atomic<uint32_t> next_connection_id_{1};
    std::atomic<bool> server_running_{true};
    
    std::atomic<uint64_t> total_connections_{0};
    std::atomic<uint64_t> active_connections_{0};
    std::atomic<uint64_t> total_index_reads_{0};
    std::atomic<uint64_t> total_sector_reads_{0};
    std::atomic<uint64_t> total_bytes_read_{0};
    std::atomic<uint64_t> total_bytes_written_{0};
    
    std::thread stats_thread_;
    std::thread monitor_thread_;
    
    // Add cleanup thread for safe client cleanup
    std::thread cleanup_thread_;
    std::queue<std::shared_ptr<ClientConnection>> cleanup_queue_;
    std::mutex cleanup_mutex_;
    std::condition_variable cleanup_cv_;
    
public:
    RDMAIndexServer(size_t buffer_size = DEFAULT_BUFFER_SIZE) :
        buffer_size_(buffer_size),
        index_uploaded_(false),
        event_channel_(nullptr),
        listen_id_(nullptr) {

        std::cerr << "Allocating buffer: " << (buffer_size_ / 1024 / 1024) << " MB ..." << std::endl;
        index_buffer_ = std::make_unique<char[]>(buffer_size_);
        memset(index_buffer_.get(), 0, buffer_size_);
        memset(&metadata_, 0, sizeof(metadata_));

        std::cerr << "RDMA Index Server initialized" << std::endl;
        std::cerr << "Buffer size: " << (buffer_size_ / 1024 / 1024) << " MB" << std::endl;
    }

    // Load a byte range from a local file into the buffer (for sharded serving)
    void load_shard_from_file(const std::string& path, uint64_t file_offset, uint64_t size) {
        if (size > buffer_size_) {
            throw std::runtime_error("Shard size " + std::to_string(size) +
                                     " exceeds buffer " + std::to_string(buffer_size_));
        }
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open " + path);

        constexpr size_t CHUNK = 1024 * 1024;  // 1 MB chunks
        uint64_t remaining = size;
        uint64_t buf_offset = 0;
        while (remaining > 0) {
            size_t to_read = std::min(remaining, (uint64_t)CHUNK);
            ssize_t n = pread(fd, index_buffer_.get() + buf_offset, to_read, file_offset + buf_offset);
            if (n <= 0) {
                ::close(fd);
                throw std::runtime_error("pread failed at offset " + std::to_string(file_offset + buf_offset));
            }
            buf_offset += n;
            remaining -= n;
            if (buf_offset % (100 * CHUNK) == 0) {
                std::cerr << "\r  Loaded " << (buf_offset / 1024 / 1024) << " / "
                          << (size / 1024 / 1024) << " MB" << std::flush;
            }
        }
        ::close(fd);
        std::cerr << "\r  Loaded " << (size / 1024 / 1024) << " / "
                  << (size / 1024 / 1024) << " MB — done." << std::endl;
        index_uploaded_ = true;
    }
    
    ~RDMAIndexServer() {
        shutdown_server();
    }
    
    void start_server(const std::string& bind_ip, uint16_t port = DEFAULT_PORT) {
        event_channel_ = rdma_create_event_channel();
        if (!event_channel_) throw std::runtime_error("Failed to create event channel");

        if (rdma_create_id(event_channel_, &listen_id_, nullptr, RDMA_PS_TCP))
            throw std::runtime_error("Failed to create listen ID");

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (!inet_pton(AF_INET, bind_ip.c_str(), &addr.sin_addr))
            throw std::runtime_error("Invalid bind IP");

        if (rdma_bind_addr(listen_id_, (sockaddr*)&addr))
            throw std::runtime_error("Failed to bind address");

        if (rdma_listen(listen_id_, 128))
            throw std::runtime_error("Failed to listen");

        std::cerr << "RDMA Index Server listening on " << bind_ip << ":" << port << std::endl;
        std::cerr << "Ready for DiskANN index data uploads and searches..." << std::endl;
        
        // Start all threads
        stats_thread_ = std::thread(&RDMAIndexServer::stats_worker, this);
        monitor_thread_ = std::thread(&RDMAIndexServer::monitor_index_changes, this);
        cleanup_thread_ = std::thread(&RDMAIndexServer::cleanup_worker, this);  // New cleanup thread
        
        event_loop();
        
        // Wait for threads to finish
        if (stats_thread_.joinable()) stats_thread_.join();
        if (monitor_thread_.joinable()) monitor_thread_.join();
        if (cleanup_thread_.joinable()) cleanup_thread_.join();
    }
    
    void shutdown_server() {
        std::cout << "\nShutting down index server..." << std::endl;
        server_running_ = false;
        
        // Notify cleanup thread to exit
        cleanup_cv_.notify_all();
        
        // Signal all clients to stop
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            for (auto& pair : clients_) {
                if (pair.second) {
                    pair.second->should_stop = true;
                    pair.second->state = ConnectionState::DISCONNECTED;
                }
            }
        }
        
        // Give threads time to exit gracefully
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        // Clear clients
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_.clear();
        }
        
        cleanup();
        print_final_stats();
    }

private:
    // New cleanup worker to safely handle client disconnections
    void cleanup_worker() {
        while (server_running_) {
            std::unique_lock<std::mutex> lock(cleanup_mutex_);
            cleanup_cv_.wait(lock, [this]() { 
                return !cleanup_queue_.empty() || !server_running_; 
            });
            
            while (!cleanup_queue_.empty()) {
                auto client = cleanup_queue_.front();
                cleanup_queue_.pop();
                lock.unlock();
                
                // Safe cleanup outside of any locks
                if (client) {
                    std::cout << "Safely cleaning up client " << client->connection_id << std::endl;
                    client->cleanup();
                }
                
                lock.lock();
            }
        }
        
        // Final cleanup
        std::lock_guard<std::mutex> lock(cleanup_mutex_);
        while (!cleanup_queue_.empty()) {
            auto client = cleanup_queue_.front();
            cleanup_queue_.pop();
            if (client) client->cleanup();
        }
    }
    
    void monitor_index_changes() {
        size_t last_check_size = 0;
        auto last_check_time = std::chrono::steady_clock::now();
        
        while (server_running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            
            size_t current_data_size = detect_data_size();
            
            if (current_data_size > last_check_size) {
                size_t new_bytes = current_data_size - last_check_size;
                total_bytes_written_ += new_bytes;
                
                std::cout << "Index data received: " << (current_data_size / 1024 / 1024) 
                          << " MB (+" << (new_bytes / 1024) << " KB)" << std::endl;
                
                last_check_size = current_data_size;
                last_check_time = std::chrono::steady_clock::now();
                
                if (current_data_size >= sizeof(IndexMetadata) && !index_uploaded_) {
                    try_parse_index_metadata();
                }
            }
            
            if (current_data_size > 0 && !index_uploaded_) {
                auto now = std::chrono::steady_clock::now();
                auto since_last_change = std::chrono::duration_cast<std::chrono::seconds>(now - last_check_time);
                
                if (since_last_change.count() >= 2) {
                    std::cout << "\n=== Index Upload Complete ===" << std::endl;
                    std::cout << "Total size: " << (current_data_size / 1024 / 1024) << " MB" << std::endl;
                    
                    if (metadata_.npts > 0) {
                        print_index_info();
                    }
                    
                    index_uploaded_ = true;
                }
            }
        }
    }
    
    void try_parse_index_metadata() {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        
        if (index_uploaded_) return;
        
        size_t metadata_offset = buffer_size_ - sizeof(IndexMetadata);
        IndexMetadata* meta = reinterpret_cast<IndexMetadata*>(
            index_buffer_.get() + metadata_offset);
        
        if (meta->npts > 0 && meta->npts < 10000000000ULL &&  // up to 10B points
            meta->ndims > 0 && meta->ndims <= 10000 &&
            meta->total_size > 0 && meta->total_size <= buffer_size_) {
            
            metadata_ = *meta;
            
            std::cout << "\n=== Index Metadata Parsed ===" << std::endl;
            std::cout << "Points: " << metadata_.npts << std::endl;
            std::cout << "Dimensions: " << metadata_.ndims << std::endl;
            std::cout << "Medoid: " << metadata_.medoid << std::endl;
            std::cout << "Max node length: " << metadata_.max_node_len << std::endl;
            std::cout << "Nodes per sector: " << metadata_.nnodes_per_sector << std::endl;
            std::cout << "Total size: " << metadata_.total_size << " bytes" << std::endl;
        }
    }
    
    void print_index_info() {
        std::cout << "\n=== Index Data Information ===" << std::endl;
        std::cout << "Total points: " << metadata_.npts << std::endl;
        std::cout << "Dimensions: " << metadata_.ndims << std::endl;
        std::cout << "Sectors per node: " << metadata_.nnodes_per_sector << std::endl;
        std::cout << "Total index data: " << (metadata_.total_size / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Memory layout compatible with DiskANN search" << std::endl;
        std::cout << "Ready for index searches by ALL CLIENTS!" << std::endl;
    }
    
    void stats_worker() {
        int counter = 0;
        while (server_running_) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            
            if (!server_running_) break;
            
            counter++;
            if (counter % 3 == 0) {
                std::cout << "\n=== Index Server Statistics ===" << std::endl;
                std::cout << "Total connections: " << total_connections_.load() << std::endl;
                std::cout << "Active connections: " << active_connections_.load() << std::endl;
                std::cout << "Index reads: " << total_index_reads_.load() << std::endl;
                std::cout << "Sector reads: " << total_sector_reads_.load() << std::endl;
                std::cout << "Bytes read: " << (total_bytes_read_.load() / 1024 / 1024) << " MB" << std::endl;
                std::cout << "Bytes written: " << (total_bytes_written_.load() / 1024 / 1024) << " MB" << std::endl;
                std::cout << "Index uploaded: " << (index_uploaded_ ? "YES" : "NO") << std::endl;
                if (metadata_.npts > 0) {
                    std::cout << "Available points: " << metadata_.npts << std::endl;
                }
                std::cout << "===============================" << std::endl;
            }
        }
    }
    
    void event_loop() {
        std::cout << "Starting index server event loop..." << std::endl;
        
        while (server_running_) {
            rdma_cm_event* ev;
            int ret = rdma_get_cm_event(event_channel_, &ev);
            if (ret) {
                if (server_running_) {
                    std::cerr << "Failed to get CM event: " << ret << std::endl;
                }
                break;
            }
            
            process_cm_event(ev);
            rdma_ack_cm_event(ev);
        }
        
        std::cout << "Index server event loop ended" << std::endl;
    }
    
    void process_cm_event(rdma_cm_event* ev) {
        switch (ev->event) {
            case RDMA_CM_EVENT_CONNECT_REQUEST:
                handle_connect_request(ev);
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                handle_connection_established(ev);
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                handle_disconnection(ev);
                break;
            case RDMA_CM_EVENT_CONNECT_ERROR:
            case RDMA_CM_EVENT_UNREACHABLE:
            case RDMA_CM_EVENT_REJECTED:
                handle_connection_error(ev);
                break;
            default:
                std::cout << "Unhandled CM event: " << ev->event << std::endl;
                break;
        }
    }
    
    void handle_connect_request(rdma_cm_event* ev) {
        rdma_cm_id* client_id = ev->id;
        uint32_t conn_id = next_connection_id_++;
        
        std::cerr << "\n=== Index Client Connection " << conn_id << " ===" << std::endl;
        
        auto client = std::make_shared<ClientConnection>();
        client->client_id = client_id;
        client->connection_id = conn_id;
        client->connect_time = std::chrono::steady_clock::now();
        client->state = ConnectionState::CONNECTING;
        
        try {
            if (setup_client_resources(client) != 0) {
                std::cerr << "Failed to setup resources for connection " << conn_id << std::endl;
                client->state = ConnectionState::ERROR;
                return;
            }
            
            if (accept_connection_async(client) != 0) {
                std::cerr << "Failed to accept connection " << conn_id << std::endl;
                client->state = ConnectionState::ERROR;
                return;
            }
            
            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                clients_[client_id] = client;
            }
            
            total_connections_++;
            std::cout << "Index client " << conn_id << " connection request processed" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Error processing index client " << conn_id << ": " << e.what() << std::endl;
            client->state = ConnectionState::ERROR;
        }
    }
    
    void handle_connection_established(rdma_cm_event* ev) {
        std::shared_ptr<ClientConnection> client;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto it = clients_.find(ev->id);
            if (it != clients_.end()) {
                client = it->second;
            }
        }
        
        if (client && client->state == ConnectionState::CONNECTING) {
            client->state = ConnectionState::CONNECTED;
            active_connections_++;
            
            std::cout << "✓ Index client " << client->connection_id << " CONNECTED!" << std::endl;
            std::cout << "Active index clients: " << active_connections_.load() << std::endl;
            
            client->worker_thread = std::thread(&RDMAIndexServer::handle_client_session, this, client);
        }
    }
    
    void handle_disconnection(rdma_cm_event* ev) {
        std::shared_ptr<ClientConnection> client;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto it = clients_.find(ev->id);
            if (it != clients_.end()) {
                client = it->second;
                clients_.erase(it);
            }
        }
        
        if (client) {
            if (client->state == ConnectionState::CONNECTED) {
                active_connections_--;
            }
            client->state = ConnectionState::DISCONNECTED;
            client->should_stop = true;
            
            std::cout << "Index client " << client->connection_id << " DISCONNECTED" << std::endl;
            
            // Schedule for safe cleanup
            {
                std::lock_guard<std::mutex> lock(cleanup_mutex_);
                cleanup_queue_.push(client);
                cleanup_cv_.notify_one();
            }
        }
    }
    
    void handle_connection_error(rdma_cm_event* ev) {
        std::shared_ptr<ClientConnection> client;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto it = clients_.find(ev->id);
            if (it != clients_.end()) {
                client = it->second;
                clients_.erase(it);
            }
        }
        
        if (client) {
            client->state = ConnectionState::ERROR;
            client->should_stop = true;
            std::cerr << "Index client " << client->connection_id << " ERROR: " << ev->event << std::endl;
        }
    }
    
    int setup_client_resources(std::shared_ptr<ClientConnection> client) {
        rdma_cm_id* client_id = client->client_id;

        // Create shared PD + MR on first connection, reuse for all clients
        {
            std::lock_guard<std::mutex> lock(shared_rdma_mutex_);
            if (!shared_pd_) {
                shared_pd_ = ibv_alloc_pd(client_id->verbs);
                if (!shared_pd_) return -1;
                shared_mr_ = ibv_reg_mr(shared_pd_, index_buffer_.get(), buffer_size_,
                                       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
                if (!shared_mr_) return -1;
                std::cerr << "Shared PD+MR created (buffer " << (buffer_size_ / 1024 / 1024) << " MB)" << std::endl;
            }
        }

        client->pd = nullptr;  // don't own PD — shared
        client->mr = nullptr;  // don't own MR — shared

        client->cq = ibv_create_cq(client_id->verbs, 256, nullptr, nullptr, 0);
        if (!client->cq) return -1;

        ibv_qp_init_attr qp_attr{};
        qp_attr.send_cq = client->cq;
        qp_attr.recv_cq = client->cq;
        qp_attr.cap.max_send_wr = 256;
        qp_attr.cap.max_recv_wr = 256;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.sq_sig_all = 0;

        if (rdma_create_qp(client_id, shared_pd_, &qp_attr)) return -1;

        return 0;
    }
    
    int accept_connection_async(std::shared_ptr<ClientConnection> client) {
        RemoteMR remote_info{};
        remote_info.addr = reinterpret_cast<uint64_t>(index_buffer_.get());
        remote_info.rkey = shared_mr_->rkey;
        remote_info.size = static_cast<uint64_t>(buffer_size_);
        
        rdma_conn_param conn_param{};
        conn_param.private_data = &remote_info;
        conn_param.private_data_len = sizeof(remote_info);
        conn_param.initiator_depth = 4;
        conn_param.responder_resources = 4;
        conn_param.retry_count = 7;
        
        return rdma_accept(client->client_id, &conn_param);
    }
    
    void handle_client_session(std::shared_ptr<ClientConnection> client) {
        std::cout << "Index client " << client->connection_id << " session started" << std::endl;
        
        // Check should_stop flag instead of just state
        while (client->state == ConnectionState::CONNECTED && 
               !client->should_stop && 
               server_running_) {
            
            ibv_wc wc[16];
            int completed = ibv_poll_cq(client->cq, 16, wc);
            if (completed > 0) {
                for (int i = 0; i < completed; i++) {
                    if (wc[i].status == IBV_WC_SUCCESS) {
                        total_index_reads_++;
                        total_sector_reads_++;
                        total_bytes_read_ += SECTOR_LEN;
                    }
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "Index client " << client->connection_id << " session ended" << std::endl;
        // Don't call cleanup here - it will be handled by cleanup_worker
    }
    
    size_t detect_data_size() {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        
        size_t data_size = 0;
        for (size_t i = buffer_size_ - 1; i > 0; i--) {
            if (index_buffer_[i] != 0) {
                data_size = i + 1;
                break;
            }
        }
        return data_size;
    }
    
    void print_final_stats() {
        std::cout << "\n=== Final Index Server Statistics ===" << std::endl;
        std::cout << "Total connections served: " << total_connections_.load() << std::endl;
        std::cout << "Total index reads: " << total_index_reads_.load() << std::endl;
        std::cout << "Total sector reads: " << total_sector_reads_.load() << std::endl;
        std::cout << "Total MB read: " << (total_bytes_read_.load() / 1024 / 1024) << std::endl;
        std::cout << "Total MB written: " << (total_bytes_written_.load() / 1024 / 1024) << std::endl;
        std::cout << "Index available: " << (index_uploaded_ ? "YES" : "NO") << std::endl;
        if (metadata_.npts > 0) {
            std::cout << "Index points: " << metadata_.npts << std::endl;
        }
        std::cout << "====================================" << std::endl;
    }
    
    void cleanup() {
        if (shared_mr_) {
            ibv_dereg_mr(shared_mr_);
            shared_mr_ = nullptr;
        }
        if (shared_pd_) {
            ibv_dealloc_pd(shared_pd_);
            shared_pd_ = nullptr;
        }
        if (listen_id_) {
            rdma_destroy_id(listen_id_);
            listen_id_ = nullptr;
        }
        if (event_channel_) {
            rdma_destroy_event_channel(event_channel_);
            event_channel_ = nullptr;
        }
    }

public:
    bool is_index_ready() const {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        return index_uploaded_ && metadata_.npts > 0;
    }
    
    uint64_t get_active_connections() const {
        return active_connections_.load();
    }
    
    uint64_t get_point_count() const {
        return metadata_.npts;
    }
};

RDMAIndexServer* g_server_instance = nullptr;

void signal_handler(int signum) {
    std::cout << "\nReceived signal " << signum << ", shutting down index server..." << std::endl;
    if (g_server_instance) {
        g_server_instance->shutdown_server();
    }
    exit(0);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <bind_ip> [port] [--local_load <file> <offset_bytes> <size_bytes>]" << std::endl;
        std::cerr << "Example: " << argv[0] << " <bind_ip> 7471" << std::endl;
        std::cerr << "Example: " << argv[0] << " <bind_ip> 7471 --local_load <disk.index> 0 <size_bytes>" << std::endl;
        return 1;
    }

    std::string bind_ip = argv[1];
    uint16_t port = DEFAULT_PORT;
    std::string load_file;
    uint64_t load_offset = 0, load_size = 0;

    // Parse optional args
    int i = 2;
    if (i < argc && std::string(argv[i]).find_first_not_of("0123456789") == std::string::npos) {
        port = (uint16_t)std::atoi(argv[i++]);
    }
    if (i < argc && std::string(argv[i]) == "--local_load") {
        if (i + 3 >= argc) {
            std::cerr << "--local_load requires <file> <offset_bytes> <size_bytes>" << std::endl;
            return 1;
        }
        load_file = argv[i + 1];
        load_offset = std::stoull(argv[i + 2]);
        load_size = std::stoull(argv[i + 3]);
        i += 4;
    }

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        size_t buf_size = load_size > 0 ? load_size : DEFAULT_BUFFER_SIZE;
        RDMAIndexServer server(buf_size);
        g_server_instance = &server;

        if (!load_file.empty()) {
            std::cerr << "Loading shard from " << load_file
                      << " offset=" << load_offset << " size=" << load_size << std::endl;
            server.load_shard_from_file(load_file, load_offset, load_size);
        }

        std::cerr << "Starting RDMA Index Server on " << bind_ip << ":" << port << std::endl;
        server.start_server(bind_ip, port);

    } catch (const std::exception& e) {
        std::cerr << "Index server error: " << e.what() << std::endl;
        return 1;
    }

    g_server_instance = nullptr;
    return 0;
}
