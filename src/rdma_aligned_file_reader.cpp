

#include "rdma_aligned_file_reader.h"
#include "ann_exception.h"
#include <iostream>
#include <cstring>
#include <iomanip>

#include "timer.h"
#include <fstream>
#include <atomic>
#include <deque>

namespace diskann {

  // Thread-local connection: claimed once from pool, never returned during search.
  // No mutex, no move, no ConnectionWrapper overhead on hot path.
  thread_local RDMAConnection* tls_conn = nullptr;
  // Keep the wrapper alive so its destructor doesn't return connection to pool.
  thread_local std::unique_ptr<ConnectionWrapper> tls_conn_owner;

  // Multi-shard thread-local connections (one per shard)
  static constexpr size_t MAX_SHARDS = 8;
  thread_local RDMAConnection* tls_shard_conns[MAX_SHARDS] = {};
  thread_local std::unique_ptr<ConnectionWrapper> tls_shard_owners[MAX_SHARDS];

  // A read batch that had to be posted as several parts (it spans memory
  // nodes, or exceeds one doorbell batch). The caller's flag is set once
  // every part has completed.
  static constexpr size_t DOORBELL_BATCH = 32;
  struct PendingGroup {
    uint64_t             *user_flag;
    std::vector<uint64_t> part_flags;  // written by the connections
  };
  thread_local std::deque<std::unique_ptr<PendingGroup>> tls_groups;

  static void resolve_groups() {
    for (auto it = tls_groups.begin(); it != tls_groups.end();) {
      bool done = true;
      for (auto f : (*it)->part_flags)
        if (f == 0) {
          done = false;
          break;
        }
      if (done) {
        *(*it)->user_flag = 1;
        it = tls_groups.erase(it);
      } else {
        ++it;
      }
    }
  }

  struct ThreadStats {
    uint64_t total_time_us = 0;
    uint64_t total_requests = 0;
    uint64_t total_bytes = 0;
    uint64_t batch_count = 0;
    uint64_t min_latency = UINT64_MAX;
    uint64_t max_latency = 0;
    ~ThreadStats() {
      if (total_requests > 0) {
        double avg_latency =
            static_cast<double>(total_time_us) / total_requests;
        double total_seconds = total_time_us / 1000000.0;
        double throughput_mbps =
            (static_cast<double>(total_bytes) / (1024.0 * 1024.0)) /
            total_seconds;
        double throughput_gbps = (static_cast<double>(total_bytes) * 8.0) /
                                 (1024.0 * 1024.0 * 1024.0) / total_seconds;

        std::cout << "FINAL Thread " << std::this_thread::get_id()
                  << " RDMA: " << std::fixed << std::setprecision(1)
                  << throughput_mbps << " MB/s (" << std::setprecision(2)
                  << throughput_gbps << " Gbps), "
                  << "Avg: " << std::setprecision(1) << avg_latency <<
                  "μs/req"
                  << std::endl;
      }
    }
  };
  thread_local ThreadStats tls_stats;

  // Simplified thread-local IOContext
  thread_local IOContext* RDMAAlignedFileReader::thread_ctx_ = nullptr;

  // Helper: get this thread's connection, claiming from pool on first call.
  // The ConnectionWrapper is kept alive in tls_conn_owner so it never returns
  // the connection to the pool. The raw pointer is cached in tls_conn for
  // zero-overhead access on the hot path.
  static RDMAConnection* get_tls_conn(RDMAConnectionPool* pool) {
    if (!tls_conn) {
      auto wrapper = pool->get_connection(std::chrono::milliseconds(5000));
      if (!wrapper.get()) {
        throw ANNException("Failed to get RDMA connection from pool", -1);
      }
      tls_conn = wrapper.get();
      // Move wrapper into persistent thread-local storage to prevent
      // destructor from returning connection to pool
      tls_conn_owner = std::make_unique<ConnectionWrapper>(std::move(wrapper));
    }
    return tls_conn;
  }

  // Helper: get shard connection for this thread
  static RDMAConnection* get_tls_shard_conn(size_t shard_idx,
                                             RDMAConnectionPool* pool) {
    if (!tls_shard_conns[shard_idx]) {
      auto wrapper = pool->get_connection(std::chrono::milliseconds(5000));
      if (!wrapper.get()) {
        throw ANNException("Failed to get RDMA connection for shard " +
                           std::to_string(shard_idx), -1);
      }
      tls_shard_conns[shard_idx] = wrapper.get();
      tls_shard_owners[shard_idx] =
          std::make_unique<ConnectionWrapper>(std::move(wrapper));
    }
    return tls_shard_conns[shard_idx];
  }

  RDMAAlignedFileReader::RDMAAlignedFileReader(const std::string& server_ip,
                                               uint16_t port, size_t pool_size,
                                               size_t buffer_size)
      : server_ip_(server_ip), port_(port), file_size_(0),
        metadata_parsed_(false), pool_size_(pool_size),
        buffer_size_(buffer_size) {
    std::cout << "Creating RDMA file reader with connection pool:" << std::endl;
    std::cout << "  Server: " << server_ip_ << ":" << port_ << std::endl;
    std::cout << "  Pool size: " << pool_size_ << std::endl;
    std::cout << "  Buffer size: " << (buffer_size_ / 1024) << " KB"
              << std::endl;
  }

  RDMAAlignedFileReader::RDMAAlignedFileReader(
      const std::vector<std::pair<std::string, uint16_t>>& servers,
      uint64_t total_index_size, size_t pool_size, size_t buffer_size)
      : server_ip_(servers[0].first), port_(servers[0].second), file_size_(0),
        metadata_parsed_(false), pool_size_(pool_size),
        buffer_size_(buffer_size) {
    num_shards_ = servers.size();
    // Sector-align shard size
    uint64_t raw_shard = total_index_size / num_shards_;
    shard_size_ = (raw_shard / 4096) * 4096;

    std::cout << "Creating SHARDED RDMA file reader:" << std::endl;
    std::cout << "  Shards: " << num_shards_ << std::endl;
    std::cout << "  Shard size: " << (shard_size_ / 1024 / 1024) << " MB" << std::endl;
    std::cout << "  Total index: " << (total_index_size / 1024 / 1024) << " MB" << std::endl;
    for (size_t i = 0; i < servers.size(); i++) {
      std::cout << "  Shard " << i << ": " << servers[i].first << ":"
                << servers[i].second << std::endl;
    }

    // Create connection pools for each shard
    for (size_t i = 0; i < num_shards_; i++) {
      auto pool = std::make_unique<RDMAConnectionPool>(
          servers[i].first.c_str(), servers[i].second, pool_size, buffer_size);
      if (!pool->initialize()) {
        throw ANNException("Failed to init pool for shard " + std::to_string(i), -1);
      }
      shard_pools_.push_back(std::move(pool));
    }
    std::cout << "All shard pools initialized." << std::endl;
  }

  RDMAAlignedFileReader::~RDMAAlignedFileReader() {
    close();
    std::cout << "RDMA file reader destroyed" << std::endl;
  }

  IOContext& RDMAAlignedFileReader::get_ctx() {
    if (!thread_ctx_) {
      thread_ctx_ = new IOContext();
    }
    return *thread_ctx_;
  }

  void RDMAAlignedFileReader::register_thread() {
    if (!thread_ctx_) {
      thread_ctx_ = new IOContext();
    }

    std::cout << "Thread " << std::this_thread::get_id()
              << " registered for RDMA file reading" << std::endl;
  }

  void RDMAAlignedFileReader::deregister_thread() {
    if (thread_ctx_) {
      delete thread_ctx_;
      thread_ctx_ = nullptr;
    }
    // Release wrapper (returns connection to pool) and clear raw pointer
    tls_conn_owner.reset();
    tls_conn = nullptr;

    std::cout << "Thread " << std::this_thread::get_id()
              << " deregistered from RDMA file reading" << std::endl;
  }

  void RDMAAlignedFileReader::deregister_all_threads() {
    std::cout << "All RDMA file reader threads deregistered" << std::endl;
  }

  void RDMAAlignedFileReader::open(const std::string& fname) {
    if (num_shards_ > 0) {
      // Multi-shard mode: pools already initialized in constructor.
      // Parse metadata from shard 0 (which holds sector 0).
      if (!rdma_pool_) {
        // Use shard 0's pool as the "default" pool for metadata parsing
        rdma_pool_ = std::make_unique<RDMAConnectionPool>(
            server_ip_.c_str(), port_, pool_size_, buffer_size_);
        if (!rdma_pool_->initialize()) {
          throw ANNException("Failed to initialize metadata pool", -1);
        }
      }
    } else {
      // Single-server mode (unchanged)
      if (!rdma_pool_) {
        std::cout << "Initializing RDMA connection pool..." << std::endl;
        rdma_pool_ = std::make_unique<RDMAConnectionPool>(
            server_ip_.c_str(), port_, pool_size_, buffer_size_);
        if (!rdma_pool_->initialize()) {
          throw ANNException("Failed to initialize RDMA connection pool", -1);
        }
        std::cout << "RDMA connection pool initialized successfully" << std::endl;
      }
    }

    parse_metadata_once();

    std::cout << "RDMA file '" << fname << "' opened, size: " << file_size_
              << " bytes" << std::endl;
  }

  void RDMAAlignedFileReader::close() {
    // Keep connection pool active for multiple file operations
    // Actual cleanup happens in destructor
  }

  void RDMAAlignedFileReader::print_thread_stats() {
    if (tls_stats.total_requests > 0) {
      double avg_latency = static_cast<double>(tls_stats.total_time_us) /
                           tls_stats.total_requests;

      double total_seconds = tls_stats.total_time_us / 1000000.0;
      double throughput_mbps =
          (static_cast<double>(tls_stats.total_bytes) / (1024.0 * 1024.0)) /
          total_seconds;
      double throughput_gbps =
          (static_cast<double>(tls_stats.total_bytes) * 8.0) /
          (1024.0 * 1024.0 * 1024.0) / total_seconds;

      std::cout << "Thread " << std::this_thread::get_id()
                << " RDMA Throughput: " << std::fixed << std::setprecision(1)
                << throughput_mbps << " MB/s (" << std::setprecision(2)
                << throughput_gbps << " Gbps), "
                << "Latency: " << std::setprecision(1) << avg_latency
                << "μs/req, "
                << "Batches: " << tls_stats.batch_count << std::endl;
    }
  }

  void RDMAAlignedFileReader::print_perf_diag() {
    // Diagnostic counters removed from hot path for performance.
  }

  void RDMAAlignedFileReader::send_read_no_alloc(IORequest& req,
                                                 IOContext& ctx) {
    if (!rdma_pool_) {
      throw ANNException("RDMA connection pool not initialized", -1);
    }

    req.finished = false;

    unsigned req_id = next_request_id_.fetch_add(1);

    auto async_task = std::make_shared<std::packaged_task<void()>>([this,
                                                                    &req]() {
      Timer read_timer;
      auto* conn = get_tls_conn(rdma_pool_.get());

      int result = conn->rdma_read_direct(req.buf, req.len, req.offset);

      auto latency = read_timer.elapsed();
      tls_stats.total_time_us += latency;
      tls_stats.total_requests++;
      tls_stats.total_bytes += req.len;

      req.finished = (result == 0);
    });

    {
      std::lock_guard<std::mutex> lock(pending_requests_mutex_);
      pending_requests_[req_id] = {&req, std::chrono::steady_clock::now(),
                                   async_task->get_future()};
    }

    std::thread([async_task]() { (*async_task)(); }).detach();
  }

  void RDMAAlignedFileReader::poll_all(IOContext& ctx) {
    if (num_shards_ > 0) {
      for (size_t s = 0; s < num_shards_; s++) {
        if (tls_shard_conns[s]) {
          tls_shard_conns[s]->poll_and_copy_completions_best();
        }
      }
    } else {
      auto* conn = get_tls_conn(rdma_pool_.get());
      conn->poll_and_copy_completions_best();
    }
    if (!tls_groups.empty())
      resolve_groups();
  }

  void RDMAAlignedFileReader::read_async(std::vector<AlignedRead>& read_reqs,
                                         IOContext& ctx, bool async,
                                         uint64_t* finished_flag) {
    const bool split = num_shards_ > 0 || read_reqs.size() > DOORBELL_BATCH;
    if (split) {
      // === Multi-part async (sharded memory nodes / large batches) ===
      // Reads are routed to the memory node that owns their offset and posted
      // as one-sided doorbell batches on that node's connection; completion
      // is tracked per part and reported through the caller's flag.
      Timer  batch_timer;
      size_t batch_bytes = 0;
      const size_t n_shards = num_shards_ > 0 ? num_shards_ : 1;
      std::vector<std::vector<RDMAReadRequest>> per_shard(n_shards);
      for (auto& req : read_reqs) {
        batch_bytes += req.len;
        size_t   s = 0;
        uint64_t off = req.offset;
        if (num_shards_ > 0) {
          s = std::min((size_t) (req.offset / shard_size_), num_shards_ - 1);
          off = req.offset - s * shard_size_;
        }
        per_shard[s].push_back({req.buf, req.len, off});
      }

      auto group = std::make_unique<PendingGroup>();
      group->user_flag = finished_flag;
      size_t n_parts = 0;
      for (auto& reqs : per_shard)
        n_parts += (reqs.size() + DOORBELL_BATCH - 1) / DOORBELL_BATCH;
      group->part_flags.assign(n_parts, 0);  // fixed size: stable addresses
      if (finished_flag)
        *finished_flag = 0;

      size_t part = 0;
      for (size_t s = 0; s < n_shards; s++) {
        auto& reqs = per_shard[s];
        if (reqs.empty())
          continue;
        auto* conn = num_shards_ > 0
                         ? get_tls_shard_conn(s, shard_pools_[s].get())
                         : get_tls_conn(rdma_pool_.get());
        for (size_t b = 0; b < reqs.size(); b += DOORBELL_BATCH) {
          std::vector<RDMAReadRequest> chunk(
              reqs.begin() + b,
              reqs.begin() + std::min(reqs.size(), b + DOORBELL_BATCH));
          if (conn->rdma_read_batch_async(chunk, &group->part_flags[part++]) != 0)
            throw ANNException("Async RDMA read failed on memory node " +
                                   std::to_string(s), -1);
        }
      }
      if (finished_flag)
        tls_groups.push_back(std::move(group));

      tls_stats.total_time_us += batch_timer.elapsed();
      tls_stats.total_requests += read_reqs.size();
      tls_stats.total_bytes += batch_bytes;
      tls_stats.batch_count++;
      return;
    }

    // === Single memory node, one doorbell batch ===
    auto* conn = get_tls_conn(rdma_pool_.get());

    Timer  batch_timer;
    size_t batch_bytes = 0;
    for (auto& req : read_reqs) {
      batch_bytes += req.len;
    }

    Timer single_timer;

    std::vector<RDMAReadRequest> batch_requests;
    batch_requests.reserve(read_reqs.size());
    for (auto& req : read_reqs) {
      batch_requests.push_back(
          {.local_buf = req.buf,
           .size = req.len,
           .remote_offset = req.offset});
    }

    int rdma_ret = conn->rdma_read_batch_async(batch_requests, finished_flag);
    if (rdma_ret != 0) {
      std::cerr << "[RDMA-DIAG] rdma_read_batch_async failed: ret=" << rdma_ret
                << " n_requests=" << batch_requests.size()
                << " thread=" << std::this_thread::get_id() << std::endl;
      throw ANNException("Batch RDMA read failed", -1);
    }
    auto single_latency = single_timer.elapsed();
    tls_stats.min_latency =
        std::min(tls_stats.min_latency, static_cast<uint64_t>(single_latency));
    tls_stats.max_latency =
        std::max(tls_stats.max_latency, static_cast<uint64_t>(single_latency));

    auto batch_latency = batch_timer.elapsed();
    tls_stats.total_time_us += batch_latency;
    tls_stats.total_requests += read_reqs.size();
    tls_stats.total_bytes += batch_bytes;
    tls_stats.batch_count++;
  }

  void RDMAAlignedFileReader::read(std::vector<AlignedRead>& read_reqs,
                                   IOContext& ctx, bool async) {
    if (num_shards_ > 0) {
      // === Multi-shard routing ===
      Timer  batch_timer;
      size_t batch_bytes = 0;

      // Group by shard
      std::vector<std::vector<RDMAReadRequest>> per_shard(num_shards_);
      for (auto& req : read_reqs) {
        batch_bytes += req.len;
        size_t s = std::min(req.offset / shard_size_, num_shards_ - 1);
        uint64_t local_off = req.offset - s * shard_size_;
        per_shard[s].push_back({req.buf, req.len, local_off});
      }

      // Issue to each shard
      for (size_t s = 0; s < num_shards_; s++) {
        if (per_shard[s].empty()) continue;
        auto* conn = get_tls_shard_conn(s, shard_pools_[s].get());
        if (conn->rdma_read_batch(per_shard[s]) != 0) {
          throw ANNException("Sharded RDMA read failed on shard " + std::to_string(s), -1);
        }
      }

      auto batch_latency = batch_timer.elapsed();
      tls_stats.total_time_us += batch_latency;
      tls_stats.total_requests += read_reqs.size();
      tls_stats.total_bytes += batch_bytes;
      tls_stats.batch_count++;
      return;
    }

    // === Single memory node, one doorbell batch ===
    auto* conn = get_tls_conn(rdma_pool_.get());

    Timer  batch_timer;
    size_t batch_bytes = 0;
    for (auto& req : read_reqs) {
      batch_bytes += req.len;
    }

    if (!async && read_reqs.size() == 1) {
      Timer single_timer;
      if (conn->rdma_read_direct(read_reqs[0].buf, read_reqs[0].len,
                                 read_reqs[0].offset) != 0) {
        throw ANNException("Direct RDMA read failed", -1);
      }
      auto single_latency = single_timer.elapsed();
      tls_stats.min_latency = std::min(tls_stats.min_latency,
                                       static_cast<uint64_t>(single_latency));
      tls_stats.max_latency = std::max(tls_stats.max_latency,
                                       static_cast<uint64_t>(single_latency));
    } else {
      Timer single_timer;
      std::vector<RDMAReadRequest> batch_requests;
      batch_requests.reserve(read_reqs.size());
      for (auto& req : read_reqs) {
        batch_requests.push_back(
            {.local_buf = req.buf,
             .size = req.len,
             .remote_offset = req.offset});
      }

      if (async) {
        uint64_t* finished_flag = nullptr;
        if (conn->rdma_read_batch_async(batch_requests, finished_flag) != 0) {
          throw ANNException("Batch RDMA read failed", -1);
        }
      } else {
        if (conn->rdma_read_batch(batch_requests) != 0) {
          throw ANNException("Batch RDMA read failed", -1);
        }
      }
      auto single_latency = single_timer.elapsed();
      tls_stats.min_latency = std::min(tls_stats.min_latency,
                                       static_cast<uint64_t>(single_latency));
      tls_stats.max_latency = std::max(tls_stats.max_latency,
                                       static_cast<uint64_t>(single_latency));
    }

    auto batch_latency = batch_timer.elapsed();
    tls_stats.total_time_us += batch_latency;
    tls_stats.total_requests += read_reqs.size();
    tls_stats.total_bytes += batch_bytes;
    tls_stats.batch_count++;
  }

  void RDMAAlignedFileReader::parse_metadata_once() {
    std::call_once(metadata_flag_, [this]() {
      if (!rdma_pool_) {
        throw ANNException("RDMA connection pool not initialized", -1);
      }

      const size_t            SECTOR_LEN = 4096;
      std::unique_ptr<char[]> metadata_buf =
          std::make_unique<char[]>(SECTOR_LEN);

      std::cout << "=== Parsing RDMA File Metadata ===" << std::endl;
      auto conn_wrapper =
          rdma_pool_->get_connection(std::chrono::milliseconds(5000));
      if (!conn_wrapper.get()) {
        throw ANNException("Failed to get connection for metadata parsing", -1);
      }
      if (!metadata_buf) {
        throw ANNException("Failed to allocate RDMA buffer for metadata", -1);
      }

      if (conn_wrapper->rdma_read(conn_wrapper->get_local_buffer(), SECTOR_LEN,
                                  0) != 0) {
        throw ANNException("Failed to read metadata via RDMA", -1);
      }

      memcpy(metadata_buf.get(), conn_wrapper->get_local_buffer(), SECTOR_LEN);

      uint64_t* metadata = reinterpret_cast<uint64_t*>(metadata_buf.get());

      std::cout << "Raw metadata values:" << std::endl;
      for (int i = 0; i < 16; i++) {
        std::cout << "  metadata[" << i << "] = " << metadata[i] << std::endl;
      }

      if (metadata[0] == 0) {
        std::cout << "WARNING: npts is 0. Server may not have index data "
                     "uploaded yet."
                  << std::endl;
        throw ANNException(
            "Invalid metadata: npts is 0. Server may not have index data.", -1);
      }

      uint64_t npts_64 = metadata[0];
      uint64_t ndims_64 = metadata[1];
      uint64_t medoid = metadata[2];
      uint64_t max_node_len = metadata[3];
      uint64_t nnodes_per_sector = metadata[4];
      uint64_t vamana_frozen_num = metadata[5];
      uint64_t vamana_frozen_loc = metadata[6];
      bool     append_reorder_data = (metadata[7] != 0);

      uint64_t metadata_index = 8;
      if (append_reorder_data) {
        metadata_index += 3;
      }

      file_size_ = metadata[metadata_index];
      metadata_parsed_ = true;

      std::cout << "Metadata parsed successfully:" << std::endl;
      std::cout << "  Points: " << npts_64 << std::endl;
      std::cout << "  Dimensions: " << ndims_64 << std::endl;
      std::cout << "  File size: " << file_size_ << " bytes" << std::endl;
      std::cout << "===============================" << std::endl;
    });
  }

  void RDMAAlignedFileReader::rdma_read_sectors(uint64_t offset, size_t size,
                                                void* dest_buf) {
    auto* conn = get_tls_conn(rdma_pool_.get());
    if (conn->rdma_read_direct(dest_buf, size, offset) != 0) {
      std::cerr << "Direct RDMA read failed" << std::endl;
      throw ANNException("Direct RDMA read failed", -1);
    }
  }

  void RDMAAlignedFileReader::rdma_read_sectors_async(uint64_t offset,
                                                      size_t   size,
                                                      void*    dest_buf) {
    auto* conn = get_tls_conn(rdma_pool_.get());
    if (conn->rdma_read_direct_async(dest_buf, size, offset) != 0) {
      std::cerr << "Direct RDMA read failed" << std::endl;
      throw ANNException("Direct RDMA read failed", -1);
    }
  }

}  // namespace diskann
