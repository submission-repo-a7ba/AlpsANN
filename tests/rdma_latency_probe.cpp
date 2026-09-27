// RDMA Latency Probe: measures single-read and batch-read latency
// to compare "one large read" vs "batch of small reads" at same total bytes.
//
// Usage: rdma_latency_probe --rdma_server <IP> [--rdma_port <PORT>]
//                           [--warmup 100] [--iters 1000] [--threads 1]
//
// Output: CSV to stdout, human-readable to stderr.

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>
#include <algorithm>
#include <numeric>
#include <atomic>

#include "rdma_env.h"
#include "rdma_connection.h"
#include <boost/program_options.hpp>

namespace po = boost::program_options;

static double percentile(const std::vector<double>& sorted, double p) {
  size_t idx = (size_t)(p * (sorted.size() - 1));
  return sorted[idx];
}

struct ProbeResult {
  std::string mode;      // "single" or "batch"
  size_t req_size;       // per-request size
  unsigned batch_count;  // number of requests (1 for single)
  size_t total_bytes;    // req_size * batch_count
  double mean_us, median_us, p99_us;
};

// Measure single read latency
static std::vector<double> measure_single(
    diskann::RDMAConnection& conn, size_t size, unsigned warmup, unsigned iters) {
  char* buf = (char*) conn.get_local_buffer();
  for (unsigned i = 0; i < warmup; i++)
    conn.rdma_read(buf, size, 0);

  std::vector<double> lats;
  lats.reserve(iters);
  for (unsigned i = 0; i < iters; i++) {
    auto t0 = std::chrono::high_resolution_clock::now();
    conn.rdma_read(buf, size, 0);
    auto t1 = std::chrono::high_resolution_clock::now();
    lats.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
  }
  std::sort(lats.begin(), lats.end());
  return lats;
}

// Measure batch read latency (multiple small reads in one doorbell)
static std::vector<double> measure_batch(
    diskann::RDMAConnection& conn, size_t req_size, unsigned batch_count,
    unsigned warmup, unsigned iters) {
  char* buf = (char*) conn.get_local_buffer();

  // Scatter requests to different remote offsets (simulate non-contiguous nodes)
  // Use 4KB spacing to avoid any caching effects
  std::vector<diskann::RDMAReadRequest> reqs(batch_count);
  size_t slot_size = std::max(req_size, (size_t)4096);
  for (unsigned b = 0; b < batch_count; b++) {
    reqs[b].local_buf = buf + b * slot_size;
    reqs[b].size = req_size;
    reqs[b].remote_offset = b * slot_size;  // scattered offsets
  }

  // Check buffer fits
  size_t total_local = batch_count * slot_size;
  if (total_local > conn.get_buffer_size()) return {};

  for (unsigned i = 0; i < warmup; i++)
    conn.rdma_read_batch(reqs);

  std::vector<double> lats;
  lats.reserve(iters);
  for (unsigned i = 0; i < iters; i++) {
    auto t0 = std::chrono::high_resolution_clock::now();
    conn.rdma_read_batch(reqs);
    auto t1 = std::chrono::high_resolution_clock::now();
    lats.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
  }
  std::sort(lats.begin(), lats.end());
  return lats;
}

int main(int argc, char* argv[]) {
  std::string rdma_server;
  uint16_t rdma_port = 7471;
  unsigned warmup, iters, n_threads;

  po::options_description desc("RDMA Latency Probe");
  desc.add_options()
    ("help,h", "Show help")
    ("rdma_server", po::value<std::string>(&rdma_server)->default_value(""),
     "RDMA server IP (or set RDMA_SERVER)")
    ("rdma_port", po::value<uint16_t>(&rdma_port),
     "RDMA server port (or set RDMA_PORT)")
    ("warmup", po::value<unsigned>(&warmup)->default_value(100), "Warmup iterations")
    ("iters", po::value<unsigned>(&iters)->default_value(1000), "Measurement iterations")
    ("threads", po::value<unsigned>(&n_threads)->default_value(1), "Concurrent threads");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) { std::cerr << desc << std::endl; return 0; }
    po::notify(vm);
    if (rdma_server.empty()) {
      rdma_server = diskann::get_env_string("RDMA_SERVER");
    }
    if (!vm.count("rdma_port")) {
      rdma_port = diskann::get_env_u16("RDMA_PORT", 7471);
    }
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << "\n" << desc << std::endl;
    return 1;
  }

  if (rdma_server.empty()) {
    std::cerr << "Error: specify --rdma_server or set RDMA_SERVER"
              << std::endl;
    return 1;
  }

  // CSV header
  std::cout << "mode,req_size,batch_count,total_bytes,threads,"
            << "mean_us,median_us,p99_us" << std::endl;

  // ---- Test configurations ----
  // Part 1: Size sweep (single read)
  std::vector<size_t> sizes = {64, 128, 256, 512, 700, 1024, 1400, 2048, 2100, 4096, 8192, 16384, 32768};

  // Part 2: Batch comparisons — same total bytes, different strategies
  // Compare: 1×2100B (single) vs 3×700B (batch)
  //          1×1400B (single) vs 2×700B (batch)
  //          1×4096B (single) vs 6×700B (batch)
  struct BatchTest {
    size_t req_size;
    unsigned batch_count;
  };
  std::vector<BatchTest> batch_tests = {
    {700, 1}, {700, 2}, {700, 3}, {700, 4}, {700, 6}, {700, 8},
    {260, 1}, {260, 2}, {260, 3}, {260, 4}, {260, 6}, {260, 8},
  };

  auto run_tests = [&](diskann::RDMAConnection& conn) {
    std::vector<ProbeResult> results;
    char* buf = (char*) conn.get_local_buffer();

    // Part 1: Single-read size sweep
    std::cerr << "\n=== Single-Read Size Sweep ===" << std::endl;
    for (size_t sz : sizes) {
      if (sz > conn.get_buffer_size()) continue;
      auto lats = measure_single(conn, sz, warmup, iters);
      if (lats.empty()) continue;
      double mean = std::accumulate(lats.begin(), lats.end(), 0.0) / lats.size();
      double med = percentile(lats, 0.5);
      double p99 = percentile(lats, 0.99);

      std::cout << "single," << sz << ",1," << sz << "," << n_threads << ","
                << std::fixed << std::setprecision(2) << mean << "," << med << "," << p99
                << std::endl;
      std::cerr << "  single " << std::setw(6) << sz << "B"
                << "  mean=" << std::setw(7) << mean << "us"
                << "  median=" << std::setw(7) << med << "us" << std::endl;
    }

    // Part 2: Batch reads
    std::cerr << "\n=== Batch Read Tests ===" << std::endl;
    for (auto& bt : batch_tests) {
      auto lats = measure_batch(conn, bt.req_size, bt.batch_count, warmup, iters);
      if (lats.empty()) {
        std::cerr << "  SKIP batch " << bt.batch_count << "x" << bt.req_size << "B\n";
        continue;
      }
      double mean = std::accumulate(lats.begin(), lats.end(), 0.0) / lats.size();
      double med = percentile(lats, 0.5);
      double p99 = percentile(lats, 0.99);
      size_t total = bt.req_size * bt.batch_count;

      std::cout << "batch," << bt.req_size << "," << bt.batch_count << "," << total
                << "," << n_threads << ","
                << std::fixed << std::setprecision(2) << mean << "," << med << "," << p99
                << std::endl;
      std::cerr << "  batch " << bt.batch_count << "x" << std::setw(4) << bt.req_size << "B"
                << " (total=" << std::setw(5) << total << "B)"
                << "  mean=" << std::setw(7) << mean << "us"
                << "  median=" << std::setw(7) << med << "us" << std::endl;
    }
  };

  if (n_threads == 1) {
    diskann::RDMAConnection conn(rdma_server.c_str(), rdma_port);
    if (conn.connect_to_server() != 0) {
      std::cerr << "Failed to connect\n"; return 1;
    }
    std::cerr << "Connected to " << rdma_server << ":" << rdma_port << std::endl;
    run_tests(conn);
  } else {
    // Multi-thread: each size tested concurrently
    // For simplicity, run all tests per-size with barrier sync
    for (size_t sz : sizes) {
      std::vector<std::vector<double>> all_lats(n_threads);
      std::atomic<unsigned> ready{0};
      std::atomic<bool> go{false};
      std::vector<std::thread> workers;

      for (unsigned t = 0; t < n_threads; t++) {
        workers.emplace_back([&, t]() {
          diskann::RDMAConnection conn(rdma_server.c_str(), rdma_port);
          if (conn.connect_to_server() != 0) return;
          char* buf = (char*) conn.get_local_buffer();
          if (sz > conn.get_buffer_size()) return;
          for (unsigned i = 0; i < warmup; i++)
            conn.rdma_read(buf, sz, 0);
          ready.fetch_add(1);
          while (!go.load()) ;
          all_lats[t].reserve(iters);
          for (unsigned i = 0; i < iters; i++) {
            auto t0 = std::chrono::high_resolution_clock::now();
            conn.rdma_read(buf, sz, 0);
            auto t1 = std::chrono::high_resolution_clock::now();
            all_lats[t].push_back(
                std::chrono::duration<double, std::micro>(t1 - t0).count());
          }
        });
      }
      while (ready.load() < n_threads) ;
      go.store(true);
      for (auto& w : workers) w.join();

      std::vector<double> merged;
      for (auto& v : all_lats) merged.insert(merged.end(), v.begin(), v.end());
      if (merged.empty()) continue;
      std::sort(merged.begin(), merged.end());
      double mean = std::accumulate(merged.begin(), merged.end(), 0.0) / merged.size();
      double med = percentile(merged, 0.5);
      double p99 = percentile(merged, 0.99);

      std::cout << "single," << sz << ",1," << sz << "," << n_threads << ","
                << std::fixed << std::setprecision(2) << mean << "," << med << "," << p99
                << std::endl;
      std::cerr << "  [T=" << n_threads << "] single " << std::setw(6) << sz << "B"
                << "  mean=" << std::setw(7) << mean << "us"
                << "  median=" << std::setw(7) << med << "us" << std::endl;
    }

    // Batch tests with multi-thread
    for (auto& bt : batch_tests) {
      std::vector<std::vector<double>> all_lats(n_threads);
      std::atomic<unsigned> ready{0};
      std::atomic<bool> go{false};
      std::vector<std::thread> workers;

      for (unsigned t = 0; t < n_threads; t++) {
        workers.emplace_back([&, t]() {
          diskann::RDMAConnection conn(rdma_server.c_str(), rdma_port);
          if (conn.connect_to_server() != 0) return;
          all_lats[t] = measure_batch(conn, bt.req_size, bt.batch_count, warmup, iters);
          // Sync not needed for batch — each thread runs independently
        });
      }
      for (auto& w : workers) w.join();

      std::vector<double> merged;
      for (auto& v : all_lats) merged.insert(merged.end(), v.begin(), v.end());
      if (merged.empty()) continue;
      std::sort(merged.begin(), merged.end());
      double mean = std::accumulate(merged.begin(), merged.end(), 0.0) / merged.size();
      double med = percentile(merged, 0.5);
      double p99 = percentile(merged, 0.99);
      size_t total = bt.req_size * bt.batch_count;

      std::cout << "batch," << bt.req_size << "," << bt.batch_count << "," << total
                << "," << n_threads << ","
                << std::fixed << std::setprecision(2) << mean << "," << med << "," << p99
                << std::endl;
      std::cerr << "  [T=" << n_threads << "] batch " << bt.batch_count << "x"
                << bt.req_size << "B"
                << "  mean=" << std::setw(7) << mean << "us"
                << "  median=" << std::setw(7) << med << "us" << std::endl;
    }
  }

  std::cerr << "\nDone.\n";
  return 0;
}
