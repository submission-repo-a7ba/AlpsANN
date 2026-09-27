// Quick RDMA write+read verification test.
// Writes 10 synthetic vectors to remote memory, reads them back, verifies content.
//
// Usage: rdma_quick_test --rdma_server <IP> [--rdma_port <PORT>]

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <vector>

#include "rdma_connection.h"
#include "rdma_env.h"
#include <boost/program_options.hpp>

namespace po = boost::program_options;

static constexpr unsigned N_VECTORS = 10;
static constexpr unsigned DIM = 128;
static constexpr size_t VEC_BYTES = DIM * sizeof(float);  // 512B per vector

int main(int argc, char* argv[]) {
  std::string rdma_server;
  uint16_t rdma_port = 7471;

  po::options_description desc("RDMA Quick Test");
  desc.add_options()
    ("help,h", "Show help")
    ("rdma_server", po::value<std::string>(&rdma_server)->default_value(""),
     "RDMA server IP (or set RDMA_SERVER)")
    ("rdma_port", po::value<uint16_t>(&rdma_port),
     "RDMA server port (or set RDMA_PORT)");

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

  // Connect
  diskann::RDMAConnection conn(rdma_server.c_str(), rdma_port);
  if (conn.connect_to_server() != 0) {
    std::cerr << "FAIL: Could not connect to " << rdma_server << ":" << rdma_port << std::endl;
    return 1;
  }
  std::cerr << "Connected to " << rdma_server << ":" << rdma_port << std::endl;

  char* buf = (char*) conn.get_local_buffer();
  size_t buf_size = conn.get_buffer_size();
  std::cerr << "Local buffer: " << buf_size << " bytes" << std::endl;

  // === Step 1: Generate 10 synthetic vectors ===
  // Vector i has all elements = (float)(i + 1) * 0.1
  // So vec[0] = {0.1, 0.1, ...}, vec[9] = {1.0, 1.0, ...}
  std::vector<std::vector<float>> vectors(N_VECTORS, std::vector<float>(DIM));
  for (unsigned i = 0; i < N_VECTORS; i++) {
    float val = (float)(i + 1) * 0.1f;
    for (unsigned d = 0; d < DIM; d++)
      vectors[i][d] = val;
  }

  // === Step 2: Write vectors to remote memory ===
  std::cerr << "\n=== Writing " << N_VECTORS << " vectors (" << VEC_BYTES
            << "B each) ===" << std::endl;

  for (unsigned i = 0; i < N_VECTORS; i++) {
    uint64_t offset = (uint64_t)i * VEC_BYTES;
    // Copy to registered local buffer, then write
    memcpy(buf, vectors[i].data(), VEC_BYTES);

    auto t0 = std::chrono::high_resolution_clock::now();
    int ret = conn.rdma_write(buf, VEC_BYTES, offset);
    auto t1 = std::chrono::high_resolution_clock::now();
    double us = std::chrono::duration<double, std::micro>(t1 - t0).count();

    if (ret != 0) {
      std::cerr << "  FAIL: write vec[" << i << "] at offset " << offset << std::endl;
      return 1;
    }
    std::cerr << "  write vec[" << i << "] val=" << std::fixed << std::setprecision(1)
              << vectors[i][0] << " offset=" << offset << " (" << us << " us)" << std::endl;
  }

  // === Step 3: Read back and verify ===
  std::cerr << "\n=== Reading back and verifying ===" << std::endl;
  unsigned pass = 0, fail = 0;

  for (unsigned i = 0; i < N_VECTORS; i++) {
    uint64_t offset = (uint64_t)i * VEC_BYTES;
    memset(buf, 0, VEC_BYTES);  // clear buffer

    auto t0 = std::chrono::high_resolution_clock::now();
    int ret = conn.rdma_read(buf, VEC_BYTES, offset);
    auto t1 = std::chrono::high_resolution_clock::now();
    double us = std::chrono::duration<double, std::micro>(t1 - t0).count();

    if (ret != 0) {
      std::cerr << "  FAIL: read vec[" << i << "]" << std::endl;
      fail++;
      continue;
    }

    // Verify content
    float* read_data = (float*) buf;
    float expected = (float)(i + 1) * 0.1f;
    bool ok = true;
    for (unsigned d = 0; d < DIM; d++) {
      if (std::abs(read_data[d] - expected) > 1e-6f) {
        ok = false;
        std::cerr << "  FAIL: vec[" << i << "] dim[" << d << "] expected="
                  << expected << " got=" << read_data[d] << std::endl;
        break;
      }
    }
    if (ok) {
      pass++;
      std::cerr << "  PASS: vec[" << i << "] val=" << std::fixed << std::setprecision(1)
                << read_data[0] << " (" << std::setprecision(2) << us << " us)" << std::endl;
    } else {
      fail++;
    }
  }

  // === Step 4: Batch read test — read vec[2], vec[5], vec[9] in one batch ===
  // Use separate heap buffer for results (not the registered RDMA buffer)
  // because rdma_read_doorbell_batch uses the registered buffer internally
  // then memcpys to req.local_buf — overlapping regions cause corruption.
  std::cerr << "\n=== Batch read test (vec[2], vec[5], vec[9]) ===" << std::endl;
  unsigned batch_ids[] = {2, 5, 9};
  std::vector<float> batch_results(3 * DIM);
  std::vector<diskann::RDMAReadRequest> reqs;
  for (unsigned j = 0; j < 3; j++) {
    diskann::RDMAReadRequest req;
    req.local_buf = (char*)&batch_results[j * DIM];
    req.size = VEC_BYTES;
    req.remote_offset = (uint64_t)batch_ids[j] * VEC_BYTES;
    reqs.push_back(req);
  }

  auto t0 = std::chrono::high_resolution_clock::now();
  int ret = conn.rdma_read_batch(reqs);
  auto t1 = std::chrono::high_resolution_clock::now();
  double batch_us = std::chrono::duration<double, std::micro>(t1 - t0).count();

  if (ret != 0) {
    std::cerr << "  FAIL: batch read" << std::endl;
    fail++;
  } else {
    bool batch_ok = true;
    for (unsigned j = 0; j < 3; j++) {
      unsigned idx = batch_ids[j];
      float* data = &batch_results[j * DIM];
      float expected = (float)(idx + 1) * 0.1f;
      if (std::abs(data[0] - expected) > 1e-6f) {
        std::cerr << "  FAIL: batch vec[" << idx << "] expected=" << expected
                  << " got=" << data[0] << std::endl;
        batch_ok = false;
        fail++;
      }
    }
    if (batch_ok) {
      pass++;
      std::cerr << "  PASS: batch read 3 vectors in " << std::fixed
                << std::setprecision(2) << batch_us << " us" << std::endl;
    }
  }

  // === Summary ===
  std::cerr << "\n=== Summary: " << pass << " passed, " << fail << " failed ===" << std::endl;
  return fail > 0 ? 1 : 0;
}
