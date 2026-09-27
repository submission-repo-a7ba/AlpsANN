// Upload an existing _disk.index file to RDMA server without rebuilding.
// After upload, reads back sample sectors and verifies byte-for-byte match.
// Usage: upload_rdma --index_path_prefix <prefix> --rdma_server <ip> --rdma_port <port>

#include <fstream>
#include <iostream>
#include <chrono>
#include <memory>
#include <cstring>
#include <vector>
#include <boost/program_options.hpp>

#include "rdma_env.h"
#include "utils.h"
#include "rdma_writer.h"
#include "rdma_connection_pool.h"

namespace po = boost::program_options;

static const size_t SECTOR_LEN = 4096;

int main(int argc, char** argv) {
  std::string index_path_prefix, rdma_server;
  uint16_t rdma_port = 7471;

  po::options_description desc{"Arguments"};
  try {
    desc.add_options()("help,h", "Print information on arguments");
    desc.add_options()("index_path_prefix",
                       po::value<std::string>(&index_path_prefix)->required(),
                       "Path prefix of existing index (e.g. bigann_100m_M8_R32_L75_B3/)");
    desc.add_options()("rdma_server",
                       po::value<std::string>(&rdma_server)->default_value(""),
                       "RDMA server IP address (or set RDMA_SERVER)");
    desc.add_options()("rdma_port",
                       po::value<uint16_t>(&rdma_port),
                       "RDMA server port number (or set RDMA_PORT)");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
      std::cout << desc;
      return 0;
    }
    po::notify(vm);
    if (rdma_server.empty()) {
      rdma_server = diskann::get_env_string("RDMA_SERVER");
    }
    if (!vm.count("rdma_port")) {
      rdma_port = diskann::get_env_u16("RDMA_PORT", 7471);
    }
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return -1;
  }

  if (rdma_server.empty()) {
    std::cerr << "Error: specify --rdma_server or set RDMA_SERVER"
              << std::endl;
    return -1;
  }

  std::string disk_index_path = index_path_prefix + "_disk.index";
  std::string rdma_info_path = index_path_prefix + "_mem.index.rdma_info";

  // Check that _disk.index exists
  std::ifstream check(disk_index_path, std::ios::binary | std::ios::ate);
  if (!check.is_open()) {
    std::cerr << "ERROR: Cannot open " << disk_index_path << std::endl;
    std::cerr << "Build the index first with 'build' or 'build_rdma'." << std::endl;
    return -1;
  }
  size_t file_size = check.tellg();
  check.close();

  size_t n_sectors = (file_size + SECTOR_LEN - 1) / SECTOR_LEN;
  std::cout << "Uploading " << disk_index_path << " (" << file_size << " bytes, "
            << (file_size / (1024.0 * 1024.0 * 1024.0)) << " GB, "
            << n_sectors << " sectors) to RDMA server "
            << rdma_server << ":" << rdma_port << std::endl;

  // ========== Phase 1: Upload ==========
  auto start = std::chrono::high_resolution_clock::now();

  const size_t BUF_SIZE = SECTOR_LEN;
  std::unique_ptr<diskann::RDMAWriter> writer;
  try {
    writer = std::make_unique<diskann::RDMAWriter>(rdma_server, rdma_port, BUF_SIZE);
    std::cout << "RDMA write connection established." << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "RDMA connection failed: " << e.what() << std::endl;
    return -1;
  }

  std::ifstream reader(disk_index_path, std::ios::binary);
  if (!reader.is_open()) {
    std::cerr << "ERROR: Cannot open " << disk_index_path << std::endl;
    return -1;
  }

  char* buf = (char*) aligned_alloc(4096, BUF_SIZE);
  if (!buf) {
    std::cerr << "ERROR: Failed to allocate buffer" << std::endl;
    return -1;
  }

  size_t total_written = 0;
  size_t chunk_count = 0;
  while (total_written < file_size) {
    size_t to_read = std::min(BUF_SIZE, file_size - total_written);
    memset(buf, 0, BUF_SIZE);
    reader.read(buf, to_read);
    size_t actually_read = reader.gcount();
    if (actually_read == 0) break;

    // Always write full sectors (zero-padded)
    writer->write(buf, BUF_SIZE);
    total_written += actually_read;
    chunk_count++;

    if (chunk_count % 100000 == 0) {
      double pct = 100.0 * total_written / file_size;
      std::cout << "  Upload progress: " << total_written << " / " << file_size
                << " (" << pct << "%)" << std::endl;
    }
  }
  reader.close();
  free(buf);

  // Save RDMA info file
  writer->save_rdma_info(rdma_info_path);
  writer->close();
  writer.reset();

  auto upload_end = std::chrono::high_resolution_clock::now();
  double upload_elapsed = std::chrono::duration<double>(upload_end - start).count();

  std::cout << "Upload complete: " << total_written << " bytes in "
            << upload_elapsed << "s ("
            << (total_written / upload_elapsed / 1e9) << " GB/s)" << std::endl;

  // ========== Phase 2: Verify ==========
  std::cout << "\n--- Verifying upload correctness ---" << std::endl;

  // Pick sample sectors to verify: first, last, and ~8 evenly spaced
  std::vector<size_t> verify_sectors;
  verify_sectors.push_back(0);  // metadata sector
  if (n_sectors > 1) verify_sectors.push_back(1);  // first data sector
  // Evenly spaced sectors
  for (int i = 1; i <= 8; i++) {
    size_t s = (size_t)((double)n_sectors * i / 9.0);
    if (s > 0 && s < n_sectors) verify_sectors.push_back(s);
  }
  if (n_sectors > 2) verify_sectors.push_back(n_sectors - 1);  // last sector

  // Deduplicate
  std::sort(verify_sectors.begin(), verify_sectors.end());
  verify_sectors.erase(std::unique(verify_sectors.begin(), verify_sectors.end()),
                       verify_sectors.end());

  // Open a read connection via pool
  diskann::RDMAConnectionPool pool(rdma_server.c_str(), rdma_port,
                                   /*pool_size=*/1,
                                   /*buffer_size=*/256 * 1024);
  if (!pool.initialize()) {
    std::cerr << "ERROR: Failed to initialize RDMA read connection for verify."
              << std::endl;
    std::cerr << "Upload succeeded but verification skipped!" << std::endl;
    return -1;
  }

  // Reopen local file for comparison
  std::ifstream local_file(disk_index_path, std::ios::binary);
  if (!local_file.is_open()) {
    std::cerr << "ERROR: Cannot reopen " << disk_index_path << " for verify."
              << std::endl;
    return -1;
  }

  char* local_buf = (char*) aligned_alloc(4096, SECTOR_LEN);
  char* rdma_buf = (char*) aligned_alloc(4096, SECTOR_LEN);
  if (!local_buf || !rdma_buf) {
    std::cerr << "ERROR: Failed to allocate verify buffers" << std::endl;
    return -1;
  }

  unsigned pass = 0, fail = 0;
  for (size_t sec : verify_sectors) {
    uint64_t offset = sec * SECTOR_LEN;

    // Read local
    memset(local_buf, 0, SECTOR_LEN);
    local_file.seekg(offset);
    size_t local_to_read = std::min(SECTOR_LEN, file_size - (size_t)offset);
    local_file.read(local_buf, local_to_read);

    // Read RDMA
    memset(rdma_buf, 0, SECTOR_LEN);
    auto conn = pool.get_connection(std::chrono::milliseconds(5000));
    if (!conn.get()) {
      std::cerr << "  Sector " << sec << ": FAIL (no RDMA connection)" << std::endl;
      fail++;
      continue;
    }
    int ret = conn->rdma_read_direct(rdma_buf, SECTOR_LEN, offset);
    if (ret != 0) {
      std::cerr << "  Sector " << sec << ": FAIL (RDMA read error)" << std::endl;
      fail++;
      continue;
    }

    // Compare
    if (memcmp(local_buf, rdma_buf, SECTOR_LEN) == 0) {
      std::cout << "  Sector " << sec << " (offset " << offset << "): PASS"
                << std::endl;
      pass++;
    } else {
      // Find first mismatch byte
      size_t mismatch_pos = 0;
      for (size_t b = 0; b < SECTOR_LEN; b++) {
        if (local_buf[b] != rdma_buf[b]) { mismatch_pos = b; break; }
      }
      std::cerr << "  Sector " << sec << " (offset " << offset << "): FAIL"
                << " (first mismatch at byte " << mismatch_pos
                << ": local=0x" << std::hex << (unsigned)(unsigned char)local_buf[mismatch_pos]
                << " rdma=0x" << (unsigned)(unsigned char)rdma_buf[mismatch_pos]
                << std::dec << ")" << std::endl;
      fail++;
    }
  }

  local_file.close();
  free(local_buf);
  free(rdma_buf);

  // Also verify metadata sector makes sense
  std::cout << "\n--- Metadata check ---" << std::endl;
  char* meta_buf = (char*) aligned_alloc(4096, SECTOR_LEN);
  {
    auto conn = pool.get_connection(std::chrono::milliseconds(5000));
    conn->rdma_read_direct(meta_buf, SECTOR_LEN, 0);
  }
  const uint64_t* meta = reinterpret_cast<const uint64_t*>(meta_buf);
  uint64_t npts = meta[0];
  uint64_t ndims = meta[1];
  uint64_t medoid = meta[2];
  uint64_t max_node_len = meta[3];
  uint64_t nnodes_per_sector = meta[4];
  std::cout << "  npts=" << npts << "  ndims=" << ndims
            << "  medoid=" << medoid << "  max_node_len=" << max_node_len
            << "  nnodes_per_sector=" << nnodes_per_sector << std::endl;

  // Sanity: nnodes_per_sector should be SECTOR_LEN / max_node_len (for skip_view)
  // or computed from extended_node_len
  if (npts == 0 || ndims == 0 || max_node_len == 0 || nnodes_per_sector == 0) {
    std::cerr << "  WARNING: metadata looks invalid (zeros detected)" << std::endl;
    fail++;
  } else {
    std::cout << "  Metadata looks valid." << std::endl;
  }
  free(meta_buf);

  std::cout << "\n=== Verification Summary ===" << std::endl;
  std::cout << "  Sectors checked: " << verify_sectors.size() << std::endl;
  std::cout << "  Passed: " << pass << std::endl;
  std::cout << "  Failed: " << fail << std::endl;

  if (fail > 0) {
    std::cerr << "VERIFICATION FAILED — data on RDMA server may be corrupt!"
              << std::endl;
    return 1;
  }

  std::cout << "All checks passed. RDMA data verified." << std::endl;
  std::cout << "RDMA info: " << rdma_info_path << std::endl;

  return 0;
}
