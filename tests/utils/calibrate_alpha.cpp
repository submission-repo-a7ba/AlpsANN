// Offline calibration of the verification factor alpha (AlpsANN §3.2).
//
// For held-out queries with ground truth, records the approximation ratio
// d_hat(v,q) / d(v,q) between the compact (PQ) distance used by the search
// and the exact distance, for every ground-truth top-k neighbor v, and sets
//     alpha = max{1, Q_{1-delta}(d_hat / d)}.
// Taking all ground-truth top-k neighbors (instead of only those discovered by
// a traversal) makes the estimate slightly conservative.
//
// Usage:
//   calibrate_alpha --data_type uint8 --index_path_prefix <prefix> \
//     --base_file base.bin --query_file heldout_query.bin \
//     --gt_file heldout_gt.bin -K 10 --delta 0.01
// Writes alpha to <prefix>_alpha.txt, which search_disk_index_rdma loads when
// --verify_alpha is not given. Only the L2 metric is supported.

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <boost/program_options.hpp>

#include "pq_table.h"
#include "utils.h"

namespace po = boost::program_options;

template<typename T>
int calibrate(const std::string &prefix, const std::string &base_file,
              const std::string &query_file, const std::string &gt_file,
              unsigned K, double delta, const std::string &out_file) {
  // Compact vectors
  diskann::FixedChunkPQTable pq_table;
  _u8                       *pq_codes = nullptr;
  size_t                     npts, n_chunks;
  diskann::load_bin<_u8>(prefix + "_pq_compressed.bin", pq_codes, npts,
                         n_chunks);
  pq_table.load_pq_centroid_bin((prefix + "_pq_pivots.bin").c_str(), n_chunks);

  // Queries and ground truth
  T     *queries = nullptr;
  size_t nq, dim, aligned_dim;
  diskann::load_aligned_bin<T>(query_file, queries, nq, dim, aligned_dim);
  unsigned *gt_ids = nullptr;
  float    *gt_dists = nullptr;
  size_t    gt_n, gt_dim;
  diskann::load_truthset(gt_file, gt_ids, gt_dists, gt_n, gt_dim);
  if (gt_n != nq || gt_dim < K) {
    std::cerr << "Ground truth does not match the query file" << std::endl;
    return -1;
  }

  // Exact vectors are read on demand from the base file.
  int fd = open(base_file.c_str(), O_RDONLY);
  if (fd < 0) {
    std::cerr << "Cannot open " << base_file << std::endl;
    return -1;
  }
  int32_t hdr[2];
  if (pread(fd, hdr, sizeof(hdr), 0) != (ssize_t) sizeof(hdr) ||
      (size_t) hdr[1] != dim || (size_t) hdr[0] != npts) {
    std::cerr << "Base file does not match the index" << std::endl;
    return -1;
  }

  std::vector<double> ratios;
  ratios.reserve(nq * K);
  std::vector<float> qf(dim);
  std::vector<T>     vec(dim);
  for (size_t q = 0; q < nq; q++) {
    const T *query = queries + q * aligned_dim;
    for (size_t d = 0; d < dim; d++)
      qf[d] = (float) query[d];
    for (unsigned i = 0; i < K; i++) {
      unsigned id = gt_ids[q * gt_dim + i];
      off_t    off = 2 * sizeof(int32_t) + (off_t) id * dim * sizeof(T);
      if (pread(fd, vec.data(), dim * sizeof(T), off) !=
          (ssize_t) (dim * sizeof(T))) {
        std::cerr << "Short read from base file" << std::endl;
        return -1;
      }
      double exact = 0;
      for (size_t d = 0; d < dim; d++) {
        double diff = (double) qf[d] - (double) vec[d];
        exact += diff * diff;
      }
      if (exact <= 0)
        continue;  // exact duplicate of the query: ratio undefined
      double approx =
          pq_table.l2_distance(qf.data(), pq_codes + (size_t) id * n_chunks);
      ratios.push_back(approx / exact);
    }
  }
  close(fd);

  std::sort(ratios.begin(), ratios.end());
  auto quantile = [&](double p) {
    size_t idx = (size_t) std::ceil(p * ratios.size());
    return ratios[std::min(ratios.size() - 1, idx == 0 ? 0 : idx - 1)];
  };
  double alpha = std::max(1.0, quantile(1.0 - delta));
  std::cout << "Ratios d_hat/d over " << ratios.size() << " (query, top-" << K
            << ") pairs: p50=" << quantile(0.5) << " p90=" << quantile(0.9)
            << " p99=" << quantile(0.99) << " max=" << ratios.back()
            << std::endl;
  std::cout << "alpha (delta=" << delta << ") = " << alpha << std::endl;

  std::ofstream out(out_file);
  out << alpha << std::endl;
  std::cout << "Wrote " << out_file << std::endl;

  delete[] pq_codes;
  diskann::aligned_free(queries);
  delete[] gt_ids;
  delete[] gt_dists;
  return 0;
}

int main(int argc, char **argv) {
  std::string data_type, prefix, base_file, query_file, gt_file, out_file;
  unsigned    K;
  double      delta;
  po::options_description desc{"Arguments"};
  try {
    desc.add_options()("help,h", "Print information on arguments");
    desc.add_options()("data_type", po::value<std::string>(&data_type)->required(),
                       "uint8 / int8 / float");
    desc.add_options()("index_path_prefix", po::value<std::string>(&prefix)->required(),
                       "Index prefix (reads <prefix>_pq_pivots.bin and _pq_compressed.bin)");
    desc.add_options()("base_file", po::value<std::string>(&base_file)->required(),
                       "Base vectors (.bin) used to build the index");
    desc.add_options()("query_file", po::value<std::string>(&query_file)->required(),
                       "Held-out calibration queries (disjoint from test queries)");
    desc.add_options()("gt_file", po::value<std::string>(&gt_file)->required(),
                       "Ground truth of the calibration queries");
    desc.add_options()("recall_at,K", po::value<unsigned>(&K)->default_value(10),
                       "k of the top-k result");
    desc.add_options()("delta", po::value<double>(&delta)->default_value(0.01),
                       "Target verification miss rate");
    desc.add_options()("output", po::value<std::string>(&out_file)->default_value(""),
                       "Output file (default <prefix>_alpha.txt)");
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
      std::cout << desc;
      return 0;
    }
    po::notify(vm);
  } catch (const std::exception &ex) {
    std::cerr << ex.what() << std::endl;
    return -1;
  }
  if (out_file.empty())
    out_file = prefix + "_alpha.txt";

  if (data_type == "uint8")
    return calibrate<uint8_t>(prefix, base_file, query_file, gt_file, K, delta, out_file);
  if (data_type == "int8")
    return calibrate<int8_t>(prefix, base_file, query_file, gt_file, K, delta, out_file);
  if (data_type == "float")
    return calibrate<float>(prefix, base_file, query_file, gt_file, K, delta, out_file);
  std::cerr << "Unsupported data type" << std::endl;
  return -1;
}
