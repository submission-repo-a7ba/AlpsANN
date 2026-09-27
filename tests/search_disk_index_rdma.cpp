// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// AlpsANN search driver: queries an index whose unified graph records live in
// passive disaggregated memory (one or more RDMA memory nodes) while the PQ
// codes and all query processing stay on this compute node.

#include <algorithm>
#include <atomic>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <sys/stat.h>
#include <omp.h>
#include <string>
#include <vector>
#include <boost/program_options.hpp>

#include "aux_utils.h"
#include "pq_flash_index.h"
#include "percentile_stats.h"
#include "rdma_aligned_file_reader.h"
#include "rdma_connection.h"
#include "timer.h"
#include "utils.h"

namespace po = boost::program_options;

template<typename T>
int search_disk_index(
    diskann::Metric &metric, const std::string &index_path_prefix,
    const std::string &disk_file_path, const std::string &result_output_prefix,
    const std::string &query_file, const std::string &gt_file,
    const unsigned num_threads, const unsigned recall_at,
    const unsigned rdma_window, const std::vector<unsigned> &Lvec,
    const std::string &rdma_server, const uint16_t rdma_port,
    const std::string &rdma_servers_str, const uint64_t total_index_size,
    const unsigned rdma_connections, const unsigned num_workers,
    diskann::AlpsSearchParams alps) {
  // load queries and ground truth
  T     *query = nullptr;
  size_t query_num, query_dim, query_aligned_dim;
  diskann::load_aligned_bin<T>(query_file, query, query_num, query_dim,
                               query_aligned_dim);

  unsigned *gt_ids = nullptr;
  float    *gt_dists = nullptr;
  size_t    gt_num, gt_dim;
  bool      calc_recall_flag = false;
  if (gt_file != std::string("null") && file_exists(gt_file)) {
    diskann::load_truthset(gt_file, gt_ids, gt_dists, gt_num, gt_dim);
    if (gt_num != query_num) {
      diskann::cout << "Error. Mismatch in number of queries and ground truth"
                    << std::endl;
      return -1;
    }
    calc_recall_flag = true;
  }

  // One-sided RDMA reader to the memory node(s)
  std::shared_ptr<AlignedFileReader> reader = nullptr;
  size_t pool_size = rdma_connections > 0 ? rdma_connections : num_threads;
  size_t rdma_buf_size = diskann::RDMAConnection::MAX_IN_FLIGHT_BATCHES *
                         diskann::RDMAConnection::BATCH_SLOT_SIZE;
  if (!rdma_servers_str.empty()) {
    // the index is range-partitioned across several memory nodes
    std::vector<std::pair<std::string, uint16_t>> servers;
    std::stringstream                             ss(rdma_servers_str);
    std::string                                   token;
    while (std::getline(ss, token, ',')) {
      auto colon = token.find(':');
      if (colon == std::string::npos) {
        std::cerr << "Invalid server spec (expected ip:port): " << token
                  << std::endl;
        return -1;
      }
      servers.emplace_back(token.substr(0, colon),
                           (uint16_t) std::stoi(token.substr(colon + 1)));
    }
    uint64_t idx_size = total_index_size;
    if (idx_size == 0) {
      struct stat st;
      if (stat(disk_file_path.c_str(), &st) != 0) {
        std::cerr << "Cannot stat " << disk_file_path
                  << ", please specify --total_index_size" << std::endl;
        return -1;
      }
      idx_size = st.st_size;
    }
    std::cout << "Connecting to " << servers.size() << " memory nodes"
              << " (index " << (idx_size >> 20) << " MB)" << std::endl;
    reader.reset(new diskann::RDMAAlignedFileReader(servers, idx_size,
                                                    pool_size, rdma_buf_size));
  } else {
    std::cout << "Connecting to memory node " << rdma_server << ":"
              << rdma_port << std::endl;
    reader.reset(new diskann::RDMAAlignedFileReader(rdma_server, rdma_port,
                                                    pool_size, rdma_buf_size));
  }

  std::unique_ptr<diskann::PQFlashIndex<T>> index(
      new diskann::PQFlashIndex<T>(reader, metric));
  int res = index->load(num_threads, index_path_prefix.c_str(), disk_file_path);
  if (res != 0)
    return res;

  // Calibrated alpha: --verify_alpha, else <index_prefix>_alpha.txt
  if (alps.verify_alpha <= 0) {
    alps.verify_alpha = 1.0f;
    std::ifstream af(index_path_prefix + "_alpha.txt");
    if (af && (af >> alps.verify_alpha))
      std::cout << "Loaded verify_alpha=" << alps.verify_alpha << " from "
                << index_path_prefix << "_alpha.txt" << std::endl;
  }
  if (alps.async_traversal && num_workers == 0)
    alps.async_traversal = false;  // evaluate on the query threads
  if (alps.async_traversal)
    index->setup_eval_workers(num_workers);
  index->set_alps_params(alps);
  {
    const char *policies[] = {"expansion_only", "calibrated", "verify_all"};
    const char *admissions[] = {"waitall", "u_only", "rho_only", "impact",
                                "ungated"};
    std::cout << "Calibrated pipeline: state_driven_reads="
              << alps.state_driven_reads
              << " verify=" << policies[alps.verify_policy]
              << " alpha=" << alps.verify_alpha << " W_R=" << rdma_window
              << std::endl;
    std::cout << "Impact-aware traversal: " << (alps.async_traversal ? "on" : "off")
              << " workers=" << num_workers
              << " admission=" << admissions[alps.admission]
              << " H=" << alps.impact_window << " eps=" << alps.impact_eps
              << " U_max=" << alps.impact_umax << std::endl;
    std::cout << "Pipeline balancing: " << (alps.balance ? "adaptive" : "fixed")
              << " theta=[" << alps.theta_low << "," << alps.theta_high
              << "] interval=" << alps.balance_interval << " W_R in ["
              << alps.rdma_window_min << "," << alps.rdma_window_max << "]"
              << std::endl;
  }

  omp_set_num_threads(num_threads);

  diskann::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  diskann::cout.precision(2);
  std::string recall_string = "Recall@" + std::to_string(recall_at);
  diskann::cout << std::setw(6) << "L" << std::setw(8) << "W_R"
                << std::setw(12) << "QPS" << std::setw(14) << "Mean Lat(us)"
                << std::setw(14) << "P99 Lat(us)" << std::setw(14)
                << "P99.9 Lat(us)" << std::setw(10) << "Mean IOs";
  if (calc_recall_flag)
    diskann::cout << std::setw(12) << recall_string;
  diskann::cout << std::endl;
  diskann::cout << std::string(90, '=') << std::endl;

  std::vector<std::vector<uint32_t>> query_result_ids(Lvec.size());
  std::vector<std::vector<float>>    query_result_dists(Lvec.size());

  for (uint32_t test_id = 0; test_id < Lvec.size(); test_id++) {
    _u64 L = Lvec[test_id];
    if (L < recall_at) {
      diskann::cout << "Ignoring search with L:" << L
                    << " since it's smaller than K:" << recall_at << std::endl;
      continue;
    }
    query_result_ids[test_id].resize(recall_at * query_num);
    query_result_dists[test_id].resize(recall_at * query_num);
    std::vector<uint64_t> result_ids_64(recall_at * query_num);
    auto *stats = new diskann::QueryStats[query_num];

    auto s = std::chrono::high_resolution_clock::now();
#pragma omp parallel for schedule(dynamic, 1)
    for (_s64 i = 0; i < (int64_t) query_num; i++) {
      index->cached_beam_search(query + (i * query_aligned_dim), recall_at, L,
                                result_ids_64.data() + (i * recall_at),
                                query_result_dists[test_id].data() +
                                    (i * recall_at),
                                rdma_window, stats + i);
    }
    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = e - s;
    float qps = (1.0 * query_num) / (1.0 * diff.count());

    diskann::convert_types<uint64_t, uint32_t>(result_ids_64.data(),
                                               query_result_ids[test_id].data(),
                                               query_num, recall_at);

    auto mean_of = [&](auto fn) {
      return diskann::get_mean_stats<float>(stats, query_num, fn);
    };
    auto total_us = [](const diskann::QueryStats &s) { return s.total_us; };
    float mean_latency = mean_of(total_us);
    float p99 = diskann::get_percentile_stats<float>(stats, query_num, 0.99,
                                                     total_us);
    float p999 = diskann::get_percentile_stats<float>(stats, query_num, 0.999,
                                                      total_us);
    float mean_ios =
        mean_of([](const diskann::QueryStats &s) { return (float) s.n_ios; });

    diskann::cout << std::setw(6) << L << std::setw(8) << rdma_window
                  << std::setw(12) << qps << std::setw(14) << mean_latency
                  << std::setw(14) << p99 << std::setw(14) << p999
                  << std::setw(10) << mean_ios;
    if (calc_recall_flag) {
      float recall = diskann::calculate_recall(
          query_num, gt_ids, gt_dists, gt_dim,
          query_result_ids[test_id].data(), recall_at, recall_at);
      diskann::cout << std::setw(12) << recall;
    }
    diskann::cout << std::endl;

    // Per-design breakdown (means per query)
    diskann::cout
        << "        reads: verify-only "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.n_verify_only; })
        << ", expand-after-verify "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.n_expand_after_verify; })
        << ", exact-verified "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.n_exact_verified; })
        << " | expansions "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.n_hops; })
        << ", eval tasks "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.n_eval_tasks; })
        << ", gate closed "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.n_gate_closed; })
        << ", max U_q "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.max_unfinished_evals; })
        << " | defer ratio "
        << mean_of([](const diskann::QueryStats &s) { return s.mean_defer_ratio; })
        << ", W_R "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.final_rdma_window; })
        << ", T_q "
        << mean_of([](const diskann::QueryStats &s) { return (float) s.final_worker_allowance; })
        << std::endl;
    delete[] stats;
  }

  diskann::cout << "Done searching. Now saving results " << std::endl;
  for (uint32_t test_id = 0; test_id < Lvec.size(); test_id++) {
    _u64 L = Lvec[test_id];
    if (L < recall_at)
      continue;
    std::string cur_result_path =
        result_output_prefix + "_" + std::to_string(L) + "_idx_uint32.bin";
    diskann::save_bin<_u32>(cur_result_path, query_result_ids[test_id].data(),
                            query_num, recall_at);
    cur_result_path =
        result_output_prefix + "_" + std::to_string(L) + "_dists_float.bin";
    diskann::save_bin<float>(cur_result_path,
                             query_result_dists[test_id].data(), query_num,
                             recall_at);
  }

  diskann::aligned_free(query);
  return 0;
}

int main(int argc, char **argv) {
  std::string data_type, dist_fn, index_path_prefix, result_path_prefix,
      query_file, gt_file, disk_file_path, rdma_server, rdma_servers_str;
  std::string verify_policy, admission;
  unsigned    num_threads, K, W, num_workers, rdma_connections;
  uint16_t    rdma_port;
  uint64_t    total_index_size;
  std::vector<unsigned>     Lvec;
  diskann::AlpsSearchParams alps;

  po::options_description desc{"Arguments"};
  try {
    desc.add_options()("help,h", "Print information on arguments");
    desc.add_options()("data_type", po::value<std::string>(&data_type)->required(),
                       "data type <int8/uint8/float>");
    desc.add_options()("dist_fn", po::value<std::string>(&dist_fn)->required(),
                       "distance function <l2/mips/cosine>");
    desc.add_options()("index_path_prefix",
                       po::value<std::string>(&index_path_prefix)->required(),
                       "Path prefix of the index (PQ codes, calibrated alpha)");
    desc.add_options()("disk_file_path",
                       po::value<std::string>(&disk_file_path)->required(),
                       "Unified graph records (<prefix>_disk.index) held by "
                       "the memory node(s)");
    desc.add_options()("result_path",
                       po::value<std::string>(&result_path_prefix)->required(),
                       "Path prefix for saving results of the queries");
    desc.add_options()("query_file", po::value<std::string>(&query_file)->required(),
                       "Query file in binary format");
    desc.add_options()("gt_file",
                       po::value<std::string>(&gt_file)->default_value("null"),
                       "Ground truth file for the queryset");
    desc.add_options()("recall_at,K", po::value<unsigned>(&K)->required(),
                       "Number of neighbors to be returned");
    desc.add_options()("search_list,L",
                       po::value<std::vector<unsigned>>(&Lvec)->multitoken(),
                       "List of candidate-set sizes L");
    desc.add_options()("num_threads,T",
                       po::value<unsigned>(&num_threads)->default_value(omp_get_num_procs()),
                       "Number of query threads");
    // --- memory node(s) ---
    desc.add_options()("rdma_server", po::value<std::string>(&rdma_server)->default_value(""),
                       "Memory node IP address");
    desc.add_options()("rdma_port", po::value<uint16_t>(&rdma_port)->default_value(7471),
                       "Memory node port");
    desc.add_options()("rdma_servers",
                       po::value<std::string>(&rdma_servers_str)->default_value(""),
                       "Comma-separated ip:port list of memory nodes; the "
                       "index is range-partitioned across them");
    desc.add_options()("total_index_size",
                       po::value<uint64_t>(&total_index_size)->default_value(0),
                       "Index size in bytes for --rdma_servers (default: size "
                       "of --disk_file_path)");
    desc.add_options()("rdma_connections",
                       po::value<unsigned>(&rdma_connections)->default_value(0),
                       "RDMA connections per memory node (0 = num_threads)");
    // --- Calibrated one-sided search pipeline ---
    desc.add_options()("rdma_window,W", po::value<unsigned>(&W)->default_value(8),
                       "RDMA window W_R: max outstanding one-sided reads per "
                       "query (initial value with --balance 1)");
    desc.add_options()("state_driven_reads",
                       po::value<bool>(&alps.state_driven_reads)->default_value(true),
                       "Fetch only the required record fields (0 = always the full record)");
    desc.add_options()("verify_policy",
                       po::value<std::string>(&verify_policy)->default_value("calibrated"),
                       "Exact verification: expansion_only | calibrated | verify_all");
    desc.add_options()("verify_alpha",
                       po::value<float>(&alps.verify_alpha)->default_value(-1.0f),
                       "Calibrated factor alpha (default: <index_prefix>_alpha.txt, else 1.0)");
    // --- Impact-aware asynchronous traversal ---
    desc.add_options()("num_workers", po::value<unsigned>(&num_workers)->default_value(0),
                       "Shared evaluation worker pool size (0 = evaluate on query threads)");
    desc.add_options()("async_traversal",
                       po::value<bool>(&alps.async_traversal)->default_value(true),
                       "Impact-aware asynchronous traversal (needs --num_workers > 0)");
    desc.add_options()("admission", po::value<std::string>(&admission)->default_value("impact"),
                       "Expansion admission: waitall | u_only | rho_only | impact | ungated");
    desc.add_options()("impact_window",
                       po::value<unsigned>(&alps.impact_window)->default_value(64),
                       "H: recent evaluations used for the candidate-update ratio rho_q");
    desc.add_options()("impact_eps", po::value<float>(&alps.impact_eps)->default_value(1.0f),
                       "epsilon: admit expansion when rho_q * U_q <= epsilon");
    desc.add_options()("impact_umax",
                       po::value<unsigned>(&alps.impact_umax)->default_value(128),
                       "U_max: hard cap on unfinished evaluations");
    desc.add_options()("u_only_bound",
                       po::value<unsigned>(&alps.u_only_bound)->default_value(32),
                       "B for --admission u_only");
    desc.add_options()("rho_only_theta",
                       po::value<float>(&alps.rho_only_theta)->default_value(0.05f),
                       "theta for --admission rho_only");
    // --- Feedback-driven pipeline balancing ---
    desc.add_options()("balance", po::value<bool>(&alps.balance)->default_value(true),
                       "Feedback-driven pipeline balancing (0 = fixed W_R and T_q)");
    desc.add_options()("theta_low", po::value<float>(&alps.theta_low)->default_value(0.30f),
                       "Lower defer-ratio threshold");
    desc.add_options()("theta_high", po::value<float>(&alps.theta_high)->default_value(0.65f),
                       "Upper defer-ratio threshold");
    desc.add_options()("balance_interval",
                       po::value<unsigned>(&alps.balance_interval)->default_value(32),
                       "Expansion completions between control decisions");
    desc.add_options()("rdma_window_min",
                       po::value<unsigned>(&alps.rdma_window_min)->default_value(1),
                       "W_R,min");
    desc.add_options()("rdma_window_max",
                       po::value<unsigned>(&alps.rdma_window_max)->default_value(16),
                       "W_R,max");
    desc.add_options()("worker_allowance",
                       po::value<unsigned>(&alps.worker_allowance_init)->default_value(1),
                       "Initial worker allowance T_q (fixed value with --balance 0)");
    desc.add_options()("worker_allowance_min",
                       po::value<unsigned>(&alps.worker_allowance_min)->default_value(1),
                       "T_min");
    desc.add_options()("worker_allowance_max",
                       po::value<unsigned>(&alps.worker_allowance_max)->default_value(0),
                       "T_max (0 = worker pool size)");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
      std::cout << desc;
      return 0;
    }
    po::notify(vm);
  } catch (const std::exception &ex) {
    std::cerr << ex.what() << '\n';
    return -1;
  }

  const std::vector<std::string> policies = {"expansion_only", "calibrated",
                                             "verify_all"};
  const std::vector<std::string> admissions = {"waitall", "u_only", "rho_only",
                                               "impact", "ungated"};
  auto vp = std::find(policies.begin(), policies.end(), verify_policy);
  auto ad = std::find(admissions.begin(), admissions.end(), admission);
  if (vp == policies.end() || ad == admissions.end()) {
    std::cerr << "Invalid --verify_policy or --admission" << std::endl;
    return -1;
  }
  alps.verify_policy = (unsigned) (vp - policies.begin());
  alps.admission = (unsigned) (ad - admissions.begin());
  if (rdma_server.empty() && rdma_servers_str.empty()) {
    std::cerr << "Specify --rdma_server or --rdma_servers" << std::endl;
    return -1;
  }

  diskann::Metric metric;
  if (dist_fn == std::string("mips")) {
    metric = diskann::Metric::INNER_PRODUCT;
  } else if (dist_fn == std::string("l2")) {
    metric = diskann::Metric::L2;
  } else if (dist_fn == std::string("cosine")) {
    metric = diskann::Metric::COSINE;
  } else {
    std::cout << "Unsupported distance function. Currently only L2/ Inner "
                 "Product/Cosine are supported."
              << std::endl;
    return -1;
  }
  if ((data_type != std::string("float")) &&
      (metric == diskann::Metric::INNER_PRODUCT)) {
    std::cout << "Currently support only floating point data for Inner Product."
              << std::endl;
    return -1;
  }

  try {
    if (data_type == std::string("float"))
      return search_disk_index<float>(
          metric, index_path_prefix, disk_file_path, result_path_prefix,
          query_file, gt_file, num_threads, K, W, Lvec, rdma_server, rdma_port,
          rdma_servers_str, total_index_size, rdma_connections, num_workers,
          alps);
    else if (data_type == std::string("int8"))
      return search_disk_index<int8_t>(
          metric, index_path_prefix, disk_file_path, result_path_prefix,
          query_file, gt_file, num_threads, K, W, Lvec, rdma_server, rdma_port,
          rdma_servers_str, total_index_size, rdma_connections, num_workers,
          alps);
    else if (data_type == std::string("uint8"))
      return search_disk_index<uint8_t>(
          metric, index_path_prefix, disk_file_path, result_path_prefix,
          query_file, gt_file, num_threads, K, W, Lvec, rdma_server, rdma_port,
          rdma_servers_str, total_index_size, rdma_connections, num_workers,
          alps);
    else {
      std::cerr << "Unsupported data type. Use float or int8 or uint8"
                << std::endl;
      return -1;
    }
  } catch (const std::exception &e) {
    std::cout << std::string(e.what()) << std::endl;
    diskann::cerr << "Index search failed." << std::endl;
    return -1;
  }
}
