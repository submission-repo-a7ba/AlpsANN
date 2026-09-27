// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <climits>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <algorithm>
#ifdef _WINDOWS
#include <numeric>
#endif
#include <string>
#include <vector>

#include "distance.h"
#include "parameters.h"

namespace diskann {
  struct QueryStats {
    float total_us = 0;  // total time to process query in micros
    float io_us = 0;     // time spent posting / polling remote reads
    float cpu_us = 0;    // time spent in local computation

    unsigned n_ios = 0;   // total # of one-sided reads issued
    unsigned n_cmps = 0;  // # compact-distance evaluations
    unsigned n_hops = 0;  // # expanded nodes

    // Calibrated one-sided search pipeline
    unsigned n_verify_only = 0;          // Verify-only reads (exact vector)
    unsigned n_expand_after_verify = 0;  // Expand-after-verification reads (adjacency)
    unsigned n_exact_verified = 0;       // candidates with an exact distance
    // Impact-aware asynchronous traversal
    unsigned n_eval_tasks = 0;           // evaluation tasks generated
    unsigned n_gate_closed = 0;          // expansion decisions blocked by the gate
    unsigned max_unfinished_evals = 0;   // max U_q observed
    // Feedback-driven pipeline balancing
    unsigned n_balance_decisions = 0;
    float    mean_defer_ratio = 0;       // mean r_q over control points
    unsigned final_rdma_window = 0;      // W_R at query end
    unsigned final_worker_allowance = 0; // T_q at query end
  };

  template<typename T>
  inline T get_percentile_stats(
      QueryStats *stats, uint64_t len, float percentile,
      const std::function<T(const QueryStats &)> &member_fn) {
    std::vector<T> vals(len);
    for (uint64_t i = 0; i < len; i++) {
      vals[i] = member_fn(stats[i]);
    }

    std::sort(vals.begin(), vals.end(),
              [](const T &left, const T &right) { return left < right; });

    auto retval = vals[(uint64_t)(percentile * len)];
    vals.clear();
    return retval;
  }

  template<typename T>
  inline double get_mean_stats(
      QueryStats *stats, uint64_t len,
      const std::function<T(const QueryStats &)> &member_fn) {
    double avg = 0;
    for (uint64_t i = 0; i < len; i++) {
      avg += (double) member_fn(stats[i]);
    }
    return avg / len;
  }

  // The following two functions are used when getting statistics while range searching on only queries with
  // non-zero gt lengths
  template<typename T>
  inline T get_percentile_stats_gt(
      QueryStats *stats, uint64_t len, float percentile,
      const std::function<T(const QueryStats &)> &member_fn, std::vector<std::vector<uint32_t>> &gt) {
    std::vector<T> vals;
    for (uint64_t i = 0; i < len; i++) {
      if (gt[i].size()) vals.push_back(member_fn(stats[i]));
    }

    std::sort(vals.begin(), vals.end(),
              [](const T &left, const T &right) { return left < right; });

    auto retval = vals[(uint64_t)(percentile * vals.size())];
    vals.clear();
    return retval;
  }

  template<typename T>
  inline double get_mean_stats_gt(
      QueryStats *stats, uint64_t len,
      const std::function<T(const QueryStats &)> &member_fn, std::vector<std::vector<uint32_t>> &gt) {
    uint32_t cnt = 0;
    double avg = 0;
    for (uint64_t i = 0; i < len; i++) {
      if (gt[i].size()) {
        ++cnt;
        avg += (double) member_fn(stats[i]);
      }
    }
    return avg / cnt;
  }
}  // namespace diskann
