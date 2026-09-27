// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <atomic>
#include <cassert>
#include <limits>
#include <string>
#include <thread>
#include <vector>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <immintrin.h>
#include "tsl/robin_map.h"
#include "tsl/robin_set.h"

#include "aligned_file_reader.h"
#include "concurrent_queue.h"
#include "neighbor.h"
#include "parameters.h"
#include "percentile_stats.h"
#include "pq_table.h"
#include "utils.h"
#include "windows_customizations.h"
#include "pq_flash_index_utils.h"

#define MAX_GRAPH_DEGREE 512
#define MAX_N_CMPS 16384
#define SECTOR_LEN (_u64) 4096
#define MAX_N_SECTOR_READS 128  // max outstanding reads per query (W_R bound)
#define MAX_PQ_CHUNKS 256

namespace diskann {
  // Search-time knobs of the three AlpsANN designs. Defaults follow the paper.
  struct AlpsSearchParams {
    // --- Calibrated one-sided search pipeline (§3.2) ---
    // State-driven remote operations: fetch only the record fields a
    // candidate still needs (Expand-and-verify / Verify-only /
    // Expand-after-verification). false = always read the full record.
    bool     state_driven_reads = true;
    // Error-calibrated exact verification: issue a Verify-only read for an
    // unexpanded candidate when d_hat(v,q) <= alpha * tau.
    enum VerifyPolicy : unsigned { EXPANSION_ONLY = 0, CALIBRATED = 1, VERIFY_ALL = 2 };
    unsigned verify_policy = CALIBRATED;
    float    verify_alpha = 1.0f;  // produced offline by calibrate_alpha

    // --- Impact-aware asynchronous traversal (§3.3) ---
    bool     async_traversal = true;  // requires an evaluation worker pool
    enum Admission : unsigned { WAIT_ALL = 0, U_ONLY = 1, RHO_ONLY = 2, IMPACT = 3, UNGATED = 4 };
    unsigned admission = IMPACT;
    unsigned impact_window = 64;     // H: recent evaluations used for rho_q
    float    impact_eps = 1.0f;      // epsilon: budget on rho_q * U_q
    unsigned impact_umax = 128;      // U_max: hard cap on unfinished evaluations
    unsigned u_only_bound = 32;      // B for the U-only policy
    float    rho_only_theta = 0.05f; // theta for the rho-only policy

    // --- Feedback-driven pipeline balancing (§3.4, Algorithm 1) ---
    bool     balance = true;         // false = fixed W_R and T_q
    float    theta_low = 0.30f;
    float    theta_high = 0.65f;
    unsigned balance_interval = 32;  // expansion completions per control point
    unsigned rdma_window_min = 1;    // W_R,min
    unsigned rdma_window_max = 16;   // W_R,max
    unsigned worker_allowance_min = 1;  // T_min
    unsigned worker_allowance_max = 0;  // T_max (0 = worker pool size)
    unsigned worker_allowance_init = 1;
  };

  template<typename T>
  struct QueryScratch {
    T *coord_scratch = nullptr;  // zero-padded copy of one exact vector

    // Buffers for Verify-only reads [MAX_N_SECTOR_READS * SECTOR_LEN]. They
    // are consumed on completion, so at most one RDMA window of them is live.
    // (Expansion records use the per-thread record pool in the search.)
    char *verify_scratch = nullptr;
    _u64  verify_idx = 0;

    float *aligned_pqtable_dist_scratch = nullptr;  // [256 * NCHUNKS]
    float *aligned_dist_scratch = nullptr;          // [MAX_GRAPH_DEGREE]
    _u8   *aligned_pq_coord_scratch = nullptr;      // [NCHUNKS * MAX_GRAPH_DEGREE]
    T     *aligned_query_T = nullptr;
    float *aligned_query_float = nullptr;

    tsl::robin_set<_u64> *visited = nullptr;

    void reset() {
      verify_idx = 0;
      visited->clear();  // does not deallocate memory.
    }
  };

  template<typename T>
  struct ThreadData {
    QueryScratch<T> scratch;
    IOContext       ctx;
  };

  // One slot of the shared evaluation worker pool (impact-aware traversal).
  // A query thread claims a free slot, hands it the newly discovered
  // neighbors of one expanded node, and later merges the results. Workers
  // sleep on a futex when idle.
  struct alignas(64) EvalWorkerSlot {
    // 0 = free, 3 = claimed by a query, 1 = task ready, 2 = done, -1 = shutdown
    std::atomic<int> state{0};

    void wake_worker() {
      syscall(SYS_futex, reinterpret_cast<int *>(&state), FUTEX_WAKE_PRIVATE,
              1, nullptr, nullptr, 0);
    }
    void wait_for_done() {
      int s;
      while ((s = state.load(std::memory_order_acquire)) != 2) {
        syscall(SYS_futex, reinterpret_cast<int *>(&state), FUTEX_WAIT_PRIVATE,
                s, nullptr, nullptr, 0);
      }
    }
    static constexpr unsigned WORKER_SPIN_ITERS = 500;  // ~5us
    void sleep_until_work() {
      for (unsigned i = 0; i < WORKER_SPIN_ITERS; i++) {
        int s = state.load(std::memory_order_acquire);
        if (s == 1 || s == -1)
          return;
        _mm_pause();
      }
      int s;
      while ((s = state.load(std::memory_order_acquire)) != 1 && s != -1) {
        syscall(SYS_futex, reinterpret_cast<int *>(&state), FUTEX_WAIT_PRIVATE,
                s, nullptr, nullptr, 0);
      }
    }

    // Task input (set by the query thread before state = 1)
    const unsigned *nbr_ids = nullptr;
    unsigned        nbr_count = 0;
    const float    *pq_dists = nullptr;  // per-query PQ distance table
    const _u8      *pq_data = nullptr;
    _u64            n_chunks = 0;
    float           dist_threshold = std::numeric_limits<float>::max();
    // Per-worker scratch
    _u8   *pq_coord_scratch = nullptr;
    float *dist_scratch = nullptr;
    // Output: neighbors whose compact distance is below dist_threshold
    struct Result {
      unsigned id;
      float    dist;
    };
    Result   results[MAX_GRAPH_DEGREE];
    unsigned n_results = 0;
    unsigned n_evaluated = 0;
    char     _padding[64];  // prevent false sharing
  };

  template<typename T>
  class PQFlashIndex {
   public:
    DISKANN_DLLEXPORT PQFlashIndex(
        std::shared_ptr<AlignedFileReader> &fileReader,
        diskann::Metric                     metric = diskann::Metric::L2);
    DISKANN_DLLEXPORT ~PQFlashIndex();

    // Loads the PQ codes kept at the compute node and opens the unified graph
    // records (`disk_index_path`) through the reader (local file or RDMA).
    DISKANN_DLLEXPORT int load(uint32_t num_threads, const char *index_prefix,
                               const std::string &disk_index_path);

    // Shared evaluation worker pool for impact-aware asynchronous traversal.
    DISKANN_DLLEXPORT void setup_eval_workers(unsigned n_workers);
    DISKANN_DLLEXPORT void destroy_eval_workers();
    unsigned num_eval_workers() const {
      return (unsigned) eval_slots_.size();
    }

    void set_alps_params(const AlpsSearchParams &p) {
      alps_ = p;
    }
    const AlpsSearchParams &get_alps_params() const {
      return alps_;
    }

    // Top-k search. `rdma_window` is the (initial) RDMA window W_R.
    DISKANN_DLLEXPORT void cached_beam_search(
        const T *query, const _u64 k_search, const _u64 l_search,
        _u64 *res_ids, float *res_dists, const _u64 rdma_window,
        QueryStats *stats = nullptr);

    std::shared_ptr<AlignedFileReader> &reader;

   protected:
    DISKANN_DLLEXPORT void use_medoids_data_as_centroids();
    DISKANN_DLLEXPORT void setup_thread_data(_u64 nthreads);
    DISKANN_DLLEXPORT void destroy_thread_data();

   private:
    // Unified graph record of node i: [exact vector | nnbrs | nbr ids], at
    // node_offset(i). Records are packed nnodes_per_sector per 4 KB sector.
    _u64 max_node_len = 0, nnodes_per_sector = 0, max_degree = 0;

    inline _u64 node_offset(_u64 node_id) const {
      return (node_id / nnodes_per_sector + 1) * SECTOR_LEN +
             (node_id % nnodes_per_sector) * max_node_len;
    }
    inline _u64 node_sector_offset(_u64 node_id) const {
      return (node_id / nnodes_per_sector + 1) * SECTOR_LEN;
    }
    inline char *node_in_sector(char *sector_buf, _u64 node_id) const {
      return sector_buf + (node_id % nnodes_per_sector) * max_node_len;
    }

    diskann::Metric metric = diskann::Metric::L2;

    // used only for inner product search to re-scale the result value
    // (due to the pre-processing of base during index build)
    float max_base_norm = 0.0f;

    // data info
    _u64 num_points = 0;
    _u64 num_frozen_points = 0;
    _u64 frozen_location = 0;
    _u64 data_dim = 0;
    _u64 aligned_dim = 0;
    _u64 disk_bytes_per_point = 0;  // exact-vector prefix of a record

    std::string disk_index_file;

    // Compact vectors at the compute node (PQ)
    // data: _u8 * n_chunks per point
    _u8              *data = nullptr;
    _u64              n_chunks;
    FixedChunkPQTable pq_table;

    // distance comparator
    std::shared_ptr<Distance<T>>     dist_cmp;
    std::shared_ptr<Distance<float>> dist_cmp_float;

    // entry points: search starts from the medoid closest to the query
    uint32_t *medoids = nullptr;
    size_t    num_medoids;
    float    *centroid_data = nullptr;

    // thread-specific scratch
    ConcurrentQueue<ThreadData<T>> thread_data;
    _u64                           max_nthreads;
    bool                           load_flag = false;

    AlpsSearchParams alps_;

    // Shared evaluation worker pool
    std::vector<EvalWorkerSlot *> eval_slots_;
    std::vector<std::thread>      eval_threads_;
  };
}  // namespace diskann
