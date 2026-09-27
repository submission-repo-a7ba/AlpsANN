// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "logger.h"
#include "pq_flash_index.h"
#include <malloc.h>
#include "percentile_stats.h"

#include <omp.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <queue>
#include <thread>
#include "distance.h"
#include "exceptions.h"
#include "parameters.h"
#include "pq_flash_index_utils.h"
#include "timer.h"
#include "utils.h"

#include "cosine_similarity.h"
#include "tsl/robin_set.h"

namespace diskann {
  template<typename T>
  PQFlashIndex<T>::PQFlashIndex(std::shared_ptr<AlignedFileReader> &fileReader,
                                diskann::Metric                     m)
      : reader(fileReader), metric(m) {
    if (m == diskann::Metric::COSINE || m == diskann::Metric::INNER_PRODUCT) {
      if (std::is_floating_point<T>::value) {
        diskann::cout << "Cosine metric chosen for (normalized) float data."
                         "Changing distance to L2 to boost accuracy."
                      << std::endl;
        m = diskann::Metric::L2;
      } else {
        diskann::cerr << "WARNING: Cannot normalize integral data types."
                      << " This may result in erroneous results or poor recall."
                      << " Consider using L2 distance with integral data types."
                      << std::endl;
      }
    }

    this->dist_cmp.reset(diskann::get_distance_function<T>(m));
    this->dist_cmp_float.reset(diskann::get_distance_function<float>(m));
  }

  template<typename T>
  PQFlashIndex<T>::~PQFlashIndex() {
    if (data != nullptr)
      delete[] data;
    if (centroid_data != nullptr)
      aligned_free(centroid_data);
    if (medoids != nullptr)
      delete[] medoids;

    this->destroy_eval_workers();
    if (load_flag) {
      this->destroy_thread_data();
      reader->close();
    }
  }

  template<typename T>
  void PQFlashIndex<T>::setup_thread_data(_u64 nthreads) {
    diskann::cout << "Setting up thread-specific contexts for nthreads: "
                  << nthreads << std::endl;
// omp parallel for to generate unique thread IDs
#pragma omp parallel for num_threads((int) nthreads)
    for (_s64 thread = 0; thread < (_s64) nthreads; thread++) {
#pragma omp critical
      {
        this->reader->register_thread();
        IOContext      &ctx = this->reader->get_ctx();
        QueryScratch<T> scratch;
        _u64            coord_alloc_size =
            ROUND_UP(sizeof(T) * this->aligned_dim, 256);
        diskann::alloc_aligned((void **) &scratch.coord_scratch,
                               coord_alloc_size, 256);
        diskann::alloc_aligned((void **) &scratch.verify_scratch,
                               (_u64) MAX_N_SECTOR_READS * (_u64) SECTOR_LEN,
                               SECTOR_LEN);
        diskann::alloc_aligned(
            (void **) &scratch.aligned_pq_coord_scratch,
            (_u64) MAX_GRAPH_DEGREE * (_u64) MAX_PQ_CHUNKS * sizeof(_u8), 256);
        diskann::alloc_aligned((void **) &scratch.aligned_pqtable_dist_scratch,
                               256 * (_u64) MAX_PQ_CHUNKS * sizeof(float), 256);
        diskann::alloc_aligned((void **) &scratch.aligned_dist_scratch,
                               (_u64) MAX_GRAPH_DEGREE * sizeof(float), 256);
        diskann::alloc_aligned((void **) &scratch.aligned_query_T,
                               this->aligned_dim * sizeof(T), 8 * sizeof(T));
        diskann::alloc_aligned((void **) &scratch.aligned_query_float,
                               this->aligned_dim * sizeof(float),
                               8 * sizeof(float));
        scratch.visited = new tsl::robin_set<_u64>(4096);

        memset(scratch.coord_scratch, 0, coord_alloc_size);
        memset(scratch.aligned_query_T, 0, this->aligned_dim * sizeof(T));
        memset(scratch.aligned_query_float, 0,
               this->aligned_dim * sizeof(float));

        ThreadData<T> data;
        data.ctx = ctx;
        data.scratch = scratch;
        this->thread_data.push(data);
      }
    }
    load_flag = true;
  }

  template<typename T>
  void PQFlashIndex<T>::destroy_thread_data() {
    diskann::cout << "Clearing scratch" << std::endl;
    assert(this->thread_data.size() == this->max_nthreads);
    while (this->thread_data.size() > 0) {
      ThreadData<T> data = this->thread_data.pop();
      while (data.scratch.verify_scratch == nullptr) {
        this->thread_data.wait_for_push_notify();
        data = this->thread_data.pop();
      }
      auto &scratch = data.scratch;
      diskann::aligned_free((void *) scratch.coord_scratch);
      diskann::aligned_free((void *) scratch.verify_scratch);
      diskann::aligned_free((void *) scratch.aligned_pq_coord_scratch);
      diskann::aligned_free((void *) scratch.aligned_pqtable_dist_scratch);
      diskann::aligned_free((void *) scratch.aligned_dist_scratch);
      diskann::aligned_free((void *) scratch.aligned_query_float);
      diskann::aligned_free((void *) scratch.aligned_query_T);
      delete scratch.visited;
    }
    this->reader->deregister_all_threads();
  }

  // Background evaluation worker: scores the newly discovered neighbors of one
  // expanded node with the PQ codes and returns those below the threshold.
  static void eval_worker_fn(EvalWorkerSlot *slot) {
    while (true) {
      slot->sleep_until_work();
      int s = slot->state.load(std::memory_order_acquire);
      if (s == -1)
        return;
      if (s != 1)
        continue;
      const unsigned n = slot->nbr_count;
      pq_flash_index_utils::aggregate_coords(slot->nbr_ids, n, slot->pq_data,
                                             slot->n_chunks,
                                             slot->pq_coord_scratch);
      pq_flash_index_utils::pq_dist_lookup(slot->pq_coord_scratch, n,
                                           slot->n_chunks, slot->pq_dists,
                                           slot->dist_scratch);
      const float thresh = slot->dist_threshold;
      unsigned    n_out = 0;
      for (unsigned i = 0; i < n; i++) {
        if (slot->dist_scratch[i] < thresh)
          slot->results[n_out++] = {slot->nbr_ids[i], slot->dist_scratch[i]};
      }
      slot->n_results = n_out;
      slot->n_evaluated = n;
      slot->state.store(2, std::memory_order_release);
      slot->wake_worker();  // wake the query thread if it waits for this task
    }
  }

  template<typename T>
  void PQFlashIndex<T>::setup_eval_workers(unsigned n_workers) {
    if (n_workers == 0 || !eval_slots_.empty())
      return;
    diskann::cout << "Setting up a shared pool of " << n_workers
                  << " evaluation workers" << std::endl;
    for (unsigned i = 0; i < n_workers; i++) {
      auto *slot = new (std::align_val_t(64)) EvalWorkerSlot();
      diskann::alloc_aligned(
          (void **) &slot->pq_coord_scratch,
          (_u64) MAX_GRAPH_DEGREE * (_u64) MAX_PQ_CHUNKS * sizeof(_u8), 256);
      diskann::alloc_aligned((void **) &slot->dist_scratch,
                             (_u64) MAX_GRAPH_DEGREE * sizeof(float), 256);
      eval_slots_.push_back(slot);
      eval_threads_.emplace_back(eval_worker_fn, slot);
    }
  }

  template<typename T>
  void PQFlashIndex<T>::destroy_eval_workers() {
    if (eval_slots_.empty())
      return;
    for (auto *slot : eval_slots_) {
      slot->state.store(-1, std::memory_order_release);
      slot->wake_worker();
    }
    for (auto &t : eval_threads_)
      if (t.joinable())
        t.join();
    for (auto *slot : eval_slots_) {
      diskann::aligned_free(slot->pq_coord_scratch);
      diskann::aligned_free(slot->dist_scratch);
      operator delete(slot, std::align_val_t(64));
    }
    eval_slots_.clear();
    eval_threads_.clear();
  }

  template<typename T>
  void PQFlashIndex<T>::use_medoids_data_as_centroids() {
    if (centroid_data != nullptr)
      aligned_free(centroid_data);
    alloc_aligned(((void **) &centroid_data),
                  num_medoids * aligned_dim * sizeof(float), 32);
    std::memset(centroid_data, 0, num_medoids * aligned_dim * sizeof(float));

    // borrow ctx
    ThreadData<T> data = this->thread_data.pop();
    while (data.scratch.verify_scratch == nullptr) {
      this->thread_data.wait_for_push_notify();
      data = this->thread_data.pop();
    }
    IOContext &ctx = data.ctx;
    diskann::cout << "Loading centroid data from medoids vector data of "
                  << num_medoids << " medoid(s)" << std::endl;
    for (uint64_t cur_m = 0; cur_m < num_medoids; cur_m++) {
      auto  medoid = medoids[cur_m];
      char *medoid_buf = nullptr;
      alloc_aligned((void **) &medoid_buf, SECTOR_LEN, SECTOR_LEN);
      std::vector<AlignedRead> medoid_read(1);
      medoid_read[0].len = SECTOR_LEN;
      medoid_read[0].buf = medoid_buf;
      medoid_read[0].offset = node_sector_offset(medoid);
      reader->read(medoid_read, ctx);

      const T *medoid_coords =
          OFFSET_TO_NODE_COORDS(node_in_sector(medoid_buf, medoid));
      for (uint32_t i = 0; i < data_dim; i++)
        centroid_data[cur_m * aligned_dim + i] = medoid_coords[i];
      aligned_free(medoid_buf);
    }

    // return ctx
    this->thread_data.push(data);
    this->thread_data.push_notify_all();
  }

  template<typename T>
  int PQFlashIndex<T>::load(uint32_t num_threads, const char *index_prefix,
                            const std::string &disk_index_path) {
    std::string pq_table_bin = std::string(index_prefix) + "_pq_pivots.bin";
    std::string pq_compressed_vectors =
        std::string(index_prefix) + "_pq_compressed.bin";
    std::string disk_index_file = disk_index_path;
    std::string medoids_file = std::string(disk_index_file) + "_medoids.bin";
    std::string centroids_file =
        std::string(disk_index_file) + "_centroids.bin";

    size_t pq_file_dim, pq_file_num_centroids;
    get_bin_metadata(pq_table_bin, pq_file_num_centroids, pq_file_dim,
                     METADATA_SIZE);

    this->disk_index_file = disk_index_file;

    if (pq_file_num_centroids != 256) {
      diskann::cout << "Error. Number of PQ centroids is not 256. Exitting."
                    << std::endl;
      return -1;
    }

    this->data_dim = pq_file_dim;
    this->disk_bytes_per_point = this->data_dim * sizeof(T);
    this->aligned_dim = ROUND_UP(pq_file_dim, 8);

    // Compact vectors kept at the compute node
    size_t npts_u64, nchunks_u64;
    diskann::load_bin<_u8>(pq_compressed_vectors, this->data, npts_u64,
                           nchunks_u64);
    this->num_points = npts_u64;
    this->n_chunks = nchunks_u64;
    pq_table.load_pq_centroid_bin(pq_table_bin.c_str(), nchunks_u64);

    diskann::cout
        << "Loaded PQ centroids and in-memory compressed vectors. #points: "
        << num_points << " #dim: " << data_dim
        << " #aligned_dim: " << aligned_dim << " #chunks: " << n_chunks
        << std::endl;

    if (n_chunks > MAX_PQ_CHUNKS) {
      std::stringstream stream;
      stream << "Error loading index. Ensure that max PQ bytes for in-memory "
                "PQ data does not exceed "
             << MAX_PQ_CHUNKS << std::endl;
      throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__,
                                  __LINE__);
    }
    if (file_exists(this->disk_index_file + "_pq_pivots.bin")) {
      diskann::cerr << "Error: records with PQ-compressed vectors (disk PQ) "
                       "are not supported; unified graph records must store "
                       "exact vectors."
                    << std::endl;
      return -1;
    }

    // read index metadata (header of the unified graph records)
    std::ifstream index_metadata(disk_index_file, std::ios::binary);
    _u32          nr, nc;  // metadata itself is stored as bin format
    READ_U32(index_metadata, nr);
    READ_U32(index_metadata, nc);

    _u64 disk_nnodes;
    _u64 disk_ndims;
    READ_U64(index_metadata, disk_nnodes);
    READ_U64(index_metadata, disk_ndims);

    if (disk_nnodes != num_points) {
      diskann::cout << "Mismatch in #points for compressed data file and disk "
                       "index file: "
                    << disk_nnodes << " vs " << num_points << std::endl;
      return -1;
    }

    size_t medoid_id_on_file;
    READ_U64(index_metadata, medoid_id_on_file);
    READ_U64(index_metadata, max_node_len);
    READ_U64(index_metadata, nnodes_per_sector);
    max_degree = ((max_node_len - disk_bytes_per_point) / sizeof(unsigned)) - 1;

    if (max_degree > MAX_GRAPH_DEGREE) {
      std::stringstream stream;
      stream << "Error loading index. Ensure that max graph degree (R) does "
                "not exceed "
             << MAX_GRAPH_DEGREE << std::endl;
      throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__,
                                  __LINE__);
    }

    // setting up concept of frozen points in disk index for streaming-DiskANN
    READ_U64(index_metadata, this->num_frozen_points);
    _u64 file_frozen_id;
    READ_U64(index_metadata, file_frozen_id);
    if (this->num_frozen_points == 1)
      this->frozen_location = file_frozen_id;
    if (this->num_frozen_points == 1) {
      diskann::cout << " Detected frozen point in index at location "
                    << this->frozen_location
                    << ". Will not output it at search time." << std::endl;
    }
    index_metadata.close();

    // open the reader (RDMA: connects to the memory node) on the records
    reader->open(disk_index_file);
    this->setup_thread_data(num_threads);
    this->max_nthreads = num_threads;

    if (file_exists(medoids_file)) {
      size_t tmp_dim;
      diskann::load_bin<uint32_t>(medoids_file, medoids, num_medoids, tmp_dim);

      if (tmp_dim != 1) {
        std::stringstream stream;
        stream << "Error loading medoids file. Expected bin format of m times "
                  "1 vector of uint32_t."
               << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__,
                                    __LINE__);
      }
      if (!file_exists(centroids_file)) {
        diskann::cout
            << "Centroid data file not found. Using corresponding vectors "
               "for the medoids "
            << std::endl;
        use_medoids_data_as_centroids();
      } else {
        size_t num_centroids, aligned_tmp_dim;
        diskann::load_aligned_bin<float>(centroids_file, centroid_data,
                                         num_centroids, tmp_dim,
                                         aligned_tmp_dim);
        if (aligned_tmp_dim != aligned_dim || num_centroids != num_medoids) {
          std::stringstream stream;
          stream << "Error loading centroids data file. Expected bin format of "
                    "m times data_dim vector of float, where m is number of "
                    "medoids "
                    "in medoids file.";
          diskann::cerr << stream.str() << std::endl;
          throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__,
                                      __LINE__);
        }
      }
    } else {
      num_medoids = 1;
      medoids = new uint32_t[1];
      medoids[0] = (_u32) (medoid_id_on_file);
      use_medoids_data_as_centroids();
    }

    std::string norm_file = std::string(disk_index_file) + "_max_base_norm.bin";

    if (file_exists(norm_file) && metric == diskann::Metric::INNER_PRODUCT) {
      _u64   dumr, dumc;
      float *norm_val;
      diskann::load_bin<float>(norm_file, norm_val, dumr, dumc);
      this->max_base_norm = norm_val[0];
      std::cout << "Setting re-scaling factor of base vectors to "
                << this->max_base_norm << std::endl;
      delete[] norm_val;
    }
    diskann::cout << "done.." << std::endl;
    return 0;
  }

  // AlpsANN search over passive disaggregated memory.
  //
  // The query thread keeps a candidate set ranked by compact (PQ) distance
  // d_hat and an exact-result set of nodes whose exact vector was fetched.
  //  - Calibrated one-sided search pipeline: every remote read is one of
  //    Expand-and-verify (full record), Verify-only (exact-vector prefix) or
  //    Expand-after-verification (adjacency suffix); unexpanded candidates are
  //    verified only if d_hat <= alpha * tau; at most W_R reads are in flight.
  //  - Impact-aware asynchronous traversal: the neighbor evaluation of each
  //    expanded node is a task for the shared worker pool; the next expansion
  //    is admitted only while rho_q * U_q <= epsilon and U_q <= U_max.
  //  - Feedback-driven pipeline balancing: every balance_interval expansions
  //    the defer ratio adjusts the worker allowance T_q and the window W_R.
  // Without a worker pool, neighbors are evaluated on the query thread.
  template<typename T>
  void PQFlashIndex<T>::cached_beam_search(const T *query1, const _u64 k_search,
                                           const _u64 l_search, _u64 *indices,
                                           float *distances,
                                           const _u64 rdma_window_init,
                                           QueryStats *stats) {
    Timer         query_timer;
    ThreadData<T> data = this->thread_data.pop();
    while (data.scratch.verify_scratch == nullptr) {
      this->thread_data.wait_for_push_notify();
      data = this->thread_data.pop();
    }
    IOContext &ctx = data.ctx;
    auto       query_scratch = &(data.scratch);
    query_scratch->reset();
    const AlpsSearchParams &alps = this->alps_;

    // ---- Query preprocessing ----
    float        query_norm = 0;
    const T     *query = query_scratch->aligned_query_T;
    const float *query_float = query_scratch->aligned_query_float;
    uint32_t     query_dim = metric == diskann::Metric::INNER_PRODUCT
                                 ? this->data_dim - 1
                                 : this->data_dim;
    for (uint32_t i = 0; i < query_dim; i++) {
      query_scratch->aligned_query_float[i] = query1[i];
      query_scratch->aligned_query_T[i] = query1[i];
      query_norm += query1[i] * query1[i];
    }
    // if inner product, we also normalize the query and set the last
    // coordinate to 0 (the extra coordinate used to convert MIPS to L2 search)
    if (metric == diskann::Metric::INNER_PRODUCT) {
      query_norm = std::sqrt(query_norm);
      query_scratch->aligned_query_T[this->data_dim - 1] = 0;
      query_scratch->aligned_query_float[this->data_dim - 1] = 0;
      for (uint32_t i = 0; i < this->data_dim - 1; i++) {
        query_scratch->aligned_query_T[i] /= query_norm;
        query_scratch->aligned_query_float[i] /= query_norm;
      }
    }

    // query <-> PQ chunk centers distances
    float *pq_dists = query_scratch->aligned_pqtable_dist_scratch;
    pq_table.populate_chunk_distances(query_float, pq_dists);
    float *dist_scratch = query_scratch->aligned_dist_scratch;
    _u8   *pq_coord_scratch = query_scratch->aligned_pq_coord_scratch;
    auto   compute_pq_dists = [&](const unsigned *ids, const _u64 n_ids,
                                float *dists_out) {
      pq_flash_index_utils::aggregate_coords(ids, n_ids, this->data,
                                             this->n_chunks, pq_coord_scratch);
      pq_flash_index_utils::pq_dist_lookup(pq_coord_scratch, n_ids,
                                           this->n_chunks, pq_dists, dists_out);
    };
    // Exact distance of a record's vector prefix (copied into zero-padded
    // scratch: the comparison runs over aligned_dim).
    auto exact_dist = [&](const char *record) -> float {
      memcpy(query_scratch->coord_scratch, record, data_dim * sizeof(T));
      return dist_cmp->compare(query, query_scratch->coord_scratch,
                               (unsigned) aligned_dim);
    };

    // ---- Candidate set (ranked by d_hat) and exact-result set ----
    std::vector<Neighbor> retset(l_search + 1);
    unsigned              cur_list_size = 0;
    unsigned              scan_hint = 0;  // no unexpanded candidate before it
    tsl::robin_set<_u64> &visited = *(query_scratch->visited);
    std::vector<Neighbor> full_retset;
    full_retset.reserve(l_search * 4);

    // ---- Calibrated one-sided search pipeline: state ----
    // verified[id] = exact distance of every node whose exact vector has been
    // fetched (already in full_retset); tau = k-th smallest of them.
    tsl::robin_map<unsigned, float> verified;
    verified.reserve(l_search * 4);
    tsl::robin_set<unsigned> adj_only;          // record buffer lacks the vector
    tsl::robin_set<unsigned> verify_in_flight;  // Verify-only read posted
    tsl::robin_set<unsigned> expand_requested;  // expansion read posted
    std::priority_queue<float> tau_heap;  // max-heap of the best k exact dists
    auto tau = [&]() -> float {
      return tau_heap.size() >= k_search ? tau_heap.top()
                                         : std::numeric_limits<float>::max();
    };
    auto record_exact = [&](unsigned id, float d) {
      verified[id] = d;
      full_retset.push_back(Neighbor(id, d, true));
      if (tau_heap.size() < k_search) {
        tau_heap.push(d);
      } else if (d < tau_heap.top()) {
        tau_heap.pop();
        tau_heap.push(d);
      }
    };
    const bool  state_driven = alps.state_driven_reads;
    const bool  verify_enabled =
        alps.verify_policy != AlpsSearchParams::EXPANSION_ONLY;
    const float verify_alpha =
        alps.verify_policy == AlpsSearchParams::VERIFY_ALL
            ? std::numeric_limits<float>::max()
            : alps.verify_alpha;
    unsigned n_verify_only_reads = 0, n_adj_only_reads = 0;
    // Discovered candidates that left (or never entered) the candidate set
    // without being expanded, as a min-heap on d_hat. Expanded nodes get their
    // exact vector through Expand-and-verify, so these are the candidates
    // that may need Verify-only reads.
    std::priority_queue<std::pair<float, unsigned>,
                        std::vector<std::pair<float, unsigned>>,
                        std::greater<std::pair<float, unsigned>>>
        verify_pool;
    auto consider_verify = [&](unsigned id, float d_hat) {
      if (verify_enabled && tau_heap.size() >= k_search &&
          d_hat <= verify_alpha * tau())
        verify_pool.emplace(d_hat, id);
    };
    // Exact distance of an expanded node. Expand-after-verification buffers
    // carry no vector: the distance comes from the Verify-only read (recorded
    // on its arrival if still in flight).
    auto verify_expanded = [&](unsigned id, const char *record) {
      if (verified.count(id) || adj_only.count(id))
        return;
      record_exact(id, exact_dist(record));
    };

    // Insert an evaluated neighbor into the candidate set; true if inserted.
    // Candidates that are rejected or evicted without expansion are offered
    // to the verification gate.
    auto insert_candidate = [&](unsigned id, float d) -> bool {
      if (cur_list_size == l_search &&
          d >= retset[cur_list_size - 1].distance) {
        consider_verify(id, d);
        return false;
      }
      auto r =
          InsertIntoPool(retset.data(), cur_list_size, Neighbor(id, d, true));
      if (r > cur_list_size)
        return false;  // duplicate
      if (cur_list_size < l_search)
        ++cur_list_size;
      else if (r < cur_list_size && !retset[cur_list_size].visited)
        consider_verify(retset[cur_list_size].id,
                        retset[cur_list_size].distance);
      if (r < scan_hint)
        scan_hint = r;
      return true;
    };

    // ---- One-sided RDMA pipeline ----
    // Record buffers of expansion reads stay live until the node is expanded
    // and its evaluation has finished. They come from a per-thread pool that
    // grows to the peak number of live records and is reused across queries.
    static thread_local std::vector<char *> rec_pool_all;
    static thread_local std::vector<char *> rec_pool_free;
    rec_pool_free.assign(rec_pool_all.begin(), rec_pool_all.end());
    tsl::robin_map<unsigned, char *> rec_owned;   // node id -> pooled buffer
    tsl::robin_map<unsigned, char *> id_buf_map;  // arrived expansion records
    auto alloc_record = [&](unsigned id) -> char * {
      if (rec_pool_free.empty()) {
        constexpr unsigned GROW = 64;
        char              *chunk = nullptr;
        diskann::alloc_aligned((void **) &chunk, (_u64) GROW * SECTOR_LEN,
                               SECTOR_LEN);
        for (unsigned i = 0; i < GROW; i++) {
          rec_pool_all.push_back(chunk + (_u64) i * SECTOR_LEN);
          rec_pool_free.push_back(chunk + (_u64) i * SECTOR_LEN);
        }
      }
      char *buf = rec_pool_free.back();
      rec_pool_free.pop_back();
      rec_owned[id] = buf;
      return buf;
    };
    auto release_record = [&](unsigned id) {
      auto it = rec_owned.find(id);
      if (it == rec_owned.end())
        return;
      rec_pool_free.push_back(it->second);
      rec_owned.erase(it);
      id_buf_map.erase(id);
    };

    enum : uint8_t {
      EXPAND_AND_VERIFY = 0,
      VERIFY_ONLY = 1,
      EXPAND_AFTER_VERIFY = 2
    };
    struct ReadOp {
      unsigned node_id;
      char    *buf;
      uint8_t  mode;
    };
    std::deque<ReadOp> in_flight;  // posted reads, in posting order
    std::deque<size_t> batches;    // (flag slot << 16) | #reads, FIFO
    static constexpr size_t MAX_PENDING_BATCHES = 64;
    std::array<uint64_t, MAX_PENDING_BATCHES> batch_flags{};
    size_t   next_flag_slot = 0;
    unsigned num_ios = 0;
    Timer    io_timer;

    // Post one batch of reads; the reader chains them behind one doorbell.
    auto post_batch = [&](std::vector<AlignedRead> &reqs) {
      if (reqs.empty())
        return;
      size_t flag_slot = next_flag_slot % MAX_PENDING_BATCHES;
      next_flag_slot++;
      batch_flags[flag_slot] = 0;
      batches.push_back((flag_slot << 16) | reqs.size());
      io_timer.reset();
      reader->read_async(reqs, ctx, true, &batch_flags[flag_slot]);
      if (stats)
        stats->io_us += (float) io_timer.elapsed();
    };

    // Completions: exact vectors update the exact-result set; records with an
    // adjacency list become ready for expansion.
    auto poll_reads = [&]() {
      io_timer.reset();
      reader->poll_all(ctx);
      if (stats)
        stats->io_us += (float) io_timer.elapsed();
      while (!batches.empty()) {
        size_t packed = batches.front();
        if (batch_flags[packed >> 16] == 0)
          break;
        batches.pop_front();
        for (size_t i = 0; i < (packed & 0xFFFF); i++) {
          ReadOp &op = in_flight.front();
          if (op.mode == VERIFY_ONLY) {
            verify_in_flight.erase(op.node_id);
            if (!verified.count(op.node_id))
              record_exact(op.node_id, exact_dist(op.buf));
          } else {
            id_buf_map[op.node_id] = op.buf;
          }
          in_flight.pop_front();
        }
      }
    };

    // RDMA window W_R (bound on reads in flight for this query)
    int64_t window = (int64_t) std::min(std::max(rdma_window_init, (_u64) 1),
                                        (_u64) MAX_N_SECTOR_READS / 2);

    // Expansion reads for the best unexpanded candidates without a request.
    auto send_expansion_reads = [&](int64_t n) {
      std::vector<AlignedRead> reqs;
      for (unsigned m = 0; m < cur_list_size && (int64_t) reqs.size() < n;
           ++m) {
        Neighbor &c = retset[m];
        if (!c.flag || c.visited)
          continue;
        c.flag = false;
        if (id_buf_map.count(c.id))
          continue;
        char *buf = alloc_record(c.id);
        expand_requested.insert(c.id);
        uint8_t mode = EXPAND_AND_VERIFY;
        if (state_driven &&
            (verified.count(c.id) || verify_in_flight.count(c.id))) {
          // Expand-after-verification: only the adjacency-list suffix. It
          // lands at its record offset so the buffer keeps the record layout.
          reqs.emplace_back(node_offset(c.id) + disk_bytes_per_point,
                            max_node_len - disk_bytes_per_point,
                            buf + disk_bytes_per_point);
          mode = EXPAND_AFTER_VERIFY;
          adj_only.insert(c.id);
          n_adj_only_reads++;
        } else {
          // Expand-and-verify: exact vector + adjacency list. This is also
          // the single read that serves an expansion and a verification of
          // the same candidate.
          reqs.emplace_back(node_offset(c.id), max_node_len, buf);
        }
        in_flight.push_back({c.id, buf, mode});
        num_ios++;
      }
      post_batch(reqs);
    };
    auto fill_window = [&]() {
      int64_t room = window - (int64_t) in_flight.size();
      if (room > 0)
        send_expansion_reads(room);
    };

    // Error-calibrated exact verification: Verify-only reads for unexpanded
    // candidates with d_hat <= alpha * tau, using the window slots left after
    // expansion reads. Each request is decided against the current state when
    // it is posted, so stale requests are never issued. Returns true if a
    // qualifying candidate still waits for a slot.
    auto issue_verify_reads = [&]() -> bool {
      if (!verify_enabled || tau_heap.size() < k_search)
        return false;
      const float bound = verify_alpha * tau();
      int64_t     slots = window - (int64_t) in_flight.size();
      std::vector<AlignedRead>                reqs;
      std::vector<std::pair<float, unsigned>> retry;
      bool                                    waiting = false;
      auto post_verify = [&](unsigned id) {
        char *buf = query_scratch->verify_scratch +
                    query_scratch->verify_idx * SECTOR_LEN;
        query_scratch->verify_idx =
            (query_scratch->verify_idx + 1) % MAX_N_SECTOR_READS;
        reqs.emplace_back(node_offset(id), disk_bytes_per_point, buf);
        in_flight.push_back({id, buf, VERIFY_ONLY});
        verify_in_flight.insert(id);
        num_ios++;
        slots--;
        n_verify_only_reads++;
      };
      // (a) unexpanded candidates beyond the expansion frontier (a later
      //     expansion then reads only the adjacency list)
      for (unsigned i = 0; i < cur_list_size && slots > 0; ++i) {
        const Neighbor &c = retset[i];
        if (c.distance > bound)
          break;
        if (c.visited || !c.flag || verified.count(c.id) ||
            verify_in_flight.count(c.id))
          continue;
        post_verify(c.id);
      }
      // (b) discovered candidates outside the candidate set
      while (!verify_pool.empty()) {
        auto [d_hat, id] = verify_pool.top();
        if (d_hat > bound)
          break;  // tau only shrinks: the rest can wait or never qualify
        if (verified.count(id) || verify_in_flight.count(id)) {
          verify_pool.pop();
          continue;
        }
        if (expand_requested.count(id)) {
          // Evicted after its full-record read was posted: use that record
          // once it arrives instead of reading the vector again.
          verify_pool.pop();
          auto it = id_buf_map.find(id);
          if (it == id_buf_map.end())
            retry.emplace_back(d_hat, id);
          else if (!adj_only.count(id))
            record_exact(id, exact_dist(it->second));
          continue;
        }
        if (slots <= 0) {
          waiting = true;
          break;
        }
        verify_pool.pop();
        post_verify(id);
      }
      for (auto &e : retry)
        verify_pool.push(e);
      post_batch(reqs);
      return waiting || !retry.empty();
    };

    // Newly discovered neighbors of an expanded record, compacted in place.
    auto new_neighbors = [&](char *record, unsigned *&ids) -> unsigned {
      unsigned *nhood = OFFSET_TO_NODE_NHOOD(record);
      unsigned  nnbrs = std::min(*nhood, (unsigned) MAX_GRAPH_DEGREE);
      ids = nhood + 1;
      unsigned n_new = 0;
      for (unsigned m = 0; m < nnbrs; m++)
        if (visited.insert(ids[m]).second)
          ids[n_new++] = ids[m];
      return n_new;
    };
    // Best-ranked unexpanded candidate whose record has arrived.
    auto pick_ready = [&](unsigned &id, char *&record) -> bool {
      for (unsigned m = scan_hint; m < cur_list_size; ++m) {
        if (retset[m].visited)
          continue;
        auto it = id_buf_map.find(retset[m].id);
        if (it == id_buf_map.end())
          continue;
        retset[m].visited = true;
        retset[m].flag = false;
        while (scan_hint < cur_list_size && retset[scan_hint].visited)
          scan_hint++;
        id = retset[m].id;
        record = it->second;
        return true;
      }
      return false;
    };
    auto has_unexpanded = [&]() -> bool {
      for (unsigned i = scan_hint; i < cur_list_size; ++i) {
        if (!retset[i].visited) {
          scan_hint = i;
          return true;
        }
      }
      return false;
    };

    // ---- Impact-aware asynchronous traversal: state ----
    const bool async_on = alps.async_traversal && !eval_slots_.empty();
    struct EvalTask {
      unsigned  node;     // expanded node (owns the record buffer)
      unsigned *ids;      // newly discovered neighbors (in the record buffer)
      unsigned  n_evals;  // neighbor evaluations in this task
      int       slot;
    };
    std::deque<EvalTask>  task_queue;     // generated, waiting for a worker
    std::vector<EvalTask> running_tasks;  // running on a worker slot
    unsigned              unfinished_evals = 0;  // U_q
    // rho_q: fraction of the last H completed evaluations that inserted a
    // candidate into the candidate set (1 until H results are available).
    const unsigned       rho_H = std::max(1u, alps.impact_window);
    std::vector<uint8_t> rho_ring(rho_H, 0);
    unsigned             rho_pos = 0, rho_filled = 0, rho_sum = 0;
    auto                 rho_push = [&](unsigned n_evals, unsigned n_inserted) {
      // spread the insertions evenly over the task's evaluations
      for (unsigned j = 0; j < n_evals; j++) {
        uint8_t x = (uint8_t) (((_u64) (j + 1) * n_inserted) / n_evals -
                               ((_u64) j * n_inserted) / n_evals);
        rho_sum += x;
        rho_sum -= rho_ring[rho_pos];
        rho_ring[rho_pos] = x;
        rho_pos = (rho_pos + 1) % rho_H;
        if (rho_filled < rho_H)
          rho_filled++;
      }
    };
    auto rho_q = [&]() -> float {
      return rho_filled < rho_H ? 1.0f : (float) rho_sum / rho_H;
    };
    // Impact gate: may the next expansion proceed?
    auto impact_gate_open = [&]() -> bool {
      if (unfinished_evals == 0)
        return true;  // nothing is hidden from traversal
      switch (alps.admission) {
        case AlpsSearchParams::WAIT_ALL:
          return false;
        case AlpsSearchParams::U_ONLY:
          return unfinished_evals <= alps.u_only_bound;
        case AlpsSearchParams::RHO_ONLY:
          return rho_q() <= alps.rho_only_theta;
        case AlpsSearchParams::UNGATED:
          return true;
        case AlpsSearchParams::IMPACT:
        default:
          return rho_q() * (float) unfinished_evals <= alps.impact_eps &&
                 unfinished_evals <= alps.impact_umax;
      }
    };

    // ---- Feedback-driven pipeline balancing: per-query knobs ----
    // W_R and T_q carry over between consecutive queries of a query thread,
    // so the controller follows the load instead of restarting every query.
    const unsigned pool_size = (unsigned) eval_slots_.size();
    const unsigned T_max = std::max(
        1u, alps.worker_allowance_max > 0
                ? std::min(alps.worker_allowance_max, pool_size)
                : pool_size);
    const unsigned T_min =
        std::min(std::max(1u, alps.worker_allowance_min), T_max);
    const unsigned W_min = std::max(1u, alps.rdma_window_min);
    const unsigned W_max = std::max(
        W_min, std::min(std::max(alps.rdma_window_max, (unsigned) window),
                        (unsigned) MAX_N_SECTOR_READS / 2));
    static thread_local unsigned tl_rdma_window = 0, tl_worker_allowance = 0;
    static thread_local _u64     tl_base_window = 0;
    if (!alps.balance || tl_base_window != rdma_window_init ||
        tl_rdma_window == 0) {
      tl_base_window = rdma_window_init;
      tl_rdma_window = std::min(std::max((unsigned) window, W_min), W_max);
      tl_worker_allowance =
          std::min(std::max(alps.worker_allowance_init, T_min), T_max);
    }
    unsigned &rdma_window = tl_rdma_window;           // W_R,q
    unsigned &worker_allowance = tl_worker_allowance;  // T_q
    unsigned  n_generated = 0;  // tasks generated since the last control point
    unsigned  n_deferred = 0;   // of those, tasks that had to wait in the queue
    unsigned  expansions_since_ctrl = 0, n_ctrl_points = 0;
    float     defer_ratio_sum = 0;
    if (async_on)
      window = rdma_window;

    auto pool_has_free_slot = [&]() -> bool {
      for (auto *slot : eval_slots_)
        if (slot->state.load(std::memory_order_relaxed) == 0)
          return true;
      return false;
    };
    // Algorithm 1
    auto balance_pipeline = [&]() {
      const float r = (float) n_deferred / (float) std::max(n_generated, 1u);
      if (r > alps.theta_high) {  // computation pressure
        if (worker_allowance < T_max && pool_has_free_slot())
          worker_allowance++;
        else
          rdma_window = std::max(rdma_window - 1, W_min);
      } else if (r < alps.theta_low) {  // low computation pressure
        if (worker_allowance > T_min)
          worker_allowance--;
        else
          rdma_window = std::min(rdma_window + 1, W_max);
      }
      window = rdma_window;
      n_generated = 0;
      n_deferred = 0;
      n_ctrl_points++;
      defer_ratio_sum += r;
    };

    auto start_task_on_slot = [&](EvalTask &t, int si) {
      auto *slot = eval_slots_[si];
      t.slot = si;
      slot->nbr_ids = t.ids;
      slot->nbr_count = t.n_evals;
      slot->pq_dists = pq_dists;
      slot->pq_data = this->data;
      slot->n_chunks = this->n_chunks;
      // return neighbors that can enter the candidate set or pass the gate
      float thresh = (cur_list_size == l_search)
                         ? retset[cur_list_size - 1].distance
                         : std::numeric_limits<float>::max();
      if (verify_enabled && tau_heap.size() >= k_search)
        thresh = std::max(thresh, verify_alpha * tau());
      slot->dist_threshold = thresh;
      slot->state.store(1, std::memory_order_release);
      slot->wake_worker();
      running_tasks.push_back(t);
    };
    // Start queued tasks while the query is within its worker allowance.
    thread_local unsigned claim_hint = 0;
    auto                  dispatch_queued_tasks = [&]() {
      while (!task_queue.empty() && running_tasks.size() < worker_allowance) {
        int si = -1;
        for (unsigned i = 0; i < pool_size; i++) {
          unsigned s = (claim_hint + i) % pool_size;
          int      expected = 0;
          if (eval_slots_[s]->state.compare_exchange_strong(
                  expected, 3, std::memory_order_acq_rel)) {
            si = (int) s;
            claim_hint = s;
            break;
          }
        }
        if (si < 0)
          return;  // shared pool saturated: tasks stay queued
        start_task_on_slot(task_queue.front(), si);
        task_queue.pop_front();
      }
    };
    // Incrementally merge completed evaluations (never blocks).
    auto merge_finished_tasks = [&]() {
      for (size_t i = 0; i < running_tasks.size();) {
        EvalTask &t = running_tasks[i];
        auto     *slot = eval_slots_[t.slot];
        if (slot->state.load(std::memory_order_acquire) != 2) {
          i++;
          continue;
        }
        unsigned inserted = 0;
        for (unsigned r = 0; r < slot->n_results; r++)
          if (insert_candidate(slot->results[r].id, slot->results[r].dist))
            inserted++;
        if (stats)
          stats->n_cmps += slot->n_evaluated;
        rho_push(slot->n_evaluated, inserted);
        unfinished_evals -= std::min(unfinished_evals, t.n_evals);
        release_record(t.node);
        int si = t.slot;
        running_tasks[i] = running_tasks.back();
        running_tasks.pop_back();
        if (!task_queue.empty() && running_tasks.size() < worker_allowance) {
          // hand the slot straight to this query's next queued task
          start_task_on_slot(task_queue.front(), si);
          task_queue.pop_front();
        } else {
          slot->state.store(0, std::memory_order_release);
        }
      }
    };
    auto tasks_outstanding = [&]() -> bool {
      return !task_queue.empty() || !running_tasks.empty();
    };

    // ---- Entry point: the medoid closest to the query ----
    _u32  best_medoid = 0;
    float best_dist = (std::numeric_limits<float>::max)();
    for (_u64 cur_m = 0; cur_m < num_medoids; cur_m++) {
      float d = dist_cmp_float->compare(
          query_float, centroid_data + aligned_dim * cur_m,
          (unsigned) aligned_dim);
      if (d < best_dist) {
        best_medoid = medoids[cur_m];
        best_dist = d;
      }
    }
    compute_pq_dists(&best_medoid, 1, dist_scratch);
    retset[cur_list_size++] = Neighbor(best_medoid, dist_scratch[0], true);
    visited.insert(best_medoid);

    // ---- Main loop ----
    // The search ends only when no unexpanded candidate remains, no
    // evaluation is unfinished (it may still add candidates) and no
    // verification can still change the exact-result set.
    Timer cpu_timer;
    bool  verify_waiting = false;
    fill_window();
    while (has_unexpanded() || tasks_outstanding() ||
           !verify_in_flight.empty() || verify_waiting) {
      poll_reads();
      cpu_timer.reset();
      unsigned id;
      char    *record;
      if (async_on) {
        merge_finished_tasks();
        dispatch_queued_tasks();
        while (true) {
          if (!impact_gate_open()) {
            if (stats)
              stats->n_gate_closed++;
            break;
          }
          if (!pick_ready(id, record))
            break;
          verify_expanded(id, record);
          unsigned *ids;
          unsigned  n_new = new_neighbors(record, ids);
          if (n_new == 0) {
            release_record(id);
          } else {
            task_queue.push_back({id, ids, n_new, -1});
            unfinished_evals += n_new;
            n_generated++;
            dispatch_queued_tasks();
            if (!task_queue.empty())
              n_deferred++;  // waits behind this query's unfinished work
            if (stats) {
              stats->n_eval_tasks++;
              stats->max_unfinished_evals =
                  std::max(stats->max_unfinished_evals, unfinished_evals);
            }
          }
          if (stats)
            stats->n_hops++;
          if (alps.balance &&
              ++expansions_since_ctrl >= alps.balance_interval) {
            expansions_since_ctrl = 0;
            balance_pipeline();
          }
          fill_window();
          merge_finished_tasks();
        }
      } else {
        // neighbor evaluation on the query thread
        while (pick_ready(id, record)) {
          verify_expanded(id, record);
          unsigned *ids;
          unsigned  n_new = new_neighbors(record, ids);
          if (n_new > 0) {
            compute_pq_dists(ids, n_new, dist_scratch);
            for (unsigned m = 0; m < n_new; m++)
              insert_candidate(ids[m], dist_scratch[m]);
            if (stats)
              stats->n_cmps += n_new;
          }
          release_record(id);
          if (stats)
            stats->n_hops++;
          fill_window();
        }
      }
      fill_window();
      verify_waiting = issue_verify_reads();
      if (stats)
        stats->cpu_us += (float) cpu_timer.elapsed();
    }

    // Reads for candidates evicted before expansion may still be in flight;
    // their buffers are reused by the next query.
    while (!in_flight.empty())
      poll_reads();

    // ---- Top-k from the exact-result set ----
    std::sort(full_retset.begin(), full_retset.end());
    _u64 t = 0;
    for (_u64 i = 0; i < full_retset.size() && t < k_search; i++) {
      if (i > 0 && full_retset[i].id == full_retset[i - 1].id)
        continue;  // deduplicate
      indices[t] = full_retset[i].id;
      if (distances != nullptr) {
        distances[t] = full_retset[i].distance;
        if (metric == diskann::Metric::INNER_PRODUCT) {
          // flip the sign to convert min to max
          distances[t] = (-distances[t]);
          // rescale to revert back to original norms (cancelling the effect
          // of base and query pre-processing)
          if (max_base_norm != 0)
            distances[t] *= (max_base_norm * query_norm);
        }
      }
      t++;
    }

    this->thread_data.push(data);
    this->thread_data.push_notify_all();

    if (stats != nullptr) {
      stats->total_us = (float) query_timer.elapsed();
      stats->n_ios = num_ios;
      stats->n_verify_only = n_verify_only_reads;
      stats->n_expand_after_verify = n_adj_only_reads;
      stats->n_exact_verified = (unsigned) verified.size();
      stats->n_balance_decisions = n_ctrl_points;
      stats->mean_defer_ratio =
          n_ctrl_points ? defer_ratio_sum / n_ctrl_points : 0.0f;
      stats->final_rdma_window = (unsigned) window;
      stats->final_worker_allowance = async_on ? worker_allowance : 0;
    }
  }

  // instantiations
  template class PQFlashIndex<_u8>;
  template class PQFlashIndex<_s8>;
  template class PQFlashIndex<float>;

}  // namespace diskann
