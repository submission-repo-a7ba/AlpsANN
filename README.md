# AlpsANN

AlpsANN is a graph-based approximate nearest neighbor search (ANNS) system for
**passive RDMA-based disaggregated memory**. Memory nodes (MNs) only store the
index and serve one-sided RDMA reads; compute nodes (CNs) keep compact PQ codes
and run every part of the query. AlpsANN combines three designs:

1. **Calibrated one-sided search pipeline.** Each MN stores one *unified graph
   record* per node: `[exact vector | neighbor IDs]`. Depending on the
   candidate's state, a read fetches only the fields it still needs:
   *Expand-and-verify* (vector + adjacency), *Verify-only* (vector) or
   *Expand-after-verification* (adjacency). An *error-calibrated gate*
   fetches exact vectors for unexpanded candidates only when
   `d_hat(v,q) <= alpha * tau`. Here `tau` is the current k-th exact distance and
   `alpha` is calibrated offline from measured PQ errors. Reads are issued
   through a bounded per-query RDMA window `W_R`, with doorbell batching and
   selective signaling.
2. **Impact-aware asynchronous traversal.** A shared background worker pool
   evaluates the neighbors of expanded nodes. The query thread fetches,
   dispatches, and incrementally merges completed results. It admits the next
   expansion only when the estimated number of hidden candidate-set updates
   `Psi_q = rho_q * U_q` is at most `epsilon` and `U_q <= U_max`. Here `rho_q`
   is the candidate-update ratio of the last `H` completed evaluations and
   `U_q` is the number of unfinished evaluations.
3. **Feedback-driven pipeline balancing.** Every few expansions, a per-query
   controller compares the defer ratio `r_q = N_deferred / N_generated` with
   `[theta_low, theta_high]`. It then adjusts the worker allowance `T_q` (the
   shared workers a query may use) and the RDMA window `W_R`, following
   Algorithm 1 of the paper.

### Where the designs live in the code

| Design | Code (`src/pq_flash_index.cpp`, `PQFlashIndex::cached_beam_search`) |
|---|---|
| Unified graph records, one-sided reads | `node_offset()`, `post_batch`, `poll_reads`; doorbell batching / selective signaling in `src/rdma_connection.cpp`; routing across memory nodes in `src/rdma_aligned_file_reader.cpp`; passive memory node in `server.cpp` |
| State-driven remote operations | `ReadOp::mode`, `send_expansion_reads` (Expand-and-verify / Expand-after-verification), `issue_verify_reads` (Verify-only) |
| Error-calibrated exact verification | `verified`, `tau_heap`, `record_exact`, `consider_verify`, `verify_pool`; offline calibration in `tests/utils/calibrate_alpha.cpp` |
| Asynchronous one-sided execution | RDMA window `window` (`W_R`), `fill_window`, per-query record pool (`alloc_record` / `release_record`) |
| Impact-aware asynchronous traversal | `rho_push` / `rho_q`, `unfinished_evals` (`U_q`), `impact_gate_open`, `dispatch_queued_tasks`, `merge_finished_tasks`; workers in `EvalWorkerSlot` / `eval_worker_fn` |
| Feedback-driven pipeline balancing | `balance_pipeline` (Algorithm 1), `rdma_window` (`W_R`), `worker_allowance` (`T_q`) |
| Search knobs | `struct AlpsSearchParams` in `include/pq_flash_index.h` |

### Repository layout

| Path | Contents |
|---|---|
| `src/pq_flash_index.cpp` | AlpsANN search |
| `src/rdma_*.cpp`, `server.cpp` | One-sided RDMA client and the passive memory node |
| `src/index.cpp`, `src/aux_utils.cpp`, `src/partition_and_pq.cpp` | Vamana graph construction, PQ training, record layout |
| `tests/build_disk_index.cpp`, `tests/upload_rdma.cpp` | Build the index; upload records to a memory node |
| `tests/search_disk_index_rdma.cpp` | Search driver |
| `tests/utils/calibrate_alpha.cpp` | Offline calibration of `alpha` |
| `tests/build_memory_index.cpp`, `tests/search_memory_index.cpp` | Graph-matched in-memory Vamana |
| `tests/rdma_latency_probe.cpp` | RDMA read latency versus read size |
| `scripts/` | Experiment configuration and driver |

## Prerequisites

- Linux (tested on Ubuntu 22.04)
- CMake >= 2.8.12
- GCC with C++14 support
- Boost (program_options)
- Intel MKL
- libaio
- gperftools (tcmalloc)
- **RDMA NIC** with libibverbs and librdmacm

Install dependencies on Ubuntu:

```bash
apt install build-essential cmake g++ libboost-all-dev libaio-dev \
    libgoogle-perftools-dev libmkl-full-dev libibverbs-dev librdmacm-dev
```

## Build

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

## Configuration

1. Set the `DATA_ROOT` environment variable to the directory containing your datasets, or edit `scripts/config_dataset.sh` directly.

2. Copy the sample config and select a dataset:

```bash
cd scripts
cp config_sample.sh config_local.sh
# Edit config_local.sh: uncomment the desired dataset function
```

3. For RDMA-based commands, export the remote endpoint before running scripts:

```bash
export RDMA_SERVER=<rdma_server_ip>
export RDMA_PORT=7471   # optional; defaults to 7471
```

With several memory nodes, export `RDMA_SERVERS=ip1:port1,ip2:port2` instead;
the records are range-partitioned across them (start each node with
`--local_load <disk.index> <offset> <size>` for its range).

## RDMA Server

The RDMA server hosts the index in disaggregated memory. Build and run it on the remote memory node:

```bash
# Build the server (standalone, not part of CMake)
g++ -O2 -o server server.cpp -lrdmacm -libverbs -lpthread

# Start the server (allocates memory and listens for RDMA connections)
./server <bind_ip> [port]

# Example
./server <bind_ip> 7471

# Pre-load an index from disk into RDMA memory
./server <bind_ip> 7471 --local_load /path/to/disk.index 0 <size_bytes>
```

The server must be running before any upload or search operation.

## Running AlpsANN

All steps are launched from `scripts/`:

```bash
cd scripts

# 1. Build the index (Vamana graph, PQ codes, unified graph records)
./run_benchmark.sh release build

# 2. Calibrate alpha on held-out queries (set CALIB_QUERY_FILE / CALIB_GT_FILE)
./run_benchmark.sh release calibrate

# 3. Start the passive MN with the index (on the memory node)
./server <bind_ip> 7471 --local_load /path/to/_disk.index 0 <size_bytes>
#    or upload an existing index to a running server:
./run_benchmark.sh release upload_rdma

# 4. Search over RDMA
./run_benchmark.sh release search
```

`calibrate` writes `<index_prefix>_alpha.txt`, and the search loads it
automatically (override with `--verify_alpha`).

### Search options (`tests/search_disk_index_rdma`)

| Option | Default | Meaning |
|---|---|---|
| `-W, --rdma_window` | 8 | RDMA window `W_R`: outstanding one-sided reads per query (initial value when balancing) |
| `--num_workers` | 0 | Size of the shared evaluation worker pool (0 = evaluate on query threads) |
| `--state_driven_reads` | 1 | Fetch only required record fields (0 = always the full record) |
| `--verify_policy` | `calibrated` | `expansion_only`, `calibrated` (`d_hat <= alpha * tau`), `verify_all` |
| `--verify_alpha` | from file | Calibrated `alpha` |
| `--async_traversal` | 1 | Impact-aware asynchronous traversal (needs `--num_workers > 0`) |
| `--admission` | `impact` | `waitall`, `u_only` (`--u_only_bound`), `rho_only` (`--rho_only_theta`), `impact`, `ungated` |
| `--impact_window`, `--impact_eps`, `--impact_umax` | 64, 1, 128 | `H`, `epsilon`, `U_max` |
| `--balance` | 1 | Feedback-driven pipeline balancing (0 = fixed `W_R` and `T_q`) |
| `--theta_low`, `--theta_high`, `--balance_interval` | 0.30, 0.65, 32 | Balanced zone and control interval (expansions) |
| `--rdma_window_min/max`, `--worker_allowance[_min/_max]` | 1/16, 1 (1/pool) | Bounds of `W_R` and `T_q` |
| `--rdma_servers ip:port,...`, `--total_index_size` | | Range-partition the records across several memory nodes |

Each search prints, for every `L`, QPS, mean / P99 / P99.9 latency, reads per
query and Recall@K, followed by a per-design breakdown: Verify-only and
Expand-after-verification reads, exact-verified candidates, expansions,
evaluation tasks, closed-gate decisions, `max U_q`, the mean defer ratio and
the final `W_R` / `T_q`.

## Datasets

Download datasets from [NeurIPS'21 Big-ANN Benchmark](https://big-ann-benchmarks.com/neurips21.html).

| Dataset | Data type | Dimensions | Distance | Queries |
|---------|-----------|------------|----------|---------|
| BIGANN | uint8 | 128 | L2 | 10,000 |
| DEEP | float | 96 | L2 | 10,000 |
| SPACEV | int8 | 100 | L2 | 30,000 |

Place datasets under `DATA_ROOT` with the following structure:

```
$DATA_ROOT/
  bigann10m/    # or bigann100m/, bigann1b/
    bigann_base.bin
    bigann_query.bin
    bigann_gt100.bin
  deep1b/
    base.10M.fbin   # or base.100M.fbin
    query.public.10K.fbin
    deep10m_gt100    # or deep100m_gt100
  spacev1b/
    base.10M.i8bin   # or base.100M.i8bin
    query.30K.i8bin
    spacev10m_gt100  # or spacev100m_gt100
```

## License

MIT License. See [LICENSE](LICENSE).
