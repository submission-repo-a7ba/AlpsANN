#pragma once

#include "distance.h"
#include "cosine_similarity.h"

#ifdef _WINDOWS
#include "windows_aligned_file_reader.h"
#else
#include "linux_aligned_file_reader.h"
#endif

#define READ_U64(stream, val) stream.read((char *) &val, sizeof(_u64))
#define READ_U32(stream, val) stream.read((char *) &val, sizeof(_u32))
#define READ_UNSIGNED(stream, val) stream.read((char *) &val, sizeof(unsigned))

// sector # on disk where node_id is present with in the graph part
#define NODE_SECTOR_NO(node_id) (((_u64)(node_id)) / nnodes_per_sector + 1)

#define OFFSET_TO_NODE_ID(offset, SECTOR_LEN, nnodes_per_sector, max_node_len) \
  (((_u64)(offset) / (SECTOR_LEN) - 1) * (nnodes_per_sector) + \
   ((_u64)(offset) % (SECTOR_LEN)) / (max_node_len))

// obtains region of sector containing node
#define OFFSET_TO_NODE(sector_buf, node_id) \
  ((char *) sector_buf + (((_u64) node_id) % nnodes_per_sector) * max_node_len)

// returns region of `node_buf` containing [NNBRS][NBR_ID(_u32)]
#define OFFSET_TO_NODE_NHOOD(node_buf) \
  (unsigned *) ((char *) node_buf + disk_bytes_per_point)

// returns region of `node_buf` containing [COORD(T)]
#define OFFSET_TO_NODE_COORDS(node_buf) (T *) (node_buf)

// sector # beyond the end of graph where data for id is present for reordering
#define VECTOR_SECTOR_NO(id) \
  (((_u64)(id)) / nvecs_per_sector + reorder_data_start_sector)

// sector # beyond the end of graph where data for id is present for reordering
#define VECTOR_SECTOR_OFFSET(id) \
  ((((_u64)(id)) % nvecs_per_sector) * data_dim * sizeof(float))

namespace diskann {
  namespace pq_flash_index_utils {
    inline void aggregate_coords(const unsigned *ids, const _u64 n_ids,
                          const _u8 *all_coords, const _u64 ndims, _u8 *out) {
      // Prefetch PQ codes 3 entries ahead to hide L3/DRAM latency
      for (_u64 i = 0; i < n_ids; i++) {
        if (i + 3 < n_ids)
          _mm_prefetch((const char *) (all_coords + (_u64) ids[i + 3] * ndims), _MM_HINT_T0);
        memcpy(out + i * ndims, all_coords + (_u64) ids[i] * ndims, ndims * sizeof(_u8));
      }
    }

    inline void pq_dist_lookup(const _u8 *pq_ids, const _u64 n_pts,
                        const _u64 pq_nchunks, const float *pq_dists,
                        float *dists_out) {
      _mm_prefetch((char *) dists_out, _MM_HINT_T0);
      _mm_prefetch((char *) pq_ids, _MM_HINT_T0);
      _mm_prefetch((char *) (pq_ids + 64), _MM_HINT_T0);
      _mm_prefetch((char *) (pq_ids + 128), _MM_HINT_T0);
      memset(dists_out, 0, n_pts * sizeof(float));
      // 4-chunk unrolled: process 4 chunks per inner-loop iteration.
      // Keeps 4 chunk tables (4KB) in L1, gives OOO engine 8 independent
      // loads per iteration (vs 2), and reduces dists_out load/store by 4x.
      _u64 chunk = 0;
      for (; chunk + 4 <= pq_nchunks; chunk += 4) {
        const float *cd0 = pq_dists + 256 * chunk;
        const float *cd1 = cd0 + 256;
        const float *cd2 = cd1 + 256;
        const float *cd3 = cd2 + 256;
        for (_u64 idx = 0; idx < n_pts; idx++) {
          const _u8 *base = pq_ids + pq_nchunks * idx + chunk;
          dists_out[idx] += cd0[base[0]] + cd1[base[1]]
                          + cd2[base[2]] + cd3[base[3]];
        }
      }
      // Remainder chunks
      for (; chunk < pq_nchunks; chunk++) {
        const float *chunk_dists = pq_dists + 256 * chunk;
        for (_u64 idx = 0; idx < n_pts; idx++) {
          _u8 pq_centerid = pq_ids[pq_nchunks * idx + chunk];
          dists_out[idx] += chunk_dists[pq_centerid];
        }
      }
    }
  }
}  // namespace
