#!/bin/sh

# Dataset configuration for AlpsANN benchmarks.
# Set DATA_ROOT to the directory containing your downloaded datasets.
# See README.md for dataset download instructions.

DATA_ROOT=${DATA_ROOT:-/path/to/datasets}

#################
#   BIGANN10M   #
#################
dataset_bigann10M() {
  BASE_PATH=${DATA_ROOT}/bigann10m/bigann_base.bin
  QUERY_FILE=${DATA_ROOT}/bigann10m/bigann_query.bin
  GT_FILE=${DATA_ROOT}/bigann10m/bigann_gt100.bin
  PREFIX=bigann_10m
  DATA_TYPE=uint8
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=128
  DATA_N=10000000
}

##################
#   BIGANN100M   #
##################
dataset_bigann100M() {
  BASE_PATH=${DATA_ROOT}/bigann100m/bigann_base.bin
  QUERY_FILE=${DATA_ROOT}/bigann100m/bigann_query.bin
  GT_FILE=${DATA_ROOT}/bigann100m/bigann_gt100.bin
  PREFIX=bigann_100m
  DATA_TYPE=uint8
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=128
  DATA_N=100000000
}

##################
#   DEEP10M      #
##################
dataset_deep10M() {
  BASE_PATH=${DATA_ROOT}/deep1b/base.10M.fbin
  QUERY_FILE=${DATA_ROOT}/deep1b/query.public.10K.fbin
  GT_FILE=${DATA_ROOT}/deep1b/deep10m_gt100
  PREFIX=deep_10m
  DATA_TYPE=float
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=96
  DATA_N=10000000
}

##################
#   SPACEV10M    #
##################
dataset_spacev10M() {
  BASE_PATH=${DATA_ROOT}/spacev1b/base.10M.i8bin
  QUERY_FILE=${DATA_ROOT}/spacev1b/query.30K.i8bin
  GT_FILE=${DATA_ROOT}/spacev1b/spacev10m_gt100
  PREFIX=spacev_10m
  DATA_TYPE=int8
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=100
  DATA_N=10000000
}

##################
#   SPACEV100M   #
##################
dataset_spacev100M() {
  BASE_PATH=${DATA_ROOT}/spacev1b/base.100M.i8bin
  QUERY_FILE=${DATA_ROOT}/spacev1b/query.30K.i8bin
  GT_FILE=${DATA_ROOT}/spacev1b/spacev100m_gt100
  PREFIX=spacev_100m
  DATA_TYPE=int8
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=100
  DATA_N=100000000
}

##################
#   DEEP100M     #
##################
dataset_deep100M() {
  BASE_PATH=${DATA_ROOT}/deep1b/base.100M.fbin
  QUERY_FILE=${DATA_ROOT}/deep1b/query.public.10K.fbin
  GT_FILE=${DATA_ROOT}/deep1b/deep100m_gt100
  PREFIX=deep_100m
  DATA_TYPE=float
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=96
  DATA_N=100000000
}

#################
#   BIGANN1B    #
#################
dataset_bigann1B() {
  BASE_PATH=${DATA_ROOT}/bigann1b/bigann_base.bin
  QUERY_FILE=${DATA_ROOT}/bigann1b/bigann_query.bin
  GT_FILE=${DATA_ROOT}/bigann1b/bigann_gt100.bin
  PREFIX=bigann_1b
  DATA_TYPE=uint8
  DIST_FN=l2
  B=3
  K=10
  DATA_DIM=128
  DATA_N=1000000000
}
