#!/bin/sh
# AlpsANN configuration template.
# Copy this file to config_local.sh and edit as needed:
#   cp config_sample.sh config_local.sh
# For RDMA-related commands, export RDMA_SERVER and optionally RDMA_PORT
# (or RDMA_SERVERS=ip:port,ip:port for several memory nodes).
source $(dirname "$0")/config_dataset.sh

# Choose the dataset by uncommenting ONE line below.
# Set DATA_ROOT env variable or edit config_dataset.sh first.
# dataset_bigann10M
# dataset_bigann100M
# dataset_deep100M
# dataset_spacev100M
# dataset_bigann1B

##################
#   Index Build  #
##################
R=128
BUILD_L=150
M=8          # RAM budget (GB) for the graph build
BUILD_T=100

#####################
#   Calibration     #
#####################
# Held-out queries (disjoint from QUERY_FILE) and their ground truth, used by
# `run_benchmark.sh release calibrate` to compute alpha -> <index>_alpha.txt.
CALIB_QUERY_FILE=${CALIB_QUERY_FILE:-$QUERY_FILE}
CALIB_GT_FILE=${CALIB_GT_FILE:-$GT_FILE}
CALIB_DELTA=0.01

##############################
#   AlpsANN search (RDMA)    #
##############################
W_LIST=(8)            # initial RDMA window W_R (per query)
T_LIST=(26)           # query threads
NUM_WORKERS=6         # shared evaluation worker pool
K=${K:-10}
LS="20 40 60 80 100 150 200"

# Calibrated one-sided search pipeline
STATE_DRIVEN_READS=1  # 0 = always read the full record
VERIFY_POLICY=calibrated   # expansion_only | calibrated | verify_all

# Impact-aware asynchronous traversal
ASYNC_TRAVERSAL=1
ADMISSION=impact      # waitall | u_only | rho_only | impact | ungated
IMPACT_H=64
IMPACT_EPS=1
IMPACT_UMAX=128

# Feedback-driven pipeline balancing
BALANCE=1             # 0 = fixed W_R and worker allowance
THETA_LOW=0.30
THETA_HIGH=0.65
BALANCE_INTERVAL=32
RDMA_WINDOW_MIN=1
RDMA_WINDOW_MAX=16
