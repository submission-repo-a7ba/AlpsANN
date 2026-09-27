#!/bin/bash
# AlpsANN experiment driver.
#   ./run_benchmark.sh [debug/release] build           build the index (graph + PQ codes)
#   ./run_benchmark.sh [debug/release] upload_rdma     upload the records to the memory node
#   ./run_benchmark.sh [debug/release] calibrate       calibrate the verification factor alpha
#   ./run_benchmark.sh [debug/release] search          search over RDMA

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CONFIG_LOCAL="${SCRIPT_DIR}/config_local.sh"

if [ ! -f "${CONFIG_LOCAL}" ]; then
  echo "ERROR: Missing ${CONFIG_LOCAL}. Copy config_sample.sh to config_local.sh and edit it for your dataset." >&2
  exit 1
fi

source "${CONFIG_LOCAL}"

require_rdma_env() {
  if [ -z "${RDMA_SERVER:-}" ] && [ -z "${RDMA_SERVERS:-}" ]; then
    echo "ERROR: export RDMA_SERVER=<memory-node-ip> (or RDMA_SERVERS=ip:port,ip:port for several memory nodes)." >&2
    exit 1
  fi
  RDMA_PORT_VALUE="${RDMA_PORT:-7471}"
}

INDEX_PREFIX_PATH="${PREFIX}_M${M}_R${R}_L${BUILD_L}_B${B}/"
SUMMARY_FILE_PATH="../indices/summary.log"

print_usage_and_exit() {
  echo "Usage: ./run_benchmark.sh [debug/release] [build/upload_rdma/calibrate/search]"
  exit 1
}

case $1 in
  debug)
    cmake -DCMAKE_BUILD_TYPE=Debug .. -B ../debug
    EXE_PATH=../debug
  ;;
  release)
    cmake -DCMAKE_BUILD_TYPE=Release .. -B ../release
    EXE_PATH=../release
  ;;
  *)
    print_usage_and_exit
  ;;
esac
pushd $EXE_PATH
make -j
popd

mkdir -p ../indices && cd ../indices

date
case $2 in
  build)
    if [ -d ${INDEX_PREFIX_PATH} ]; then
      echo "Directory ${INDEX_PREFIX_PATH} already exists. Remove or rename it and then re-run."
      exit 1
    fi
    mkdir -p ${INDEX_PREFIX_PATH}
    echo "Building index..."
    time ${EXE_PATH}/tests/build_disk_index \
      --data_type $DATA_TYPE \
      --dist_fn $DIST_FN \
      --data_path $BASE_PATH \
      --index_path_prefix $INDEX_PREFIX_PATH \
      -R $R \
      -L $BUILD_L \
      -B $B \
      -M $M \
      -T $BUILD_T > ${INDEX_PREFIX_PATH}build.log
  ;;
  upload_rdma)
    require_rdma_env
    # Allow overriding index path: ./run_benchmark.sh release upload_rdma /path/to/index_dir/
    UPLOAD_PATH="${3:-${INDEX_PREFIX_PATH}}"
    if [ ! -d ${UPLOAD_PATH} ]; then
      echo "ERROR: Index directory ${UPLOAD_PATH} does not exist. Build the index first."
      exit 1
    fi
    echo "Uploading ${UPLOAD_PATH}_disk.index to the memory node..."
    time ${EXE_PATH}/tests/upload_rdma \
      --index_path_prefix $UPLOAD_PATH \
      --rdma_server "${RDMA_SERVER}" \
      --rdma_port "${RDMA_PORT_VALUE}" 2>&1 | tee ${UPLOAD_PATH}upload_rdma.log
  ;;
  calibrate)
    # Offline calibration of alpha on held-out queries (disjoint from
    # QUERY_FILE). Writes <prefix>_alpha.txt, loaded by the search.
    echo "Calibrating alpha (delta=${CALIB_DELTA})..."
    ${EXE_PATH}/tests/utils/calibrate_alpha --data_type $DATA_TYPE \
      --index_path_prefix $INDEX_PREFIX_PATH \
      --base_file $BASE_PATH \
      --query_file $CALIB_QUERY_FILE \
      --gt_file $CALIB_GT_FILE \
      -K $K \
      --delta ${CALIB_DELTA} | tee ${INDEX_PREFIX_PATH}calibrate.log
  ;;
  search)
    require_rdma_env
    if [ ! -d "$INDEX_PREFIX_PATH" ]; then
      echo "Directory $INDEX_PREFIX_PATH does not exist. Build it first."
      exit 1
    fi
    mkdir -p ${INDEX_PREFIX_PATH}search ${INDEX_PREFIX_PATH}result
    if [ -n "${RDMA_SERVERS:-}" ]; then
      MN_ARGS="--rdma_servers ${RDMA_SERVERS}"
    else
      MN_ARGS="--rdma_server ${RDMA_SERVER} --rdma_port ${RDMA_PORT_VALUE}"
    fi
    log_arr=()
    for W in ${W_LIST[@]}
    do
      for T in ${T_LIST[@]}
      do
        SEARCH_LOG=${INDEX_PREFIX_PATH}search/search_K${K}_W${W}_T${T}_WK${NUM_WORKERS}_${VERIFY_POLICY}_${ADMISSION}_BAL${BALANCE}.log
        echo "Searching... W_R=${W} T=${T} workers=${NUM_WORKERS} L=${LS} log: ${SEARCH_LOG}"
        ${EXE_PATH}/tests/search_disk_index_rdma --data_type $DATA_TYPE \
          --dist_fn $DIST_FN \
          --index_path_prefix $INDEX_PREFIX_PATH \
          --disk_file_path ${INDEX_PREFIX_PATH}_disk.index \
          --query_file $QUERY_FILE \
          --gt_file $GT_FILE \
          -K $K \
          -L ${LS} \
          -T $T \
          --result_path ${INDEX_PREFIX_PATH}result/result \
          ${MN_ARGS} \
          --rdma_window $W \
          --num_workers ${NUM_WORKERS} \
          --state_driven_reads ${STATE_DRIVEN_READS} \
          --verify_policy ${VERIFY_POLICY} \
          --async_traversal ${ASYNC_TRAVERSAL} \
          --admission ${ADMISSION} \
          --impact_window ${IMPACT_H} \
          --impact_eps ${IMPACT_EPS} \
          --impact_umax ${IMPACT_UMAX} \
          --balance ${BALANCE} \
          --theta_low ${THETA_LOW} \
          --theta_high ${THETA_HIGH} \
          --balance_interval ${BALANCE_INTERVAL} \
          --rdma_window_min ${RDMA_WINDOW_MIN} \
          --rdma_window_max ${RDMA_WINDOW_MAX} > ${SEARCH_LOG} 2>&1
        log_arr+=( ${SEARCH_LOG} )
      done
    done
    for f in "${log_arr[@]}"
    do
      printf "$f\n" | tee -a $SUMMARY_FILE_PATH
      grep -E "^\s+L\s+|^\s+[0-9]+\s+[0-9]+\s+[0-9.]+|reads:" $f | tee -a $SUMMARY_FILE_PATH
      printf "\n" >> $SUMMARY_FILE_PATH
    done
  ;;
  *)
    print_usage_and_exit
  ;;
esac
