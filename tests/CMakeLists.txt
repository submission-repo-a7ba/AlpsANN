# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

set(CMAKE_CXX_STANDARD 14)

# Index construction (Vamana graph + PQ codes + unified graph records)
add_executable(build_disk_index build_disk_index.cpp)
target_link_libraries(build_disk_index ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} ${DISKANN_ASYNC_LIB} Boost::program_options)

# Upload an index to a passive memory node
add_executable(upload_rdma upload_rdma.cpp)
target_link_libraries(upload_rdma ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

# AlpsANN search over passive disaggregated memory
add_executable(search_disk_index_rdma search_disk_index_rdma.cpp)
target_link_libraries(search_disk_index_rdma ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

# In-memory Vamana (graph-matched in-memory reference)
add_executable(build_memory_index build_memory_index.cpp)
target_link_libraries(build_memory_index ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(search_memory_index search_memory_index.cpp)
target_link_libraries(search_memory_index ${PROJECT_NAME} ${DISKANN_ASYNC_LIB} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

# RDMA micro-benchmarks
add_executable(rdma_latency_probe rdma_latency_probe.cpp)
target_link_libraries(rdma_latency_probe ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)

add_executable(rdma_quick_test rdma_quick_test.cpp)
target_link_libraries(rdma_quick_test ${PROJECT_NAME} ${DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS} Boost::program_options)
