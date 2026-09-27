# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

set(CMAKE_CXX_STANDARD 17)

if(MSVC)
    add_subdirectory(dll)
else()
    #file(GLOB CPP_SOURCES *.cpp)
    set(CPP_SOURCES ann_exception.cpp aux_utils.cpp distance.cpp index.cpp
        linux_aligned_file_reader.cpp math_utils.cpp natural_number_map.cpp
        natural_number_set.cpp memory_mapper.cpp partition_and_pq.cpp
        pq_flash_index.cpp logger.cpp utils.cpp
        rdma_writer.cpp rdma_aligned_file_reader.cpp rdma_connection.cpp rdma_connection_pool.cpp)
    add_library(${PROJECT_NAME} ${CPP_SOURCES})
    add_library(${PROJECT_NAME}_s STATIC ${CPP_SOURCES})
    target_link_libraries(${PROJECT_NAME} ${DISKANN_RDMA_LIBRARIES})
    target_link_libraries(${PROJECT_NAME}_s ${DISKANN_RDMA_LIBRARIES})
endif()
install()
