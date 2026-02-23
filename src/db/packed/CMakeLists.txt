include(${PROJECT_ROOT_DIR}/cmake/bazel.cmake)
include(${PROJECT_ROOT_DIR}/cmake/option.cmake)

cc_library(
    NAME zvec_packed STATIC STRICT ALWAYS_LINK
    SRCS *.cc
    LIBS    glog
            zvec_ailego
            zvec_core
            zvec_common
            zvec_proto
            roaring
            rocksdb
            libprotobuf
            Arrow::arrow_static
            Arrow::arrow_compute
            Arrow::arrow_dataset
    INCS .  ${CMAKE_SOURCE_DIR}/src
    VERSION "${PROXIMA_ZVEC_VERSION}"
)
