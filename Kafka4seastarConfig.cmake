get_filename_component(kafka4seastar_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
include(CMakeFindDependencyMacro)

find_dependency(Seastar REQUIRED)

if(NOT TARGET Kafka4seastar::kafka4seastar)
    include("${kafka4seastar_CMAKE_DIR}/Kafka4seastarTargets.cmake")
endif()
