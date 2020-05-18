#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2018 Scylladb, Ltd.
#

cmake_minimum_required (VERSION 3.5)
project (kafka4seastar)

find_package (Seastar REQUIRED)

set(GEN_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)

set(HEADER_DIRECTORY include/kafka4seastar)

set(HEADERS
        ${HEADER_DIRECTORY}/connection/connection_manager.hh
        ${HEADER_DIRECTORY}/connection/kafka_connection.hh
        ${HEADER_DIRECTORY}/connection/tcp_connection.hh
        ${HEADER_DIRECTORY}/producer/batcher.hh
        ${HEADER_DIRECTORY}/producer/kafka_producer.hh
        ${HEADER_DIRECTORY}/producer/producer_properties.hh
        ${HEADER_DIRECTORY}/producer/sender.hh
        ${HEADER_DIRECTORY}/protocol/kafka_error_code.hh
        ${HEADER_DIRECTORY}/protocol/kafka_primitives.hh
        ${HEADER_DIRECTORY}/protocol/kafka_records.hh
        ${HEADER_DIRECTORY}/protocol/api_versions_request.hh
        ${HEADER_DIRECTORY}/protocol/api_versions_response.hh
        ${HEADER_DIRECTORY}/protocol/headers.hh
        ${HEADER_DIRECTORY}/protocol/metadata_request.hh
        ${HEADER_DIRECTORY}/protocol/metadata_response.hh
        ${HEADER_DIRECTORY}/protocol/produce_request.hh
        ${HEADER_DIRECTORY}/protocol/produce_response.hh
        ${HEADER_DIRECTORY}/utils/defaults.hh
        ${HEADER_DIRECTORY}/utils/metadata_manager.hh
        ${HEADER_DIRECTORY}/utils/partitioner.hh
        ${HEADER_DIRECTORY}/utils/retry_helper.hh)

set(SOURCES
        src/connection/connection_manager.cc
        src/connection/kafka_connection.cc
        src/connection/tcp_connection.cc
        src/producer/batcher.cc
        src/producer/kafka_producer.cc
        src/producer/sender.cc
        src/protocol/kafka_error_code.cc
        src/protocol/kafka_records.cc
        src/protocol/api_versions_request.cc
        src/protocol/api_versions_response.cc
        src/protocol/headers.cc
        src/protocol/metadata_request.cc
        src/protocol/metadata_response.cc
        src/protocol/produce_request.cc
        src/protocol/produce_response.cc
        src/utils/defaults.cc
        src/utils/metadata_manager.cc
        src/utils/partitioner.cc)

add_library(kafka4seastar STATIC ${HEADERS})

target_include_directories(kafka4seastar
        PUBLIC
            $<INSTALL_INTERFACE:include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${GEN_BINARY_DIR}/include>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${GEN_BINARY_DIR}/src)

target_sources(kafka4seastar PRIVATE ${SOURCES})

add_subdirectory(tests/unit)

add_executable (kafka_demo
        demo/kafka_demo.cc)

target_link_libraries (kafka4seastar
        Seastar::seastar
        Seastar::seastar_testing)

target_link_libraries(kafka_demo
        kafka4seastar)