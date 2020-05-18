##
## This file is open source software, licensed to you under the terms
## of the Apache License, Version 2.0 (the "License").  See the NOTICE file
## distributed with this work for additional information regarding copyright
## ownership.  You may not use this file except in compliance with the License.
##
## You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
#
##
## Copyright (C) 2018 Scylladb, Ltd.
##
#

add_custom_target(unit_tests)

function(add_kafka_test name)

    cmake_parse_arguments (parsed_args
            ""
            ""
            "SOURCES;LIBRARIES"
            ${ARGN})

    set(target test_unit_${name})

    set(libraries ${parsed_args_LIBRARIES})
    list(APPEND libraries
            kafka4seastar)

    add_executable(${target} ${parsed_args_SOURCES})
    target_link_libraries(${target}
            PRIVATE ${libraries})

    add_dependencies(unit_tests ${target})

    target_compile_definitions (${target}
            PRIVATE SEASTAR_TESTING_MAIN)

endfunction()

add_kafka_test(kafka_connection
        SOURCES kafka_connection_test.cc)

add_kafka_test(kafka_protocol
        SOURCES kafka_protocol_test.cc)

add_kafka_test(kafka_retry_helper
        SOURCES kafka_retry_helper_test.cc)
