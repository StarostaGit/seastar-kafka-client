/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <kafka4seastar/protocol/kafka_primitives.hh>

namespace kafka4seastar {

class produce_response_batch_index_and_error_message {
private:
    kafka_int32_t batch_index;
    kafka_nullable_string_t batch_index_error_message;
public:
    [[nodiscard]] const kafka_int32_t& get_batch_index() const;

    [[nodiscard]] const kafka_nullable_string_t& get_batch_index_error_message() const;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class produce_response_partition_produce_response {
public:
    kafka_int32_t partition_index;
    kafka_error_code_t error_code;
    kafka_int64_t base_offset;
    kafka_int64_t log_append_time_ms;
    kafka_int64_t log_start_offset;
    kafka_array_t<produce_response_batch_index_and_error_message> record_errors;
    kafka_nullable_string_t error_message;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class produce_response_topic_produce_response {
public:
    kafka_string_t name;
    kafka_array_t<produce_response_partition_produce_response> partitions;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class produce_response {
public:
    kafka_array_t<produce_response_topic_produce_response> responses;
    kafka_int32_t throttle_time_ms;
    kafka_error_code_t error_code;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}
