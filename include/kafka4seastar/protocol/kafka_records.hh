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

#include <vector>

namespace kafka4seastar {

class kafka_record_header {
public:
    seastar::sstring header_key;
    seastar::sstring value;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class kafka_record {
public:
    kafka_varint_t timestamp_delta;
    kafka_varint_t offset_delta;
    std::optional<seastar::sstring> key;
    std::optional<seastar::sstring> value;
    std::vector<kafka_record_header> headers;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

enum class kafka_record_compression_type {
    NO_COMPRESSION = 0, GZIP = 1, SNAPPY = 2, LZ4 = 3, ZSTD = 4
};

enum class kafka_record_timestamp_type {
    CREATE_TIME = 0, LOG_APPEND_TIME = 1
};

class kafka_record_batch {
public:
    kafka_int64_t base_offset;
    kafka_int32_t partition_leader_epoch;
    kafka_int8_t magic;

    kafka_record_compression_type compression_type;
    kafka_record_timestamp_type timestamp_type;
    bool is_transactional;
    bool is_control_batch;

    kafka_int64_t first_timestamp;
    kafka_int64_t producer_id;
    kafka_int16_t producer_epoch;
    kafka_int32_t base_sequence;

    std::vector<kafka_record> records;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class kafka_records {
public:
    std::vector<kafka_record_batch> record_batches;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}
