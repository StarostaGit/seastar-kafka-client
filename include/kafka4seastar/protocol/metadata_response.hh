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

class metadata_response_broker {
public:
    kafka_int32_t node_id;
    kafka_string_t host;
    kafka_int32_t port;
    kafka_nullable_string_t rack;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class metadata_response_partition {
public:
    kafka_error_code_t error_code;
    kafka_int32_t partition_index;
    kafka_int32_t leader_id;
    kafka_int32_t leader_epoch;
    kafka_array_t<kafka_int32_t> replica_nodes;
    kafka_array_t<kafka_int32_t> isr_nodes;
    kafka_array_t<kafka_int32_t> offline_replicas;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class metadata_response_topic {
public:
    kafka_error_code_t error_code;
    kafka_string_t name;
    kafka_bool_t is_internal;
    kafka_array_t<metadata_response_partition> partitions;
    kafka_int32_t topic_authorized_operations;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

class metadata_response {
public:
    kafka_int32_t throttle_time_ms;
    kafka_array_t<metadata_response_broker> brokers;
    kafka_nullable_string_t cluster_id;
    kafka_int32_t controller_id;
    kafka_array_t<metadata_response_topic> topics;
    kafka_int32_t cluster_authorized_operations;
    kafka_error_code_t error_code;

    void serialize(std::ostream& os, int16_t api_version) const;

    void deserialize(std::istream& is, int16_t api_version);
};

}
