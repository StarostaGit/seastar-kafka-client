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

#include <kafka4seastar/protocol/produce_response.hh>

using namespace seastar;

namespace kafka4seastar {

void produce_response_batch_index_and_error_message::serialize(std::ostream& os, int16_t api_version) const {
    batch_index.serialize(os, api_version);
    batch_index_error_message.serialize(os, api_version);
}

void produce_response_batch_index_and_error_message::deserialize(std::istream& is, int16_t api_version) {
    batch_index.deserialize(is, api_version);
    batch_index_error_message.deserialize(is, api_version);
}

void produce_response_partition_produce_response::serialize(std::ostream& os, int16_t api_version) const {
    partition_index.serialize(os, api_version);
    error_code.serialize(os, api_version);
    base_offset.serialize(os, api_version);
    if (api_version >= 2) {
        log_append_time_ms.serialize(os, api_version);
    }
    if (api_version >= 5) {
        log_start_offset.serialize(os, api_version);
    }
    if (api_version >= 8) {
        record_errors.serialize(os, api_version);
        error_message.serialize(os, api_version);
    }
}

void produce_response_partition_produce_response::deserialize(std::istream& is, int16_t api_version) {
    partition_index.deserialize(is, api_version);
    error_code.deserialize(is, api_version);
    base_offset.deserialize(is, api_version);
    if (api_version >= 2) {
        log_append_time_ms.deserialize(is, api_version);
    }
    if (api_version >= 5) {
        log_start_offset.deserialize(is, api_version);
    }
    if (api_version >= 8) {
        record_errors.deserialize(is, api_version);
        error_message.deserialize(is, api_version);
    }
}

void produce_response_topic_produce_response::serialize(std::ostream& os, int16_t api_version) const {
    name.serialize(os, api_version);
    partitions.serialize(os, api_version);
}

void produce_response_topic_produce_response::deserialize(std::istream& is, int16_t api_version) {
    name.deserialize(is, api_version);
    partitions.deserialize(is, api_version);
}

void produce_response::serialize(std::ostream& os, int16_t api_version) const {
    responses.serialize(os, api_version);
    if (api_version >= 1) {
        throttle_time_ms.serialize(os, api_version);
    }
}

void produce_response::deserialize(std::istream& is, int16_t api_version) {
    responses.deserialize(is, api_version);
    if (api_version >= 1) {
        throttle_time_ms.deserialize(is, api_version);
    }
}

}
