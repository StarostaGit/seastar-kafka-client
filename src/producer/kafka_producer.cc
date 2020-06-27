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

#include <sstream>
#include <vector>
#include <iostream>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>

#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>

#include <kafka4seastar/producer/kafka_producer.hh>
#include <kafka4seastar/producer/producer_properties.hh>
#include <kafka4seastar/protocol/kafka_primitives.hh>
#include <kafka4seastar/protocol/metadata_request.hh>
#include <kafka4seastar/protocol/metadata_response.hh>
#include <kafka4seastar/protocol/api_versions_request.hh>
#include <kafka4seastar/protocol/api_versions_response.hh>
#include <kafka4seastar/connection/tcp_connection.hh>
#include <kafka4seastar/protocol/produce_request.hh>
#include <kafka4seastar/protocol/produce_response.hh>

using namespace seastar;

namespace kafka4seastar {

kafka_producer::kafka_producer(producer_properties&& properties)
    : _properties(std::move(properties)),
      _connection_manager(_properties.client_id),
      _metadata_manager(_connection_manager, _properties.metadata_refresh),
      _batcher(_metadata_manager, _connection_manager, _properties.retries,
              _properties.acks, _properties.request_timeout, _properties.linger,
              _properties.buffer_memory, std::move(_properties.retry_backoff_strategy)) {}

seastar::future<> kafka_producer::init() {
    return _connection_manager.init(_properties.servers, _properties.request_timeout).then([this] {
        _metadata_manager.start_refresh();
        return _metadata_manager.refresh_metadata();
    }).then([this] {
        _batcher.start_flush();
    });
}

seastar::future<> kafka_producer::produce(seastar::sstring topic_name,
                                          seastar::sstring key, seastar::sstring value) {
    return produce(std::move(topic_name), std::optional(std::move(key)), std::optional(std::move(value)));
}

seastar::future<> kafka_producer::produce(seastar::sstring topic_name,
        std::optional<seastar::sstring> key, std::optional<seastar::sstring> value) {
    const auto& metadata =_metadata_manager.get_metadata();
    auto partition_index = 0;
    for (const auto& topic : *metadata.topics) {
        if (*topic.name == topic_name) {
            partition_index = *_properties.partitioning_strategy->get_partition(
                    key.value_or(""), topic.partitions).partition_index;
            break;
        }
    }

    sender_message message;
    message.topic = std::move(topic_name);
    message.key = std::move(key);
    message.value = std::move(value);
    message.partition_index = partition_index;

    auto send_future = message.promise.get_future();
    _batcher.queue_message(std::move(message));
    return send_future;
}

seastar::future<> kafka_producer::flush() {
    return _batcher.flush();
}

seastar::future<> kafka_producer::disconnect() {
    return _batcher.stop_flush().then([this] {
        return _metadata_manager.stop_refresh();
    }).then([this] () {
        return _connection_manager.disconnect_all();
    });
}

}
