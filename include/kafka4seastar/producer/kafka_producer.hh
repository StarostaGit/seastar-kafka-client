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

#include <string>

#include <seastar/core/future.hh>
#include <seastar/net/net.hh>

#include <kafka4seastar/producer/producer_properties.hh>
#include <kafka4seastar/connection/connection_manager.hh>
#include <kafka4seastar/utils/partitioner.hh>
#include <kafka4seastar/utils/metadata_manager.hh>
#include <kafka4seastar/producer/batcher.hh>

namespace kafka4seastar {

class kafka_producer final {

    producer_properties _properties;
    connection_manager _connection_manager;
    metadata_manager _metadata_manager;
    batcher _batcher;

public:
    explicit kafka_producer(producer_properties&& properties);
    seastar::future<> init();
    seastar::future<> produce(seastar::sstring topic_name, seastar::sstring key, seastar::sstring value);
    seastar::future<> produce(seastar::sstring topic_name,
            std::optional<seastar::sstring> key, std::optional<seastar::sstring> value);
    seastar::future<> flush();
    seastar::future<> disconnect();

};

}
