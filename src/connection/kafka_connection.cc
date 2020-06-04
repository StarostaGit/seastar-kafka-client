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

#include <kafka4seastar/connection/kafka_connection.hh>

using namespace seastar;

namespace kafka4seastar {

future<std::unique_ptr<kafka_connection>> kafka_connection::connect(const seastar::sstring& host, uint16_t port,
        const seastar::sstring& client_id, uint32_t timeout_ms) {
    return tcp_connection::connect(host, port, timeout_ms)
    .then([client_id] (tcp_connection connection) {
        return std::make_unique<kafka_connection>(std::move(connection), client_id);
    }).then([] (std::unique_ptr<kafka_connection> connection) {
        auto f = connection->init();
        return f.then([connection = std::move(connection)] () mutable {
            return std::move(connection);
        });
    });
}

future<> kafka_connection::init() {
    api_versions_request request;
    return send(request, api_versions_request::MAX_SUPPORTED_VERSION)
            .then([this](api_versions_response response) {
                _api_versions = response;
            });
}

future<> kafka_connection::close() {
    return _connection.close();
}

}
