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

#include <kafka4seastar/connection/connection_manager.hh>
#include <seastar/core/thread.hh>

#include <memory>
#include <utility>

using namespace seastar;

namespace kafka4seastar {

future<connection_manager::connection_iterator> connection_manager::connect(const seastar::sstring& host, uint16_t port, uint32_t timeout) {
    auto conn = _connections.find({host, port});
    return conn != _connections.end()
       ? make_ready_future<connection_manager::connection_iterator>(conn)
       : kafka_connection::connect(host, port, _client_id, timeout)
       .then([this, host, port] (std::unique_ptr<kafka_connection> conn) {
            return make_ready_future<connection_manager::connection_iterator>(_connections.emplace(std::make_pair<>(host, port), std::move(conn)).first);
        });
}

future<> connection_manager::init(const std::set<connection_id>& servers, uint32_t request_timeout) {
    std::vector<future<>> fs;

    fs.reserve(servers.size());

    for (auto& server : servers) {
        fs.push_back(connect(server.first, server.second, request_timeout).discard_result());
    }

    return when_all(fs.begin(), fs.end()).then([] (auto&& results) {
        int failures = 0;
        for (auto& f : results) {
            try {
                f.get();
            } catch (...) {
                ++failures;
            }
        }
        if (failures == results.size()) {
            throw connection_exception("Couldn't connect to any initial brokers");
        }
    });
}

connection_manager::connection_iterator connection_manager::get_connection(const connection_id& connection) {
    return _connections.find(connection);
}

future<> connection_manager::disconnect(const connection_id& connection) {
    auto conn = _connections.find(connection);
    if (conn != _connections.end()) {
        auto conn_ptr = std::move(conn->second);
        _connections.erase(conn);
        auto f = conn_ptr->close();
        return f.finally([conn_ptr = std::move(conn_ptr)]{});
    }
    return make_ready_future();
}

future<metadata_response> connection_manager::ask_for_metadata(metadata_request&& request) {
    auto conn_id = std::optional<connection_id>();
    return seastar::do_with(metadata_response(), [this, request = std::move(request), conn_id = std::move(conn_id)] (metadata_response& metadata) mutable {
        return seastar::repeat([this, request = std::move(request), conn_id = std::move(conn_id), &metadata] () mutable {
            auto it = !conn_id ? _connections.begin() : _connections.upper_bound(*conn_id);
            if (it == _connections.end()) {
                throw metadata_refresh_exception("No brokers responded.");
            }
            conn_id = it->first;
            return it->second->send(request).then([this, &metadata](metadata_response res) mutable {
                if (res.error_code == error::kafka_error_code::NONE) {
                    metadata = std::move(res);
                    return seastar::stop_iteration::yes;
                }
                else {
                    return seastar::stop_iteration::no;
                }
            });
        }).then([&metadata] () mutable {
            return std::move(metadata);
        });
    });

}

future<> connection_manager::disconnect_all() {
    while (_connections.begin() != _connections.end()) {
        auto it = _connections.begin();
        auto f = disconnect({it->first.first, it->first.second});
        _pending_queue = _pending_queue.then([this, f = std::move(f)] () mutable {
            return std::move(f);
        });
    }

    return _pending_queue.discard_result();
}

}
