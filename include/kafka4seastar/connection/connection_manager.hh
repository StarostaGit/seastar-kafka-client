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

#include <kafka4seastar/connection/kafka_connection.hh>
#include <kafka4seastar/protocol/metadata_response.hh>
#include <kafka4seastar/protocol/metadata_request.hh>

#include <map>

namespace kafka4seastar {

struct metadata_refresh_exception : public std::runtime_error {
public:
    explicit metadata_refresh_exception(const seastar::sstring& message) : runtime_error(message) {}
};

struct connection_exception : public std::runtime_error {
public:
    explicit connection_exception(const seastar::sstring& message) : runtime_error(message) {}
};

class connection_manager {
public:

    using connection_id = std::pair<seastar::sstring, uint16_t>;
    using connection_iterator = std::map<connection_id, std::unique_ptr<kafka_connection>>::iterator;

private:

    std::map<connection_id, std::unique_ptr<kafka_connection>> _connections;
    seastar::sstring _client_id;

    seastar::semaphore _send_semaphore;
    seastar::future<> _pending_queue;

    seastar::future<connection_iterator> connect(const seastar::sstring& host, uint16_t port, uint32_t timeout);
    seastar::future<> disconnect(const connection_id& connection);

    connection_iterator get_connection(const connection_id& connection);

    template<typename RequestType>
    seastar::future<seastar::future<typename RequestType::response_type>> perform_request(connection_iterator& conn, RequestType& request, bool with_response) {
        auto send_future = with_response
                           ? conn->second->send(std::move(request))
                           : conn->second->send_without_response(std::move(request));

        seastar::promise<> promise;
        auto f = promise.get_future();
        _pending_queue = _pending_queue.then([f = std::move(f)] () mutable {
            return std::move(f);
        });

        send_future = send_future.then([promise = std::move(promise)] (auto response) mutable {
            promise.set_value();
            return response;
        });

        return seastar::make_ready_future<decltype(send_future)>(std::move(send_future));
    }

public:

    explicit connection_manager(seastar::sstring client_id)
        : _client_id(std::move(client_id)),
        _send_semaphore(1),
        _pending_queue(seastar::make_ready_future<>()) {}

    seastar::future<> init(const std::set<connection_id>& servers, uint32_t request_timeout);

    template<typename RequestType>
    seastar::future<typename RequestType::response_type> send(RequestType&& request, const seastar::sstring& host,
            uint16_t port, uint32_t timeout, bool with_response=true) {
        // In order to preserve ordering of sends, a semaphore with
        // count = 1 is used due to its FIFO guarantees.
        //
        // It is important that connect() and send() are done
        // with semaphore, as naive implementation
        //
        // connect(host, port).then([](auto conn) { conn.send(req1); });
        // connect(host, port).then([](auto conn) { conn.send(req2); });
        //
        // could introduce reordering of requests: after both
        // connects resolve as ready futures, the continuations (sends)
        // are not guaranteed to run with any specific order.
        //
        // In order to not limit concurrency, send_future is
        // returned as future<future<response>> and "unpacked"
        // outside the semaphore - scheduling inside semaphore
        // (only 1 at the time) and waiting for result outside it.
        return with_semaphore(_send_semaphore, 1, [this, request = std::move(request), host, port, timeout, with_response] () mutable {
            auto conn = get_connection({host, port});
            if (conn != _connections.end()) {
                return perform_request<RequestType>(conn, request, with_response);
            } else {
                return connect(host, port, timeout).then([this, request = std::move(request), with_response](connection_iterator conn) mutable {
                    return perform_request<RequestType>(conn, request, with_response);
                });
            }
        }).then([] (seastar::future<typename RequestType::response_type> send_future) {
            return send_future;
        }).then([this, host, port] (typename RequestType::response_type response) {
            if (response.error_code == error::kafka_error_code::REQUEST_TIMED_OUT ||
                response.error_code == error::kafka_error_code::CORRUPT_MESSAGE ||
                response.error_code == error::kafka_error_code::NETWORK_EXCEPTION) {
                _pending_queue = _pending_queue.then([this, host, port] {
                    return disconnect({host, port});
                });
            }
            return response;
        }).handle_exception([this, host, port] (std::exception_ptr ep) {
            try {
                _pending_queue = _pending_queue.then([this, host, port] {
                    return disconnect({host, port});
                });
                std::rethrow_exception(ep);
            } catch (seastar::timed_out_error& e) {
                typename RequestType::response_type response;
                response.error_code = error::kafka_error_code::REQUEST_TIMED_OUT;
                return response;
            } catch (...) {
                typename RequestType::response_type response;
                response.error_code = error::kafka_error_code::NETWORK_EXCEPTION;
                return response;
            }
        });
    }

    seastar::future<metadata_response> ask_for_metadata(metadata_request&& request);

    seastar::future<> disconnect_all();

};

}
