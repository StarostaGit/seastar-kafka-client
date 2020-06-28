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

#include <seastar/core/future.hh>
#include <seastar/net/api.hh>
#include <seastar/net/net.hh>
#include <seastar/net/inet_address.hh>
#include <string>

namespace kafka4seastar {

struct tcp_connection_exception final : public std::runtime_error {
    explicit tcp_connection_exception(const seastar::sstring& message) : runtime_error(message) {}
};

class tcp_connection final {

    seastar::net::inet_address _host;
    uint16_t _port;
    uint32_t _timeout_ms;
    seastar::connected_socket _fd;
    seastar::input_stream<char> _read_buf;
    seastar::output_stream<char> _write_buf;

public:
    static seastar::future<tcp_connection> connect(const seastar::sstring& host, uint16_t port, uint32_t timeout_ms);

    tcp_connection(const seastar::net::inet_address& host, uint16_t port, uint32_t timeout_ms, seastar::connected_socket&& fd) noexcept
            : _host(host)
            , _port(port)
            , _timeout_ms(timeout_ms)
            , _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output()) {
        _fd.set_nodelay(true);
    };

    tcp_connection(tcp_connection&& other) = default;
    tcp_connection(tcp_connection& other) = delete;

    seastar::future<> write(seastar::temporary_buffer<char> buff);
    seastar::future<seastar::temporary_buffer<char>> read(size_t bytes_to_read);
    seastar::future<> close();

};

}
