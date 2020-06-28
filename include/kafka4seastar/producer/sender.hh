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
#include <vector>
#include <map>
#include <chrono>

#include <seastar/core/future.hh>

#include <kafka4seastar/producer/producer_properties.hh>
#include <kafka4seastar/protocol/metadata_response.hh>
#include <kafka4seastar/protocol/produce_response.hh>
#include <kafka4seastar/connection/connection_manager.hh>
#include <kafka4seastar/utils/metadata_manager.hh>

namespace kafka4seastar {

struct send_exception : public std::runtime_error {
public:
    explicit send_exception(const seastar::sstring& message) : runtime_error(message) {}
};

struct sender_message {
    std::optional<seastar::sstring> key;
    std::optional<seastar::sstring> value;

    std::chrono::time_point<std::chrono::system_clock> timestamp;

    seastar::sstring topic;
    int32_t partition_index;

    kafka_error_code_t error_code;
    seastar::promise<> promise;

    sender_message() :
        timestamp(std::chrono::system_clock::now()),
        partition_index(0),
        error_code(error::kafka_error_code::UNKNOWN_SERVER_ERROR) {}
    sender_message(sender_message&& s) = default;
    sender_message& operator=(sender_message&& s) = default;
    sender_message(sender_message& s) = delete;

    size_t size() const noexcept {
        auto key_size = key ? key->size() : 0;
        auto value_size = value ? value->size() : 0;
        return key_size + value_size;
    }
};

class sender {
public:
    using connection_id = std::pair<seastar::sstring, uint16_t>;
    using topic_partition = std::pair<seastar::sstring, int32_t>;

private:
    connection_manager& _connection_manager;
    metadata_manager& _metadata_manager;
    std::vector<sender_message> _messages;

    std::map<connection_id, std::map<seastar::sstring, std::map<int32_t, std::vector<sender_message*>>>> _messages_split_by_broker_topic_partition;
    std::map<topic_partition, std::vector<sender_message*>> _messages_split_by_topic_partition;
    std::vector<seastar::future<std::pair<connection_id, produce_response>>> _responses;

    uint32_t _connection_timeout;

    ack_policy _acks;

    std::optional<connection_id> broker_for_topic_partition(const seastar::sstring& topic, int32_t partition_index);
    connection_id broker_for_id(int32_t id);

    void set_error_code_for_broker(const connection_id& broker, const error::kafka_error_code& error_code);
    void set_success_for_broker(const connection_id& broker);
    void set_error_code_for_topic_partition(const seastar::sstring& topic, int32_t partition_index,
            const error::kafka_error_code& error_code);
    void set_success_for_topic_partition(const seastar::sstring& topic, int32_t partition_index);

    void split_messages();
    void queue_requests();

    void set_error_codes_for_responses(std::vector<seastar::future<std::pair<connection_id, produce_response>>>& responses);
    void filter_messages();
    seastar::future<> process_messages_errors();
    
public:
    sender(connection_manager& connection_manager, metadata_manager& metadata_manager,
            uint32_t connection_timeout, ack_policy acks);

    void move_messages(std::vector<sender_message>& messages);
    size_t messages_size() const;
    bool messages_empty() const;

    void send_requests();
    seastar::future<> receive_responses();
    void close();
};

}
