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

#include <seastar/core/future-util.hh>

#include <kafka4seastar/protocol/produce_request.hh>
#include <kafka4seastar/producer/sender.hh>

using namespace seastar;

namespace kafka4seastar {

sender::sender(connection_manager& connection_manager,
        metadata_manager& metadata_manager,
        uint32_t connection_timeout,
        ack_policy acks)
            : _connection_manager(connection_manager),
            _metadata_manager(metadata_manager),
            _connection_timeout(connection_timeout),
            _acks(acks) {}

std::optional<sender::connection_id> sender::broker_for_topic_partition(const seastar::sstring& topic, int32_t partition_index) {
    const auto& metadata = _metadata_manager.get_metadata();

    auto topic_candidate = std::lower_bound(metadata.topics->begin(), metadata.topics->end(), topic, [](auto& a, auto& b) {
        return *a.name < b;
    });

    if (topic_candidate != metadata.topics->end() && *topic_candidate->name == topic && topic_candidate->error_code == error::kafka_error_code::NONE) {
        auto it = std::lower_bound(topic_candidate->partitions->begin(), topic_candidate->partitions->end(), partition_index, [](auto& a, auto& b) {
            return *a.partition_index < b;
        });

        if (it != topic_candidate->partitions->end() && *it->partition_index == partition_index && it->error_code == error::kafka_error_code::NONE) {
            return broker_for_id(*it->leader_id);
        }
    }

    return std::nullopt;
}

sender::connection_id sender::broker_for_id(int32_t id) {
    const auto& metadata = _metadata_manager.get_metadata();
    auto it = std::lower_bound(metadata.brokers->begin(), metadata.brokers->end(), id, [] (auto& a, auto& b) {
        return *a.node_id < b;
    });

    if (*it->node_id == id) {
        return {*it->host, *it->port};
    }

    return {};
}

void sender::split_messages() {
    _messages_split_by_topic_partition.clear();
    _messages_split_by_broker_topic_partition.clear();

    for (auto& message : _messages) {
        auto broker = broker_for_topic_partition(message.topic, message.partition_index);
        if (broker) {
            _messages_split_by_broker_topic_partition[*broker][message.topic][message.partition_index].push_back(&message);
            _messages_split_by_topic_partition[{message.topic, message.partition_index}].push_back(&message);
        } else {
            // TODO: Differentiate between unknown topic, leader not available etc.
            message.error_code = error::kafka_error_code::UNKNOWN_TOPIC_OR_PARTITION;
        }
    }
}

void sender::queue_requests() {
    _responses.clear();
    _responses.reserve(_messages_split_by_broker_topic_partition.size());

    for (auto& [broker, messages_by_topic_partition] : _messages_split_by_broker_topic_partition) {
        produce_request req;
        req.acks = static_cast<int16_t>(_acks);
        req.timeout_ms = _connection_timeout;

        kafka_array_t<produce_request_topic_produce_data> topics{
                std::vector<produce_request_topic_produce_data>()};
        req.topics = std::move(topics);

        for (auto& [topic, messages_by_partition] : messages_by_topic_partition) {
            produce_request_topic_produce_data topic_data;
            topic_data.name = topic;

            kafka_array_t<produce_request_partition_produce_data> partitions{
                    std::vector<produce_request_partition_produce_data>()};
            topic_data.partitions = std::move(partitions);

            for (auto& [partition, messages] : messages_by_partition) {
                produce_request_partition_produce_data partition_data;
                partition_data.partition_index = partition;

                kafka_records records;
                kafka_record_batch record_batch;

                record_batch.base_offset = 0;
                record_batch.partition_leader_epoch = -1;
                record_batch.magic = 2;
                record_batch.compression_type = kafka_record_compression_type::NO_COMPRESSION;
                record_batch.timestamp_type = kafka_record_timestamp_type::CREATE_TIME;

                auto first_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(messages[0]->timestamp.time_since_epoch()).count();
                record_batch.first_timestamp = first_timestamp;
                record_batch.producer_id = -1;
                record_batch.producer_epoch = -1;
                record_batch.base_sequence = -1;
                record_batch.is_transactional = false;
                record_batch.is_control_batch = false;

                for (size_t i = 0; i < messages.size(); i++) {
                    auto current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(messages[i]->timestamp.time_since_epoch()).count();

                    kafka_record record;
                    record.timestamp_delta = current_timestamp - first_timestamp;
                    record.offset_delta = i;
                    record.key = messages[i]->key;
                    record.value = messages[i]->value;
                    record_batch.records.emplace_back(std::move(record));
                }

                records.record_batches.emplace_back(std::move(record_batch));
                partition_data.records = std::move(records);

                topic_data.partitions->emplace_back(std::move(partition_data));
            }
            req.topics->emplace_back(std::move(topic_data));
        }

        auto with_response = _acks != ack_policy::NONE;
        _responses.emplace_back(_connection_manager.send(std::move(req), broker.first, broker.second, _connection_timeout, with_response)
            .then([broker] (auto response) {
                return std::make_pair(broker, response);
        }));
    }
}

void sender::set_error_code_for_broker(const sender::connection_id& broker, const error::kafka_error_code& error_code) {
    for (auto& [topic, messages_by_partition] : _messages_split_by_broker_topic_partition[broker]) {
        for (auto& [partition, messages] : messages_by_partition) {
            for (auto& message : messages) {
                (void)topic; (void)partition;
                message->error_code = error_code;
            }
        }
    }
}

void sender::set_success_for_broker(const sender::connection_id& broker) {
    for (auto& [topic, messages_by_partition] : _messages_split_by_broker_topic_partition[broker]) {
        for (auto& [partition, messages] : messages_by_partition) {
            for (auto& message : messages) {
                (void)topic; (void)partition;
                message->error_code = error::kafka_error_code::NONE;
                message->promise.set_value();
            }
        }
    }
}

void sender::set_error_code_for_topic_partition(const seastar::sstring& topic, int32_t partition_index,
        const error::kafka_error_code& error_code) {
    for (auto& message : _messages_split_by_topic_partition[{topic, partition_index}]) {
        message->error_code = error_code;
    }
}

void sender::set_success_for_topic_partition(const seastar::sstring& topic, int32_t partition_index) {
    for (auto& message : _messages_split_by_topic_partition[{topic, partition_index}]) {
        message->error_code = error::kafka_error_code::NONE;
        message->promise.set_value();
    }
}

void sender::move_messages(std::vector<sender_message>& messages) {
    _messages.reserve(_messages.size() + messages.size());
    _messages.insert(_messages.end(), std::make_move_iterator(messages.begin()),
              std::make_move_iterator(messages.end()));
    messages.clear();
}

size_t sender::messages_size() const {
    return _messages.size();
}

bool sender::messages_empty() const {
    return _messages.empty();
}

void sender::send_requests() {
    split_messages();
    queue_requests();
}

future<> sender::receive_responses() {
    return when_all(_responses.begin(), _responses.end()).then(
            [this](std::vector<future<std::pair<connection_id, produce_response>>> responses) {
        set_error_codes_for_responses(responses);
        filter_messages();
        return process_messages_errors();
    });
}

future<> sender::process_messages_errors() {
    for (auto& message : _messages) {
        if (message.error_code->invalidates_metadata) {
            return _metadata_manager.refresh_metadata();
        }
    }

    return make_ready_future<>();
}

void sender::filter_messages() {
    _messages.erase(std::remove_if(_messages.begin(), _messages.end(), [](auto& message) {
        if (message.error_code == error::kafka_error_code::NONE) {
            return true;
        }
        if (!message.error_code->retriable) {
            message.promise.set_exception(send_exception(message.error_code->error_message));
            return true;
        }
        return false;
    }), _messages.end());
}

void sender::set_error_codes_for_responses(std::vector<future<std::pair<connection_id, produce_response>>>& responses) {
    for (auto& response : responses) {
        auto [broker, response_message] = response.get0();
        if (response_message.error_code != error::kafka_error_code::NONE) {
            set_error_code_for_broker(broker, *response_message.error_code);
            continue;
        }
        if (response_message.responses.is_null()) {
            // No detailed information (when ack_policy::NONE) so set success for the broker.
            set_success_for_broker(broker);
            continue;
        }
        for (auto& topic_response : *response_message.responses) {
            for (auto& partition_response : *topic_response.partitions) {
                if (partition_response.error_code == error::kafka_error_code::NONE) {
                    set_success_for_topic_partition(*topic_response.name, *partition_response.partition_index);
                } else {
                    set_error_code_for_topic_partition(*topic_response.name,
                                                       *partition_response.partition_index, *partition_response.error_code);
                }
            }
        }
    }
}

void sender::close() {
    for (auto& message : _messages) {
        message.promise.set_exception(send_exception(message.error_code->error_message));
    }
    _messages.clear();
}

}
