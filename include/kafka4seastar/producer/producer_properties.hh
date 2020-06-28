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

#include <vector>
#include <string>
#include <set>
#include <functional>

#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <kafka4seastar/utils/defaults.hh>
#include <kafka4seastar/utils/partitioner.hh>

namespace kafka4seastar {

enum class ack_policy {
    NONE = 0,
    LEADER = 1,
    ALL = -1,
};

struct enable_idempotence_tag {};
using enable_idempotence = seastar::bool_class<enable_idempotence_tag>;

class producer_properties final {

public:

    // Number of acknowledgments from the server to be waited for
    // before considering a request complete.
    // NONE     -> don't wait
    // LEADER   -> wait for the leader to acknowledge, no guarantee the record has been replicated
    // ALL      -> wait for all in-sync replicas to acknowledge receiving the record
    ack_policy acks = ack_policy::LEADER;

    // Enabling this ensures that exactly one copy of each message will be written to the stream.
    // CURRENTLY NOT IMPLEMENTED
    enable_idempotence idempotance_enabled = enable_idempotence::no;

    // number of ms to wait before sending a request, this allows to wait for potential
    // batches to form even when there is no load
    uint16_t linger = 0;
    // max bytes stored in one batch
    uint32_t buffer_memory = 32 * 1024 * 1024;
    // maximum number of retries to be performed before considering the request as failed
    uint32_t retries = 10;
    // max number of requests in one batch
    uint32_t batch_size = 16384;
    // number of ms after which the connection attempt is considered to have timed out
    uint32_t request_timeout = 500;
    // max time in ms after which a new metadata refresh will be sent, even if no changes have been noticed
    uint32_t metadata_refresh = 300000;

    // Identifier of the created producer instance
    seastar::sstring client_id {};
    // a list of host-port pairs to use for establishing the initial connection to the cluster
    std::set<std::pair<seastar::sstring, uint16_t>> servers {};

    // Strategy according to which we should choose the target partition,
    // based on the given key (or lack thereof)
    std::unique_ptr<partitioner> partitioning_strategy = defaults::round_robin_partitioner();
    // Strategy describing how long to wait between consecutive retries,
    // based on how many have already been performed
    seastar::noncopyable_function<seastar::future<>(uint32_t)> retry_backoff_strategy = defaults::exp_retry_backoff(20, 1000);

};

}
