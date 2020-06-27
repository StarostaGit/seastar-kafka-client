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

#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <vector>

#include <kafka4seastar/utils/retry_helper.hh>
#include <kafka4seastar/utils/defaults.hh>

using namespace seastar;
namespace k4s = kafka4seastar;

SEASTAR_THREAD_TEST_CASE(kafka_retry_helper_test_early_stop) {
    k4s::retry_helper helper(5, k4s::defaults::exp_retry_backoff(1, 1000));
    auto retry_count = 0;
    auto data = 0;
    helper.with_retry([&retry_count, data]() mutable {
        retry_count++;
        data++;
        if (data >= 3) {
            return k4s::do_retry::no;
        }
        return k4s::do_retry::yes;
    }).wait();
    BOOST_REQUIRE_EQUAL(retry_count, 3);
}

SEASTAR_THREAD_TEST_CASE(kafka_retry_helper_test_capped_retries) {
    k4s::retry_helper helper(5, k4s::defaults::exp_retry_backoff(1, 1000));
    auto retry_count = 0;
    helper.with_retry([&retry_count] {
        retry_count++;
        return k4s::do_retry::yes;
    }).wait();
    BOOST_REQUIRE_EQUAL(retry_count, 5);
}

SEASTAR_THREAD_TEST_CASE(kafka_retry_helper_test_future) {
    k4s::retry_helper helper(5, k4s::defaults::exp_retry_backoff(1, 1000));
    auto retry_count = 0;
    helper.with_retry([&retry_count] {
        retry_count++;
        return make_ready_future<k4s::do_retry>(k4s::do_retry::yes);
    }).wait();
    BOOST_REQUIRE_EQUAL(retry_count, 5);
}

SEASTAR_THREAD_TEST_CASE(kafka_retry_helper_test_modify_data) {
    k4s::retry_helper helper(5, k4s::defaults::exp_retry_backoff(1, 1000));
    auto retry_count = 0;
    std::vector<int> data{1, 2, 3};
    std::vector<int> retry_data;

    helper.with_retry([data = std::move(data), &retry_count, &retry_data]() mutable {
        if (data.empty()) {
            return k4s::do_retry::no;
        }
        retry_data.push_back(data.back());
        data.pop_back();
        retry_count++;
        return k4s::do_retry::yes;
    }).wait();

    BOOST_REQUIRE_EQUAL(retry_count, 3);
    std::vector<int> expected_data{3, 2, 1};
    BOOST_TEST(retry_data == expected_data, boost::test_tools::per_element());
}
