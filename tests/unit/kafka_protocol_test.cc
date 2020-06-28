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
 * Copyright (C) 2019 ScyllaDB
 */

#define BOOST_TEST_MODULE kafka

#include <cstdint>

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/test/included/unit_test.hpp>

#include <kafka4seastar/protocol/metadata_request.hh>
#include <kafka4seastar/protocol/metadata_response.hh>
#include <kafka4seastar/protocol/kafka_primitives.hh>
#include <kafka4seastar/protocol/api_versions_response.hh>
#include <kafka4seastar/protocol/kafka_records.hh>
#include <kafka4seastar/protocol/produce_request.hh>
#include <kafka4seastar/protocol/produce_response.hh>
#include <kafka4seastar/protocol/headers.hh>
#include <kafka4seastar/protocol/kafka_error_code.hh>

using namespace seastar;
namespace k4s = kafka4seastar;

template <typename KafkaType>
void test_deserialize_serialize(std::vector<unsigned char> data,
                                KafkaType &kafka_value, int16_t api_version) {
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(reinterpret_cast<char *>(data.data()),
                                                                          data.size());

    kafka_value.deserialize(input_stream, api_version);

    std::vector<unsigned char> output(data.size());
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(reinterpret_cast<char *>(output.data()),
                                                                         output.size());
    kafka_value.serialize(output_stream, api_version);

    BOOST_REQUIRE(!output_stream.bad());

    BOOST_REQUIRE_EQUAL(output_stream.tellp(), output.size());

    BOOST_TEST(output == data, boost::test_tools::per_element());
}

template <typename KafkaType>
void test_deserialize_throw(std::vector<unsigned char> data,
                            KafkaType &kafka_value, int16_t api_version) {
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(reinterpret_cast<char *>(data.data()),
                                                                          data.size());

    BOOST_REQUIRE_THROW(kafka_value.deserialize(input_stream, api_version), k4s::parsing_exception);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_number_test) {
    k4s::kafka_number_t<uint32_t> number(15);
    BOOST_REQUIRE_EQUAL(*number, 15);

    test_deserialize_serialize({0x12, 0x34, 0x56, 0x78}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 0x12345678);

    test_deserialize_throw({0x17, 0x27}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 0x12345678);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_varint_test) {
    k4s::kafka_varint_t number(155);

    test_deserialize_serialize({0x00}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 0);

    test_deserialize_serialize({0x08}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 4);

    test_deserialize_serialize({0x07}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -4);

    test_deserialize_serialize({0xAC, 0x02}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 150);

    test_deserialize_serialize({0xAB, 0x02}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -150);

    test_deserialize_throw({0xAC}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -150);

    test_deserialize_serialize({0xFF, 0xFF, 0xFF, 0xFF, 0xF}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -2147483648);

    test_deserialize_throw({0xFF, 0xFF, 0xFF, 0xFF, 0x1F}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -2147483648);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_string_test) {
    k4s::kafka_string_t string("321");
    BOOST_REQUIRE_EQUAL(*string, "321");

    test_deserialize_serialize({0, 5, 'a', 'b', 'c', 'd', 'e'}, string, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    test_deserialize_throw({0, 4, 'a', 'b', 'c'}, string, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    test_deserialize_throw({0}, string, 0);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_nullable_string_test) {
    k4s::kafka_nullable_string_t string;
    BOOST_REQUIRE(string.is_null());
    BOOST_REQUIRE_THROW((void) *string, std::exception);

    test_deserialize_serialize({0, 5, 'a', 'b', 'c', 'd', 'e'}, string, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    test_deserialize_serialize({0xFF, 0xFF}, string, 0);
    BOOST_REQUIRE(string.is_null());
}

BOOST_AUTO_TEST_CASE(kafka_primitives_bytes_test) {
    k4s::kafka_bytes_t bytes;

    test_deserialize_serialize({0, 0, 0, 5, 'a', 'b', 'c', 'd', 'e'}, bytes, 0);
    BOOST_REQUIRE_EQUAL(*bytes, "abcde");
    BOOST_REQUIRE_EQUAL(bytes->size(), 5);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_array_test) {
    k4s::kafka_array_t<k4s::kafka_string_t> strings;

    test_deserialize_serialize({0, 0, 0, 2, 0, 5, 'a', 'b', 'c', 'd', 'e', 0, 2, 'f', 'g'}, strings, 0);

    BOOST_REQUIRE_EQUAL(strings->size(), 2);
    BOOST_REQUIRE_EQUAL(*strings[0], "abcde");
    BOOST_REQUIRE_EQUAL(*strings[1], "fg");

    test_deserialize_throw({0, 0, 0, 2, 0, 5, 'A', 'B', 'C', 'D', 'E', 0, 2, 'F'}, strings, 0);
    BOOST_REQUIRE_EQUAL(strings->size(), 2);
    BOOST_REQUIRE_EQUAL(*strings[0], "abcde");
    BOOST_REQUIRE_EQUAL(*strings[1], "fg");
}


BOOST_AUTO_TEST_CASE(kafka_request_header_parsing_test) {
    k4s::request_header header;
    test_deserialize_serialize({
                                       0x00, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x42, 0x00, 0x05, 0x61, 0x62, 0x63, 0x64, 0x65
                               }, header, 0);

    BOOST_REQUIRE_EQUAL(*header.api_key, 5);
    BOOST_REQUIRE_EQUAL(*header.api_version, 1);
    BOOST_REQUIRE_EQUAL(*header.correlation_id, 0x42);
    BOOST_REQUIRE_EQUAL(*header.client_id, "abcde");
}

BOOST_AUTO_TEST_CASE(kafka_response_header_parsing_test) {
    k4s::response_header header;
    test_deserialize_serialize({
                                       0x00, 0x05, 0x00, 0x01
                               }, header, 0);

    BOOST_REQUIRE_EQUAL(*header.correlation_id, 0x50001);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_error_code_test) {
    const k4s::error::kafka_error_code &error =
            k4s::error::kafka_error_code::INVALID_FETCH_SIZE;
    k4s::kafka_error_code_t error_code(error);
    BOOST_REQUIRE_EQUAL((*error_code).error_code, 4);
    BOOST_REQUIRE_EQUAL(error_code == error, true);
    BOOST_REQUIRE_EQUAL(error_code != error, false);
    test_deserialize_serialize({0x00, 0x04}, error_code, 0);
    BOOST_REQUIRE_EQUAL((*error_code).error_code, 4);
    test_deserialize_throw({0xAC, 0x02}, error_code, 0);
}

BOOST_AUTO_TEST_CASE(kafka_api_versions_response_parsing_test) {
    k4s::api_versions_response response;
    test_deserialize_serialize({
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x00,
                                       0x00, 0x0b, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08, 0x00, 0x04,
                                       0x00, 0x00, 0x00, 0x02, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x05,
                                       0x00, 0x07, 0x00, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00, 0x00, 0x00, 0x07, 0x00, 0x09, 0x00, 0x00,
                                       0x00, 0x05, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x00, 0x0c,
                                       0x00, 0x00, 0x00, 0x03, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03,
                                       0x00, 0x0f, 0x00, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x11, 0x00, 0x00,
                                       0x00, 0x01, 0x00, 0x12, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x00, 0x00, 0x00, 0x03, 0x00, 0x14,
                                       0x00, 0x00, 0x00, 0x03, 0x00, 0x15, 0x00, 0x00, 0x00, 0x01, 0x00, 0x16, 0x00, 0x00, 0x00, 0x01,
                                       0x00, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00, 0x18, 0x00, 0x00, 0x00, 0x01, 0x00, 0x19, 0x00, 0x00,
                                       0x00, 0x01, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c,
                                       0x00, 0x00, 0x00, 0x02, 0x00, 0x1d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x01,
                                       0x00, 0x1f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x21, 0x00, 0x00,
                                       0x00, 0x01, 0x00, 0x22, 0x00, 0x00, 0x00, 0x01, 0x00, 0x23, 0x00, 0x00, 0x00, 0x01, 0x00, 0x24,
                                       0x00, 0x00, 0x00, 0x01, 0x00, 0x25, 0x00, 0x00, 0x00, 0x01, 0x00, 0x26, 0x00, 0x00, 0x00, 0x01,
                                       0x00, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x28, 0x00, 0x00, 0x00, 0x01, 0x00, 0x29, 0x00, 0x00,
                                       0x00, 0x01, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2c,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                               }, response, 2);

    BOOST_REQUIRE_EQUAL(*response.throttle_time_ms, 0);
    BOOST_REQUIRE(
            response.error_code ==
            k4s::error::kafka_error_code::NONE
    );
    BOOST_REQUIRE_EQUAL(response.api_keys->size(), 45);
    BOOST_REQUIRE_EQUAL(*response.api_keys[0].api_key, 0);
    BOOST_REQUIRE_EQUAL(*response.api_keys[0].min_version, 0);
    BOOST_REQUIRE_EQUAL(*response.api_keys[0].max_version, 7);
    BOOST_REQUIRE_EQUAL(*response.api_keys[1].api_key, 1);
    BOOST_REQUIRE_EQUAL(*response.api_keys[1].min_version, 0);
    BOOST_REQUIRE_EQUAL(*response.api_keys[1].max_version, 11);
}

BOOST_AUTO_TEST_CASE(kafka_metadata_request_parsing_test) {
    k4s::metadata_request request;
    test_deserialize_serialize({
                                       0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x01, 0x00, 0x00
                               }, request, 8);

    BOOST_REQUIRE_EQUAL(request.topics->size(), 1);
    BOOST_REQUIRE_EQUAL(*request.topics[0].name, "test5");
    BOOST_REQUIRE(*request.allow_auto_topic_creation);
    BOOST_REQUIRE(!*request.include_cluster_authorized_operations);
    BOOST_REQUIRE(!*request.include_topic_authorized_operations);
}

BOOST_AUTO_TEST_CASE(kafka_metadata_response_parsing_test) {
    k4s::metadata_response response;
    test_deserialize_serialize({
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x0a, 0x31, 0x37,
                                       0x32, 0x2e, 0x31, 0x33, 0x2e, 0x30, 0x2e, 0x31, 0x00, 0x00, 0x23, 0x84, 0xff, 0xff, 0x00, 0x16,
                                       0x6b, 0x4c, 0x5a, 0x35, 0x6a, 0x50, 0x76, 0x44, 0x52, 0x30, 0x43, 0x77, 0x31, 0x79, 0x34, 0x31,
                                       0x41, 0x66, 0x35, 0x48, 0x55, 0x67, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                                       0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                                       0x03, 0xe9, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                               }, response, 8);

    BOOST_REQUIRE_EQUAL(*response.throttle_time_ms, 0);
    BOOST_REQUIRE_EQUAL(response.brokers->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.brokers[0].node_id, 0x3e9);
    BOOST_REQUIRE_EQUAL(*response.brokers[0].host, "172.13.0.1");
    BOOST_REQUIRE_EQUAL(*response.brokers[0].port, 0x2384);
    BOOST_REQUIRE(response.brokers[0].rack.is_null());
    BOOST_REQUIRE_EQUAL(*response.cluster_id, "kLZ5jPvDR0Cw1y41Af5HUg");
    BOOST_REQUIRE_EQUAL(*response.controller_id, 0x3e9);
    BOOST_REQUIRE_EQUAL(response.topics->size(), 1);
    BOOST_REQUIRE(
            response.topics[0].error_code ==
            k4s::error::kafka_error_code::NONE
    );
    BOOST_REQUIRE_EQUAL(*response.topics[0].name, "test5");
    BOOST_REQUIRE(!*response.topics[0].is_internal);
    BOOST_REQUIRE_EQUAL(response.topics[0].partitions->size(), 1);
    BOOST_REQUIRE(
            response.topics[0].partitions[0].error_code ==
            k4s::error::kafka_error_code::NONE
    );
    BOOST_REQUIRE_EQUAL(*response.topics[0].partitions[0].partition_index, 0);
    BOOST_REQUIRE_EQUAL(*response.topics[0].partitions[0].leader_id, 0x3e9);
    BOOST_REQUIRE_EQUAL(*response.topics[0].partitions[0].leader_epoch, 0);
    BOOST_REQUIRE_EQUAL(response.topics[0].partitions[0].replica_nodes->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.topics[0].partitions[0].replica_nodes[0], 0x3e9);
    BOOST_REQUIRE_EQUAL(response.topics[0].partitions[0].isr_nodes->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.topics[0].partitions[0].isr_nodes[0], 0x3e9);
    BOOST_REQUIRE_EQUAL(*response.topics[0].topic_authorized_operations, 0);
    BOOST_REQUIRE_EQUAL(*response.cluster_authorized_operations, 0);
}

BOOST_AUTO_TEST_CASE(kafka_record_parsing_test) {
    k4s::kafka_record record;
    test_deserialize_serialize({
                                       0x20, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00
                               }, record, 0);
    BOOST_REQUIRE_EQUAL(*record.timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*record.offset_delta, 0);
    std::string expected_key{"\x00\x00\x00\x01", 4};
    BOOST_REQUIRE_EQUAL(*record.key, expected_key);
    std::string expected_value{"\x00\x00\x00\x00\x00\x00", 6};
    BOOST_REQUIRE_EQUAL(*record.value, expected_value);
    BOOST_REQUIRE_EQUAL(record.headers.size(), 0);

    k4s::kafka_record record2;
    test_deserialize_serialize({
                                       0x10, 0x00, 0x00, 0x00, 0x02, 0x34, 0x02, 0x36, 0x00
                               }, record2, 0);
    BOOST_REQUIRE_EQUAL(*record2.timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*record2.offset_delta, 0);
    BOOST_REQUIRE_EQUAL(*record2.key, "4");
    BOOST_REQUIRE_EQUAL(*record2.value, "6");
    BOOST_REQUIRE_EQUAL(record2.headers.size(), 0);
}

BOOST_AUTO_TEST_CASE(kafka_record_batch_parsing_test) {
    k4s::kafka_record_batch batch;
    test_deserialize_serialize({
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00,
                                       0x02, 0x6f, 0x51, 0x95, 0x17, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3,
                                       0x2b, 0x03, 0x41, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x03, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00,
                                       0x00, 0x02, 0x34, 0x02, 0x34, 0x00
                               }, batch, 0);

    BOOST_REQUIRE_EQUAL(*batch.base_offset, 4);
    BOOST_REQUIRE_EQUAL(*batch.partition_leader_epoch, 0);
    BOOST_REQUIRE_EQUAL(*batch.magic, 2);
    BOOST_REQUIRE(batch.compression_type == k4s::kafka_record_compression_type::NO_COMPRESSION);
    BOOST_REQUIRE(batch.timestamp_type == k4s::kafka_record_timestamp_type::CREATE_TIME);
    BOOST_REQUIRE(batch.is_transactional);
    BOOST_REQUIRE(!batch.is_control_batch);
    BOOST_REQUIRE_EQUAL(*batch.first_timestamp, 0x16eb32b0341);
    BOOST_REQUIRE_EQUAL(*batch.producer_id, 0);
    BOOST_REQUIRE_EQUAL(*batch.producer_epoch, 0);
    BOOST_REQUIRE_EQUAL(*batch.base_sequence, 3);
    BOOST_REQUIRE_EQUAL(batch.records.size(), 1);
    BOOST_REQUIRE_EQUAL(*batch.records[0].timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*batch.records[0].offset_delta, 0);
    BOOST_REQUIRE_EQUAL(*batch.records[0].key, "4");
    BOOST_REQUIRE_EQUAL(*batch.records[0].value, "4");
    BOOST_REQUIRE_EQUAL(batch.records[0].headers.size(), 0);
}

BOOST_AUTO_TEST_CASE(kafka_records_parsing_test) {
    k4s::kafka_records records;
    test_deserialize_serialize({
                                       0x00, 0x00, 0x02, 0x8e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3a,
                                       0x00, 0x00, 0x00, 0x00, 0x02, 0xc6, 0x4c, 0x35, 0x56, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x01, 0x4b, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x01, 0x4b, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x01, 0x10, 0x00, 0x00, 0x00, 0x02, 0x31, 0x02, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x01, 0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00, 0x02, 0x90, 0xe7, 0x99, 0x55, 0x00,
                                       0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x02, 0x0a, 0x00, 0x00, 0x01,
                                       0x6e, 0xb3, 0x2b, 0x02, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00, 0x02, 0x32, 0x02, 0x32, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00, 0x00,
                                       0x02, 0xb2, 0x80, 0xcd, 0x9a, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3,
                                       0x2b, 0x02, 0xbb, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x02, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x20, 0x00, 0x00,
                                       0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00, 0x02, 0x25,
                                       0x73, 0x58, 0xe3, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x02,
                                       0xf0, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x02, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00, 0x02,
                                       0x33, 0x02, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x3a,
                                       0x00, 0x00, 0x00, 0x00, 0x02, 0x6f, 0x51, 0x95, 0x17, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x03, 0x41, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x03, 0x41, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
                                       0x01, 0x10, 0x00, 0x00, 0x00, 0x02, 0x34, 0x02, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x05, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00, 0x00, 0x02, 0xfb, 0x4e, 0xb4, 0x07, 0x00,
                                       0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x03, 0x96, 0x00, 0x00, 0x01,
                                       0x6e, 0xb3, 0x2b, 0x03, 0x96, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
                                       0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x20, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
                                       0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
                                       0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00, 0x02, 0xa0, 0xdd, 0x37, 0x0b, 0x00, 0x10, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x03, 0xf5, 0x00, 0x00, 0x01, 0x6e, 0xb3,
                                       0x2b, 0x03, 0xf5, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x04, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00, 0x02, 0x35, 0x02, 0x35, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00, 0x02, 0x8b,
                                       0x5e, 0xf1, 0x92, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x04,
                                       0x28, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x04, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00, 0x02,
                                       0x36, 0x02, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x42,
                                       0x00, 0x00, 0x00, 0x00, 0x02, 0xa3, 0x73, 0x3e, 0xe0, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x04, 0x6a, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x04, 0x6a, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
                                       0x01, 0x20, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00, 0x00
                               }, records, 0);

    BOOST_REQUIRE_EQUAL(records.record_batches.size(), 9);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].base_offset, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].partition_leader_epoch, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].magic, 2);
    BOOST_REQUIRE(records.record_batches[0].compression_type == k4s::kafka_record_compression_type::NO_COMPRESSION);
    BOOST_REQUIRE(records.record_batches[0].timestamp_type == k4s::kafka_record_timestamp_type::CREATE_TIME);
    BOOST_REQUIRE(records.record_batches[0].is_transactional);
    BOOST_REQUIRE(!records.record_batches[0].is_control_batch);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].first_timestamp, 0x16eb32b014b);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].producer_id, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].producer_epoch, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].base_sequence, 0);
    BOOST_REQUIRE_EQUAL(records.record_batches[0].records.size(), 1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].offset_delta, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].key, "1");
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].value, "1");
    BOOST_REQUIRE_EQUAL(records.record_batches[0].records[0].headers.size(), 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].base_offset, 2);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].partition_leader_epoch, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].magic, 2);
    BOOST_REQUIRE(records.record_batches[2].compression_type == k4s::kafka_record_compression_type::NO_COMPRESSION);
    BOOST_REQUIRE(records.record_batches[2].timestamp_type == k4s::kafka_record_timestamp_type::CREATE_TIME);
    BOOST_REQUIRE(records.record_batches[2].is_transactional);
    BOOST_REQUIRE(records.record_batches[2].is_control_batch);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].first_timestamp, 0x16eb32b02bb);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].producer_id, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].producer_epoch, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].base_sequence, -1);
    BOOST_REQUIRE_EQUAL(records.record_batches[2].records.size(), 1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].records[0].timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].records[0].offset_delta, 0);
    std::string expected_key{"\x00\x00\x00\x01", 4};
    std::string expected_value{"\x00\x00\x00\x00\x00\x00", 6};
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].records[0].key, expected_key);
    BOOST_REQUIRE_EQUAL(*records.record_batches[2].records[0].value, expected_value);
    BOOST_REQUIRE_EQUAL(records.record_batches[2].records[0].headers.size(), 0);
}

BOOST_AUTO_TEST_CASE(kafka_produce_request_parsing_test) {
    k4s::produce_request request;
    test_deserialize_serialize({
                                       0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x75, 0x30, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x65,
                                       0x73, 0x74, 0x35, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3a, 0xff, 0xff, 0xff, 0xff, 0x02,
                                       0x06, 0x76, 0x5e, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0x5b, 0x6e,
                                       0xba, 0x2c, 0x00, 0x00, 0x01, 0x6e, 0x5b, 0x6e, 0xba, 0x2c, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                                       0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00,
                                       0x02, 0x30, 0x02, 0x30, 0x00
                               }, request, 7);

    BOOST_REQUIRE(request.transactional_id.is_null());
    BOOST_REQUIRE_EQUAL(*request.acks, -1);
    BOOST_REQUIRE_EQUAL(*request.timeout_ms, 30000);
    BOOST_REQUIRE_EQUAL(request.topics->size(), 1);
    BOOST_REQUIRE_EQUAL(*request.topics[0].name, "test5");
    BOOST_REQUIRE_EQUAL(request.topics[0].partitions->size(), 1);
    BOOST_REQUIRE_EQUAL(*request.topics[0].partitions[0].partition_index, 0);
    const auto &records = request.topics[0].partitions[0].records;
    BOOST_REQUIRE_EQUAL(records.record_batches.size(), 1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].base_offset, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].partition_leader_epoch, -1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].magic, 2);
    BOOST_REQUIRE(records.record_batches[0].compression_type == k4s::kafka_record_compression_type::NO_COMPRESSION);
    BOOST_REQUIRE(records.record_batches[0].timestamp_type == k4s::kafka_record_timestamp_type::CREATE_TIME);
    BOOST_REQUIRE(!records.record_batches[0].is_transactional);
    BOOST_REQUIRE(!records.record_batches[0].is_control_batch);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].first_timestamp, 0x16e5b6eba2c);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].producer_id, -1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].producer_epoch, -1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].base_sequence, -1);
    BOOST_REQUIRE_EQUAL(records.record_batches[0].records.size(), 1);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].offset_delta, 0);
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].key, "0");
    BOOST_REQUIRE_EQUAL(*records.record_batches[0].records[0].value, "0");
    BOOST_REQUIRE_EQUAL(records.record_batches[0].records[0].headers.size(), 0);
}

BOOST_AUTO_TEST_CASE(kafka_produce_response_parsing_test) {
    k4s::produce_response response;
    test_deserialize_serialize({
                                       0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x00, 0x00, 0x00, 0x01, 0x00,
                                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46, 0xff, 0xff, 0xff,
                                       0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                       0x00
                               }, response, 7);

    BOOST_REQUIRE_EQUAL(response.responses->size(), 1);
    BOOST_REQUIRE_EQUAL(*response.throttle_time_ms, 0);

    const auto &inner_response = response.responses[0];
    BOOST_REQUIRE_EQUAL(*inner_response.name, "test5");
    BOOST_REQUIRE_EQUAL(inner_response.partitions->size(), 1);

    const auto &partition = inner_response.partitions[0];
    BOOST_REQUIRE_EQUAL(*partition.partition_index, 0);
    BOOST_REQUIRE(
            partition.error_code
            == k4s::error::kafka_error_code::NONE
    );
    BOOST_REQUIRE_EQUAL(*partition.base_offset, 0x46);
    BOOST_REQUIRE_EQUAL(*partition.log_append_time_ms, -1);
    BOOST_REQUIRE_EQUAL(*partition.log_start_offset, 0);
}