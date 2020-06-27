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

#include <kafka4seastar/protocol/kafka_records.hh>

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <cstdint>
#include <smmintrin.h>

// https://bidetly.io/2017/02/08/crc-part-1
std::uint32_t crc32c(const char* first, const char* last) {
    std::uint32_t code = ~0U;

    for (;first < last;) {
        if (reinterpret_cast<std::uintptr_t>(first) % 8 == 0 && first + 8 <= last) {
            code = _mm_crc32_u64(code, *reinterpret_cast<const std::uint64_t*>(first));
            first += 8;
        }
        else if (reinterpret_cast<std::uintptr_t>(first) % 4 == 0 && first + 4 <= last) {
            code = _mm_crc32_u32(code, *reinterpret_cast<const std::uint32_t*>(first));
            first += 4;
        }
        else if (reinterpret_cast<std::uintptr_t>(first) % 2 == 0 && first + 2 <= last) {
            code = _mm_crc32_u16(code, *reinterpret_cast<const std::uint16_t*>(first));
            first += 2;
        }
        else {
            code = _mm_crc32_u8(code, *reinterpret_cast<const std::uint8_t*>(first));
            first += 1;
        }
    }

    return ~code;
}

using namespace seastar;

namespace kafka4seastar {

void kafka_record_header::serialize(std::ostream& os, int16_t api_version) const {
    kafka_varint_t header_key_length(header_key.size());
    header_key_length.serialize(os, api_version);
    os.write(header_key.data(), header_key.size());

    kafka_varint_t header_value_length(value.size());
    header_value_length.serialize(os, api_version);
    os.write(value.data(), value.size());
}

void kafka_record_header::deserialize(std::istream& is, int16_t api_version) {
    kafka_buffer_t<kafka_varint_t> header_key;
    header_key.deserialize(is, api_version);
    this->header_key.swap(*header_key);

    kafka_buffer_t<kafka_varint_t> value;
    value.deserialize(is, api_version);
    this->value.swap(*value);
}

void kafka_record::serialize(std::ostream& os, int16_t api_version) const {
    std::vector<char> record_data;
    boost::iostreams::back_insert_device<std::vector<char>> record_data_sink{record_data};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> record_data_stream{record_data_sink};

    kafka_int8_t attributes(0);
    attributes.serialize(record_data_stream, api_version);

    timestamp_delta.serialize(record_data_stream, api_version);
    offset_delta.serialize(record_data_stream, api_version);

    if (key) {
        kafka_varint_t key_length(key->size());
        key_length.serialize(record_data_stream, api_version);
        record_data_stream.write(key->data(), key->size());
    } else {
        kafka_varint_t null_indicator(-1);
        null_indicator.serialize(record_data_stream, api_version);
    }

    if (value) {
        kafka_varint_t value_length(value->size());
        value_length.serialize(record_data_stream, api_version);
        record_data_stream.write(value->data(), value->size());
    } else {
        kafka_varint_t null_indicator(-1);
        null_indicator.serialize(record_data_stream, api_version);
    }

    kafka_varint_t header_count(headers.size());
    header_count.serialize(record_data_stream, api_version);

    for (const auto& header : headers) {
        header.serialize(record_data_stream, api_version);
    }
    record_data_stream.flush();

    kafka_varint_t length(record_data.size());
    length.serialize(os, api_version);

    os.write(record_data.data(), record_data.size());
}

void kafka_record::deserialize(std::istream& is, int16_t api_version) {
    kafka_varint_t length;
    length.deserialize(is, api_version);
    if (*length < 0) {
        throw parsing_exception("Length of record is invalid");
    }

    auto expected_end_of_record = is.tellg();
    expected_end_of_record += *length;

    kafka_int8_t attributes;
    attributes.deserialize(is, api_version);

    timestamp_delta.deserialize(is, api_version);
    offset_delta.deserialize(is, api_version);

    kafka_buffer_t<kafka_varint_t> key;
    key.deserialize(is, api_version);
    this->key.emplace(std::move(*key));

    kafka_buffer_t<kafka_varint_t> value;
    value.deserialize(is, api_version);
    this->value.emplace(std::move(*value));

    kafka_array_t<kafka_record_header, kafka_varint_t> headers;
    headers.deserialize(is, api_version);
    this->headers.swap(*headers);

    if (is.tellg() != expected_end_of_record) {
        throw parsing_exception("Stream ended prematurely when reading record");
    }
}

void kafka_record_batch::serialize(std::ostream& os, int16_t api_version) const {
    if (*magic != 2) {
        // TODO: Implement parsing of versions 0, 1.
        throw parsing_exception("Unsupported version of record batch");
    }

    // Payload stores the data after CRC field.
    std::vector<char> payload;
    boost::iostreams::back_insert_device<std::vector<char>> payload_sink{payload};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> payload_stream{payload_sink};

    kafka_int16_t attributes(0);
    attributes = *attributes | static_cast<int16_t>(compression_type);
    attributes = *attributes | (static_cast<int16_t>(timestamp_type) << 3);
    if (is_transactional) {
        attributes = *attributes | 0x10;
    }
    if (is_control_batch) {
        attributes = *attributes | 0x20;
    }

    attributes.serialize(payload_stream, api_version);

    kafka_int32_t last_offset_delta(0);
    if (!records.empty()) {
        last_offset_delta = *records.back().offset_delta;
    }

    last_offset_delta.serialize(payload_stream, api_version);

    first_timestamp.serialize(payload_stream, api_version);

    int32_t max_timestamp_delta = 0;
    for (const auto& record : records) {
        max_timestamp_delta = std::max(max_timestamp_delta, *record.timestamp_delta);
    }
    kafka_int64_t max_timestamp(*first_timestamp + max_timestamp_delta);
    max_timestamp.serialize(payload_stream, api_version);

    producer_id.serialize(payload_stream, api_version);

    producer_epoch.serialize(payload_stream, api_version);

    base_sequence.serialize(payload_stream, api_version);

    std::vector<char> serialized_records;
    boost::iostreams::back_insert_device<std::vector<char>> serialized_records_sink{serialized_records};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> serialized_records_stream{serialized_records_sink};

    for (const auto& record : records) {
        record.serialize(serialized_records_stream, api_version);
    }

    serialized_records_stream.flush();

    if (compression_type != kafka_record_compression_type::NO_COMPRESSION) {
        // TODO: Add support for compression.
        throw parsing_exception("Unsupported compression type");
    }

    kafka_int32_t records_count(records.size());
    records_count.serialize(payload_stream, api_version);

    payload_stream.write(serialized_records.data(), serialized_records.size());

    payload_stream.flush();

    base_offset.serialize(os, api_version);

    kafka_int32_t batch_length(0);
    batch_length = *batch_length + payload.size();
    // fields before the CRC field.
    batch_length = *batch_length + 4 + 4 + 1;
    batch_length.serialize(os, api_version);

    partition_leader_epoch.serialize(os, api_version);

    magic.serialize(os, api_version);

    kafka_int32_t crc(crc32c(payload.data(), payload.data() + payload.size()));
    crc.serialize(os, api_version);

    os.write(payload.data(), payload.size());
}

void kafka_record_batch::deserialize(std::istream& is, int16_t api_version) {
    // Move to magic byte, read it and return back to start.
    auto start_position = is.tellg();
    is.seekg(8 + 4 + 4, std::ios_base::cur);
    magic.deserialize(is, api_version);
    is.seekg(start_position);

    if (*magic != 2) {
        // TODO: Implement parsing of versions 0, 1.
        throw parsing_exception("Unsupported record batch version");
    }

    base_offset.deserialize(is, api_version);

    kafka_int32_t batch_length;
    batch_length.deserialize(is, api_version);

    auto expected_end_of_batch = is.tellg();
    expected_end_of_batch += *batch_length;

    partition_leader_epoch.deserialize(is, api_version);

    magic.deserialize(is, api_version);

    kafka_int32_t crc;
    crc.deserialize(is, api_version);

    // TODO: Missing validation of returned CRC value.

    kafka_int16_t attributes;
    attributes.deserialize(is, api_version);

    auto compression_type = *attributes & 0x7;

    if (compression_type < 0 || compression_type > 4) {
        throw parsing_exception("Unsupported compression type");
    }
    this->compression_type = static_cast<kafka_record_compression_type>(compression_type);

    timestamp_type = (*attributes & 0x8) ?
                      kafka_record_timestamp_type::LOG_APPEND_TIME
                      : timestamp_type = kafka_record_timestamp_type::CREATE_TIME;

    is_transactional = bool(*attributes & 0x10);
    is_control_batch = bool(*attributes & 0x20);

    kafka_int32_t last_offset_delta;
    last_offset_delta.deserialize(is, api_version);

    first_timestamp.deserialize(is, api_version);

    kafka_int64_t max_timestamp;
    max_timestamp.deserialize(is, api_version);

    producer_id.deserialize(is, api_version);

    producer_epoch.deserialize(is, api_version);

    base_sequence.deserialize(is, api_version);

    kafka_int32_t records_count;
    records_count.deserialize(is, api_version);

    if (*records_count < 0) {
        throw parsing_exception("Record count in batch is invalid");
    }
    records.resize(*records_count);

    auto remaining_bytes = expected_end_of_batch - is.tellg();
    std::vector<char> records_payload(remaining_bytes);

    is.read(records_payload.data(), remaining_bytes);
    if (is.gcount() != remaining_bytes) {
        throw parsing_exception("Stream ended prematurely when reading record batch");
    }

    boost::iostreams::stream<boost::iostreams::array_source> records_stream(records_payload.data(), records_payload.size());
    for (auto& record : records) {
        record.deserialize(records_stream, api_version);
    }

    if (records_stream.tellg() != remaining_bytes) {
        throw parsing_exception("Stream ended prematurely when reading record batch");
    }
}

void kafka_records::serialize(std::ostream& os, int16_t api_version) const {
    std::vector<char> serialized_batches;
    boost::iostreams::back_insert_device<std::vector<char>> serialized_batches_sink{serialized_batches};
    boost::iostreams::stream<boost::iostreams::back_insert_device<std::vector<char>>> serialized_batches_stream{serialized_batches_sink};

    for (const auto& batch : record_batches) {
        batch.serialize(serialized_batches_stream, api_version);
    }

    serialized_batches_stream.flush();

    kafka_int32_t records_length(serialized_batches.size());
    records_length.serialize(os, api_version);

    os.write(serialized_batches.data(), serialized_batches.size());
}

void kafka_records::deserialize(std::istream& is, int16_t api_version) {
    kafka_int32_t records_length;
    records_length.deserialize(is, api_version);
    if (*records_length < 0) {
        throw parsing_exception("Records length is invalid");
    }

    auto expected_end_of_records = is.tellg();
    expected_end_of_records += *records_length;

    record_batches.clear();
    while (is.tellg() < expected_end_of_records) {
        record_batches.emplace_back();
        record_batches.back().deserialize(is, api_version);
    }

    if (is.tellg() != expected_end_of_records) {
        throw parsing_exception("Stream ended prematurely when reading records");
    }
}

}
