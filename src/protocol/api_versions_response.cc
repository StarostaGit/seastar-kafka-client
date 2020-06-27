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

#include <kafka4seastar/protocol/api_versions_response.hh>

#include <algorithm>

using namespace seastar;

namespace kafka4seastar {

void api_versions_response_key::serialize(std::ostream& os, int16_t api_version) const {
    api_key.serialize(os, api_version);
    min_version.serialize(os, api_version);
    max_version.serialize(os, api_version);
}

void api_versions_response_key::deserialize(std::istream& is, int16_t api_version) {
    api_key.deserialize(is, api_version);
    min_version.deserialize(is, api_version);
    max_version.deserialize(is, api_version);
}

bool api_versions_response_key::operator<(const api_versions_response_key& other) const noexcept {
    return *api_key < *other.api_key;
}

bool api_versions_response_key::operator<(int16_t api_key) const noexcept {
    return *this->api_key < api_key;
}

api_versions_response_key api_versions_response::operator[](int16_t api_key) const {
    auto it = std::lower_bound(api_keys->begin(), api_keys->end(), api_key);
    if (it != api_keys->end() && *it->api_key == api_key) {
        return *it;
    }
    api_versions_response_key null_response;
    null_response.api_key = -1;
    return null_response;
}

bool api_versions_response::contains(int16_t api_key) const {
    auto it = std::lower_bound(api_keys->begin(), api_keys->end(), api_key);
    return it != api_keys->end() && *it->api_key == api_key;
}

void api_versions_response::serialize(std::ostream& os, int16_t api_version) const {
    error_code.serialize(os, api_version);
    api_keys.serialize(os, api_version);
    if (api_version >= 1) {
        throttle_time_ms.serialize(os, api_version);
    }
}

void api_versions_response::deserialize(std::istream& is, int16_t api_version) {
    error_code.deserialize(is, api_version);
    api_keys.deserialize(is, api_version);
    std::sort(api_keys->begin(), api_keys->end());
    if (api_version >= 1) {
        throttle_time_ms.deserialize(is, api_version);
    }
}

}
