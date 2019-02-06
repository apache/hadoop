/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * The following features are not currently implemented
 * - Deprecated values
 * - Make filename and config file contents unicode-safe
 * - Config redirection/environment substitution
 *
 * - getInts (comma separated))
 * - getStrings (comma separated))
 * - getIntegerRange
 * - getSocketAddr
 * - getTimeDuration
 * - getBytes (e.g. 1M or 1G)
 * - hex values
 */

#include "configuration.h"
#include "hdfspp/uri.h"

#include <strings.h>
#include <sstream>
#include <map>
#include <rapidxml/rapidxml.hpp>
#include <rapidxml/rapidxml_utils.hpp>

namespace hdfs {

/*
 * Configuration class
 */
std::vector<std::string> Configuration::GetDefaultFilenames() {
  auto result = std::vector<std::string>();
  result.push_back("core-site.xml");
  return result;
}


optional<std::string> Configuration::Get(const std::string& key) const {
  std::string caseFixedKey = fixCase(key);
  auto found = raw_values_.find(caseFixedKey);
  if (found != raw_values_.end()) {
    return std::experimental::make_optional(found->second.value);
  } else {
    return optional<std::string>();
  }
}

std::string Configuration::GetWithDefault(
    const std::string& key, const std::string& default_value) const {
  return Get(key).value_or(default_value);
}

optional<int64_t> Configuration::GetInt(const std::string& key) const {
  auto raw = Get(key);
  if (raw) {
    errno = 0;
    char* end = nullptr;
    optional<int64_t> result =
        std::experimental::make_optional(static_cast<int64_t>(strtol(raw->c_str(), &end, 10)));
    if (end == raw->c_str()) {
      /* strtoll will set end to input if no conversion was done */
      return optional<int64_t>();
    }
    if (errno == ERANGE) {
      return optional<int64_t>();
    }

    return result;
  } else {
    return optional<int64_t>();
  }
}

int64_t Configuration::GetIntWithDefault(const std::string& key,
                                         int64_t default_value) const {
  return GetInt(key).value_or(default_value);
}

optional<double> Configuration::GetDouble(const std::string& key) const {
  auto raw = Get(key);
  if (raw) {
    errno = 0;
    char* end = nullptr;
    auto result = std::experimental::make_optional(strtod(raw->c_str(), &end));
    if (end == raw->c_str()) {
      /* strtod will set end to input if no conversion was done */
      return optional<double>();
    }
    if (errno == ERANGE) {
      return optional<double>();
    }

    return result;
  } else {
    return optional<double>();
  }
}

double Configuration::GetDoubleWithDefault(const std::string& key,
                                           double default_value) const {
  return GetDouble(key).value_or(default_value);
}

optional<bool> Configuration::GetBool(const std::string& key) const {
  auto raw = Get(key);
  if (!raw) {
    return optional<bool>();
  }

  if (!strcasecmp(raw->c_str(), "true")) {
    return std::experimental::make_optional(true);
  }
  if (!strcasecmp(raw->c_str(), "false")) {
    return std::experimental::make_optional(false);
  }

  return optional<bool>();
}

bool Configuration::GetBoolWithDefault(const std::string& key,
                                       bool default_value) const {
  return GetBool(key).value_or(default_value);
}

optional<URI> Configuration::GetUri(const std::string& key) const {
  optional<std::string> raw = Get(key);
  if (raw) {
    try {
      return std::experimental::make_optional(URI::parse_from_string(*raw));
    } catch (const uri_parse_error& e) {
      // Return empty below
    }
  }
  return optional<URI>();
}

URI Configuration::GetUriWithDefault(const std::string& key,
                                     std::string default_value) const {
  optional<URI> result = GetUri(key);
  if (result) {
    return *result;
  } else {
    try {
      return URI::parse_from_string(default_value);
    } catch (const uri_parse_error& e) {
      return URI();
    }
  }
}


}
