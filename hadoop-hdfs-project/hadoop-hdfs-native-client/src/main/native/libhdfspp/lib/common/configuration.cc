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

#include <strings.h>
#include <sstream>
#include <map>
#include <rapidxml/rapidxml.hpp>
#include <rapidxml/rapidxml_utils.hpp>

namespace hdfs {

/*
 * Configuration class
 */

Configuration::Configuration() {}

bool is_valid_bool(const std::string& raw) {
  if (!strcasecmp(raw.c_str(), "true")) {
    return true;
  }
  if (!strcasecmp(raw.c_str(), "false")) {
    return true;
  }
  return false;
}

bool str_to_bool(const std::string& raw) {
  if (!strcasecmp(raw.c_str(), "true")) {
    return true;
  }

  return false;
}

optional<Configuration> Configuration::Load(const std::string& xmlData) {
  Configuration result;
  return result.OverlayResourceString(xmlData);
}

optional<Configuration> Configuration::OverlayResourceString(
    const std::string& xmlData) const {
  if (xmlData.size() == 0) {
    return optional<Configuration>();
  }

  int length = xmlData.size();
  std::vector<char> raw_bytes;
  raw_bytes.reserve(length + 1);
  std::copy(xmlData.begin(), xmlData.end(), std::back_inserter(raw_bytes));
  raw_bytes.push_back('\0');

  ConfigMap map(raw_values_);
  bool success = UpdateMapWithResource(map, raw_bytes);

  if (success) {
    return optional<Configuration>(Configuration(map));
  } else {
    return optional<Configuration>();
  }
}

bool Configuration::UpdateMapWithResource(ConfigMap& map,
                                          std::vector<char>& raw_bytes) {
  rapidxml::xml_document<> dom;
  dom.parse<rapidxml::parse_trim_whitespace>(&raw_bytes[0]);

  /* File must contain a single <configuration> stanza */
  auto config_node = dom.first_node("configuration", 0, false);
  if (!config_node) {
    return false;
  }

  /* Walk all of the <property> nodes, ignoring the rest */
  for (auto property_node = config_node->first_node("property", 0, false);
       property_node;
       property_node = property_node->next_sibling("property", 0, false)) {
    auto name_node = property_node->first_node("name", 0, false);
    auto value_node = property_node->first_node("value", 0, false);

    if (name_node && value_node) {
      auto mapValue = map.find(name_node->value());
      if (mapValue != map.end() && mapValue->second.final) {
        continue;
      }

      map[name_node->value()] = value_node->value();
      auto final_node = property_node->first_node("final", 0, false);
      if (final_node && is_valid_bool(final_node->value())) {
        map[name_node->value()].final = str_to_bool(final_node->value());
      }
    }

    auto name_attr = property_node->first_attribute("name", 0, false);
    auto value_attr = property_node->first_attribute("value", 0, false);

    if (name_attr && value_attr) {
      auto mapValue = map.find(name_attr->value());
      if (mapValue != map.end() && mapValue->second.final) {
        continue;
      }

      map[name_attr->value()] = value_attr->value();
      auto final_attr = property_node->first_attribute("final", 0, false);
      if (final_attr && is_valid_bool(final_attr->value())) {
        map[name_attr->value()].final = str_to_bool(final_attr->value());
      }
    }
  }

  return true;
}

optional<std::string> Configuration::Get(const std::string& key) const {
  auto found = raw_values_.find(key);
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
    auto result =
        std::experimental::make_optional(strtol(raw->c_str(), &end, 10));
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
}
