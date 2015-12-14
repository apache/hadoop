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

#include "configuration_loader.h"

#include <strings.h>
#include <sstream>
#include <map>
#include <rapidxml/rapidxml.hpp>
#include <rapidxml/rapidxml_utils.hpp>

namespace hdfs {

/*
 * ConfigurationBuilder class
 */

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


bool ConfigurationLoader::UpdateMapWithString(ConfigMap & map,
                                                   const std::string &xml_data) {
  if (xml_data.size() == 0) {
    return false;
  }

  std::vector<char> raw_bytes(xml_data.begin(), xml_data.end());
  raw_bytes.push_back('\0');

  bool success = UpdateMapWithBytes(map, raw_bytes);

  if (success) {
    return true;
  } else {
    return false;
  }
}

bool ConfigurationLoader::UpdateMapWithBytes(ConfigMap& map,
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

}
