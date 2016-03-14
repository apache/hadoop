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

#include <fstream>
#include <strings.h>
#include <sstream>
#include <map>
#include <sys/stat.h>
#include <rapidxml/rapidxml.hpp>
#include <rapidxml/rapidxml_utils.hpp>

namespace hdfs {

/*
 * ConfigurationLoader class
 */

#if defined(WIN32) || defined(_WIN32)
static const char kFileSeparator = '\\';
#else
static const char kFileSeparator = '/';
#endif

static const char kSearchPathSeparator = ':';

bool is_valid_bool(const std::string& raw) {
  if (raw.empty()) {
    return false;
  }

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

void ConfigurationLoader::SetDefaultSearchPath() {
  // Try (in order, taking the first valid one):
  //    $HADOOP_CONF_DIR
  //    /etc/hadoop/conf
  const char * hadoop_conf_dir_env = getenv("HADOOP_CONF_DIR");
  if (hadoop_conf_dir_env) {
    AddToSearchPath(hadoop_conf_dir_env);
  } else {
    AddToSearchPath("/etc/hadoop/conf");
  }
}

void ConfigurationLoader::ClearSearchPath()
{
  search_path_.clear();
}

void ConfigurationLoader::SetSearchPath(const std::string & searchPath)
{
  search_path_.clear();

  std::vector<std::string> paths;
  std::string::size_type start = 0;
  std::string::size_type end = searchPath.find(kSearchPathSeparator);

  while (end != std::string::npos) {
     paths.push_back(searchPath.substr(start, end-start));
     start = ++end;
     end = searchPath.find(kSearchPathSeparator, start);
  }
  paths.push_back(searchPath.substr(start, searchPath.length()));

  for (auto path: paths) {
    AddToSearchPath(path);
  }

}

void ConfigurationLoader::AddToSearchPath(const std::string & searchPath)
{
  if (searchPath.empty())
    return;

  if (searchPath.back() != kFileSeparator) {
    std::string pathWithSlash(searchPath);
    pathWithSlash += kFileSeparator;
    search_path_.push_back(pathWithSlash);
  } else {
    search_path_.push_back(searchPath);
  }
}

std::string ConfigurationLoader::GetSearchPath()
{
  std::stringstream result;
  bool first = true;
  for(std::string item: search_path_) {
    if (!first) {
      result << kSearchPathSeparator;
    }

    result << item;
    first = false;
  }

  return result.str();
}

bool ConfigurationLoader::UpdateMapWithFile(ConfigMap & map, const std::string & path) const
{
  if (path.front() == kFileSeparator) { // Absolute path
    std::ifstream stream(path, std::ifstream::in);
    if ( stream.is_open() ) {
      return UpdateMapWithStream(map, stream);
    } else {
      return false;
    }
  } else { // Use search path
    for(auto dir: search_path_) {
      std::ifstream stream(dir + path);
      if ( stream.is_open() ) {
        if (UpdateMapWithStream(map, stream))
          return true;
      }
    }
  }

  return false;
}

bool ConfigurationLoader::UpdateMapWithStream(ConfigMap & map,
                                              std::istream & stream) {
  std::streampos start = stream.tellg();
  stream.seekg(0, std::ios::end);
  std::streampos end = stream.tellg();
  stream.seekg(start, std::ios::beg);

  int length = end - start;

  if (length <= 0 || start == -1 || end == -1)
    return false;

  std::vector<char> raw_bytes((int64_t)length + 1);
  stream.read(&raw_bytes[0], length);
  raw_bytes[length] = 0;

  return UpdateMapWithBytes(map, raw_bytes);
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
  try {
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
        std::string final_value;
        auto final_node = property_node->first_node("final", 0, false);
        if (final_node) {
          final_value = final_node->value();
        }
        UpdateMapWithValue(map, name_node->value(), value_node->value(), final_value);
      }

      auto name_attr = property_node->first_attribute("name", 0, false);
      auto value_attr = property_node->first_attribute("value", 0, false);

      if (name_attr && value_attr) {
        std::string final_value;
        auto final_attr = property_node->first_attribute("final", 0, false);
        if (final_attr) {
          final_value = final_attr->value();
        }
        UpdateMapWithValue(map, name_attr->value(), value_attr->value(), final_value);
      }
    }

    return true;
  } catch (const rapidxml::parse_error &e) {
    // TODO: Capture the result in a Status object
    return false;
  }
}

bool ConfigurationLoader::UpdateMapWithValue(ConfigMap& map,
                                             const std::string& key, const std::string& value,
                                             const std::string& final_text)
{
  std::string caseFixedKey = Configuration::fixCase(key);
  auto mapValue = map.find(caseFixedKey);
  if (mapValue != map.end() && mapValue->second.final) {
    return false;
  }

  bool final_value = false;
  if (is_valid_bool(final_text)) {
    final_value = str_to_bool(final_text);
  }

  map[caseFixedKey].value = value;
  map[caseFixedKey].final = final_value;
  return true;
}

}