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

#ifndef COMMON_CONFIGURATION_H_
#define COMMON_CONFIGURATION_H_

#include <string>
#include <map>
#include <vector>
#include <set>
#include <istream>
#include <stdint.h>
#include <optional.hpp>

namespace hdfs {

template <class T>
using optional = std::experimental::optional<T>;

/**
 * Configuration class that parses XML.
 *
 * Files should be an XML file of the form
 * <configuration>
 *  <property>
 *    <name>Name</name>
 *    <value>Value</value>
 *  </property>
 * <configuration>
 *
 * This class is not thread-safe.
 */
class Configuration {
 public:
  /* Creates a new Configuration from input xml */
  static optional<Configuration> Load(const std::string &xml_data);

  /* Constructs a configuration with no resources loaded */
  Configuration();

  /* Loads resources from a file or a stream */
  optional<Configuration> OverlayResourceString(
      const std::string &xml_data) const;

  // Gets values
  std::string GetWithDefault(const std::string &key,
                             const std::string &default_value) const;
  optional<std::string> Get(const std::string &key) const;
  int64_t GetIntWithDefault(const std::string &key, int64_t default_value) const;
  optional<int64_t> GetInt(const std::string &key) const;
  double GetDoubleWithDefault(const std::string &key,
                              double default_value) const;
  optional<double> GetDouble(const std::string &key) const;
  bool GetBoolWithDefault(const std::string &key, bool default_value) const;
  optional<bool> GetBool(const std::string &key) const;

 private:
  /* Transparent data holder for property values */
  struct ConfigData {
    std::string value;
    bool final;
    ConfigData() : final(false){};
    ConfigData(const std::string &value) : value(value), final(false) {}
    void operator=(const std::string &new_value) {
      value = new_value;
      final = false;
    }
  };
  typedef std::map<std::string, ConfigData> ConfigMap;

  Configuration(ConfigMap &src_map) : raw_values_(src_map){};
  static bool UpdateMapWithResource(ConfigMap &map,
                                    std::vector<char> &raw_bytes);

  const ConfigMap raw_values_;
};
}

#endif
