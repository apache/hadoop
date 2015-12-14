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

#ifndef COMMON_CONFIGURATION_BUILDER_H_
#define COMMON_CONFIGURATION_BUILDER_H_

#include "configuration.h"

namespace hdfs {


class ConfigurationLoader {
public:
  // Creates a new, empty Configuration object
  //    T must be Configuration or a subclass
  template<class T>
  T           New();

  // Loads Configuration XML contained in a string and returns a parsed
  //    Configuration object
  //    T must be Configuration or a subclass
  template<class T>
  optional<T> Load(const std::string &xml_data);

  // Loads Configuration XML contained in a string and produces a new copy that
  //    is the union of the src and xml_data
  //    Any parameters from src will be overwritten by the xml_data unless they
  //    are marked as "final" in src.
  //    T must be Configuration or a subclass
  template<class T>
  optional<T> OverlayResourceString(const T &src, const std::string &xml_data) const;

protected:
  using ConfigMap = Configuration::ConfigMap;

  // Updates the src map with data from the XML
  static bool UpdateMapWithString( Configuration::ConfigMap & src,
                                   const std::string &xml_data);
  // Updates the src map with data from the XML
  static bool UpdateMapWithBytes(Configuration::ConfigMap &map,
                                 std::vector<char> &raw_bytes);
};

}

#include "configuration_loader_impl.h"

#endif
