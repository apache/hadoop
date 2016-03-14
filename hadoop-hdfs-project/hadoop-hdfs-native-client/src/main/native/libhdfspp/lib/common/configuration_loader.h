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

  /****************************************************************************
   *                    LOADING CONFIG FILES
   ***************************************************************************/

  // Loads Configuration XML contained in a string/stream/file and returns a parsed
  //    Configuration object.
  //    T must be Configuration or a subclass
  template<class T>
  optional<T> Load(const std::string &xml_data);
  // Streams must be seekable
  template<class T>
  optional<T> LoadFromStream(std::istream & stream);
  // The ConfigurationBuilder's search path will be searched for the filename
  //    unless it is an absolute path
  template<class T>
  optional<T> LoadFromFile(const std::string &filename);

  // Loads Configuration XML contained in a string and produces a new copy that
  //    is the union of the src and xml_data
  //    Any parameters from src will be overwritten by the xml_data unless they
  //    are marked as "final" in src.
  //    T must be Configuration or a subclass
  template<class T>
  optional<T> OverlayResourceString(const T &src, const std::string &xml_data) const;
  // Streams must be seekable
  template<class T>
  optional<T> OverlayResourceStream(const T &src, std::istream &stream) const;
  //    The ConfigurationBuilder's search path will be searched for the filename
  //       unless it is an absolute path
  template<class T>
  optional<T> OverlayResourceFile(const T &src, const std::string &path) const;

  // Attempts to update the map.  If the update failed (because there was
  // an existing final value, for example), returns the original map
  template<class T>
  optional<T> OverlayValue(const T &src, const std::string &key, const std::string &value) const;

  // Returns an instance of the Configuration with all of the default resource
  //    files loaded.
  //    T must be Configuration or a subclass
  template<class T>
  optional<T> LoadDefaultResources();


  /****************************************************************************
   *                    SEARCH PATH METHODS
   ***************************************************************************/

  // Sets the search path to the default search path (namely, "$HADOOP_CONF_DIR" or "/etc/hadoop/conf")
  void SetDefaultSearchPath();

  // Clears out the search path
  void ClearSearchPath();
  // Sets the search path to ":"-delimited paths
  void SetSearchPath(const std::string & searchPath);
  // Adds an element to the search path
  void AddToSearchPath(const std::string & searchPath);
  // Returns the search path in ":"-delmited form
  std::string GetSearchPath();

protected:
  using ConfigMap = Configuration::ConfigMap;

  // Updates the src map with data from the XML in the path
  //   The search path will be searched for the filename
  bool UpdateMapWithFile(ConfigMap & map, const std::string & path) const;

  // Updates the src map with data from the XML in the stream
  //   The stream must be seekable
  static bool UpdateMapWithStream(ConfigMap & map,
                                  std::istream & stream);
  // Updates the src map with data from the XML
  static bool UpdateMapWithString(Configuration::ConfigMap & src,
                                  const std::string &xml_data);
  // Updates the src map with data from the XML
  static bool UpdateMapWithBytes(Configuration::ConfigMap &map,
                                 std::vector<char> &raw_bytes);

  // Attempts to update the map.  If the update failed (because there was
  // an existing final value, for example), returns false
  static bool UpdateMapWithValue(ConfigMap& map,
        const std::string& key, const std::string& value, const std::string& final_text);

  std::vector<std::string> search_path_;
};

}

#include "configuration_loader_impl.h"

#endif
