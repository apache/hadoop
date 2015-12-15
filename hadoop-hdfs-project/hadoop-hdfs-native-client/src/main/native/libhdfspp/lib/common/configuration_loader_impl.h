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

#ifndef COMMON_CONFIGURATION_BUILDER_IMPL_H_
#define COMMON_CONFIGURATION_BUILDER_IMPL_H_

namespace hdfs {


template<class T>
T ConfigurationLoader::New() {
  return T();
}

template<class T>
optional<T> ConfigurationLoader::Load(const std::string &xml_data) {
  return OverlayResourceString<T>(T(), xml_data);
}
template<class T>
optional<T> ConfigurationLoader::LoadFromStream(std::istream &stream) {
  return OverlayResourceStream<T>(T(), stream);
}
template<class T>
optional<T> ConfigurationLoader::LoadFromFile(const std::string &path) {
  return OverlayResourceFile<T>(T(), path);
}


template<class T>
optional<T> ConfigurationLoader::OverlayResourceFile(const T& src, const std::string &path) const {
  ConfigMap map(src.raw_values_);
  bool success = UpdateMapWithFile(map, path);

  if (success) {
    return std::experimental::make_optional<T>(map);
  } else {
    return optional<T>();
  }
}

template<class T>
optional<T> ConfigurationLoader::OverlayResourceStream(const T& src, std::istream & stream) const {
  ConfigMap map(src.raw_values_);
  bool success = UpdateMapWithStream(map, stream);

  if (success) {
    return std::experimental::make_optional<T>(map);
  } else {
    return optional<T>();
  }
}

template<class T>
optional<T> ConfigurationLoader::OverlayResourceString(const T& src, const std::string &xml_data) const {
  if (xml_data.size() == 0) {
    return optional<T>();
  }

  std::vector<char> raw_bytes(xml_data.begin(), xml_data.end());
  raw_bytes.push_back('\0');

  ConfigMap map(src.raw_values_);
  bool success = UpdateMapWithBytes(map, raw_bytes);

  if (success) {
    return std::experimental::make_optional<T>(map);
  } else {
    return optional<T>();
  }
}

template <class T>
optional<T> ConfigurationLoader::LoadDefaultResources() {
  std::vector<std::string> default_filenames = T::GetDefaultFilenames();

  ConfigMap result;
  bool success = true;

  for (auto fn: default_filenames) {
    success &= UpdateMapWithFile(result, fn);
    if (!success)
      break;
  }

  if (success) {
    return std::experimental::make_optional<T>(result);
  } else {
    return optional<T>();
  }
}


}

#endif
