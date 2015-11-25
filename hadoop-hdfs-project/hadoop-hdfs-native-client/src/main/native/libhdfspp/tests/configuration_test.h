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
#ifndef TESTS_CONFIGURATION_H_
#define TESTS_CONFIGURATION_H_

#include "common/configuration.h"
#include <cstdio>
#include <fstream>
#include <istream>
#include <gmock/gmock.h>

namespace hdfs {

template <typename T, typename U>
void simpleConfigStreamProperty(std::stringstream& out, T key, U value) {
  out << "<property>"
      << "<name>" << key << "</name>"
      << "<value>" << value << "</value>"
      << "</property>";
}

template <typename T, typename U, typename... Args>
void simpleConfigStreamProperty(std::stringstream& out, T key, U value,
                                Args... args) {
  simpleConfigStreamProperty(out, key, value);
  simpleConfigStreamProperty(out, args...);
}

template <typename... Args>
void simpleConfigStream(std::stringstream& out, Args... args) {
  out << "<configuration>";
  simpleConfigStreamProperty(out, args...);
  out << "</configuration>";
}

template <typename... Args>
optional<Configuration> simpleConfig(Args... args) {
  Configuration result;
  std::stringstream stream;
  simpleConfigStream(stream, args...);
  optional<Configuration> parse = result.Load(stream.str());
  EXPECT_TRUE((bool)parse);

  return parse;
}

template <typename... Args>
void writeSimpleConfig(const std::string& filename, Args... args) {
  std::stringstream stream;
  simpleConfigStream(stream, args...);

  std::ofstream out;
  out.open(filename);
  out << stream.rdbuf();
}
}

#endif
