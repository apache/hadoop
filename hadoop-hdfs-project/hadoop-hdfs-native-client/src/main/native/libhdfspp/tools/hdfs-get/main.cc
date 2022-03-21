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

#include <cstdlib>
#include <exception>
#include <iostream>

#include <google/protobuf/stubs/common.h>

#include "hdfs-get.h"

int main(int argc, char *argv[]) {
  const auto result = std::atexit([]() -> void {
    // Clean up static data on exit and prevent valgrind memory leaks
    google::protobuf::ShutdownProtobufLibrary();
  });
  if (result != 0) {
    std::cerr << "Error: Unable to schedule clean-up tasks for HDFS "
                 "get tool, exiting"
              << std::endl;
    std::exit(EXIT_FAILURE);
  }

  hdfs::tools::Get get(argc, argv);
  auto success = false;

  try {
    success = get.Do();
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
  }

  if (!success) {
    std::exit(EXIT_FAILURE);
  }
  return 0;
}
