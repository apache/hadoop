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
#ifndef HDFSPP_CONTENT_SUMMARY_H_
#define HDFSPP_CONTENT_SUMMARY_H_

#include <string>
#include <cstdint>

namespace hdfs {

/**
 * Content summary is assumed to be unchanging for the duration of the operation
 */
struct ContentSummary {
  uint64_t length;
  uint64_t filecount;
  uint64_t directorycount;
  uint64_t quota;
  uint64_t spaceconsumed;
  uint64_t spacequota;
  std::string path;

  ContentSummary();

  //Converts ContentSummary object to std::string (hdfs_count format)
  std::string str(bool include_quota) const;

  //Converts ContentSummary object to std::string (hdfs_du format)
  std::string str_du() const;
};

}

#endif
