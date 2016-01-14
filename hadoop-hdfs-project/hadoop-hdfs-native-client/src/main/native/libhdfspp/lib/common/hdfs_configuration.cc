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

#include "common/hdfs_configuration.h"

namespace hdfs {

// Constructs a configuration with no search path and no resources loaded
HdfsConfiguration::HdfsConfiguration() : Configuration() {}

// Constructs a configuration with a copy of the input data
HdfsConfiguration::HdfsConfiguration(ConfigMap &src_map) : Configuration(src_map) {}
HdfsConfiguration::HdfsConfiguration(const ConfigMap &src_map) : Configuration(src_map) {}

std::vector<std::string> HdfsConfiguration::GetDefaultFilenames() {
  auto result = Configuration::GetDefaultFilenames();
  result.push_back("hdfs-site.xml");
  return result;
}

// Sets a value iff the optional<T> has a value
template <class T, class U>
void OptionalSet(T& target, optional<U> value) {
  if (value)
    target = *value;
}

// Interprets the resources to build an Options object
Options HdfsConfiguration::GetOptions() {
  Options result;

  OptionalSet(result.rpc_timeout, GetInt(kDfsClientSocketTimeoutKey));
  OptionalSet(result.max_rpc_retries, GetInt(kIpcClientConnectMaxRetriesKey));
  OptionalSet(result.rpc_retry_delay_ms, GetInt(kIpcClientConnectRetryIntervalKey));

  return result;
}


}
