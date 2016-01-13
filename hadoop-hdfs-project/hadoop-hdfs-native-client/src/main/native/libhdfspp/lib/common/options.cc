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

#include "hdfspp/options.h"

namespace hdfs {

// The linker needs a place to put all of those constants
const int Options::kDefaultRpcTimeout;
const int Options::kNoRetry;
const int Options::kDefaultMaxRpcRetries;
const int Options::kDefaultRpcRetryDelayMs;
const unsigned int Options::kDefaultHostExclusionDuration;

Options::Options() : rpc_timeout(kDefaultRpcTimeout), max_rpc_retries(kDefaultMaxRpcRetries),
                     rpc_retry_delay_ms(kDefaultRpcRetryDelayMs),
                     host_exclusion_duration(kDefaultHostExclusionDuration) {}
}
