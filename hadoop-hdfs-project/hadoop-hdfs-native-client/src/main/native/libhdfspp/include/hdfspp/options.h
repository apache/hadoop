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
#ifndef LIBHDFSPP_OPTIONS_H_
#define LIBHDFSPP_OPTIONS_H_

#include "common/uri.h"

namespace hdfs {

/**
 * Options to control the behavior of the libhdfspp library.
 **/
struct Options {
  /**
   * Time out of RPC requests in milliseconds.
   * Default: 30000
   **/
  int rpc_timeout;
  static const int kDefaultRpcTimeout = 30000;

  /**
   * Maximum number of retries for RPC operations
   **/
  int max_rpc_retries;
  static const int kNoRetry = -1;
  static const int kDefaultMaxRpcRetries = kNoRetry;

  /**
   * Number of ms to wait between retry of RPC operations
   **/
  int rpc_retry_delay_ms;
  static const int kDefaultRpcRetryDelayMs = 10000;

  /**
   * Exclusion time for failed datanodes in milliseconds.
   * Default: 60000
   **/
  unsigned int host_exclusion_duration;
  static const unsigned int kDefaultHostExclusionDuration = 600000;

  /**
   * URI to connect to if no host:port are specified in connect
   */
  URI defaultFS;

  /**
   * Which form of authentication to use with the server
   * Default: simple
   */
  enum Authentication {
      kSimple,
      kKerberos
  };
  Authentication authentication;
  static const Authentication kDefaultAuthentication = kSimple;

  Options();
};
}
#endif
