/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.monitor.probe;

/**
 * Config keys for monitoring
 */
public interface MonitorKeys {

  /**
   * Default probing key : DNS check enabled {@value}.
   */
  String DEFAULT_PROBE_DNS_CHECK_ENABLED = "dns.check.enabled";
  /**
   * Default probing default : DNS check enabled {@value}.
   */
  boolean DEFAULT_PROBE_DNS_CHECK_ENABLED_DEFAULT = false;
  /**
   * Default probing key : DNS checking address IP:port {@value}.
   */
  String DEFAULT_PROBE_DNS_ADDRESS = "dns.address";
  /**
   * Port probing key : port to attempt to create a TCP connection to {@value}.
   */
  String PORT_PROBE_PORT = "port";
  /**
   * Port probing key : timeout for the the connection attempt {@value}.
   */
  String PORT_PROBE_CONNECT_TIMEOUT = "timeout";
  /**
   * Port probing default : timeout for the connection attempt {@value}.
   */
  int PORT_PROBE_CONNECT_TIMEOUT_DEFAULT = 1000;

  /**
   * Web probing key : URL {@value}.
   */
  String WEB_PROBE_URL = "url";
  /**
   * Web probing key : min success code {@value}.
   */
  String WEB_PROBE_MIN_SUCCESS = "min.success";
  /**
   * Web probing key : max success code {@value}.
   */
  String WEB_PROBE_MAX_SUCCESS = "max.success";
  /**
   * Web probing default : min successful response code {@value}.
   */
  int WEB_PROBE_MIN_SUCCESS_DEFAULT = 200;
  /**
   * Web probing default : max successful response code {@value}.
   */
  int WEB_PROBE_MAX_SUCCESS_DEFAULT = 299;
  /**
   * Web probing key : timeout for the connection attempt {@value}
   */
  String WEB_PROBE_CONNECT_TIMEOUT = "timeout";
  /**
   * Port probing default : timeout for the connection attempt {@value}.
   */
  int WEB_PROBE_CONNECT_TIMEOUT_DEFAULT = 1000;
}
