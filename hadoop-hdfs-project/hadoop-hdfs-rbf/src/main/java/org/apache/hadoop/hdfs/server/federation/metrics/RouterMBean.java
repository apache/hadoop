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
package org.apache.hadoop.hdfs.server.federation.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * JMX interface for the router specific metrics.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface RouterMBean {

  /**
   * When the router started.
   * @return Date as a string the router started.
   */
  String getRouterStarted();

  /**
   * Get the version of the router.
   * @return Version of the router.
   */
  String getVersion();

  /**
   * Get the compilation date of the router.
   * @return Compilation date of the router.
   */
  String getCompiledDate();

  /**
   * Get the compilation info of the router.
   * @return Compilation info of the router.
   */
  String getCompileInfo();

  /**
   * Get the host and port of the router.
   * @return Host and port of the router.
   */
  String getHostAndPort();

  /**
   * Get the identifier of the router.
   * @return Identifier of the router.
   */
  String getRouterId();

  /**
   * Get the current state of the router.
   *
   * @return String label for the current router state.
   */
  String getRouterStatus();

  /**
   * Gets the cluster ids of the namenodes.
   * @return the cluster ids of the namenodes.
   */
  String getClusterId();

  /**
   * Gets the block pool ids of the namenodes.
   * @return the block pool ids of the namenodes.
   */
  String getBlockPoolId();

  /**
   * Get the current number of delegation tokens in memory.
   * @return number of DTs
   */
  long getCurrentTokensCount();

  /**
   * Gets the safemode status.
   *
   * @return the safemode status.
   */
  String getSafemode();

  /**
   * Gets if security is enabled.
   *
   * @return true, if security is enabled.
   */
  boolean isSecurityEnabled();
}
