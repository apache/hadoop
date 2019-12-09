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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is the JMX management interface for NameNode status information.
 * End users shouldn't be implementing these interfaces, and instead
 * access this information through the JMX APIs. *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface NameNodeStatusMXBean {

  /**
   * Gets the NameNode role.
   *
   * @return the NameNode role.
   */
  public String getNNRole();

  /**
   * Gets the NameNode state.
   *
   * @return the NameNode state.
   */
  public String getState();

  /**
   * Gets the host and port colon separated.
   *
   * @return host and port colon separated.
   */
  public String getHostAndPort();

  /**
   * Gets if security is enabled.
   *
   * @return true, if security is enabled.
   */
  public boolean isSecurityEnabled();

  /**
   * Gets the most recent HA transition time in milliseconds from the epoch.
   *
   * @return the most recent HA transition time in milliseconds from the epoch.
   */
  public long getLastHATransitionTime();

  /**
   * Gets number of bytes in blocks with future generation stamps.
   * @return number of bytes that can be deleted if exited from safe mode.
   */
  long getBytesWithFutureGenerationStamps();

  /**
   * Retrieves information about slow DataNodes, if the feature is
   * enabled. The report is in a JSON format.
   */
  String getSlowPeersReport();


  /**
   *  Gets the topN slow disks in the cluster, if the feature is enabled.
   *
   *  @return JSON string of list of diskIDs and latencies
   */
  String getSlowDisksReport();
}
