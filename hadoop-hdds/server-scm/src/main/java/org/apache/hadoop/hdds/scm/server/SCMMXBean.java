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

package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfo;

import java.util.Map;

/**
 *
 * This is the JMX management interface for scm information.
 */
@InterfaceAudience.Private
public interface SCMMXBean extends ServiceRuntimeInfo {

  /**
   * Get the SCM RPC server port that used to listen to datanode requests.
   * @return SCM datanode RPC server port
   */
  String getDatanodeRpcPort();

  /**
   * Get the SCM RPC server port that used to listen to client requests.
   * @return SCM client RPC server port
   */
  String getClientRpcPort();

  /**
   * Get container report info that includes container IO stats of nodes.
   * @return The datanodeUUid to report json string mapping
   */
  Map<String, String> getContainerReport();

  /**
   * Returns safe mode status.
   * @return boolean
   */
  boolean isInSafeMode();

  /**
   * Returns live safe mode container threshold.
   * @return String
   */
  double getSafeModeCurrentContainerThreshold();

  /**
   * Returns the container count in all states.
   */
  Map<String, Integer> getContainerStateCount();
}
