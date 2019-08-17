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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Map;

/**
 *
 * This is the JMX management interface for node manager information.
 */
@InterfaceAudience.Private
public interface NodeManagerMXBean {

  /**
   * Get the number of data nodes that in all states.
   *
   * @return A state to number of nodes that in this state mapping
   */
  Map<String, Integer> getNodeCount();

  /**
   * Get the disk metrics like capacity, usage and remaining based on the
   * storage type.
   */
  Map<String, Long> getNodeInfo();

}
