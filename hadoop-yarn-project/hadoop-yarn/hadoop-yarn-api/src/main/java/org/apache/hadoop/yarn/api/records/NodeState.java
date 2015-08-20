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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>State of a <code>Node</code>.</p>
 */
@Public
@Unstable
public enum NodeState {
  /** New node */
  NEW, 
  
  /** Running node */
  RUNNING, 
  
  /** Node is unhealthy */
  UNHEALTHY, 
  
  /** Node is out of service */
  DECOMMISSIONED, 
  
  /** Node has not sent a heartbeat for some configured time threshold*/
  LOST, 
  
  /** Node has rebooted */
  REBOOTED,

  /** Node decommission is in progress */
  DECOMMISSIONING,

  /** Node has shutdown gracefully. */
  SHUTDOWN;

  public boolean isUnusable() {
    return (this == UNHEALTHY || this == DECOMMISSIONED
        || this == LOST || this == SHUTDOWN);
  }
}
