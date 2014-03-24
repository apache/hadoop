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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;

@InterfaceAudience.Private
@InterfaceStability.Evolving
/**
 * Response to {@link DatanodeProtocol#sendHeartbeat}
 */
public class HeartbeatResponse {
  /** Commands returned from the namenode to the datanode */
  private final DatanodeCommand[] commands;
  
  /** Information about the current HA-related state of the NN */
  private final NNHAStatusHeartbeat haStatus;

  private final RollingUpgradeStatus rollingUpdateStatus;
  
  public HeartbeatResponse(DatanodeCommand[] cmds,
      NNHAStatusHeartbeat haStatus, RollingUpgradeStatus rollingUpdateStatus) {
    commands = cmds;
    this.haStatus = haStatus;
    this.rollingUpdateStatus = rollingUpdateStatus;
  }
  
  public DatanodeCommand[] getCommands() {
    return commands;
  }
  
  public NNHAStatusHeartbeat getNameNodeHaState() {
    return haStatus;
  }

  public RollingUpgradeStatus getRollingUpdateStatus() {
    return rollingUpdateStatus;
  }
}
