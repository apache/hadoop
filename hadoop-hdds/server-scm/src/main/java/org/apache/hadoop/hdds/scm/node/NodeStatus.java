/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

/**
 * This class is used to capture the current status of a datanode. This
 * includes its health (healthy, stale or dead) and its operation status (
 * in_service, decommissioned and maintenance mode.
 */
public class NodeStatus {

  private HddsProtos.NodeOperationalState operationalState;
  private HddsProtos.NodeState health;

  NodeStatus(HddsProtos.NodeOperationalState operationalState,
             HddsProtos.NodeState health) {
    this.operationalState = operationalState;
    this.health = health;
  }

  public HddsProtos.NodeState getHealth() {
    return health;
  }

  public HddsProtos.NodeOperationalState getOperationalState() {
    return operationalState;
  }

  public void setOperationalState(
      HddsProtos.NodeOperationalState newOperationalState) {
    assert newOperationalState != null;
    operationalState = newOperationalState;
  }

  public void setHealth(HddsProtos.NodeState newHealth) {
    assert newHealth != null;
    health = newHealth;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    NodeStatus other = (NodeStatus) obj;
    if (this.operationalState == other.operationalState &&
        this.health == other.health) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(health, operationalState);
  }

}
