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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * ReplicaUnderConstruction contains information about replicas (or blocks
 * belonging to a block group) while they are under construction.
 *
 * The GS, the length and the state of the replica is as reported by the
 * datanode.
 *
 * It is not guaranteed, but expected, that datanodes actually have
 * corresponding replicas.
 */
class ReplicaUnderConstruction extends Block {
  private final DatanodeStorageInfo expectedLocation;
  private HdfsServerConstants.ReplicaState state;
  private boolean chosenAsPrimary;

  ReplicaUnderConstruction(Block block,
      DatanodeStorageInfo target,
      HdfsServerConstants.ReplicaState state) {
    super(block);
    this.expectedLocation = target;
    this.state = state;
    this.chosenAsPrimary = false;
  }

  /**
   * Expected block replica location as assigned when the block was allocated.
   * This defines the pipeline order.
   * It is not guaranteed, but expected, that the data-node actually has
   * the replica.
   */
  DatanodeStorageInfo getExpectedStorageLocation() {
    return expectedLocation;
  }

  /**
   * Get replica state as reported by the data-node.
   */
  HdfsServerConstants.ReplicaState getState() {
    return state;
  }

  /**
   * Whether the replica was chosen for recovery.
   */
  boolean getChosenAsPrimary() {
    return chosenAsPrimary;
  }

  /**
   * Set replica state.
   */
  void setState(HdfsServerConstants.ReplicaState s) {
    state = s;
  }

  /**
   * Set whether this replica was chosen for recovery.
   */
  void setChosenAsPrimary(boolean chosenAsPrimary) {
    this.chosenAsPrimary = chosenAsPrimary;
  }

  /**
   * Is data-node the replica belongs to alive.
   */
  boolean isAlive() {
    return expectedLocation.getDatanodeDescriptor().isAlive();
  }

  @Override // Block
  public int hashCode() {
    return super.hashCode();
  }

  @Override // Block
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(50);
    appendStringTo(b);
    return b.toString();
  }

  @Override
  public void appendStringTo(StringBuilder sb) {
    sb.append("ReplicaUC[")
        .append(expectedLocation)
        .append("|")
        .append(state)
        .append("]");
  }
}

