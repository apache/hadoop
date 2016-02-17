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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An implementation of @see BlockPlacementStatus for
 * @see BlockPlacementPolicyWithNodeGroup
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementStatusWithNodeGroup implements BlockPlacementStatus {

  private final BlockPlacementStatus parentBlockPlacementStatus;
  private final Set<String> currentNodeGroups;
  private final int requiredNodeGroups;

  /**
   * @param parentBlockPlacementStatus the parent class' status
   * @param currentNodeGroups the current set of node groups of the replicas
   * @param requiredNodeGroups the number of required node groups
   */
  public BlockPlacementStatusWithNodeGroup(
      BlockPlacementStatus parentBlockPlacementStatus,
      Set<String> currentNodeGroups, int requiredNodeGroups) {
    this.parentBlockPlacementStatus = parentBlockPlacementStatus;
    this.currentNodeGroups = currentNodeGroups;
    this.requiredNodeGroups = requiredNodeGroups;
  }

  @Override
  public boolean isPlacementPolicySatisfied() {
    return parentBlockPlacementStatus.isPlacementPolicySatisfied()
        && isNodeGroupPolicySatisfied();
  }

  private boolean isNodeGroupPolicySatisfied() {
    return requiredNodeGroups <= currentNodeGroups.size();
  }

  @Override
  public String getErrorDescription() {
    if (isPlacementPolicySatisfied()) {
      return null;
    }

    StringBuilder errorDescription = new StringBuilder();
    if (!parentBlockPlacementStatus.isPlacementPolicySatisfied()) {
      errorDescription.append(parentBlockPlacementStatus.getErrorDescription());
    }

    if (!isNodeGroupPolicySatisfied()) {
      if (errorDescription.length() != 0) {
        errorDescription.append(" ");
      }
      errorDescription.append("The block has " + requiredNodeGroups
          + " replicas. But it only has " + currentNodeGroups.size()
          + " node groups " + currentNodeGroups + ".");
    }
    return errorDescription.toString();
  }
}