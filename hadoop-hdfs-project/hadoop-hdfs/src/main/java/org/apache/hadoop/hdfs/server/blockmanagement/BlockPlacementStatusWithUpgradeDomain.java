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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Set;

/**
 * An implementation of @see BlockPlacementStatus for
 * @see BlockPlacementPolicyWithUpgradeDomain
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementStatusWithUpgradeDomain implements
    BlockPlacementStatus {

  private final BlockPlacementStatus parentBlockPlacementStatus;
  private final Set<String> upgradeDomains;
  private final int numberOfReplicas;
  private final int upgradeDomainFactor;

  /**
   * @param parentBlockPlacementStatus the parent class' status
   * @param upgradeDomains the set of upgrade domains of the replicas
   * @param numberOfReplicas the number of replicas of the block
   * @param upgradeDomainFactor the configured upgrade domain factor
   */
  public BlockPlacementStatusWithUpgradeDomain(
      BlockPlacementStatus parentBlockPlacementStatus,
      Set<String> upgradeDomains, int numberOfReplicas,
      int upgradeDomainFactor){
    this.parentBlockPlacementStatus = parentBlockPlacementStatus;
    this.upgradeDomains = upgradeDomains;
    this.numberOfReplicas = numberOfReplicas;
    this.upgradeDomainFactor = upgradeDomainFactor;
  }

  @Override
  public boolean isPlacementPolicySatisfied() {
    return parentBlockPlacementStatus.isPlacementPolicySatisfied() &&
        isUpgradeDomainPolicySatisfied();
  }

  private boolean isUpgradeDomainPolicySatisfied() {
    if (numberOfReplicas <= upgradeDomainFactor) {
      return (numberOfReplicas <= upgradeDomains.size());
    } else {
      return upgradeDomains.size() >= upgradeDomainFactor;
    }
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
    if (!isUpgradeDomainPolicySatisfied()) {
      if (errorDescription.length() != 0) {
        errorDescription.append(" ");
      }
      errorDescription.append("The block has " + numberOfReplicas +
          " replicas. But it only has " + upgradeDomains.size() +
              " upgrade domains " + upgradeDomains +".");
    }
    return errorDescription.toString();
  }

  @Override
  public int getAdditionalReplicasRequired() {
    if (isPlacementPolicySatisfied()) {
      return 0;
    } else {
      // It is possible for a block to have the correct number of upgrade
      // domains, but only a single rack, or be on multiple racks, but only in
      // one upgrade domain.
      int parent = parentBlockPlacementStatus.getAdditionalReplicasRequired();
      int child;

      if (numberOfReplicas <= upgradeDomainFactor) {
        child = numberOfReplicas - upgradeDomains.size();
      } else {
        child = upgradeDomainFactor - upgradeDomains.size();
      }
      return Math.max(parent, child);
    }
  }
}
