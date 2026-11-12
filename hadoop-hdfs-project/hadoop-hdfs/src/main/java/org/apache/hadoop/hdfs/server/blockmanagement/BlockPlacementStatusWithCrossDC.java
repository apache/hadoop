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


public class BlockPlacementStatusWithCrossDC implements BlockPlacementStatus {

  private final int minDatacentersRequired;
  private final int datacenterCountOfBlock;
  private final int additionalReplicasForPreferredDC;
  private final int datacenterCountOfCluster;

  public BlockPlacementStatusWithCrossDC(int datacenterCountOfBlock, int minDatacentersRequired,
      int additionalReplicasForPreferredDC, int datacenterCountOfCluster) {
    this.datacenterCountOfBlock = datacenterCountOfBlock;
    this.minDatacentersRequired = minDatacentersRequired;
    this.additionalReplicasForPreferredDC = additionalReplicasForPreferredDC;
    this.datacenterCountOfCluster = datacenterCountOfCluster;
  }

  @Override
  public boolean isPlacementPolicySatisfied() {
    // If cluster has fewer datacenters than required, consider policy satisfied
    // to avoid infinite replication loops
    return (datacenterCountOfBlock >= minDatacentersRequired || datacenterCountOfBlock >= datacenterCountOfCluster)
        && additionalReplicasForPreferredDC <= 0;
  }

  @Override
  public String getErrorDescription() {
    if (isPlacementPolicySatisfied()) {
      return null;
    }
    return "Block should be replicated on 2 or more datacenters and half of replicas should be replicated on major datacenter.";
  }

  @Override
  public int getAdditionalReplicasRequired() {
    if (isPlacementPolicySatisfied()) {
      return 0;
    }
    if (additionalReplicasForPreferredDC > 0) {
      return additionalReplicasForPreferredDC;
    }

    if (datacenterCountOfCluster > 1) {
      return Math.min(minDatacentersRequired - datacenterCountOfBlock, datacenterCountOfCluster);
    }
    return 0;
  }
}
