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

  private final int requiredDatacenters;
  private final int currentDatacenters;
  private final int neededReplicaCount;
  private final int totalDatacenters;

  public BlockPlacementStatusWithCrossDC(int currentDatacenters, int requiredDatacenters,
      int neededReplicaCount, int totalDatacenters) {
    this.currentDatacenters = currentDatacenters;
    this.requiredDatacenters = requiredDatacenters;
    this.neededReplicaCount = neededReplicaCount;
    this.totalDatacenters = totalDatacenters;
  }

  @Override
  public boolean isPlacementPolicySatisfied() {
    // If cluster has fewer datacenters than required, consider policy satisfied
    // to avoid infinite replication loops
    return (currentDatacenters >= requiredDatacenters || currentDatacenters >= totalDatacenters)
        && neededReplicaCount <= 0;
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
    if (neededReplicaCount > 0) {
      return neededReplicaCount;
    }

    if (totalDatacenters > 1) {
      return Math.min(requiredDatacenters - currentDatacenters, totalDatacenters);
    }
    return 0;
  }
}
