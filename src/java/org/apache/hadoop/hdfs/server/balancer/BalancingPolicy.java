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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * Balancing policy.
 * Since a datanode may contain multiple block pools,
 * {@link Pool} implies {@link Node}
 * but NOT the other way around
 */
@InterfaceAudience.Private
abstract class BalancingPolicy {
  long totalCapacity;
  long totalUsedSpace;
  private double avgUtilization;

  void reset() {
    totalCapacity = 0L;
    totalUsedSpace = 0L;
    avgUtilization = 0.0;
  }

  /** Accumulate used space and capacity. */
  abstract void accumulateSpaces(DatanodeInfo d);

  void initAvgUtilization() {
    this.avgUtilization = totalUsedSpace*100.0/totalCapacity;
  }
  double getAvgUtilization() {
    return avgUtilization;
  }

  /** Return the utilization of a datanode */
  abstract double getUtilization(DatanodeInfo d);

  /**
   * Cluster is balanced if each node is balance.
   */
  static class Node extends BalancingPolicy {
    static Node INSTANCE = new Node();
    private Node() {}

    @Override
    void accumulateSpaces(DatanodeInfo d) {
      totalCapacity += d.getCapacity();
      totalUsedSpace += d.getDfsUsed();  
    }
    
    @Override
    double getUtilization(DatanodeInfo d) {
      return d.getDfsUsed()*100.0/d.getCapacity();
    }
  }

  /**
   * Cluster is balanced if each pool in each node is balance.
   */
  static class Pool extends BalancingPolicy {
    static Pool INSTANCE = new Pool();
    private Pool() {}

    @Override
    void accumulateSpaces(DatanodeInfo d) {
      totalCapacity += d.getCapacity();
      totalUsedSpace += d.getBlockPoolUsed();  
    }

    @Override
    double getUtilization(DatanodeInfo d) {
      return d.getBlockPoolUsed()*100.0/d.getCapacity();
    }
  }
}
