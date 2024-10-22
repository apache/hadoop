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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.EnumDoubles;

/**
 * Balancing policy.
 * Since a datanode may contain multiple block pools,
 * {@link Pool} implies {@link Node}
 * but NOT the other way around
 */
@InterfaceAudience.Private
abstract class BalancingPolicy {
  final EnumCounters<StorageType> totalCapacities
      = new EnumCounters<StorageType>(StorageType.class);
  final EnumCounters<StorageType> totalUsedSpaces
      = new EnumCounters<StorageType>(StorageType.class);
  final EnumDoubles<StorageType> avgUtilizations
      = new EnumDoubles<StorageType>(StorageType.class);

  void reset() {
    totalCapacities.reset();
    totalUsedSpaces.reset();
    avgUtilizations.reset();
  }

  /** Get the policy name. */
  abstract String getName();

  /** Accumulate used space and capacity. */
  abstract void accumulateSpaces(DatanodeStorageReport r);

  void initAvgUtilization() {
    for(StorageType t : StorageType.asList()) {
      final long capacity = totalCapacities.get(t);
      if (capacity > 0L) {
        final double avg  = totalUsedSpaces.get(t)*100.0/capacity;
        avgUtilizations.set(t, avg);
      }
    }
  }

  double getAvgUtilization(StorageType t) {
    return avgUtilizations.get(t);
  }

  /** @return the utilization of a particular storage type of a datanode;
   *          or return null if the datanode does not have such storage type.
   */
  abstract Double getUtilization(DatanodeStorageReport r, StorageType t);
  
  @Override
  public String toString() {
    return BalancingPolicy.class.getSimpleName()
        + "." + getClass().getSimpleName();
  }

  /** Get all {@link BalancingPolicy} instances*/
  static BalancingPolicy parse(String s) {
    final BalancingPolicy [] all = {BalancingPolicy.Node.INSTANCE,
                                    BalancingPolicy.Pool.INSTANCE,
                                    NodeActual.INSTANCE};
    for(BalancingPolicy p : all) {
      if (p.getName().equalsIgnoreCase(s))
        return p;
    }
    throw new IllegalArgumentException("Cannot parse string \"" + s + "\"");
  }

  /**
   * Cluster is balanced if each node is balanced.
   */
  static class Node extends BalancingPolicy {
    static final Node INSTANCE = new Node();
    private Node() {}

    @Override
    String getName() {
      return "datanode";
    }

    @Override
    void accumulateSpaces(DatanodeStorageReport r) {
      for(StorageReport s : r.getStorageReports()) {
        final StorageType t = s.getStorage().getStorageType();
        totalCapacities.add(t, s.getCapacity());
        totalUsedSpaces.add(t, s.getCapacity() - s.getRemaining());
      }
    }
    
    @Override
    Double getUtilization(DatanodeStorageReport r, final StorageType t) {
      long capacity = 0L;
      long totalUsed = 0L;
      for(StorageReport s : r.getStorageReports()) {
        if (s.getStorage().getStorageType() == t) {
          capacity += s.getCapacity();
          totalUsed += s.getCapacity() - s.getRemaining();
        }
      }
      return capacity == 0L ? null : totalUsed * 100.0 / capacity;
    }
  }

  /**
   * Cluster is balanced if each node is balanced. Unlike {@link Node},
   * The policy take into account non-DFS used spaces.
   * <p>
   * Actual DFS capacity (capacity - non-DFS used) is calculated by
   * (DFS used + remaining).
   */
  static final class NodeActual extends BalancingPolicy {
    static final NodeActual INSTANCE = new NodeActual();
    private NodeActual() {}

    @Override
    String getName() {
      return "datanode-actual";
    }

    @Override
    void accumulateSpaces(DatanodeStorageReport r) {
      for(StorageReport s : r.getStorageReports()) {
        final StorageType t = s.getStorage().getStorageType();
        totalCapacities.add(t, s.getDfsUsed() + s.getRemaining());
        totalUsedSpaces.add(t, s.getDfsUsed());
      }
    }

    @Override
    Double getUtilization(DatanodeStorageReport r, final StorageType t) {
      long capacity = 0L;
      long dfsUsed = 0L;
      for(StorageReport s : r.getStorageReports()) {
        if (s.getStorage().getStorageType() == t) {
          capacity += s.getDfsUsed() + s.getRemaining();
          dfsUsed += s.getDfsUsed();
        }
      }
      return capacity == 0L? null: dfsUsed*100.0/capacity;
    }
  }

  /**
   * Cluster is balanced if each pool in each node is balanced.
   */
  static class Pool extends BalancingPolicy {
    static final Pool INSTANCE = new Pool();
    private Pool() {}

    @Override
    String getName() {
      return "blockpool";
    }

    @Override
    void accumulateSpaces(DatanodeStorageReport r) {
      for(StorageReport s : r.getStorageReports()) {
        final StorageType t = s.getStorage().getStorageType();
        // Use s.getRemaining() + s.getBlockPoolUsed() instead of
        // s.getCapacity() here to avoid moving blocks towards nodes with
        // little actual available space.
        // The util is computed as blockPoolUsed/(remaining+blockPoolUsed),
        // which means nodes with more remaining space and less blockPoolUsed
        // will serve as the recipient during the balancing process.
        totalCapacities.add(t, s.getRemaining() + s.getBlockPoolUsed());
        totalUsedSpaces.add(t, s.getBlockPoolUsed());
      }
    }

    @Override
    Double getUtilization(DatanodeStorageReport r, final StorageType t) {
      long capacity = 0L;
      long blockPoolUsed = 0L;
      for(StorageReport s : r.getStorageReports()) {
        if (s.getStorage().getStorageType() == t) {
          capacity += s.getRemaining() + s.getBlockPoolUsed();
          blockPoolUsed += s.getBlockPoolUsed();
        }
      }
      return capacity == 0L ? null : blockPoolUsed * 100.0 / capacity;
    }
  }
}
