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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.net.Node;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extension of DFSNetworkTopology that tracks the number of non-empty datacenters.
 * This is used by BlockPlacementPolicyCrossDC to efficiently get the datacenter count
 * without iterating through all nodes on every verifyBlockPlacement call.
 *
 * Thread-safe: The add/remove operations are protected by netlock.writeLock() to ensure
 * atomicity between datacenterNodeCounts updates and the parent topology changes.
 */
public class DFSNetworkTopologyWithDatacenterCount extends DFSNetworkTopology {

  // Map from datacenter name to node count in that datacenter
  private final ConcurrentHashMap<String, AtomicInteger> datacenterNodeCounts =
      new ConcurrentHashMap<>();

  @Override
  public void add(Node node) {
    netlock.writeLock().lock();
    try {
      beforeMapUpdateInAdd();
      if (node != null) {
        String dc = getDatacenter(node);
        datacenterNodeCounts.computeIfAbsent(dc, k -> new AtomicInteger(0))
            .incrementAndGet();
      }
      super.add(node);
    } finally {
      netlock.writeLock().unlock();
    }
  }

  @Override
  public void remove(Node node) {
    netlock.writeLock().lock();
    try {
      super.remove(node);
      if (node != null) {
        String dc = getDatacenter(node);
        AtomicInteger count = datacenterNodeCounts.get(dc);
        if (count != null) {
          int newCount = count.decrementAndGet();
          afterDecrementInRemove();
          if (newCount <= 0) {
            datacenterNodeCounts.remove(dc, count);
          }
        }
      }
    } finally {
      netlock.writeLock().unlock();
    }
  }

  /** Hook for testing - called before map update in add(). */
  protected void beforeMapUpdateInAdd() {
    // No-op in production. Overridden in tests to inject delay.
  }

  /** Hook for testing - called after decrement in remove(). */
  protected void afterDecrementInRemove() {
    // No-op in production. Overridden in tests to inject delay.
  }

  /**
   * Get the number of non-empty datacenters in the cluster.
   * @return the number of datacenters that have at least one datanode
   */
  public int getNumOfNonEmptyDatacenters() {
    return datacenterNodeCounts.size();
  }

  public int getNumOfNodesInDatacenter(String dc) {
    AtomicInteger count = datacenterNodeCounts.get(dc);
    return count == null ? 0 : count.get();
  }

  /**
   * Extract datacenter from node's network location.
   * Network location format: /datacenter/rack
   *
   * @param node Node to extract datacenter from
   * @return Datacenter string (e.g., "/dc1")
   */
  public static String getDatacenter(Node node) {
    if (node == null) {
      return "/default-dc";
    }

    String location = node.getNetworkLocation();
    if (location == null || location.isEmpty()) {
      return "/default-dc";
    }

    // Extract datacenter from network location
    // "/dc1/rack1" -> "/dc1"
    int secondSlash = location.indexOf('/', 1);
    if (secondSlash > 0) {
      return location.substring(0, secondSlash);
    }

    return location;
  }
}