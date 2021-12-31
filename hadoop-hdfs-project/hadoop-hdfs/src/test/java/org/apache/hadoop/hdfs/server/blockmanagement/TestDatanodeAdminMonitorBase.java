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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.hadoop.hdfs.protocol.DatanodeID;

public class TestDatanodeAdminMonitorBase {
  public static final Logger LOG = LoggerFactory.getLogger(TestDatanodeAdminMonitorBase.class);

  // Sort by lastUpdate time descending order, such that unhealthy
  // nodes are de-prioritized given they cannot be decommissioned.
  private static final int NUM_DATANODE = 10;
  private static final int[] UNORDERED_LAST_UPDATE_TIMES =
      new int[] {0, 5, 2, 11, 0, 3, 1001, 5, 1, 103};
  private static final int[] ORDERED_LAST_UPDATE_TIMES =
      new int[] {1001, 103, 11, 5, 5, 3, 2, 1, 0, 0};
  private static final int[] REVERSE_ORDER_LAST_UPDATE_TIMES =
      new int[] {0, 0, 1, 2, 3, 5, 5, 11, 103, 1001};

  private static final DatanodeDescriptor[] NODES;

  static {
    NODES = new DatanodeDescriptor[NUM_DATANODE];
    for (int i = 0; i < NUM_DATANODE; i++) {
      NODES[i] = new DatanodeDescriptor(DatanodeID.EMPTY_DATANODE_ID);
      NODES[i].setLastUpdate(UNORDERED_LAST_UPDATE_TIMES[i]);
      NODES[i].setLastUpdateMonotonic(UNORDERED_LAST_UPDATE_TIMES[i]);
    }
  }

  /**
   * Verify that DatanodeAdminManager pendingNodes priority queue
   * correctly orders the nodes by lastUpdate time descending.
   */
  @Test
  public void testPendingNodesQueueOrdering() {
    final PriorityQueue<DatanodeDescriptor> pendingNodes =
        new PriorityQueue<>(DatanodeAdminMonitorBase.PENDING_NODES_QUEUE_COMPARATOR);

    pendingNodes.addAll(Arrays.asList(NODES));

    for (int i = 0; i < NUM_DATANODE; i++) {
      final DatanodeDescriptor dn = pendingNodes.poll();
      Assert.assertNotNull(dn);
      Assert.assertEquals(ORDERED_LAST_UPDATE_TIMES[i], dn.getLastUpdate());
    }
  }

  /**
   * Verify that DatanodeAdminManager logic to sort unhealthy nodes
   * correctly orders the nodes by lastUpdate time ascending.
   */
  @Test
  public void testPendingNodesQueueReverseOrdering() {
    final List<DatanodeDescriptor> nodes = Arrays.asList(NODES);
    final List<DatanodeDescriptor> reverseOrderNodes =
        nodes.stream().sorted(DatanodeAdminMonitorBase.PENDING_NODES_QUEUE_COMPARATOR.reversed())
            .collect(Collectors.toList());

    Assert.assertEquals(NUM_DATANODE, reverseOrderNodes.size());
    for (int i = 0; i < NUM_DATANODE; i++) {
      Assert.assertEquals(REVERSE_ORDER_LAST_UPDATE_TIMES[i],
          reverseOrderNodes.get(i).getLastUpdate());
    }
  }
}
