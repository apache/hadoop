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

import junit.framework.TestCase;
import java.lang.System;

import org.apache.hadoop.hdfs.protocol.Block;

/**
 * This class tests the internals of PendingReplicationBlocks.java,
 * as well as how PendingReplicationBlocks acts in BlockManager
 *
 */
public class TestPendingReplication extends TestCase {
  final static int TIMEOUT = 3;     // 3 seconds

  public void testPendingReplication() {
    PendingReplicationBlocks pendingReplications;
    pendingReplications = new PendingReplicationBlocks(TIMEOUT * 1000);
    pendingReplications.start();

    //
    // Add 10 blocks to pendingReplications.
    //
    for (int i = 0; i < 10; i++) {
      Block block = new Block(i, i, 0);
      pendingReplications.increment(block, i);
    }
    assertEquals("Size of pendingReplications ",
                 10, pendingReplications.size());


    //
    // remove one item and reinsert it
    //
    Block blk = new Block(8, 8, 0);
    pendingReplications.decrement(blk);             // removes one replica
    assertEquals("pendingReplications.getNumReplicas ",
                 7, pendingReplications.getNumReplicas(blk));

    for (int i = 0; i < 7; i++) {
      pendingReplications.decrement(blk);           // removes all replicas
    }
    assertTrue(pendingReplications.size() == 9);
    pendingReplications.increment(blk, 8);
    assertTrue(pendingReplications.size() == 10);

    //
    // verify that the number of replicas returned
    // are sane.
    //
    for (int i = 0; i < 10; i++) {
      Block block = new Block(i, i, 0);
      int numReplicas = pendingReplications.getNumReplicas(block);
      assertTrue(numReplicas == i);
    }

    //
    // verify that nothing has timed out so far
    //
    assertTrue(pendingReplications.getTimedOutBlocks() == null);

    //
    // Wait for one second and then insert some more items.
    //
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    for (int i = 10; i < 15; i++) {
      Block block = new Block(i, i, 0);
      pendingReplications.increment(block, i);
    }
    assertTrue(pendingReplications.size() == 15);

    //
    // Wait for everything to timeout.
    //
    int loop = 0;
    while (pendingReplications.size() > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      loop++;
    }
    System.out.println("Had to wait for " + loop +
                       " seconds for the lot to timeout");

    //
    // Verify that everything has timed out.
    //
    assertEquals("Size of pendingReplications ",
                 0, pendingReplications.size());
    Block[] timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue(timedOut != null && timedOut.length == 15);
    for (int i = 0; i < timedOut.length; i++) {
      assertTrue(timedOut[i].getBlockId() < 15);
    }
    pendingReplications.stop();
  }
}
