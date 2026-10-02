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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link DFSNetworkTopologyWithDatacenterCount},
 * focusing on concurrent add/remove correctness.
 *
 * <p>The key race condition without proper locking:
 * <pre>
 * Thread R (remove last node)           Thread A (add new node)
 * ─────────────────────────             ──────────────────────
 * count.decrementAndGet() → 0
 *                                       computeIfAbsent → same AtomicInteger(0)
 *                                       incrementAndGet() → 1
 * map.remove(dc, count) → deletes!
 *   (same reference, so removes it)
 *
 * Result: node exists in dc1 but map entry is lost.
 * </pre>
 *
 * <p>The fix wraps the entire add/remove (including datacenterNodeCounts
 * updates) in {@code netlock.writeLock()}. The lock verification tests
 * use a subclass that checks {@code isWriteLockedByCurrentThread()}
 * during the overridden add/remove, so they fail deterministically
 * if the lock is ever removed.
 */
public class TestDFSNetworkTopologyWithDatacenterCount {

  private DFSNetworkTopologyWithDatacenterCount cluster;

  /**
   * Subclass that verifies the write lock is held with hold count >= 2 during
   * the NetworkTopology.add()/remove() call. This indicates that
   * DFSNetworkTopologyWithDatacenterCount has acquired an outer lock
   * before calling super.add()/super.remove().
   *
   * <p>When DFSNetworkTopologyWithDatacenterCount.add() properly holds the
   * write lock around the entire operation:
   * <ol>
   *   <li>DFSNetworkTopologyWithDatacenterCount.add() acquires lock (count=1)</li>
   *   <li>It calls super.add() → NetworkTopology.add()</li>
   *   <li>NetworkTopology.add() acquires lock again (count=2, reentrant)</li>
   *   <li>NetworkTopology.add() calls incrementRacks() if new rack</li>
   *   <li>We check hold count here - should be 2</li>
   * </ol>
   *
   * <p>If the outer lock is removed, hold count would be only 1 during
   * incrementRacks(), because only NetworkTopology.add()'s lock is held.
   */
  static class LockVerifyingTopology
      extends DFSNetworkTopologyWithDatacenterCount {

    volatile int maxWriteHoldCountDuringAdd = 0;
    volatile int maxWriteHoldCountDuringRemove = 0;

    static LockVerifyingTopology create() {
      LockVerifyingTopology topology = new LockVerifyingTopology();
      topology.init(DFSTopologyNodeImpl.FACTORY);
      return topology;
    }

    private int getWriteHoldCount() {
      return ((ReentrantReadWriteLock) netlock).getWriteHoldCount();
    }

    /**
     * Called by NetworkTopology.add() when a new rack is added.
     * At this point, if outer lock is held, hold count should be 2.
     */
    @Override
    protected void incrementRacks() {
      maxWriteHoldCountDuringAdd = Math.max(
          maxWriteHoldCountDuringAdd, getWriteHoldCount());
      super.incrementRacks();
    }

    /**
     * Called by NetworkTopology.remove() to get the rack node.
     * At this point, if outer lock is held, hold count should be 2.
     * (Note: getNode acquires readLock, but ReentrantReadWriteLock allows
     * write lock holder to acquire read lock, so this is safe)
     */
    @Override
    public Node getNode(String loc) {
      maxWriteHoldCountDuringRemove = Math.max(
          maxWriteHoldCountDuringRemove, getWriteHoldCount());
      return super.getNode(loc);
    }
  }

  /**
   * Subclass that injects delay at critical points to widen race windows.
   * This is used to make race condition tests deterministic.
   */
  static class DelayInjectingTopology
      extends DFSNetworkTopologyWithDatacenterCount {

    private final int delayMs;

    DelayInjectingTopology(int delayMs) {
      this.delayMs = delayMs;
    }

    static DelayInjectingTopology create(int delayMs) {
      DelayInjectingTopology topology = new DelayInjectingTopology(delayMs);
      topology.init(DFSTopologyNodeImpl.FACTORY);
      return topology;
    }

    private void delay() {
      if (delayMs > 0) {
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    protected void beforeMapUpdateInAdd() {
      delay();
    }

    @Override
    protected void afterDecrementInRemove() {
      delay();
    }
  }

  private static DFSNetworkTopologyWithDatacenterCount createCluster() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDatacenterCount.class.getName());
    return (DFSNetworkTopologyWithDatacenterCount)
        DFSNetworkTopology.getInstance(conf);
  }

  @BeforeEach
  public void setUp() {
    cluster = createCluster();
  }

  private DatanodeDescriptor createDatanode(int id, String rack) {
    String ip = id + "." + id + "." + id + "." + id;
    DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
        "s" + id, ip, rack, "host" + id);
    return storage.getDatanodeDescriptor();
  }

  @Test
  public void testBasicAddRemove() {
    DatanodeDescriptor dn1 = createDatanode(1, "/dc1/rack1");
    DatanodeDescriptor dn2 = createDatanode(2, "/dc1/rack2");
    DatanodeDescriptor dn3 = createDatanode(3, "/dc2/rack1");

    cluster.add(dn1);
    cluster.add(dn2);
    cluster.add(dn3);

    assertEquals(2, cluster.getNumOfNonEmptyDatacenters());
    assertEquals(2, cluster.getNumOfNodesInDatacenter("/dc1"));
    assertEquals(1, cluster.getNumOfNodesInDatacenter("/dc2"));

    cluster.remove(dn1);
    assertEquals(2, cluster.getNumOfNonEmptyDatacenters());
    assertEquals(1, cluster.getNumOfNodesInDatacenter("/dc1"));

    cluster.remove(dn2);
    assertEquals(1, cluster.getNumOfNonEmptyDatacenters());
    assertEquals(0, cluster.getNumOfNodesInDatacenter("/dc1"));

    cluster.remove(dn3);
    assertEquals(0, cluster.getNumOfNonEmptyDatacenters());
  }

  /**
   * Verifies the write lock is held during add()'s datacenterNodeCounts
   * update. If someone removes the netlock from add(), this test fails
   * immediately and deterministically.
   *
   * <p>We check that write hold count is >= 2 during incrementRacks(),
   * which means both DFSNetworkTopologyWithDatacenterCount.add() AND
   * NetworkTopology.add() have acquired the lock (reentrant).
   * If the outer lock is removed, hold count would be only 1.
   */
  @Test
  public void testAddHoldsWriteLockDuringMapUpdate() {
    LockVerifyingTopology topology = LockVerifyingTopology.create();

    // Add first node to a new rack - this triggers incrementRacks()
    DatanodeDescriptor dn = createDatanode(1, "/dc1/rack1");
    topology.add(dn);

    assertTrue(topology.maxWriteHoldCountDuringAdd >= 2,
        "write lock hold count must be >= 2 during add(), "
        + "indicating outer lock is held. Actual: "
        + topology.maxWriteHoldCountDuringAdd);
  }

  /**
   * Verifies the write lock is held during remove()'s datacenterNodeCounts
   * update. If someone removes the netlock from remove(), this test fails
   * immediately and deterministically.
   *
   * <p>We check that write hold count is >= 2 during getNode() call
   * inside NetworkTopology.remove(), which means both
   * DFSNetworkTopologyWithDatacenterCount.remove() AND
   * NetworkTopology.remove() have acquired the lock (reentrant).
   * If the outer lock is removed, hold count would be only 1.
   */
  @Test
  public void testRemoveHoldsWriteLockDuringMapUpdate() {
    LockVerifyingTopology topology = LockVerifyingTopology.create();

    DatanodeDescriptor dn = createDatanode(1, "/dc1/rack1");
    topology.add(dn);
    topology.maxWriteHoldCountDuringRemove = 0; // reset
    topology.remove(dn);

    assertTrue(topology.maxWriteHoldCountDuringRemove >= 2,
        "write lock hold count must be >= 2 during remove(), "
        + "indicating outer lock is held. Actual: "
        + topology.maxWriteHoldCountDuringRemove);
  }

  /**
   * Stress test targeting the specific race condition:
   * One thread removes dc1's last node while another adds a new node to dc1.
   *
   * <p>Without proper locking:
   * <ol>
   *   <li>Remover: count.decrementAndGet() → 0</li>
   *   <li>Adder: computeIfAbsent returns SAME AtomicInteger, incrementAndGet → 1</li>
   *   <li>Remover: map.remove(dc, count) deletes the entry!</li>
   *   <li>Result: dc1 has a node but datacenterNodeCounts has no entry</li>
   * </ol>
   *
   * <p>Uses {@link DelayInjectingTopology} to widen the race window and make
   * the test deterministic. With proper locking, all operations are serialized
   * and no inconsistency occurs even with the delay. Without locking, the delay
   * guarantees the race is hit.
   */
  @Test
  @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
  public void testStressConcurrentAddRemoveCycles() throws Exception {
    final int iterations = 100;
    int inconsistentCount = 0;

    for (int iter = 0; iter < iterations; iter++) {
      // Create topology with delay injection for race testing
      DelayInjectingTopology localCluster = DelayInjectingTopology.create(10);

      // Start with exactly 1 node in dc1
      int baseId = iter * 10;
      DatanodeDescriptor existingNode = createDatanode(baseId + 1, "/dc1/rack1");
      DatanodeDescriptor newNode = createDatanode(baseId + 2, "/dc1/rack1");

      localCluster.add(existingNode);

      CyclicBarrier barrier = new CyclicBarrier(2);

      Thread remover = new Thread(() -> {
        try {
          barrier.await();
          localCluster.remove(existingNode);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Thread adder = new Thread(() -> {
        try {
          barrier.await();
          localCluster.add(newNode);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      remover.start();
      adder.start();
      remover.join();
      adder.join();

      // After: existingNode removed, newNode added
      // dc1 should have exactly 1 node (newNode)
      int reportedCount = localCluster.getNumOfNodesInDatacenter("/dc1");
      boolean nodeExists = localCluster.contains(newNode);

      // The race manifests as: node exists but count is wrong
      if (nodeExists && reportedCount != 1) {
        inconsistentCount++;
      }
    }

    assertEquals(0, inconsistentCount,
        "datacenterNodeCounts became inconsistent with actual nodes in "
        + inconsistentCount + " out of " + iterations + " iterations");
  }

  /**
   * Concurrently remove all nodes from a datacenter while reading
   * datacenter counts, verifying no negative counts or stale entries.
   */
  @Test
  @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
  public void testConcurrentRemoveAllWithRead() throws Exception {
    final int nodeCount = 100;

    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[nodeCount];
    for (int i = 0; i < nodeCount; i++) {
      datanodes[i] = createDatanode(i + 1, "/dc1/rack1");
      cluster.add(datanodes[i]);
    }

    DatanodeDescriptor dc2Node = createDatanode(nodeCount + 1, "/dc2/rack1");
    cluster.add(dc2Node);

    CyclicBarrier barrier = new CyclicBarrier(nodeCount + 1);
    ExecutorService executor = Executors.newFixedThreadPool(nodeCount + 1);
    List<Future<?>> futures = new ArrayList<>();

    for (DatanodeDescriptor dn : datanodes) {
      futures.add(executor.submit(() -> {
        try {
          barrier.await();
          cluster.remove(dn);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    futures.add(executor.submit(() -> {
      try {
        barrier.await();
        for (int i = 0; i < 1000; i++) {
          int dcCount = cluster.getNumOfNonEmptyDatacenters();
          int dc1Nodes = cluster.getNumOfNodesInDatacenter("/dc1");
          if (dc1Nodes < 0) {
            fail("dc1 node count went negative: " + dc1Nodes);
          }
          if (dcCount < 1 || dcCount > 2) {
            fail("unexpected datacenter count: " + dcCount);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));

    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();

    assertEquals(0, cluster.getNumOfNodesInDatacenter("/dc1"));
    assertEquals(1, cluster.getNumOfNonEmptyDatacenters());
    assertEquals(1, cluster.getNumOfNodesInDatacenter("/dc2"));
  }
}