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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestBlockReportRateLimiting {
  static final Logger LOG =
      LoggerFactory.getLogger(TestBlockReportRateLimiting.class);

  private static void setFailure(AtomicReference<String> failure,
                                 String what) {
    failure.compareAndSet("", what);
    LOG.error("Test error: " + what);
  }

  @After
  public void restoreNormalBlockManagerFaultInjector() {
    BlockManagerFaultInjector.instance = new BlockManagerFaultInjector();
  }

  @BeforeClass
  public static void raiseBlockManagerLogLevels() {
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockReportLeaseManager.LOG, Level.ALL);
  }

  @Test(timeout=180000)
  public void testRateLimitingDuringDataNodeStartup() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES, 1);
    conf.setLong(DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
        20L * 60L * 1000L);

    final Semaphore fbrSem = new Semaphore(0);
    final HashSet<DatanodeID> expectedFbrDns = new HashSet<>();
    final HashSet<DatanodeID> fbrDns = new HashSet<>();
    final AtomicReference<String> failure = new AtomicReference<String>("");

    final BlockManagerFaultInjector injector = new BlockManagerFaultInjector() {
      private int numLeases = 0;

      @Override
      public void incomingBlockReportRpc(DatanodeID nodeID,
                    BlockReportContext context) throws IOException {
        LOG.info("Incoming full block report from " + nodeID +
            ".  Lease ID = 0x" + Long.toHexString(context.getLeaseId()));
        if (context.getLeaseId() == 0) {
          setFailure(failure, "Got unexpected rate-limiting-" +
              "bypassing full block report RPC from " + nodeID);
        }
        fbrSem.acquireUninterruptibly();
        synchronized (this) {
          fbrDns.add(nodeID);
          if (!expectedFbrDns.remove(nodeID)) {
            setFailure(failure, "Got unexpected full block report " +
                "RPC from " + nodeID + ".  expectedFbrDns = " +
                Joiner.on(", ").join(expectedFbrDns));
          }
          LOG.info("Proceeding with full block report from " +
              nodeID + ".  Lease ID = 0x" +
              Long.toHexString(context.getLeaseId()));
        }
      }

      @Override
      public void requestBlockReportLease(DatanodeDescriptor node,
                                          long leaseId) {
        if (leaseId == 0) {
          return;
        }
        synchronized (this) {
          numLeases++;
          expectedFbrDns.add(node);
          LOG.info("requestBlockReportLease(node=" + node +
              ", leaseId=0x" + Long.toHexString(leaseId) + ").  " +
              "expectedFbrDns = " +  Joiner.on(", ").join(expectedFbrDns));
          if (numLeases > 1) {
            setFailure(failure, "More than 1 lease was issued at once.");
          }
        }
      }

      @Override
      public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
        LOG.info("removeBlockReportLease(node=" + node +
                 ", leaseId=0x" + Long.toHexString(leaseId) + ")");
        synchronized (this) {
          numLeases--;
        }
      }
    };
    BlockManagerFaultInjector.instance = injector;

    final int NUM_DATANODES = 5;
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    for (int n = 1; n <= NUM_DATANODES; n++) {
      LOG.info("Waiting for " + n + " datanode(s) to report in.");
      fbrSem.release();
      Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
      final int currentN = n;
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          synchronized (injector) {
            if (fbrDns.size() > currentN) {
              setFailure(failure, "Expected at most " + currentN +
                  " datanodes to have sent a block report, but actually " +
                  fbrDns.size() + " have.");
            }
            return (fbrDns.size() >= currentN);
          }
        }
      }, 25, 50000);
    }
    cluster.shutdown();
    Assert.assertEquals("", failure.get());
  }

  /**
   * Start a 2-node cluster with only one block report lease.  When the
   * first datanode gets a lease, kill it.  Then wait for the lease to
   * expire, and the second datanode to send a full block report.
   */
  @Test(timeout=180000)
  public void testLeaseExpiration() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES, 1);
    conf.setLong(DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 100L);

    final Semaphore gotFbrSem = new Semaphore(0);
    final AtomicReference<String> failure = new AtomicReference<>();
    final AtomicReference<MiniDFSCluster> cluster =
        new AtomicReference<>();
    final AtomicReference<String> datanodeToStop = new AtomicReference<>();
    final BlockManagerFaultInjector injector = new BlockManagerFaultInjector() {

      @Override
      public void incomingBlockReportRpc(DatanodeID nodeID,
                BlockReportContext context) throws IOException {
        if (context.getLeaseId() == 0) {
          setFailure(failure, "Got unexpected rate-limiting-" +
              "bypassing full block report RPC from " + nodeID);
        }
        if (nodeID.getXferAddr().equals(datanodeToStop.get())) {
          throw new IOException("Injecting failure into block " +
              "report RPC for " + nodeID);
        }
        gotFbrSem.release();
      }

      @Override
      public void requestBlockReportLease(DatanodeDescriptor node,
                                          long leaseId) {
        if (leaseId == 0) {
          return;
        }
        datanodeToStop.compareAndSet(null, node.getXferAddr());
      }

      @Override
      public void removeBlockReportLease(DatanodeDescriptor node, long leaseId) {
      }
    };
    try {
      BlockManagerFaultInjector.instance = injector;
      cluster.set(new MiniDFSCluster.Builder(conf).numDataNodes(2).build());
      cluster.get().waitActive();
      Assert.assertNotNull(cluster.get().stopDataNode(datanodeToStop.get()));
      gotFbrSem.acquire();
      Assert.assertNull(failure.get());
    } finally {
      if (cluster.get() != null) {
        cluster.get().shutdown();
      }
    }
  }
}
