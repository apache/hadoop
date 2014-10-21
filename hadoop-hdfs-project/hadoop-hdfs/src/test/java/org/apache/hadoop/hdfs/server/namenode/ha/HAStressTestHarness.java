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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;

/**
 * Utility class to start an HA cluster, and then start threads
 * to periodically fail back and forth, accelerate block deletion
 * processing, etc.
 */
public class HAStressTestHarness {
  final Configuration conf;
  private MiniDFSCluster cluster;
  static final int BLOCK_SIZE = 1024;
  final TestContext testCtx = new TestContext();
  
  public HAStressTestHarness() {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    // Increase max streams so that we re-replicate quickly.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 16);
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY, 16);
  }

  /**
   * Start and return the MiniDFSCluster.
   */
  public MiniDFSCluster startCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    return cluster;
  }

  /**
   * Return a filesystem with client-failover configured for the
   * cluster.
   */
  public FileSystem getFailoverFs() throws IOException, URISyntaxException {
    return HATestUtil.configureFailoverFs(cluster, conf);
  }

  /**
   * Add a thread which periodically triggers deletion reports,
   * heartbeats, and NN-side block work.
   * @param interval millisecond period on which to run
   */
  public void addReplicationTriggerThread(final int interval) {

    testCtx.addThread(new RepeatingTestThread(testCtx) {
      
      @Override
      public void doAnAction() throws Exception {
        for (DataNode dn : cluster.getDataNodes()) {
          DataNodeTestUtils.triggerDeletionReport(dn);
          DataNodeTestUtils.triggerHeartbeat(dn);
        }
        for (int i = 0; i < 2; i++) {
          NameNode nn = cluster.getNameNode(i);
          BlockManagerTestUtil.computeAllPendingWork(
              nn.getNamesystem().getBlockManager());
        }
        Thread.sleep(interval);
      }
    });
  }

  /**
   * Add a thread which periodically triggers failover back and forth between
   * the two namenodes.
   */
  public void addFailoverThread(final int msBetweenFailovers) {
    testCtx.addThread(new RepeatingTestThread(testCtx) {
      
      @Override
      public void doAnAction() throws Exception {
        System.err.println("==============================\n" +
            "Failing over from 0->1\n" +
            "==================================");
        cluster.transitionToStandby(0);
        cluster.transitionToActive(1);
        
        Thread.sleep(msBetweenFailovers);
        System.err.println("==============================\n" +
            "Failing over from 1->0\n" +
            "==================================");

        cluster.transitionToStandby(1);
        cluster.transitionToActive(0);
        Thread.sleep(msBetweenFailovers);
      }
    });
  }

  /**
   * Start all of the threads which have been added.
   */
  public void startThreads() {
    this.testCtx.startThreads();
  }

  /**
   * Stop threads, propagating any exceptions that might have been thrown.
   */
  public void stopThreads() throws Exception {
    this.testCtx.stop();
  }

  /**
   * Shutdown the minicluster, as well as any of the running threads.
   */
  public void shutdown() throws Exception {
    this.testCtx.stop();
    if (cluster != null) {
      this.cluster.shutdown();
      cluster = null;
    }
  }
}
