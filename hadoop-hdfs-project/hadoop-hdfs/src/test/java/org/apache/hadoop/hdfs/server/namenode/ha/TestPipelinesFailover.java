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

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.TestDFSClientFailover;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test cases regarding pipeline recovery during NN failover.
 */
public class TestPipelinesFailover {
  static {
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(BlockManager.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(
        "org.apache.hadoop.io.retry.RetryInvocationHandler")).getLogger().setLevel(Level.ALL);

    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
  }
  
  protected static final Log LOG = LogFactory.getLog(
      TestPipelinesFailover.class);
  private static final Path TEST_PATH =
    new Path("/test-file");
  private static final int BLOCK_SIZE = 4096;
  private static final int BLOCK_AND_A_HALF = BLOCK_SIZE * 3 / 2;

  /**
   * Tests continuing a write pipeline over a failover.
   */
  @Test(timeout=30000)
  public void testWriteOverFailover() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    // Don't check replication periodically.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1000);
    
    FSDataOutputStream stm = null;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a block and a half
      AppendTestUtil.write(stm, 0, BLOCK_AND_A_HALF);
      
      // Make sure all of the blocks are written out before failover.
      stm.hflush();

      LOG.info("Failing over to NN 1");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      assertTrue(fs.exists(TEST_PATH));
      FSNamesystem ns1 = cluster.getNameNode(1).getNamesystem();
      BlockManagerTestUtil.updateState(ns1.getBlockManager());
      assertEquals(0, ns1.getPendingReplicationBlocks());
      assertEquals(0, ns1.getCorruptReplicaBlocks());
      assertEquals(0, ns1.getMissingBlocksCount());

      // write another block and a half
      AppendTestUtil.write(stm, BLOCK_AND_A_HALF, BLOCK_AND_A_HALF);

      stm.close();
      stm = null;
      
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_SIZE * 3);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }
  
  /**
   * Tests continuing a write pipeline over a failover when a DN fails
   * after the failover - ensures that updating the pipeline succeeds
   * even when the pipeline was constructed on a different NN.
   */
  @Test(timeout=30000)
  public void testWriteOverFailoverWithDnFail() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    
    FSDataOutputStream stm = null;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(5)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a block and a half
      AppendTestUtil.write(stm, 0, BLOCK_AND_A_HALF);
      
      // Make sure all the blocks are written before failover
      stm.hflush();

      LOG.info("Failing over to NN 1");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      assertTrue(fs.exists(TEST_PATH));
      
      cluster.stopDataNode(0);

      // write another block and a half
      AppendTestUtil.write(stm, BLOCK_AND_A_HALF, BLOCK_AND_A_HALF);
      stm.hflush(); // TODO: see above
      
      LOG.info("Failing back to NN 0");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
      cluster.stopDataNode(1);
      
      AppendTestUtil.write(stm, BLOCK_AND_A_HALF*2, BLOCK_AND_A_HALF);
      stm.hflush(); // TODO: see above
      
      
      stm.close();
      stm = null;
      
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_AND_A_HALF * 3);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }
  
  /**
   * Tests lease recovery if a client crashes. This approximates the
   * use case of HBase WALs being recovered after a NN failover.
   */
  @Test(timeout=30000)
  public void testLeaseRecoveryAfterFailover() throws Exception {
    final Configuration conf = new Configuration();
    // Disable permissions so that another user can recover the lease.
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    
    FSDataOutputStream stm = null;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);

      LOG.info("Starting with NN 0 active");
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      stm = fs.create(TEST_PATH);
      
      // write a block and a half
      AppendTestUtil.write(stm, 0, BLOCK_AND_A_HALF);
      stm.hflush();
      
      LOG.info("Failing over to NN 1");
      
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
      assertTrue(fs.exists(TEST_PATH));
      
      FileSystem fsOtherUser = UserGroupInformation.createUserForTesting(
          "otheruser", new String[] { "othergroup"})
          .doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
              return HATestUtil.configureFailoverFs(cluster, conf);
            }
          });
      ((DistributedFileSystem)fsOtherUser).recoverLease(TEST_PATH);
      
      AppendTestUtil.check(fs, TEST_PATH, BLOCK_AND_A_HALF);
    } finally {
      IOUtils.closeStream(stm);
      cluster.shutdown();
    }
  }

}
