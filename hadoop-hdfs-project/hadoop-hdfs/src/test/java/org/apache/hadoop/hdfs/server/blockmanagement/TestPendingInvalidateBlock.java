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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Test if we can correctly delay the deletion of blocks.
 */
public class TestPendingInvalidateBlock {
  {
    ((Log4JLogger)BlockManager.LOG).getLogger().setLevel(Level.DEBUG);    
  }

  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 2;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    // block deletion pending period
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY, 5L);
    // set the block report interval to 2s
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 2000);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    // disable the RPC timeout for debug
    conf.setLong(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPendingDeletion() throws Exception {
    final Path foo = new Path("/foo");
    DFSTestUtil.createFile(dfs, foo, BLOCKSIZE, REPLICATION, 0);
    // restart NN
    cluster.restartNameNode(true);
    dfs.delete(foo, true);
    Assert.assertEquals(0, cluster.getNamesystem().getBlocksTotal());
    Assert.assertEquals(REPLICATION, cluster.getNamesystem()
        .getPendingDeletionBlocks());
    Thread.sleep(6000);
    Assert.assertEquals(0, cluster.getNamesystem().getBlocksTotal());
    Assert.assertEquals(0, cluster.getNamesystem().getPendingDeletionBlocks());
  }

  /**
   * Test whether we can delay the deletion of unknown blocks in DataNode's
   * first several block reports.
   */
  @Test
  public void testPendingDeleteUnknownBlocks() throws Exception {
    final int fileNum = 5; // 5 files
    final Path[] files = new Path[fileNum];
    final DataNodeProperties[] dnprops = new DataNodeProperties[REPLICATION];
    // create a group of files, each file contains 1 block
    for (int i = 0; i < fileNum; i++) {
      files[i] = new Path("/file" + i);
      DFSTestUtil.createFile(dfs, files[i], BLOCKSIZE, REPLICATION, i);
    }
    // wait until all DataNodes have replicas
    waitForReplication();
    for (int i = REPLICATION - 1; i >= 0; i--) {
      dnprops[i] = cluster.stopDataNode(i);
    }
    Thread.sleep(2000);
    // delete 2 files, we still have 3 files remaining so that we can cover
    // every DN storage
    for (int i = 0; i < 2; i++) {
      dfs.delete(files[i], true);
    }

    // restart NameNode
    cluster.restartNameNode(false);
    InvalidateBlocks invalidateBlocks = (InvalidateBlocks) Whitebox
        .getInternalState(cluster.getNamesystem().getBlockManager(),
            "invalidateBlocks");
    InvalidateBlocks mockIb = Mockito.spy(invalidateBlocks);
    Mockito.doReturn(1L).when(mockIb).getInvalidationDelay();
    Whitebox.setInternalState(cluster.getNamesystem().getBlockManager(),
        "invalidateBlocks", mockIb);

    Assert.assertEquals(0L, cluster.getNamesystem().getPendingDeletionBlocks());
    // restart DataNodes
    for (int i = 0; i < REPLICATION; i++) {
      cluster.restartDataNode(dnprops[i], true);
    }
    cluster.waitActive();

    for (int i = 0; i < REPLICATION; i++) {
      DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(i));
    }
    Thread.sleep(2000);
    // make sure we have received block reports by checking the total block #
    Assert.assertEquals(3, cluster.getNamesystem().getBlocksTotal());
    Assert.assertEquals(4, cluster.getNamesystem().getPendingDeletionBlocks());

    cluster.restartNameNode(true);
    Thread.sleep(6000);
    Assert.assertEquals(3, cluster.getNamesystem().getBlocksTotal());
    Assert.assertEquals(0, cluster.getNamesystem().getPendingDeletionBlocks());
  }

  private long waitForReplication() throws Exception {
    for (int i = 0; i < 10; i++) {
      long ur = cluster.getNamesystem().getUnderReplicatedBlocks();
      if (ur == 0) {
        return 0;
      } else {
        Thread.sleep(1000);
      }
    }
    return cluster.getNamesystem().getUnderReplicatedBlocks();
  }

}
