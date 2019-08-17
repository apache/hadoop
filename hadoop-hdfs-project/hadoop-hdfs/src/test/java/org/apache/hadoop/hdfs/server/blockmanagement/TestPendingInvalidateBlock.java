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

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


import com.google.common.base.Supplier;

/**
 * Test if we can correctly delay the deletion of blocks.
 */
public class TestPendingInvalidateBlock {
  {
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.DEBUG);
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
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
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
      cluster = null;
    }
  }

  @Test
  public void testPendingDeletion() throws Exception {
    final Path foo = new Path("/foo");
    DFSTestUtil.createFile(dfs, foo, BLOCKSIZE, REPLICATION, 0);
    DFSTestUtil.waitForReplication(dfs, foo, REPLICATION, 10000);

    // restart NN
    cluster.restartNameNode(true);
    InvalidateBlocks invalidateBlocks =
        (InvalidateBlocks) Whitebox.getInternalState(cluster.getNamesystem()
            .getBlockManager(), "invalidateBlocks");
    InvalidateBlocks mockIb = Mockito.spy(invalidateBlocks);
    // Return invalidation delay to delay the block's deletion
    Mockito.doReturn(1L).when(mockIb).getInvalidationDelay();
    Whitebox.setInternalState(cluster.getNamesystem().getBlockManager(),
        "invalidateBlocks", mockIb);
    dfs.delete(foo, true);

    waitForNumPendingDeletionBlocks(REPLICATION);
    Assert.assertEquals(0, cluster.getNamesystem().getBlocksTotal());
    Assert.assertEquals(REPLICATION, cluster.getNamesystem()
        .getPendingDeletionBlocks());
    Assert.assertEquals(REPLICATION,
        dfs.getPendingDeletionBlocksCount());
    Mockito.doReturn(0L).when(mockIb).getInvalidationDelay();

    waitForNumPendingDeletionBlocks(0);
    Assert.assertEquals(0, cluster.getNamesystem().getBlocksTotal());
    Assert.assertEquals(0, cluster.getNamesystem().getPendingDeletionBlocks());
    Assert.assertEquals(0, dfs.getPendingDeletionBlocksCount());
    long nnStarted = cluster.getNamesystem().getNNStartedTimeInMillis();
    long blockDeletionStartTime = cluster.getNamesystem()
        .getBlockDeletionStartTime();
    Assert.assertTrue(String.format(
        "Expect blockDeletionStartTime = %d > nnStarted = %d.",
        blockDeletionStartTime, nnStarted), blockDeletionStartTime > nnStarted);

    // test client protocol compatibility
    Method method = DFSClient.class.
        getDeclaredMethod("getStateByIndex", int.class);
    method.setAccessible(true);
    // get number of pending deletion blocks by its index
    long validState = (Long) method.invoke(dfs.getClient(),
        ClientProtocol.GET_STATS_PENDING_DELETION_BLOCKS_IDX);
    // get an out of index value
    long invalidState = (Long) method.invoke(dfs.getClient(),
        ClientProtocol.STATS_ARRAY_LENGTH);
    Assert.assertEquals(0, validState);
    Assert.assertEquals(-1, invalidState);
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
      cluster.restartDataNode(dnprops[i]);
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
    waitForNumPendingDeletionBlocks(0);
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

  private void waitForNumPendingDeletionBlocks(final int numBlocks)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          cluster.triggerBlockReports();

          if (cluster.getNamesystem().getPendingDeletionBlocks()
              == numBlocks) {
            return true;
          }
        } catch (Exception e) {
          // Ignore the exception
        }

        return false;
      }
    }, 6000, 60000);
  }
}
