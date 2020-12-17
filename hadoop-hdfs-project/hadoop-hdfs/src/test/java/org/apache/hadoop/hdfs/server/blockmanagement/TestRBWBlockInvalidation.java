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

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils.MaterializedReplica;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.TestDNFencing.RandomDeleterPolicy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Test when RBW block is removed. Invalidation of the corrupted block happens
 * and then the under replicated block gets replicated to the datanode.
 */
public class TestRBWBlockInvalidation {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRBWBlockInvalidation.class);
  
  private static NumberReplicas countReplicas(final FSNamesystem namesystem,
      ExtendedBlock block) {
    final BlockManager blockManager = namesystem.getBlockManager();
    return blockManager.countNodes(blockManager.getStoredBlock(
        block.getLocalBlock()));
  }

  /**
   * Test when a block's replica is removed from RBW folder in one of the
   * datanode, namenode should ask to invalidate that corrupted block and
   * schedule replication for one more replica for that under replicated block.
   */
  @Test(timeout=600000)
  public void testBlockInvalidationWhenRBWReplicaMissedInDN()
      throws IOException, InterruptedException {
    // This test cannot pass on Windows due to file locking enforcement.  It will
    // reject the attempt to delete the block file from the RBW folder.
    assumeNotWindows();

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 300);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    FSDataOutputStream out = null;
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/tmp/TestRBWBlockInvalidation", "foo1");
      out = fs.create(testPath, (short) 2);
      out.writeBytes("HDFS-3157: " + testPath);
      out.hsync();
      cluster.startDataNodes(conf, 1, true, null, null, null);
      ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, testPath);

      // Delete partial block and its meta information from the RBW folder
      // of first datanode.
      MaterializedReplica replica = cluster.getMaterializedReplica(0, blk);

      replica.deleteData();
      replica.deleteMeta();

      out.close();
      
      int liveReplicas = 0;
      while (true) {
        if ((liveReplicas = countReplicas(namesystem, blk).liveReplicas()) < 2) {
          // This confirms we have a corrupt replica
          LOG.info("Live Replicas after corruption: " + liveReplicas);
          break;
        }
        Thread.sleep(100);
      }
      assertEquals("There should be less than 2 replicas in the "
          + "liveReplicasMap", 1, liveReplicas);
      
      while (true) {
        if ((liveReplicas =
              countReplicas(namesystem, blk).liveReplicas()) > 1) {
          //Wait till the live replica count becomes equal to Replication Factor
          LOG.info("Live Replicas after Rereplication: " + liveReplicas);
          break;
        }
        Thread.sleep(100);
      }
      assertEquals("There should be two live replicas", 2, liveReplicas);

      while (true) {
        Thread.sleep(100);
        if (countReplicas(namesystem, blk).corruptReplicas() == 0) {
          LOG.info("Corrupt Replicas becomes 0");
          break;
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
      cluster.shutdown();
    }
  }
  
  /**
   * Regression test for HDFS-4799, a case where, upon restart, if there
   * were RWR replicas with out-of-date genstamps, the NN could accidentally
   * delete good replicas instead of the bad replicas.
   */
  @Test(timeout=120000)
  public void testRWRInvalidation() throws Exception {
    Configuration conf = new HdfsConfiguration();

    // Set the deletion policy to be randomized rather than the default.
    // The default is based on disk space, which isn't controllable
    // in the context of the test, whereas a random one is more accurate
    // to what is seen in real clusters (nodes have random amounts of free
    // space)
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        RandomDeleterPolicy.class, BlockPlacementPolicy.class); 

    // Speed up the test a bit with faster heartbeats.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    int numFiles = 10;
    // Test with a bunch of separate files, since otherwise the test may
    // fail just due to "good luck", even if a bug is present.
    List<Path> testPaths = Lists.newArrayList();
    for (int i = 0; i < numFiles; i++) {
      testPaths.add(new Path("/test" + i));
    }
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    try {
      List<FSDataOutputStream> streams = Lists.newArrayList();
      try {
        // Open the test files and write some data to each
        for (Path path : testPaths) {
          FSDataOutputStream out = cluster.getFileSystem().create(path, (short)2);
          streams.add(out);

          out.writeBytes("old gs data\n");
          out.hflush();
        }

        for (Path path : testPaths) {
          DFSTestUtil.waitReplication(cluster.getFileSystem(), path, (short)2);
        }

        // Shutdown one of the nodes in the pipeline
        DataNodeProperties oldGenstampNode = cluster.stopDataNode(0);

        // Write some more data and flush again. This data will only
        // be in the latter genstamp copy of the blocks.
        for (int i = 0; i < streams.size(); i++) {
          Path path = testPaths.get(i);
          FSDataOutputStream out = streams.get(i);

          out.writeBytes("new gs data\n");
          out.hflush();

          // Set replication so that only one node is necessary for this block,
          // and close it.
          cluster.getFileSystem().setReplication(path, (short)1);
          out.close();
        }

        for (Path path : testPaths) {
          DFSTestUtil.waitReplication(cluster.getFileSystem(), path, (short)1);
        }

        // Upon restart, there will be two replicas, one with an old genstamp
        // and one current copy. This test wants to ensure that the old genstamp
        // copy is the one that is deleted.

        LOG.info("=========================== restarting cluster");
        DataNodeProperties otherNode = cluster.stopDataNode(0);
        cluster.restartNameNode();
        
        // Restart the datanode with the corrupt replica first.
        cluster.restartDataNode(oldGenstampNode);
        cluster.waitActive();

        // Then the other node
        cluster.restartDataNode(otherNode);
        cluster.waitActive();
        
        // Compute and send invalidations, waiting until they're fully processed.
        cluster.getNameNode().getNamesystem().getBlockManager()
          .computeInvalidateWork(2);
        cluster.triggerHeartbeats();
        HATestUtil.waitForDNDeletions(cluster);
        cluster.triggerDeletionReports();

        waitForNumTotalBlocks(cluster, numFiles);
        // Make sure we can still read the blocks.
        for (Path path : testPaths) {
          String ret = DFSTestUtil.readFile(cluster.getFileSystem(), path);
          assertEquals("old gs data\n" + "new gs data\n", ret);
        }
      } finally {
        IOUtils.cleanupWithLogger(LOG, streams.toArray(new Closeable[0]));
      }
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testRWRShouldNotAddedOnDNRestart() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
        "false");
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build()) {
      Path path = new Path("/testRBW");
      FSDataOutputStream out = cluster.getFileSystem().create(path, (short) 2);
      out.writeBytes("old gs data\n");
      out.hflush();
      // stop one datanode
      DataNodeProperties dnProp = cluster.stopDataNode(0);
      String dnAddress = dnProp.getDatanode().getXferAddress().toString();
      if (dnAddress.startsWith("/")) {
        dnAddress = dnAddress.substring(1);
      }
      //Write some more data after DN stopped.
      out.writeBytes("old gs data\n");
      out.hflush();
      cluster.restartDataNode(dnProp, true);
      // wait till the block report comes
      Thread.sleep(3000);
      // check the block locations, this should not contain restarted datanode
      BlockLocation[] locations = cluster.getFileSystem()
          .getFileBlockLocations(path, 0, Long.MAX_VALUE);
      String[] names = locations[0].getNames();
      for (String node : names) {
        if (node.equals(dnAddress)) {
          fail("Old GS DN should not be present in latest block locations.");
        }
      }
      out.close();
    }
  }

  private void waitForNumTotalBlocks(final MiniDFSCluster cluster,
      final int numTotalBlocks) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          cluster.triggerBlockReports();

          // Wait total blocks
          if (cluster.getNamesystem().getBlocksTotal() == numTotalBlocks) {
            return true;
          }
        } catch (Exception ignored) {
          // Ignore the exception
        }

        return false;
      }
    }, 1000, 60000);
  }
}
