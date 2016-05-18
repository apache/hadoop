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

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.junit.Test;

public class TestOverReplicatedBlocks {
  /** Test processOverReplicatedBlock can handle corrupt replicas fine.
   * It make sure that it won't treat corrupt replicas as valid ones 
   * thus prevents NN deleting valid replicas but keeping
   * corrupt ones.
   */
  @Test
  public void testProcesOverReplicateBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000L);
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
        Integer.toString(2));
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      final Path fileName = new Path("/foo1");
      DFSTestUtil.createFile(fs, fileName, 2, (short)3, 0L);
      DFSTestUtil.waitReplication(fs, fileName, (short)3);
      
      // corrupt the block on datanode 0
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);
      cluster.corruptReplica(0, block);
      DataNodeProperties dnProps = cluster.stopDataNode(0);
      // remove block scanner log to trigger block scanning
      File scanCursor = new File(new File(MiniDFSCluster.getFinalizedDir(
          cluster.getInstanceStorageDir(0, 0),
          cluster.getNamesystem().getBlockPoolId()).getParent()).getParent(),
          "scanner.cursor");
      //wait for one minute for deletion to succeed;
      for(int i = 0; !scanCursor.delete(); i++) {
        assertTrue("Could not delete " + scanCursor.getAbsolutePath() +
            " in one minute", i < 60);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
      }
      
      // restart the datanode so the corrupt replica will be detected
      cluster.restartDataNode(dnProps);
      DFSTestUtil.waitReplication(fs, fileName, (short)2);
      
      String blockPoolId = cluster.getNamesystem().getBlockPoolId();
      final DatanodeID corruptDataNode = 
        InternalDataNodeTestUtils.getDNRegistrationForBP(
            cluster.getDataNodes().get(2), blockPoolId);
         
      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();
      final HeartbeatManager hm = bm.getDatanodeManager().getHeartbeatManager();
      try {
        namesystem.writeLock();
        synchronized(hm) {
          // set live datanode's remaining space to be 0 
          // so they will be chosen to be deleted when over-replication occurs
          String corruptMachineName = corruptDataNode.getXferAddr();
          for (DatanodeDescriptor datanode : hm.getDatanodes()) {
            if (!corruptMachineName.equals(datanode.getXferAddr())) {
              datanode.getStorageInfos()[0].setUtilizationForTesting(100L, 100L, 0, 100L);
              datanode.updateHeartbeat(
                  BlockManagerTestUtil.getStorageReportsForDatanode(datanode),
                  0L, 0L, 0, 0, null);
            }
          }

          // decrease the replication factor to 1; 
          NameNodeAdapter.setReplication(namesystem, fileName.toString(), (short)1);

          // corrupt one won't be chosen to be excess one
          // without 4910 the number of live replicas would be 0: block gets lost
          assertEquals(1, bm.countNodes(
              bm.getStoredBlock(block.getLocalBlock())).liveReplicas());
        }
      } finally {
        namesystem.writeUnlock();
      }
      
    } finally {
      cluster.shutdown();
    }
  }

  static final long SMALL_BLOCK_SIZE =
    DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
  static final long SMALL_FILE_LENGTH = SMALL_BLOCK_SIZE * 4;

  /**
   * The test verifies that replica for deletion is chosen on a node,
   * with the oldest heartbeat, when this heartbeat is larger than the
   * tolerable heartbeat interval.
   * It creates a file with several blocks and replication 4.
   * The last DN is configured to send heartbeats rarely.
   * 
   * Test waits until the tolerable heartbeat interval expires, and reduces
   * replication of the file. All replica deletions should be scheduled for the
   * last node. No replicas will actually be deleted, since last DN doesn't
   * send heartbeats. 
   */
  @Test
  public void testChooseReplicaToDelete() throws Exception {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, SMALL_BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      fs = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();

      conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 300);
      cluster.startDataNodes(conf, 1, true, null, null, null);
      DataNode lastDN = cluster.getDataNodes().get(3);
      DatanodeRegistration dnReg = InternalDataNodeTestUtils.
          getDNRegistrationForBP(lastDN, namesystem.getBlockPoolId());
      String lastDNid = dnReg.getDatanodeUuid();

      final Path fileName = new Path("/foo2");
      DFSTestUtil.createFile(fs, fileName, SMALL_FILE_LENGTH, (short)4, 0L);
      DFSTestUtil.waitReplication(fs, fileName, (short)4);

      // Wait for tolerable number of heartbeats plus one
      DatanodeDescriptor nodeInfo = null;
      long lastHeartbeat = 0;
      long waitTime = DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT * 1000 *
        (DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT + 1);
      do {
        nodeInfo = bm.getDatanodeManager().getDatanode(dnReg);
        lastHeartbeat = nodeInfo.getLastUpdateMonotonic();
      } while (monotonicNow() - lastHeartbeat < waitTime);
      fs.setReplication(fileName, (short)3);

      BlockLocation locs[] = fs.getFileBlockLocations(
          fs.getFileStatus(fileName), 0, Long.MAX_VALUE);

      // All replicas for deletion should be scheduled on lastDN.
      // And should not actually be deleted, because lastDN does not heartbeat.
      namesystem.readLock();
      final int dnBlocks = bm.getExcessSize4Testing(dnReg.getDatanodeUuid());
      assertEquals("Replicas on node " + lastDNid + " should have been deleted",
          SMALL_FILE_LENGTH / SMALL_BLOCK_SIZE, dnBlocks);
      namesystem.readUnlock();
      for(BlockLocation location : locs)
        assertEquals("Block should still have 4 replicas",
            4, location.getNames().length);
    } finally {
      if(fs != null) fs.close();
      if(cluster != null) cluster.shutdown();
    }
  }

  /**
   * Test over replicated block should get invalidated when decreasing the
   * replication for a partial block.
   */
  @Test
  public void testInvalidateOverReplicatedBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .build();
    try {
      final FSNamesystem namesystem = cluster.getNamesystem();
      final BlockManager bm = namesystem.getBlockManager();
      FileSystem fs = cluster.getFileSystem();
      Path p = new Path(MiniDFSCluster.getBaseDirectory(), "/foo1");
      FSDataOutputStream out = fs.create(p, (short) 2);
      out.writeBytes("HDFS-3119: " + p);
      out.hsync();
      fs.setReplication(p, (short) 1);
      out.close();
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, p);
      assertEquals("Expected only one live replica for the block", 1, bm
          .countNodes(bm.getStoredBlock(block.getLocalBlock())).liveReplicas());
    } finally {
      cluster.shutdown();
    }
  }
}

