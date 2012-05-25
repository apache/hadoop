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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import junit.framework.TestCase;

public class TestOverReplicatedBlocks extends TestCase {
  /** Test processOverReplicatedBlock can handle corrupt replicas fine.
   * It make sure that it won't treat corrupt replicas as valid ones 
   * thus prevents NN deleting valid replicas but keeping
   * corrupt ones.
   */
  public void testProcesOverReplicateBlock() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, Integer.toString(2));
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      final Path fileName = new Path("/foo1");
      DFSTestUtil.createFile(fs, fileName, 2, (short)3, 0L);
      DFSTestUtil.waitReplication(fs, fileName, (short)3);
      
      // corrupt the block on datanode 0
      Block block = DFSTestUtil.getFirstBlock(fs, fileName);
      assertTrue(cluster.corruptReplica(block.getBlockName(), 0));
      DataNodeProperties dnProps = cluster.stopDataNode(0);
      // remove block scanner log to trigger block scanning
      File scanLog = new File(System.getProperty("test.build.data"),
          "dfs/data/data1" + MiniDFSCluster.FINALIZED_DIR_NAME + 
          "dncp_block_verification.log.curr");
      //wait for one minute for deletion to succeed;
      for(int i=0; !scanLog.delete(); i++) {
        assertTrue("Could not delete log file in one minute", i < 60);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
      }
      
      // restart the datanode so the corrupt replica will be detected
      cluster.restartDataNode(dnProps);
      DFSTestUtil.waitReplication(fs, fileName, (short)2);
      
      final DatanodeID corruptDataNode = 
        cluster.getDataNodes().get(2).dnRegistration;
      final FSNamesystem namesystem = cluster.getNamesystem();
      synchronized (namesystem.heartbeats) {
        // set live datanode's remaining space to be 0 
        // so they will be chosen to be deleted when over-replication occurs
        for (DatanodeDescriptor datanode : namesystem.heartbeats) {
          if (!corruptDataNode.equals(datanode)) {
            datanode.updateHeartbeat(100L, 100L, 0L, 0, 0);
          }
        }
        
        // decrease the replication factor to 1; 
        namesystem.setReplication(fileName.toString(), (short)1);

        // corrupt one won't be chosen to be excess one
        // without 4910 the number of live replicas would be 0: block gets lost
        assertEquals(1, namesystem.blockManager.countNodes(block).liveReplicas());
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
  public void testChooseReplicaToDelete() throws IOException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, SMALL_BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      fs = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 300);
      cluster.startDataNodes(conf, 1, true, null, null, null);
      DataNode lastDN = cluster.getDataNodes().get(3);
      String lastDNid = lastDN.getDatanodeRegistration().getStorageID();

      final Path fileName = new Path("/foo2");
      DFSTestUtil.createFile(fs, fileName, SMALL_FILE_LENGTH, (short)4, 0L);
      DFSTestUtil.waitReplication(fs, fileName, (short)4);

      // Wait for tolerable number of heartbeats plus one
      DatanodeDescriptor nodeInfo = null;
      long lastHeartbeat = 0;
      long waitTime = DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT * 1000 *
        (DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT + 1);
      do {
        synchronized (namesystem.heartbeats) {
          synchronized (namesystem.datanodeMap) {
            nodeInfo = namesystem.getDatanode(lastDN.getDatanodeRegistration());
            lastHeartbeat = nodeInfo.getLastUpdate();
          }
        }
      } while(now() - lastHeartbeat < waitTime);
      fs.setReplication(fileName, (short)3);

      BlockLocation locs[] = fs.getFileBlockLocations(
          fs.getFileStatus(fileName), 0, Long.MAX_VALUE);

      // All replicas for deletion should be scheduled on lastDN.
      // And should not actually be deleted, because lastDN does not heartbeat.
      namesystem.readLock();
      Collection<Block> dnBlocks = 
        namesystem.blockManager.excessReplicateMap.get(lastDNid);
      assertEquals("Replicas on node " + lastDNid + " should have been deleted",
          SMALL_FILE_LENGTH / SMALL_BLOCK_SIZE, dnBlocks.size());
      namesystem.readUnlock();
      for(BlockLocation location : locs)
        assertEquals("Block should still have 4 replicas",
            4, location.getNames().length);
    } finally {
      if(fs != null) fs.close();
      if(cluster != null) cluster.shutdown();
    }
  }
}
