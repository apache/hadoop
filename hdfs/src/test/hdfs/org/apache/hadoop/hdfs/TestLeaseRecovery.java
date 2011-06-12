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
package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;

public class TestLeaseRecovery extends junit.framework.TestCase {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short)3;
  private static final long LEASE_PERIOD = 300L;

  static void checkMetaInfo(Block b, DataNode dn
      ) throws IOException {
    TestInterDatanodeProtocol.checkMetaInfo(b, dn);
  }
  
  static int min(Integer... x) {
    int m = x[0];
    for(int i = 1; i < x.length; i++) {
      if (x[i] < m) {
        m = x[i];
      }
    }
    return m;
  }

  void waitLeaseRecovery(MiniDFSCluster cluster) {
    cluster.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
    // wait for the lease to expire
    try {
      Thread.sleep(2 * 3000);  // 2 heartbeat intervals
    } catch (InterruptedException e) {
    }
  }

  /**
   * The following test first creates a file with a few blocks.
   * It randomly truncates the replica of the last block stored in each datanode.
   * Finally, it triggers block synchronization to synchronize all stored block.
   */
  public void testBlockSynchronization() throws Exception {
    final int ORG_FILE_SIZE = 3000; 
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 5, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, ORG_FILE_SIZE, REPLICATION_NUM, 0L);
      assertTrue(dfs.dfs.exists(filestr));
      DFSTestUtil.waitReplication(dfs, filepath, REPLICATION_NUM);

      //get block info for the last block
      LocatedBlock locatedblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.getNamenode(), filestr);
      DatanodeInfo[] datanodeinfos = locatedblock.getLocations();
      assertEquals(REPLICATION_NUM, datanodeinfos.length);

      //connect to data nodes
      DataNode[] datanodes = new DataNode[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        datanodes[i] = cluster.getDataNode(datanodeinfos[i].getIpcPort());
        assertTrue(datanodes[i] != null);
      }
      
      //verify Block Info
      Block lastblock = locatedblock.getBlock();
      DataNode.LOG.info("newblocks=" + lastblock);
      for(int i = 0; i < REPLICATION_NUM; i++) {
        checkMetaInfo(lastblock, datanodes[i]);
      }


      DataNode.LOG.info("dfs.dfs.clientName=" + dfs.dfs.clientName);
      cluster.getNameNode().append(filestr, dfs.dfs.clientName);

      // expire lease to trigger block recovery.
      waitLeaseRecovery(cluster);

      Block[] updatedmetainfo = new Block[REPLICATION_NUM];
      long oldSize = lastblock.getNumBytes();
      lastblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.getNamenode(), filestr).getBlock();
      long currentGS = lastblock.getGenerationStamp();
      for(int i = 0; i < REPLICATION_NUM; i++) {
        updatedmetainfo[i] =
          datanodes[i].data.getStoredBlock(lastblock.getBlockId());
        assertEquals(lastblock.getBlockId(), updatedmetainfo[i].getBlockId());
        assertEquals(oldSize, updatedmetainfo[i].getNumBytes());
        assertEquals(currentGS, updatedmetainfo[i].getGenerationStamp());
      }

      // verify that lease recovery does not occur when namenode is in safemode
      System.out.println("Testing that lease recovery cannot happen during safemode.");
      filestr = "/foo.safemode";
      filepath = new Path(filestr);
      dfs.create(filepath, (short)1);
      cluster.getNameNode().setSafeMode(FSConstants.SafeModeAction.SAFEMODE_ENTER);
      assertTrue(dfs.dfs.exists(filestr));
      DFSTestUtil.waitReplication(dfs, filepath, (short)1);
      waitLeaseRecovery(cluster);
      // verify that we still cannot recover the lease
      LeaseManager lm = cluster.getNamesystem().leaseManager;
      assertTrue("Found " + lm.countLease() + " lease, expected 1", lm.countLease() == 1);
      cluster.getNameNode().setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
